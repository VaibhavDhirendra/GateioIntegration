// std
#include <iostream>
#include <sched.h>
#include <cstring>
#include <map>
#include <thread>
#include <set>
#include <mutex>

// json
#include <nlohmann/json.hpp>

// interface
#include <interface/include/WSHandler.h>
#include <interface/include/ChannelDataHandler.h>
#include <interface/include/AlgorithmInterface.h>

// singular/network/
#include <singular/network/libhv/http_client.h>
#include <singular/network/include/WebSocketServer.h>
#include <singular/network/include/WebSocketChannel.h>
#include <singular/network/include/GlobalWebsocket.h>

// #include <singular/utility/include/LatencyMeasure.h>
#include <singular/utility/include/LatencyManager.h>

// singular/types
#include <singular/types/include/GatewayStatus.h>

//singular/db
#include <singular/db/include/DBManager.h>

// system
#include <system/include/Engine.h>
#include <system/include/OrderbookManagementSystem.h>

using namespace hv;

bool cpupin(int cpuid)
{
    cpu_set_t my_set;
    CPU_ZERO(&my_set);
    CPU_SET(cpuid, &my_set);
    if (sched_setaffinity(0, sizeof(cpu_set_t), &my_set))
    {
        // std::cerr << "sched_setaffinity error: " << strerror(errno) << std::endl;
        return false;
    }
    return true;
}

void syncWithNTP(std::chrono::seconds interval)
{
    while(true){
        int result = system("chronyd -q 'server pool.ntp.org iburst' > /dev/null 2>&1");
    std::this_thread::sleep_for(interval);
    }
}

// Global map to store session contexts
std::unordered_map<std::string, WebSocketChannelPtr> session_map;

// Synchronization objects
std::condition_variable cv;
std::mutex cv_m;
bool message_received = false;
std::mutex session_mutex;

// Map to hold handlers for different paths
std::map<std::string, std::shared_ptr<singular::network::ChannelDataHandler>> handlers;

std::string log_service_name = "OMS";


// Get Instruments code for exchanges

bool get_gateio_spot_instruments(nlohmann::json &result, hv::HttpClient &client){
    try
    {
        HttpRequest req;
        HttpResponse resp;

        singular::utility::log_event(log_service_name, 
            singular::utility::OEMSEvent::ALGOINT_DEBUG, 
            "Entered GATEIO Instruments");

        // 2. Setup HTTP client
        req.method = HTTP_GET;
         
        req.headers["Connection"] = "keep-alive";
        req.timeout = 10;
        req.url = "https://api.gateio.ws/api/v4/spot/currency_pairs";
        int ret = client.send(&req, &resp);
        if (ret != 0)
        {
            nlohmann::json log_data;
            log_data["instrument type"] = "usdt";
            log_data["error code"] = ret;
            singular::utility::log_event(log_service_name, 
                singular::utility::OEMSEvent::ALGOINT_ERROR, 
                "Request for instType failed", 
                log_data);
            return false;
        }

        // 4. Process successful response
        if (resp.status_code == 200)
        {
            nlohmann::json payload = nlohmann::json::parse(resp.body.c_str());

            // 5. Transform exchange data to required format
            for (auto &it : payload)
            {
                nlohmann::json instrument_config;
                
                // Required fields
                instrument_config["exchange"] = "GATEIO";
                instrument_config["symbol"] = static_cast<std::string>(it["id"]);

                // Set all required and optional fields
                instrument_config["instrument_type"] = "SPOT";
                instrument_config["min_price_precision"] = static_cast<double>(it["precision"]);
                instrument_config["min_quantity_precision"] = static_cast<double>(it["amount_precision"]);
                instrument_config["contract_multiplier"] = 0.0;
                instrument_config["base"] = it["base"];
                instrument_config["quote"] = it["quote"];

                result["instruments"].push_back(instrument_config);
            }
        }
        else
        {
            singular::utility::log_event(log_service_name, 
                singular::utility::OEMSEvent::ALGOINT_ERROR, 
                "Error getting instruments for gateio spot");
            return false;
        }
        return true;
    }
    catch (const std::exception &e)
    {
        singular::utility::log_event(log_service_name, 
            singular::utility::OEMSEvent::ALGOINT_ERROR, 
            std::string("Error getting Gateio instruments: ") + e.what());
        return false;
    }
}

bool get_gateio_futures_instruments(nlohmann::json &result, hv::HttpClient &client){
    try
    {
        singular::utility::log_event(log_service_name, 
            singular::utility::OEMSEvent::ALGOINT_DEBUG, 
            "Entered GATEIO Instruments");

        // 2. Setup HTTP client
        HttpRequest req;
        
        req.method = HTTP_GET;
         
        req.headers["Connection"] = "keep-alive";
        req.timeout = 10;

        std::string urls[2] = {
        "https://api.gateio.ws/api/v4/futures/usdt/contracts",
        "https://api.gateio.ws/api/v4/futures/btc/contracts"
        };

        std::string future_type[2] = {"usdt","btc"};

        int iter = 0;

        for(auto &url : urls){
            HttpResponse resp;
            req.url = urls[iter];
            int ret = client.send(&req, &resp);
            if (ret != 0)
            {
                nlohmann::json log_data;
                log_data["instrument type"] = future_type[iter];
                log_data["error code"] = ret;
                singular::utility::log_event(log_service_name, 
                    singular::utility::OEMSEvent::ALGOINT_ERROR, 
                    "Request for instType failed", 
                    log_data);
                return false;
            }

            // 4. Process successful response
            if (resp.status_code == 200)
            {
                nlohmann::json payload = nlohmann::json::parse(resp.body.c_str());

                // 5. Transform exchange data to required format
                for (auto &it : payload)
                {
                    nlohmann::json instrument_config;
                    
                    // Required fields
                    instrument_config["exchange"] = "GATEIO";
                    instrument_config["symbol"] = static_cast<std::string>(it["name"]);

                    std::string bq_name = static_cast<std::string>(it["name"]);

                    std::regex reg("_");
                    std::sregex_token_iterator it2(bq_name.begin(), bq_name.end(), reg, -1);
                    std::sregex_token_iterator end;

                    std::vector<std::string> bq(it2, end);

                    // Set all required and optional fields
                    if(it["type"] == "direct"){
                        instrument_config["instrument_type"] = "LINEAR_PERPETUAL";
                    }
                    else if(it["type"] == "inverse"){
                        instrument_config["instrument_type"] = "INVERSE_PERPETUAL";
                    }
                    else{
                        instrument_config["instrument_type"] = "UNKNOWN";
                    }
                    
                    instrument_config["min_price_precision"] = std::stod(static_cast<std::string>(it["order_price_round"]));
                    instrument_config["min_quantity_precision"] = static_cast<double>(it["order_size_min"]);
                    instrument_config["contract_multiplier"] = std::stod(static_cast<std::string>(it["quanto_multiplier"]));
                    instrument_config["base"] = bq[0];
                    instrument_config["quote"] = bq[1];

                    result["instruments"].push_back(instrument_config);
                }
            }
            else
            {
                singular::utility::log_event(log_service_name, 
                    singular::utility::OEMSEvent::ALGOINT_ERROR, 
                    "Error getting instruments for gateio perpetual futures");
                return false;
            }
            iter++;
        }
        return true;
    }
    catch (const std::exception &e)
    {
        singular::utility::log_event(log_service_name, 
            singular::utility::OEMSEvent::ALGOINT_ERROR, 
            std::string("Error getting Gateio instruments: ") + e.what());
        return false;
    }
}

bool get_gateio_delivery_instruments(nlohmann::json &result, hv::HttpClient &client){
    try
    {
        singular::utility::log_event(log_service_name, 
            singular::utility::OEMSEvent::ALGOINT_DEBUG, 
            "Entered GATEIO Instruments");

        // 2. Setup HTTP client
        HttpRequest req;
        
        req.method = HTTP_GET;
         
        req.headers["Connection"] = "keep-alive";
        req.timeout = 10;

        HttpResponse resp;
        req.url = "https://api.gateio.ws/api/v4/delivery/usdt/contracts";
        int ret = client.send(&req, &resp);
        if (ret != 0)
        {
            nlohmann::json log_data;
            log_data["instrument type"] = "usdt";
            log_data["error code"] = ret;
            singular::utility::log_event(log_service_name, 
                singular::utility::OEMSEvent::ALGOINT_ERROR, 
                "Request for instType failed", 
                log_data);
            return false;
        }

        // 4. Process successful response
        if (resp.status_code == 200)
        {
            nlohmann::json payload = nlohmann::json::parse(resp.body.c_str());

            // 5. Transform exchange data to required format
            for (auto &it : payload)
            {
                nlohmann::json instrument_config;
                
                // Required fields
                instrument_config["exchange"] = "GATEIO";
                instrument_config["symbol"] = static_cast<std::string>(it["name"]);

                std::string bq_name = static_cast<std::string>(it["underlying"]);

                std::regex reg("_");
                std::sregex_token_iterator it2(bq_name.begin(), bq_name.end(), reg, -1);
                std::sregex_token_iterator end;

                std::vector<std::string> bq(it2, end);

                // Set all required and optional fields
                if(it["type"] == "direct"){
                    instrument_config["instrument_type"] = "LINEAR_FUTURE";
                }
                else if(it["type"] == "inverse"){
                    instrument_config["instrument_type"] = "INVERSE_FUTURE";
                }
                else{
                    instrument_config["instrument_type"] = "UNKNOWN";
                }
                instrument_config["min_price_precision"] = std::stod(static_cast<std::string>(it["order_price_round"]));
                instrument_config["min_quantity_precision"] = static_cast<double>(it["order_size_min"]);
                instrument_config["contract_multiplier"] = std::stod(static_cast<std::string>(it["quanto_multiplier"]));
                instrument_config["base"] = bq[0];
                instrument_config["quote"] = bq[1];

                result["instruments"].push_back(instrument_config);
            }
        }
        else
        {
            singular::utility::log_event(log_service_name, 
                singular::utility::OEMSEvent::ALGOINT_ERROR, 
                "Error getting instruments for gateio delivery");
            return false;
        }
        return true;
    }
    catch (const std::exception &e)
    {
        singular::utility::log_event(log_service_name, 
            singular::utility::OEMSEvent::ALGOINT_ERROR, 
            std::string("Error getting Gateio delivery instruments: ") + e.what());
        return false;
    }
}

std::vector<std::string> get_gateio_options_underlying(hv::HttpClient &client){
    std::vector<std::string> underlyings; 
    try
    { 
        HttpRequest req;
        req.method = HTTP_GET;
        req.headers["Connection"] = "keep-alive";
        req.timeout = 10;

        HttpResponse resp;
        req.url = "https://api.gateio.ws/api/v4/options/underlyings";
        int ret = client.send(&req, &resp);
        if (ret != 0)
        {
            nlohmann::json log_data;
            log_data["instrument type"] = "usdt";
            log_data["error code"] = ret;
            singular::utility::log_event(log_service_name, 
                singular::utility::OEMSEvent::ALGOINT_ERROR, 
                "Request for instType failed", 
                log_data);
            //return false;
            return underlyings;
        }

        // 4. Process successful response
        if (resp.status_code == 200)
        {
            nlohmann::json payload = nlohmann::json::parse(resp.body.c_str());

            // 5. Transform exchange data to required format
            for (auto &it : payload)
            {
                underlyings.push_back(static_cast<std::string>(it["name"]));
            }
        }
        else
        {
            singular::utility::log_event(log_service_name, 
                singular::utility::OEMSEvent::ALGOINT_ERROR, 
                "Error getting underlyings for gateio options");
            return underlyings;
        }

        return underlyings;
    }
    catch (const std::exception &e)
    {
        singular::utility::log_event(log_service_name, 
            singular::utility::OEMSEvent::ALGOINT_ERROR, 
            std::string("Error getting Gateio options underlying: ") + e.what());
        return underlyings;
    }
}

bool get_gateio_options_instruments(nlohmann::json &result, hv::HttpClient &client){
    try
    {
        std::vector<std::string> underlyings = get_gateio_options_underlying(client);
        
        singular::utility::log_event(log_service_name, 
            singular::utility::OEMSEvent::ALGOINT_DEBUG, 
            "Entered GATEIO Instruments");

        // 2. Setup HTTP client
        HttpRequest req;
        
        req.method = HTTP_GET;
         
        req.headers["Connection"] = "keep-alive";
        req.timeout = 10;
        HttpResponse resp;
        for(auto &underlying : underlyings){
            std::string url = "https://api.gateio.ws/api/v4/options/contracts?underlying=" + underlying;

            req.url = url;
            int ret = client.send(&req, &resp);
            if (ret != 0)
            {
                nlohmann::json log_data;
                log_data["instrument type"] = "usdt";
                log_data["error code"] = ret;
                singular::utility::log_event(log_service_name, 
                    singular::utility::OEMSEvent::ALGOINT_ERROR, 
                    "Request for instType failed", 
                    log_data);
                return false;
            }

            // 4. Process successful response
            if (resp.status_code == 200)
            {
                nlohmann::json payload = nlohmann::json::parse(resp.body.c_str());

                // 5. Transform exchange data to required format
                for (auto &it : payload)
                {
                    nlohmann::json instrument_config;
                    
                    // Required fields
                    instrument_config["exchange"] = "GATEIO";
                    instrument_config["symbol"] = static_cast<std::string>(it["name"]);

                    std::string bq_name = static_cast<std::string>(it["underlying"]);

                    std::regex reg("_");
                    std::sregex_token_iterator it2(bq_name.begin(), bq_name.end(), reg, -1);
                    std::sregex_token_iterator end;

                    std::vector<std::string> bq(it2, end);

                    // Set all required and optional fields
                    instrument_config["instrument_type"] = "OPTION";
                    instrument_config["min_price_precision"] = std::stod(static_cast<std::string>(it["order_price_round"]));
                    instrument_config["min_quantity_precision"] = static_cast<double>(it["order_size_min"]);
                    instrument_config["contract_multiplier"] = std::stod(static_cast<std::string>(it["multiplier"]));
                    instrument_config["base"] = bq[0];
                    instrument_config["quote"] = bq[1];

                    result["instruments"].push_back(instrument_config);
                }
            }
            else
            {
                singular::utility::log_event(log_service_name, 
                    singular::utility::OEMSEvent::ALGOINT_ERROR, 
                    "Error getting instruments for gateio options.");
                return false;
            }
        }
        return true;
    }
    catch (const std::exception &e)
    {
        singular::utility::log_event(log_service_name, 
            singular::utility::OEMSEvent::ALGOINT_ERROR, 
            std::string("Error getting Gateio options instruments: ") + e.what());
        return false;
    }
}

bool get_gateio_instruments()
{
    hv::HttpClient client;

    nlohmann::json result;
    //Call different types of instruments from various api

    bool spot = get_gateio_spot_instruments(result, client);
    bool futures = get_gateio_futures_instruments(result, client);
    bool delivery = get_gateio_delivery_instruments(result, client);
    bool options = get_gateio_options_instruments(result, client);

    if(result.contains("instruments")){
        //(*config_map)["instruments"]["GATEIOUSDT"] = result["instruments"];
        //applyConfigFromMap(result);
    }

    if(spot && futures && delivery && options){
        return true;
    }
    return false;
    
}

int main(int argc, char **argv)
{   
    bool inst = get_gateio_instruments();

    std::cout << "Starting GoTrade C++..." << std::endl;
    handlers["/ws"] = std::make_shared<singular::network::WSHandler>();

    // singular::utility::LatencyMeasure::getInstance().setCPUFrequency(2.20);

    singular::utility::LatencyManager::initialize(3.50);
    // test_db_connection();
    // Create a new thread that runs the syncWithNTP function at fixed intervals
    auto ntp_thread = std::thread(syncWithNTP, std::chrono::seconds(60));

    // Detach the NTP synchronization thread
    ntp_thread.detach();

    // Prevent main from exiting immediately
    std::this_thread::sleep_for(std::chrono::seconds(1));

    hv::EventLoopPtr executor(new hv::EventLoop);

    auto executor_thread = std::thread([&executor]()
                                       {
        cpupin(1);
        executor->run(); });

    executor_thread.detach();
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // Initialize the Engine without the static configuration file
    goquant::system::Engine engine(executor);

    loadEnvFile(".env");

    const char *oems_algo_db = std::getenv("POSTGRES_DB_OEMS");
    const char *algo_table_user = std::getenv("POSTGRES_USER_OEMS");
    const char *algo_table_pass = std::getenv("POSTGRES_PASSWORD_OEMS");
    const char *algo_table_host = std::getenv("POSTGRES_HOST_OEMS");

    const char *oems_algo_table = std::getenv("POSTGRES_ALGO_TABLE_NAME");
    const char *oems_internal_order_id_table = std::getenv("POSTGRES_INTERNAL_ORDER_ID_TABLE_NAME");
    const char *credential = std::getenv("POSTGRES_CREDENTIAL_TABLE_NAME");
    const char* db_port_value = std::getenv("DB_PORT");

    // Initialize Database
    singular::db::DBManager &db = singular::db::DBManager::getInstance(oems_algo_db, algo_table_user, algo_table_pass, algo_table_host, db_port_value);

    if (!db.check_table_exists(oems_algo_table))
    {
        // Define columns for the new table
        std::vector<std::pair<std::string, std::string>> columns = {
            {"algorithm_id", "INT PRIMARY KEY"}, // Algorithm ID - INT
        };

        db.create_table(oems_algo_table, columns);
    }
    if (!db.check_table_exists(oems_internal_order_id_table))
    {
        // Define columns for the new table
        std::vector<std::pair<std::string, std::string>> columns = {
            {"internal_order_id", "INT PRIMARY KEY"}, // Algorithm ID - INT
        };

        db.create_table(oems_internal_order_id_table, columns);
    }
    // if (!db.check_table_exists(credential))
    // {
    //     // Define columns for the new table
    //     std::vector<std::pair<std::string, std::string>> columns = {
    //         {"id", "VARCHAR PRIMARY KEY"},
    //         {"exchange", "VARCHAR"},
    //         {"api_key", "VARCHAR"},
    //         {"secret", "VARCHAR"},
    //         {"passphrase", "VARCHAR"},
    //         {"name", "VARCHAR"},
    //         {"mode", "VARCHAR"},
    //         {"created_at", "TIMESTAMP"},
    //         {"updated_at", "TIMESTAMP"}};

    //     db.create_table(credential, columns);
    // }

    goquant::algorithm::AlgorithmInterface algorithm_interface(engine);

    if (!engine.config_map)
    {
        engine.config_map = new std::map<std::string, nlohmann::json>();
    }

    // load the instruments on startup
    engine.initialize_instrument_map();

    //added code for initialise CPU freq (HARD CODED)
     singular::utility::RTSCTimer::getInstance().setCPUFrequency(3.1) ;

    // WebSocket Server to handle incoming requests
    singular::network::WebSocketService ws;
    ws.onopen = [&engine, &algorithm_interface](const WebSocketChannelPtr &channel, const HttpRequestPtr &req)
    {
        auto path = singular::utility::getPathWithoutQuery(req->path);
        auto query_string = singular::utility::getQueryString(req->path);
        auto query_params = singular::utility::parseQueryParams(query_string);

        if (handlers.find(path) != handlers.end())
        {
            auto ctx = channel->newContextPtr<singular::network::MyContext>();

            std::string session_id = std::to_string(channel->id());
            ctx->session_id = session_id;
            ctx->query_string = query_string;  // Set the query string in context
            session_map[session_id] = channel; // Add to the session map

            std::string user_name = "test3";
            if (query_params.find("name") != query_params.end())
            {
                user_name = query_params["name"];
            }

            handlers[path]->handleOpen(channel, ctx.get(), engine, algorithm_interface, user_name, session_map);
            ctx->path = path;
        }
        else
        {
            // std::cout << "No handler for path: " << path << std::endl;
            singular::utility::log_event(log_service_name, singular::utility::OEMSEvent::OMS_DEBUG, "No handler for path: " + path);
        }
    };

    ws.onmessage = [&engine, &algorithm_interface](const WebSocketChannelPtr &channel, const std::string &msg)
    {
        auto ctx = channel->getContextPtr<singular::network::MyContext>(); // Retrieve the context
        auto path = ctx->path;                                             // Get the stored path from the context
        if (handlers.find(path) != handlers.end())
        {
            handlers[path]->handleMessage(channel, msg, ctx.get(), engine, algorithm_interface, session_mutex, session_map);
        }
        else
        {
            // std::cout << "No handler for path: " << path << std::endl;
            singular::utility::log_event(log_service_name, singular::utility::OEMSEvent::OMS_DEBUG, "No handler for path: " + path);
        }
    };

    ws.onclose = [&engine](const WebSocketChannelPtr &channel)
    {
        // std::cout << "WebSocket Client Disconnected: " << channel->id() << std::endl;
        singular::utility::log_event(log_service_name, singular::utility::OEMSEvent::OMS_DEBUG, "WebSocket Client Disconnected: " + std::to_string(channel->id()));
        auto ctx = channel->getContextPtr<singular::network::MyContext>();
        if (ctx)
        {
            engine.handle_session_close(ctx->session_id);
            // engine.remove_user_session(ctx->session_id);
            session_map.erase(ctx->session_id);
        }
        if (ctx->timerID != INVALID_TIMER_ID)
        {
            hv::killTimer(ctx->timerID);
        }
        for (auto it = singular::network::globalWebSocketChannels.begin(); it != singular::network::globalWebSocketChannels.end(); ++it)
        {
            if (it->second == channel)
            {
                singular::network::globalWebSocketChannels.erase(it);
                // std::cout << "WebSocket Client Disconnected: " << channel->id() << std::endl;
                singular::utility::log_event(log_service_name, singular::utility::OEMSEvent::OMS_DEBUG, "WebSocket Client Disconnected: " + std::to_string(channel->id()));
                break;
            }
        }
    };

    websocket_server_t server;
    const char* ws_server_port_value = std::getenv("WS_SERVER_PORT");
    server.port = std::stoi(ws_server_port_value);
    server.ws = &ws;
    websocket_server_run(&server, 0);

    auto market_data_thread = std::thread([&engine]()
    {
        while (true)
        {
            // cpupin(4);
            engine.consume_market_data();
            std::this_thread::sleep_for(std::chrono::nanoseconds(10));
        }
    });
    market_data_thread.detach();
    // Engine thread
    auto engine_thread = std::thread([&engine]()
    {
        while (true)
        {
            // cpupin(3);
            engine.update();
            std::this_thread::sleep_for(std::chrono::nanoseconds(10));

        }
    });
    engine_thread.detach();

    auto redis_order_last_min_data_thread = std::thread([&engine]()
    {
        while (true)
        {
            // cpupin(4);
            engine.redis_last_min_order_update();
            std::this_thread::sleep_for(std::chrono::seconds(5));
        }
    });
    redis_order_last_min_data_thread.detach();

    while (true)
    {
        cpupin(2);
        algorithm_interface.update();
        std::this_thread::sleep_for(std::chrono::nanoseconds(10));
    }

    // Ensure the main thread waits or continues as needed
    std::this_thread::sleep_for(std::chrono::seconds(1));

    market_data_thread.join();
    engine_thread.join();
    redis_order_last_min_data_thread.join();
    singular::utility::LatencyManager::destroy();

    return 0;
}

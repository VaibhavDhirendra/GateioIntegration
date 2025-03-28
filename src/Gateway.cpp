#include <chrono>
#include <iomanip>
#include <sstream>
#include <cstdio>

#include "gateio/include/Gateway.h"
#include <gateway/include/GatewayFactoryManager.h>

namespace singular
{
  namespace gateway
  {
    namespace gateio
    {

      std::unique_ptr<singular::gateway::AbstractGateway_V2> create_gateio_gateway(
          hv::EventLoopPtr &executor, bool authenticate,
          const std::string &account_name, const std::string &key,
          const std::string &secret, const std::string &passphrase,
          const std::string &mode)
      {
        return std::make_unique<Gateway>(
            executor, authenticate, account_name, key, secret, passphrase, mode);
      }

      namespace
      {
        struct Registrar
        {
          Registrar()
          {
            singular::gateway::GatewayFactoryManager::register_factory(
                singular::types::Exchange::GATEIO, create_gateio_gateway);
            std::cout << "Registered Gateio Gateway factory." << std::endl;
          }
        };
        Registrar registrar;
      }

            Gateway::Gateway(hv::EventLoopPtr &executor,
                       bool authenticate,
                       const std::string &name,
                       const std::string &key,
                       const std::string &secret,
                       const std::string &passphrase,
                       const std::string &mode)
          : AbstractGateway_V2(executor, 120, 20),
            authenticate_(authenticate),
            name_(name),
            key_(key),
            secret_(secret),
            passphrase_(passphrase),
            mode_(mode)
      {
        loadEnvFile(".env");
        private_spot_url=getExchangeUrl("GATEIO_ENV_MODE", "DEV_GATEIO_PRIVATE_SPOT_URL", "PROD_GATEIO_PRIVATE_SPOT_URL");
        public_spot_url=getExchangeUrl("GATEIO_ENV_MODE", "DEV_GATEIO_PUBLIC_SPOT_URL", "PROD_GATEIO_PUBLIC_SPOT_URL");
        private_futures_url=getExchangeUrl("GATEIO_ENV_MODE", "DEV_GATEIO_PRIVATE_FUTURES_USDT_URL", "PROD_GATEIO_PRIVATE_FUTURES_USDT_URL");
        public_futures_url=getExchangeUrl("GATEIO_ENV_MODE", "DEV_GATEIO_PUBLIC_FUTURES_USDT_URL", "PROD_GATEIO_PUBLIC_FUTURES_USDT_URL");
        public_futures_usdt_url=getExchangeUrl("GATEIO_ENV_MODE", "DEV_GATEIO_PUBLIC_FUTURES_USDT_URL", "PROD_GATEIO_PUBLIC_FUTURES_USDT_URL");
        public_futures_btc_url=getExchangeUrl("GATEIO_ENV_MODE", "DEV_GATEIO_PUBLIC_FUTURES_BTC_URL", "PROD_GATEIO_PUBLIC_FUTURES_BTC_URL");
        env_mode = std::getenv("GATEIO_ENV_MODE");
        //Here spot PROD and DEV are same url that is PROD url, as we dont have DEV for spot
        
        if (strcmp(env_mode, "DEV") == 0)
        {
          sim_trading = true;
        }
        else
        {
            sim_trading = false;
        }
          
        if (!public_futures_url || !public_spot_url)
        {
            std::cerr << "Public URL not set. Please check your .env file." << std::endl;
        }

        if ((!private_futures_url || !public_spot_url) && authenticate)
        {
          std::cerr << "Private URL not set. Please check your .env file." << std::endl;
        }
        

        public_client_spot = std::make_unique<singular::network::WebsocketClient>(
            executor, public_spot_url);

        public_client_futures_usdt = std::make_unique<singular::network::WebsocketClient>(
            executor, public_futures_usdt_url);

        public_client_futures_btc = std::make_unique<singular::network::WebsocketClient>(
            executor, public_futures_btc_url);

        if (authenticate)
        {
          private_spot_client_ = std::make_unique<singular::network::WebsocketClient>(
              executor, private_spot_url);
          private_futures_client_ = std::make_unique<singular::network::WebsocketClient>(
              executor, private_futures_url);
        }
        // Function to initialize maps with LOAD FACTOR and INITIALIZE MAP SIZE
        initializeMaps();

        spot_login_status = true;
        futures_login_status = true;
        latency_measure = singular::utility::LatencyManager::get();
      }
      void Gateway::close_private_socket()
      {
        private_spot_client_->close();
        private_spot_status_ = singular::types::GatewayStatus::OFFLINE;
        private_futures_client_->close();
        private_futures_status_ = singular::types::GatewayStatus::OFFLINE;
      }

      void Gateway::close_public_socket()
      {
        public_client_spot->close();
        public_client_futures_btc->close();
        public_client_futures_usdt->close();
        public_status_ = singular::types::GatewayStatus::OFFLINE;
      }

      void Gateway::purge()
      {
        close_public_socket();
        close_private_socket();
        authenticated_ = false;
        spot_login_status = false;
        futures_login_status = false;
        is_purged_ = true;
      }

      void Gateway::initializeMaps()
      {
        internal_to_client_id_map_.max_load_factor(LOAD_FACTOR);
        internal_id_symbol_map_.max_load_factor(LOAD_FACTOR);
        internal_to_credential_id_map_.max_load_factor(LOAD_FACTOR);
        symbol_volume_map_.max_load_factor(LOAD_FACTOR);

        internal_to_client_id_map_.reserve(INITIAL_MAP_SIZE);
        internal_id_symbol_map_.reserve(INITIAL_MAP_SIZE);
        internal_to_credential_id_map_.reserve(INITIAL_MAP_SIZE);
        symbol_volume_map_.reserve(INITIAL_MAP_SIZE);
      }

      void Gateway::initialize_callback_funcs()
      {
        try
        {
            //add_callback(std::bind(&Gateway::run_private_spot_ws,this));//commented as Testnet is not available for spot.
            add_callback(std::bind(&Gateway::run_private_futures_ws,this));
        }
        catch(std::exception &e)
        {
          singular::utility::log_event(log_service_name,singular::utility::OEMSEvent::LOGIN_EXCHANGE_ERROR,"Failed to intialize clients:"+std::string(e.what()));
        }
      }

      void Gateway::do_place(singular::types::Symbol symbol, singular::types::InstrumentType type,
                             singular::types::OrderId order_id, singular::types::OrderType order_type,
                             singular::types::Side side, double price, double quantity, singular::types::RequestSource source,
                             std::string credential_id, std::string td_mode)
      {
        std::string channel=type==singular::types::InstrumentType::SPOT?"spot.order_place":"futures.order_place";
        auto client_id = get_client_id(order_id);
        std::string type_string = singular::types::get_order_type_string[order_type];
        std::string contract=symbol;
        std::string contract_symbol;
        std::string tif;//added if needed for future
        std::string text="t-Z-"+std::to_string(client_id);//custom string(to match gateio set rules)
        const std::string side_string = side == singular::types::Side::BUY ? "buy" : "sell";
        std::size_t pos = contract.find('@');//Symbol is BTC-USD@FUTURE
          contract_symbol = contract.substr(0, pos);//Contract symbol=BTC-USD
        std::string orderid_string=std::to_string(order_id);
        std::string price_string=std::to_string(price);

        nlohmann::json message;
      
        message["channel"] = channel;
        message["event"] = "api";
        message["payload"]["req_id"] = orderid_string;
        message["payload"]["req_param"]["price"] = price_string;
        message["payload"]["req_param"]["text"] = text;  

        if(type==singular::types::InstrumentType::LINEAR_PERPETUAL||type==singular::types::InstrumentType::INVERSE_PERPETUAL)      
        {
          message["payload"]["req_param"]["size"] = quantity;
          message["payload"]["req_param"]["contract"] = contract_symbol;
        }
        
        // if(tif.size()>0)//commented for now as tif is not used 
        // {
        //    message["payload"]["req_param"]["tif"] = tif;
        // }

        if(type==singular::types::InstrumentType::SPOT)      
        {
          message["payload"]["req_param"]["currency_pair"] = contract_symbol;
          message["payload"]["req_param"]["type"] = type_string;
          message["payload"]["req_param"]["account"] = "spot";
          message["payload"]["req_param"]["side"] = side_string;
          message["payload"]["req_param"]["amount"] = quantity;
        }
        auto timestamp = std::chrono::duration_cast<std::chrono::seconds>(
                             std::chrono::system_clock::now().time_since_epoch())
                             .count();

        message["time"] = timestamp;
        // Internal Latency Measurement end
        // timer.stopMeasurement("OKX do_place");

        // singular::utility::RTSCTimer::getInstance().stopMeasurement(order_id, "InternalLatency");
        // auto end_internal_time = std::chrono::high_resolution_clock::now();
        // singular::utility::LatencyStorage::setCredentialId(order_id, credential_id);
        // singular::utility::LatencyStorage::setEndInternalTime(order_id, std::chrono::duration_cast<std::chrono::nanoseconds>(end_internal_time.time_since_epoch()));
        auto end_time_rtsc = singular::utility::LatencyMeasure::captureTimestamp();
        latency_measure->stopMeasurement(order_id, end_time_rtsc);
        
        if(type!=singular::types::InstrumentType::SPOT)
        {
          private_futures_client_->send(message.dump());
        }
        else
        {
          private_spot_client_->send(message.dump());
        }

        client_id_to_side_map_[client_id] = side;
        client_id_to_price_map_[client_id] = price;
        client_id_to_qty_map_[client_id] = quantity;
        client_id_to_symbol_map_[client_id] = symbol;
        client_to_internal_id_map_[client_id] = order_id;
        internal_to_client_id_map_[order_id] = client_id;
        client_id_to_source_map_[client_id] = source;

        if (credential_id != "")
        {
          internal_to_credential_id_map_[order_id] = credential_id;
        }

        std::string source_string = singular::types::get_request_source_string[source];
        if ((source_string == "market") || (source_string == "limit"))
        {
          stream_order_data({{"id", std::to_string(client_id)}}, "received");
        }

        char log_message[100];
        int chars_count = std::sprintf(log_message, "Sent a place %s order for %s@%s at %f", side_string.c_str(), symbol.c_str(), type_string.c_str(), price);
        log_message[chars_count] = '\0';
        singular::utility::log_event(log_service_name, singular::utility::OEMSEvent::PLACE_ORDER_DEBUG, log_message);
      }

      void Gateway::do_send_native_order_latency(long long internal_order_id)
      {
        auto latency_info = latency_measure->get_latency(internal_order_id);

        if (!latency_info)
        {
          singular::utility::log_event(
              log_service_name,
              singular::utility::OEMSEvent::OMS_ERROR,
              "Latency information is missing for internal_order_id: " + std::to_string(internal_order_id));
          return;
          }

        // Safely retrieve credential_id from the map
        auto it = internal_to_credential_id_map_.find(internal_order_id);
        if (it == internal_to_credential_id_map_.end())
        {
          singular::utility::log_event(
              log_service_name,
              singular::utility::OEMSEvent::OMS_ERROR,
              "Credential ID is missing for internal_order_id: " + std::to_string(internal_order_id));
          return; // Exit the function as credential_id is missing
        }

        auto credential_id = it->second;
        do_websocket_task_latency(*latency_info, internal_order_id, credential_id);
      }

      void Gateway::send_final_latency_info(singular::types::AlgorithmId algo_id, char *credential_id)
      {
       auto latency_info = latency_measure->get_avg_latency_by_algorithm(algo_id);
        if (latency_info && credential_id != nullptr && credential_id[0] != '\0')
        {
          // std::cout<<"Sending avg latency info"<<std::endl;
          do_websocket_task_latency(*latency_info, algo_id, credential_id);
        }
        else
        {
          singular::utility::log_event(
              log_service_name,
              singular::utility::OEMSEvent::OMS_ERROR,
              "Latency info or credential_id is missing.");
        }
      }

      void Gateway::do_websocket_task_latency(singular::utility::TimePoint latency_info, long long internal_order_id, std::string credential_id)
      {
           for (const auto &session_id_value : order_execution_quality_session_map)
        {
          auto it = singular::network::globalWebSocketChannels.find(session_id_value);
          nlohmann::json final_data = nlohmann::json::object();
          nlohmann::json subs_data = nlohmann::json::array();
          nlohmann::json latency_data = nlohmann::json::array();
          nlohmann::json slippage_value = (latency_info.slippage_percentage == std::numeric_limits<double>::max())
                                              ? nlohmann::json()
                                              : nlohmann::json(latency_info.slippage_percentage);
          latency_data.push_back({
              {"start_time", latency_info.start_time.count()},                 // Start time in nanoseconds
              {"end_time", latency_info.end_time.count()},                     // End time in nanoseconds
              {"internal_latency", latency_info.internal_latency.count()},     // Internal latency in microseconds
              {"exchange_latency", latency_info.exchange_latency.count()},     // Exchange latency in microseconds
              {"round_trip_latency", latency_info.round_trip_latency.count()}, // Round Trip Latency in microseconds
              {"algorithm_id", latency_info.algo_id},                          // Algorithm ID
              {"slippage_percentage", slippage_value}                          // Slippage
          });
          subs_data.push_back({{"channel", "order_execution_quality"}, {"data", latency_data}});
          final_data["exchange"] = "GATEIO";
          final_data["data"] = subs_data;
          final_data["name"] = name_;
          if (credential_id != "")
          {
            final_data["credential_id"] = credential_id;
          }
          if (it != singular::network::globalWebSocketChannels.end() && it->second)
          {
            singular::utility::log_event(log_service_name, singular::utility::OEMSEvent::WS_CONNECTION_DEBUG, "Sending to Global Websocket Server");
            it->second->send(final_data.dump());
          }
        }
      }

      void Gateway::do_cancel(singular::types::OrderId order_id, singular::types::RequestSource source)
      {
        auto it = internal_to_client_id_map_.find(order_id);
        if (it == internal_to_client_id_map_.end())
        {
          singular::utility::log_event(log_service_name, singular::utility::OEMSEvent::CANCEL_ORDER_ERROR, "Order ID not found in internal_to_client_id_map.");
          return;
        }
        auto client_id = it->second;

        auto symbol = internal_id_symbol_map_[order_id];
        std::string contract_symbol;
        std::size_t pos = symbol.find('@');//Symbol is BTC-USD@FUTURE
          contract_symbol = symbol.substr(0, pos);
        std::string new_client_id = source + "-" + std::to_string(client_id);
        const std::string instrument = client_id_to_symbol_map_[client_id];
        singular::types::InstrumentType type= internal_to_type_map_[order_id];
        std::string channel_cancel;
        if(type==singular::types::InstrumentType::SPOT)
        {
          channel_cancel="spot.order_cancel";        
        }
        else
        {
          channel_cancel="futures.order_cancel";    
        }
        nlohmann::json message;
        
        message["channel"] = channel_cancel;
        message["payload"]["req_id"] = std::to_string(order_id);
        message["payload"]["req_param"]["order_id"] =order_id;

         if(type==singular::types::InstrumentType::SPOT)      
        {
          message["payload"]["req_param"]["currency_pair"] =contract_symbol;  
        }
      
        auto timestamp = std::chrono::duration_cast<std::chrono::seconds>(
                             std::chrono::system_clock::now().time_since_epoch())
                             .count();
        message["time"] = timestamp;

        if(type!=singular::types::InstrumentType::SPOT)
        {
          private_futures_client_->send(message.dump());
        }
        else
        {
          private_spot_client_->send(message.dump());
        }

        // Enable Log in Debug mode
        std::string log_message = "Sent a cancel order request for symbol ";
        log_message += instrument + " with clOrdId: ";
        log_message += new_client_id;
        singular::utility::log_event(log_service_name, singular::utility::OEMSEvent::CANCEL_ORDER_DEBUG, log_message);
      }

      void Gateway::do_cancel(std::string exchange_order_id, singular::types::Instrument *instrument, singular::types::RequestSource source)
      {
        auto symbol = instrument->symbol_;
         std::string contract_symbol;
         std::size_t pos = symbol.find('@');//Symbol is BTC-USD@FUTURE
          contract_symbol = symbol.substr(0, pos);

        singular::types::InstrumentType type= instrument->type_;
        std::string channel_cancel;
        if(type==singular::types::InstrumentType::SPOT)
        {
          channel_cancel="spot.order_cancel";        
        }
        else
        {
          channel_cancel="futures.order_cancel";    
        }
        nlohmann::json message;
        
        message["channel"] = channel_cancel;
        message["payload"]["req_id"] = exchange_order_id;
        message["payload"]["req_param"]["order_id"] =exchange_order_id;

         if(type==singular::types::InstrumentType::SPOT)      
        {
          message["payload"]["req_param"]["currency_pair"] =contract_symbol;  
        }
      
        auto timestamp = std::chrono::duration_cast<std::chrono::seconds>(
                             std::chrono::system_clock::now().time_since_epoch())
                             .count();
        message["time"] = timestamp;

        if(type!=singular::types::InstrumentType::SPOT)
        {
          private_futures_client_->send(message.dump());
        }
        else
        {
          private_spot_client_->send(message.dump());
        }

        // Enable Log in Debug mode
        std::string log_message = "Sent a cancel order request for clOrdId: ";
        log_message += exchange_order_id;
        singular::utility::log_event(log_service_name, singular::utility::OEMSEvent::CANCEL_ORDER_DEBUG, log_message);
      }

      void Gateway::do_modify(singular::types::Order *order, double quantity, double price, singular::types::RequestSource source)
      {
        auto client_id = internal_to_client_id_map_[order->id_];
        auto symbol = internal_id_symbol_map_[order->id_];
        auto credential_id = internal_to_credential_id_map_[client_id];
        modify_order_vect.push_back(client_id);
        do_cancel(order->id_, source);
        if (order->instrument_ == nullptr)
        {
          singular::utility::log_event(log_service_name, singular::utility::OEMSEvent::MODIFY_ORDER_ERROR, "Order instrument is null in do_modify.");
          return; // Exit the function as instrument data is missing
        }
        do_place(order->instrument_->symbol_, order->instrument_->type_, order->id_, order->type_, order->side_, price, quantity, source, credential_id);
      }

      void Gateway::do_subscribe_positions()
      {
        
      }

      void Gateway::do_subscribe_account()
      {
       
      }

      void Gateway::do_unsubscribe_positions()
      {
        
      }

      void Gateway::do_subscribe_orderbooks(std::vector<singular::types::Symbol> &symbols)
      {
        nlohmann::json message;
        auto split_symbols = splitSymbols(symbols);
        auto now = std::chrono::system_clock::now();
        auto time_int = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();
        for (auto &split_symbol : split_symbols)
        {
          nlohmann::json payload;
          std::string channel;
          if(split_symbol.second == "SPOT"){
            channel = "spot.order_book_update";
            payload = {split_symbol.first, "100ms"};

            message = {
            {"time", time_int},
            {"channel", channel},
            {"event", "subscribe"},
            {"payload", payload}};

            public_client_spot->send(message.dump());
          }
          else if(split_symbol.second == "FUTURE"){
            channel = "futures.order_book_update";
            payload = {split_symbol.first,"100ms","20"};

            message = {
            {"time", time_int},
            {"channel", channel},
            {"event", "subscribe"},
            {"payload", payload}};

            if(is_btc(split_symbol.first))
            {
              public_client_futures_btc->send(message.dump());
            }
            else
            {
              public_client_futures_usdt->send(message.dump());
            }
          }
        }
        singular::utility::log_event(log_service_name, singular::utility::OEMSEvent::ORDERBOOK_SUBSCRIBE_SUCCESS, "Sent a subscribe message for orderbooks channel");
      }

      void Gateway::do_unsubscribe_orderbooks(std::vector<singular::types::Symbol> &symbols)
      {
        nlohmann::json message;
        auto split_symbols = splitSymbols(symbols);
        auto now = std::chrono::system_clock::now();
        auto time_int = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();
        for (auto &split_symbol : split_symbols)
        {
          nlohmann::json payload;
          std::string channel;
          if(split_symbol.second == "SPOT"){
            channel = "spot.order_book_update";
            payload = {split_symbol.first, "100ms"};

            message = {
            {"time", time_int},
            {"channel", channel},
            {"event", "unsubscribe"},
            {"payload", payload}};

            public_client_spot->send(message.dump());
          }
          else if(split_symbol.second == "FUTURE"){
            channel = "futures.order_book_update";
            payload = {split_symbol.first,"100ms","20"};

            message = {
            {"time", time_int},
            {"channel", channel},
            {"event", "unsubscribe"},
            {"payload", payload}};

            if(is_btc(split_symbol.first))
            {
              public_client_futures_btc->send(message.dump());
            }
            else
            {
              public_client_futures_usdt->send(message.dump());
            }
          }
        }

        singular::utility::log_event(log_service_name, singular::utility::OEMSEvent::ORDERBOOK_SUBSCRIBE_SUCCESS, "Sent a unsubscribe message for orderbooks channel");
      }

      void Gateway::do_subscribe_tickers(std::vector<singular::types::Symbol> &symbols)
      {
        nlohmann::json message1, message2;
        auto split_symbols = splitSymbols(symbols);
        auto now = std::chrono::system_clock::now();
        auto time_int = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();
        for (auto &split_symbol : split_symbols)
        {
          nlohmann::json payload = {split_symbol.first};
          std::string channel1;
          std::string channel2;
          if(split_symbol.second == "SPOT"){
            channel1 = "spot.tickers";
            channel2 = "spot.book_ticker";
            payload = {split_symbol.first};

            message1 = {
              {"time", time_int},
              {"channel", channel1},
              {"event", "subscribe"},
              {"payload", payload}};

            message2 = {
              {"time", time_int},
              {"channel", channel2},
              {"event", "subscribe"},
              {"payload", payload}};

            public_client_spot->send(message1.dump());

            public_client_spot->send(message2.dump());
          }
          else if(split_symbol.second == "FUTURE"){
            channel1 = "futures.tickers";
            channel2 = "futures.book_ticker";
            payload = {split_symbol.first};

            message1 = {
              {"time", time_int},
              {"channel", channel1},
              {"event", "subscribe"},
              {"payload", payload}};

            message2 = {
              {"time", time_int},
              {"channel", channel2},
              {"event", "subscribe"},
              {"payload", payload}};

            if(is_btc(split_symbol.first))
            {
              public_client_futures_btc->send(message1.dump());
              public_client_futures_btc->send(message2.dump());
            }
            else
            {
              public_client_futures_usdt->send(message1.dump());
              public_client_futures_usdt->send(message2.dump());
            }
          }
        }

        singular::utility::log_event(log_service_name, singular::utility::OEMSEvent::TICKER_SUBSCRIBE_SUCCESS, "Sent a subscribe message for both the ticker channels");
      }

      void Gateway::do_unsubscribe_tickers(std::vector<singular::types::Symbol> &symbols)
      {
        nlohmann::json message1, message2;
        auto split_symbols = splitSymbols(symbols);
        auto now = std::chrono::system_clock::now();
        auto time_int = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();
        for (auto &split_symbol : split_symbols)
        {
          nlohmann::json payload = {split_symbol.first};
          std::string channel1;
          std::string channel2;
          if(split_symbol.second == "SPOT"){
            channel1 = "spot.tickers";
            channel2 = "spot.book_ticker";
            payload = {split_symbol.first};

            message1 = {
            {"time", time_int},
            {"channel", channel1},
            {"event", "unsubscribe"},
            {"payload", payload}};

            message2 = {
            {"time", time_int},
            {"channel", channel2},
            {"event", "unsubscribe"},
            {"payload", payload}};

            public_client_spot->send(message1.dump());

            public_client_spot->send(message2.dump());
          }
          else if(split_symbol.second == "FUTURE"){
            channel1 = "futures.tickers";
            channel2 = "futures.book_ticker";
            payload = {split_symbol.first};

            message1 = {
            {"time", time_int},
            {"channel", channel1},
            {"event", "unsubscribe"},
            {"payload", payload}};

            message2 = {
            {"time", time_int},
            {"channel", channel2},
            {"event", "unsubscribe"},
            {"payload", payload}};

            if(is_btc(split_symbol.first))  //check if symbol belongs to btc websocket
            {
              public_client_futures_btc->send(message1.dump());
              public_client_futures_btc->send(message2.dump());
            }
            else
            {
              public_client_futures_usdt->send(message1.dump());
              public_client_futures_usdt->send(message2.dump());
            }
          }
        }

        singular::utility::log_event(log_service_name, singular::utility::OEMSEvent::TICKER_SUBSCRIBE_SUCCESS, "Sent a unsubscribe message for ticker channel");
      
      }

      void Gateway::do_subscribe_top_of_book(std::vector<singular::types::Symbol> &symbols)
      {
        
      }

      void Gateway::do_subscribe_trades(std::vector<singular::types::Symbol> &symbols)
      {
        nlohmann::json message;
        auto split_symbols = splitSymbols(symbols);
        auto now = std::chrono::system_clock::now();
        auto time_int = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();
        for (auto &split_symbol : split_symbols)
        {
          nlohmann::json payload = {split_symbol.first};
          std::string channel;
          if(split_symbol.second == "SPOT")
          {
            channel = "spot.trades";
            
            message = {
            {"time", time_int},
            {"channel", channel},
            {"event", "subscribe"},
            {"payload", payload}};

            public_client_spot->send(message.dump());
          }
          else if(split_symbol.second == "FUTURE")
          {
            channel = "futures.trades";

            message = {
            {"time", time_int},
            {"channel", channel},
            {"event", "subscribe"},
            {"payload", payload}};

            if(is_btc(split_symbol.first))  //check if symbol belongs to btc websocket
            {
              public_client_futures_btc->send(message.dump());
            }
            else
            {
              public_client_futures_usdt->send(message.dump());
            }
          } 
        }

        singular::utility::log_event(log_service_name, singular::utility::OEMSEvent::LASTTRADES_SUBSCRIBE_SUCCESS, "Sent a subscribe message for last trades channel", message["payload"].dump());
      }

      void Gateway::do_unsubscribe_trades(std::vector<singular::types::Symbol> &symbols)
      {
        nlohmann::json message;
        auto split_symbols = splitSymbols(symbols);
        auto now = std::chrono::system_clock::now();
        auto time_int = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();
        for (auto &split_symbol : split_symbols)
        {
          nlohmann::json payload = {split_symbol.first};
          std::string channel;
          if(split_symbol.second == "SPOT")
          {
            channel = "spot.trades";
            
            message = {
            {"time", time_int},
            {"channel", channel},
            {"event", "unsubscribe"},
            {"payload", payload}};

            public_client_spot->send(message.dump());
          }
          else if(split_symbol.second == "FUTURE")
          {
            channel = "futures.trades";

            message = {
            {"time", time_int},
            {"channel", channel},
            {"event", "unsubscribe"},
            {"payload", payload}};

            if(is_btc(split_symbol.first))  //check if symbol belongs to btc websocket
            {
              public_client_futures_btc->send(message.dump());
            }
            else
            {
              public_client_futures_usdt->send(message.dump());
            }
          } 
        }

        singular::utility::log_event(log_service_name, singular::utility::OEMSEvent::LASTTRADES_SUBSCRIBE_SUCCESS, "Sent a unsubscribe message for last trades channel", message["payload"].dump());
      }

      void Gateway::do_subscribe_funding(std::vector<singular::types::Symbol> &symbols)
      {
        //singular::utility::log_event(log_service_name, singular::utility::OEMSEvent::ACCOUNT_SUBSCRIBE_SUCCESS, "Sent a subscribe message for funding-rate channel", message["args"].dump());
      }

      void Gateway::run_public_ws_spot()
      {

      }
      void Gateway::run_public_ws_futures_btc()
      {
      
      }
      void Gateway::run_public_ws_futures_usdt()
      {
       
      }

      singular::types::GatewayStatus Gateway::status()
      {
        // Temporary code Remove after test
            return singular::types::GatewayStatus::ONLINE;
          
      }

      std::vector<std::pair<std::string,std::string>> Gateway::splitSymbols(std::vector<singular::types::Symbol> &symbols)
      {
        std::vector<std::pair<std::string,std::string>> split_symbols;
        for(auto symbol : symbols)
        {
          size_t atPos = symbol.find('@');
          if (atPos == std::string::npos) 
          {
            split_symbols.push_back(std::make_pair(symbol, ""));
          }
          std::string first = symbol.substr(0, atPos);
          std::string second = symbol.substr(atPos + 1);
          split_symbols.push_back(std::make_pair(first, second));
        }
        return split_symbols;
      }

      bool Gateway::is_btc(std::string &symbol)
      {
        return symbol == "BTC_USD"; //true if the symbol is BTC_USD
      }

      std::string Gateway::getCurrentTimestamp()
      {
        auto now = std::chrono::system_clock::now();
        auto in_time_t = std::chrono::system_clock::to_time_t(now);
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;

        std::stringstream ss;
        ss << std::put_time(std::gmtime(&in_time_t), "%Y-%m-%dT%H:%M:%S");
        ss << '.' << std::setfill('0') << std::setw(3) << ms.count();
        ss << "Z";
        return ss.str();
      }

      std::string Gateway::iso_timestamp()
      {
        auto now = std::chrono::system_clock::now();
        auto now_c = std::chrono::system_clock::to_time_t(now);
        auto now_tm = *std::gmtime(&now_c);
        char buf[20];
        strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%S", &now_tm);
        auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;
        std::string iso_timestamp(buf);
        iso_timestamp += ".";
        iso_timestamp += std::to_string(now_ms.count());
        return iso_timestamp;
      }

      nlohmann::json Gateway::get_open_orders()
      {
        //Temporary code
        nlohmann::json result;
        result["response"] = "SUCCESS";
        return result;
      }

      void Gateway::logout()
      {
         if (!is_purged_)
            send_gateway_disconnect(singular::types::Exchange::GATEIO, name_);
      }

      nlohmann::json Gateway::get_account_data()
      {
        return account_info;
      }

      nlohmann::json Gateway::get_position_data()
      {
        return position_data_;
      }

      nlohmann::json Gateway::get_order_data()
      {
        return nlohmann::json::array();
      }

      void Gateway::set_order_channel_status(std::string session_id, std::string credential_id)
      {
        session_map.push_back(session_id);
      }

      void Gateway::unset_order_channel_status(std::string session_id)
      {
         auto it = std::remove(session_map.begin(), session_map.end(), session_id);
        session_map.erase(it, session_map.end());
      }

      void Gateway::unset_order_execution_quality_channel_status(std::string session_id)
      {
        auto it = std::remove(order_execution_quality_session_map.begin(), order_execution_quality_session_map.end(), session_id);
        order_execution_quality_session_map.erase(it, order_execution_quality_session_map.end());
      }
      void Gateway::set_order_execution_quality_channel_status(std::string session_id, std::string credential_id)
      {
        order_execution_quality_session_map.push_back(session_id);
      }

      nlohmann::json Gateway::get_orderbook_data()
      {
        return nlohmann::json::array();
      }

      nlohmann::json Gateway::get_last_trades_data()
      {
        return nlohmann::json::array();
      }

      void Gateway::subscribe_fills()
      {
        
      }

      void Gateway::unsubscribe_fills()
      {
        
      }

      void Gateway::run_private_spot_ws()
      {
        if(spot_login_status)
        {
          //log
          if(private_spot_client_)
          {
            private_spot_client_->run(
              [this](const HttpResponsePtr &response){

              },
              [this](){
                private_spot_status_=singular::types::GatewayStatus::OFFLINE;
                authenticated_=false;
              },
              [this](const std::string &message)
              {
                parse_websocket_private(message);
              }
            );
            while(!private_spot_client_->is_open());
            login_spot_private();
          }
        }
      }
      void Gateway::run_private_futures_ws()
      {
        if(futures_login_status)
        {
          //log
          if(private_futures_client_)
          {
            private_futures_client_->run(
              [this](const HttpResponsePtr &response){

              },
              [this](){
                private_futures_status_=singular::types::GatewayStatus::OFFLINE;
                authenticated_=false;
              },
              [this](const std::string &message)
              {
                parse_websocket_private(message);
              }
            );
            while(!private_futures_client_->is_open());
            login_futures_private();
          }
        }
      }
       std::string Gateway::generate_hmac_sha512_hex(const std::string &message, const std::string &secret_key) 
       {
          unsigned char hmac_result[EVP_MAX_MD_SIZE] = {0};
          unsigned int hmac_length = 0;

          HMAC_CTX *hmac_ctx = HMAC_CTX_new();
          HMAC_Init_ex(hmac_ctx, secret_key.c_str(), secret_key.size(), EVP_sha512(), nullptr);
          HMAC_Update(hmac_ctx, reinterpret_cast<const unsigned char *>(message.c_str()), message.size());
          HMAC_Final(hmac_ctx, hmac_result, &hmac_length);
          HMAC_CTX_free(hmac_ctx);

          std::ostringstream hex_stream;
          for (unsigned int i = 0; i < hmac_length; ++i) 
          {
              hex_stream << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(hmac_result[i]);
          }
          return hex_stream.str();
        }
      void Gateway::login_spot_private() //need to call this twice for 2 private clients
      { 
        auto timestamp = std::chrono::duration_cast<std::chrono::seconds>(
                             std::chrono::system_clock::now().time_since_epoch())
                             .count();
        std::string event = "api";
        std::string signature_str;
        signature_str.append("api")
                     .append("\n")
                     .append("spot.login")
                     .append("\n")
                     .append("")
                     .append("\n")
                     .append(std::to_string(timestamp));

        std::string signature = generate_hmac_sha512_hex(signature_str, secret_);

        nlohmann::json message;
        message["time"] = timestamp;
        message["channel"] = "spot.login";
        message["event"] = "api";
        message["payload"]["api_key"] = key_;
        message["payload"]["req_id"] = name_;
        message["payload"]["timestamp"] = std::to_string(timestamp);
        message["payload"]["signature"] = signature;
    
        private_spot_client_->send(message.dump());
        singular::utility::log_event(log_service_name, singular::utility::OEMSEvent::LOGIN_EXCHANGE_SUCCESS, "Sent a private login message to the exchange");
      }

      void Gateway::login_futures_private() //need to call this twice for 2 private clients
      { 
        auto timestamp = std::chrono::duration_cast<std::chrono::seconds>(
                             std::chrono::system_clock::now().time_since_epoch())
                             .count();
        std::string event = "api";
        std::string signature_str;
        signature_str.append("api")
                     .append("\n")
                     .append("futures.login")
                     .append("\n")
                     .append("")
                     .append("\n")
                     .append(std::to_string(timestamp));

        std::string signature = generate_hmac_sha512_hex(signature_str, secret_);

        nlohmann::json message;
        message["time"] = timestamp;
        message["channel"] = "futures.login";
        message["event"] = "api";
        message["payload"]["api_key"] = key_;
        message["payload"]["req_id"] = name_;
        message["payload"]["timestamp"] = std::to_string(timestamp);
        message["payload"]["signature"] = signature;
    
        private_futures_client_->send(message.dump());

        singular::utility::log_event(log_service_name, singular::utility::OEMSEvent::LOGIN_EXCHANGE_SUCCESS, "Sent a private login message to the exchange");
      }

      void Gateway::login_public()
      {
        
      }

      unsigned long long Gateway::get_client_id(singular::types::OrderId order_id)
      {
        // Get current timestamp in seconds
        const auto seconds = std::chrono::duration_cast<std::chrono::seconds>(
                                 std::chrono::system_clock::now().time_since_epoch())
                                 .count();

        // Pre-calculate the multiplier based on number of digits in order_id
        // This avoids string operations entirely
        unsigned long long multiplier = 10;
        unsigned long long temp_order_id = order_id;
        while (temp_order_id /= 10)
        {
          multiplier *= 10;
        }

        // Combine timestamp and order_id mathematically instead of string concatenation
        return seconds * multiplier + order_id;
      }

      void Gateway::parse_websocket_private(const std::string &buffer)
      {
        nlohmann::json message;
        try
        {
          message = nlohmann::json::parse(buffer);//Parsing message
        }
        catch (const nlohmann::json::parse_error &e)
        {
          singular::utility::log_event(log_service_name, singular::utility::OEMSEvent::WS_CONNECTION_ERROR, std::string("JSON parse error: ") + e.what());
          return; // Exit the function as parsing failed
        }

        if (message.contains("header"))
        { 
          std::string channel=message["header"]["channel"];

          try
          {
            int status = std::stoi(static_cast<std::string>(message["header"]["status"]));
            if(channel=="futures.login"||channel=="spot.login")//checks if related to login
            {
              if (status==200)
              {
                authenticated_ = true;
                public_status_ = singular::types::GatewayStatus::ONLINE;
                private_spot_status_ = singular::types::GatewayStatus::ONLINE;
                private_futures_status_ = singular::types::GatewayStatus::ONLINE;

                // Send Login Success Response to the endpoint
                singular::types::EventDetail detail("OK",
                                                    status,
                                                    "SUCCESS",
                                                    singular::event::EventType::LOGIN_ACCEPT,
                                                    std::nullopt);
                send_operation_response("SUCCESS", detail);
                singular::utility::log_event(log_service_name, singular::utility::OEMSEvent::LOGIN_EXCHANGE_SUCCESS, "Logged in successfully");

                subscribe_fills();
                do_subscribe_positions();
                do_subscribe_account();
              }
              else 
              {
                // Send Login Fail Response to the endpoint
                singular::types::EventDetail detail(message["data"]["errs"]["label"],
                                                    std::stoi(static_cast<std::string>(message["header"]["status"])),
                                                    "FAILED",
                                                    singular::event::EventType::LOGIN_FAIL,
                                                    std::nullopt);
                send_operation_response("ERROR", detail);
                singular::utility::log_event(log_service_name, singular::utility::OEMSEvent::LOGIN_EXCHANGE_ERROR, "Login unsuccessful");
                do_unsubscribe_positions();
                unsubscribe_fills();
                send_gateway_disconnect(singular::types::Exchange::GATEIO, name_);
                close_public_socket();
                close_private_socket();
              }
            }
            else if(channel=="futures.order_place"||channel=="spot.order_place")//checks if response is related to placing order
            {  
              if (status==200)
              {
                singular::types::EventDetail detail("OK",
                                                    status,
                                                    "Order Request sent",
                                                    singular::event::EventType::FILL,
                                                    std::nullopt);
                send_operation_response("SUCCESS", detail);
                singular::utility::log_event(log_service_name, singular::utility::OEMSEvent::PLACE_ORDER_SUCCESS, "Order request sent");

              }
              else
              {
                singular::types::EventDetail detail(message["data"]["errs"]["label"],
                                                    status,
                                                    "FAILED",
                                                    singular::event::EventType::FILL,
                                                    std::nullopt);
                send_operation_response("ERROR", detail);
                singular::utility::log_event(log_service_name, singular::utility::OEMSEvent::PLACE_ORDER_ERROR, "Place Order failed");
              }
            }
            
            else if(channel=="futures.order_cancel"||channel=="spot.order_cancel")//checks if response is related to cancelling order
            {
              if (status==200)
              {
                singular::types::EventDetail detail("OK",
                                                    status,
                                                    "Cancel Request sent",
                                                    singular::event::EventType::CANCEL_ACCEPT,
                                                    std::nullopt);
                send_operation_response("SUCCESS", detail);
                singular::utility::log_event(log_service_name, singular::utility::OEMSEvent::CANCEL_ORDER_SUCCESS, "Cancellation request sent");

              }
              else
              {
                singular::types::EventDetail detail(message["data"]["errs"]["label"],
                                                    status,
                                                    "FAILED",
                                                    singular::event::EventType::CANCEL_FAIL,
                                                    std::nullopt);
                send_operation_response("ERROR", detail);
                singular::utility::log_event(log_service_name, singular::utility::OEMSEvent::CANCEL_ORDER_SUCCESS, "Cancellation failed");
              }
            }

            else if(channel=="futures.order_amend"||channel=="spot.order_amend")//checks if response is related to updating order
            {
              if (status==200)
              {
                singular::types::EventDetail detail("OK",
                                                    status,
                                                    "Modify Request sent",
                                                    singular::event::EventType::MODIFY_ACCEPT,
                                                    std::nullopt);
                send_operation_response("SUCCESS", detail);
                singular::utility::log_event(log_service_name, singular::utility::OEMSEvent::MODIFY_ORDER_SUCCESS, "Modification request sent");

              }
              else
              {
                singular::types::EventDetail detail(message["data"]["errs"]["label"],
                                                    status,
                                                    "FAILED",
                                                    singular::event::EventType::MODIFY_FAIL,
                                                    std::nullopt);
                send_operation_response("ERROR", detail);
                singular::utility::log_event(log_service_name, singular::utility::OEMSEvent::MODIFY_ORDER_ERROR, "Modification of order failed");
              }
            }

            else //Incase there is some other issue
            {
              // Send Unknown Response to the endpoint
              singular::types::EventDetail detail(message["data"]["errs"]["label"],
                                                  std::stoi(static_cast<std::string>(message["header"]["status"])),
                                                  "FAILED",
                                                  singular::event::EventType::NONE,
                                                  std::nullopt);
              send_operation_response("ERROR", detail);
              singular::utility::log_event(log_service_name, singular::utility::OEMSEvent::WS_CONNECTION_ERROR, "Unexpected Response!");
              do_unsubscribe_positions();
              unsubscribe_fills();
              send_gateway_disconnect(singular::types::Exchange::GATEIO, name_);
              close_public_socket();
              close_private_socket();
            }
          }
          catch (const std::exception &exception)
          {
            singular::utility::log_event(log_service_name, singular::utility::OEMSEvent::WS_CONNECTION_ERROR, std::string("Error reading event websocket message: ") + exception.what());
          }
      }
      }

      void Gateway::stream_order_data(nlohmann::json message, const std::string order_state)
      {
        auto client_id = std::stoull(static_cast<std::string>(message["id"]));
        message["internal_order_id"] = client_to_internal_id_map_[client_id];
        auto req_source = client_id_to_source_map_[client_id];
        message["request_source"] = singular::types::get_request_source_string[req_source];
        message["data"][0]["state"] = order_state;
        message["data"][0]["symbol"] = client_id_to_symbol_map_[client_id];
        message["data"][0]["price"] = client_id_to_price_map_[client_id];
        message["data"][0]["quantity"] = client_id_to_qty_map_[client_id];
        message["data"][0]["side"] = singular::types::SideDescription(static_cast<int>(client_id_to_side_map_[client_id]));
        message["algorithm_id"] = NULL;
        for (const auto &pair : singular::types::getAlgorithmReferenceMap())
        {
          if (std::find(pair.second.begin(), pair.second.end(), message["internal_order_id"]) != pair.second.end())
          {
            message["algorithm_id"] = pair.first;
            break; // Remove break if you want to find all keys containing the value
          }
        }

        // Check if order_id is empty and generate dummy one
        if (message["data"][0]["ordId"] == "")
        {
          message["data"][0]["ordId"] = std::to_string(get_client_id(message["internal_order_id"].get<unsigned long>()));
        }

                nlohmann::json final_data = nlohmann::json::object();
                nlohmann::json subs_data = nlohmann::json::array();
                subs_data.push_back({{"channel", "order"}, {"data", message}});
                final_data["exchange"] = "GATEIO";
                final_data["name"] = name_;
                final_data["data"] = subs_data;
                if (internal_to_credential_id_map_.find(message["internal_order_id"]) != internal_to_credential_id_map_.end())
                {
                    final_data["credential_id"] = internal_to_credential_id_map_[message["internal_order_id"]];
                }
                if(order_state != "received"){
                auto redis_score = singular::utility::timestamp<std::chrono::seconds>();
                redis_helper_.save_to_sorted_set("gateio_order_last_min_data", final_data, static_cast<double>(redis_score));
                }

                for (const auto &session_id_value : session_map)
                {
                    auto it = singular::network::globalWebSocketChannels.find(session_id_value);
                    if (it != singular::network::globalWebSocketChannels.end() && it->second)
                    {
                        // std::cerr<<"Sending"<<std::endl;
                        singular::utility::log_event(log_service_name, singular::utility::OEMSEvent::WS_CONNECTION_DEBUG, "Sending to Global Websocket Server");
                        it->second->send(final_data.dump());
                    }
                }
            }

      void Gateway::send_reject_response(const nlohmann::json &message)
      {
        // Define a constant order state within the function
        const std::string order_state = "order_reject"; // Set the order state to reject

        // Create a mutable copy of the order_id
        std::string ordId;

        // Check if order_id is empty and generate new one if needed
        if (!message.contains("order_id") || message["order_id"].get<std::string>().empty())
        {
          if (message.contains("algorithm_id"))
          {
            ordId = std::to_string(get_client_id(message["algorithm_id"].get<unsigned long>()));
          }
          else
          {
            singular::utility::log_event(log_service_name, singular::utility::OEMSEvent::OMS_ERROR, "Missing order_id and algorithm_id in reject response.");
          }
        }
        else
        {
          ordId = message["order_id"].get<std::string>();
        }

        // Final data structure based on the provided format using data from the message itself
        nlohmann::json final_data = {
            {"exchange", message["exchange"]},           // Replaced with message's exchange
            {"name", message["name"]},                   // Replaced with message's name
            {"credential_id", message["credential_id"]}, // Replaced with message's credential_id
            {"data", nlohmann::json::array({{{"channel", "order"},
                                             {"data", {
                                                          {"algorithm_id", message["algorithm_id"]}, // Replaced with message's algorithm_id
                                                          {"code", "1"},                             // Hardcoded as per your example
                                                          {"data", nlohmann::json::array({{
                                                                       {"clOrdId", ""}, // Empty as per request
                                                                       {"ordId", ordId},
                                                                       {"price", message["price"]}, // Replaced with price from message
                                                                       {"side", singular::types::SideDescription(static_cast<int>(message["side"]))},
                                                                       {"quantity", message["quantity"]}, // Replaced with quantity from message
                                                                       {"sCode", "000"},                  // Hardcoded to "000" as requested
                                                                       {"sMsg", message["message"]},      // Error message
                                                                       {"state", order_state},            // Constant "order_reject"
                                                                       {"symbol", message["symbol"]},     // Replaced with symbol from message
                                                                       {"tag", ""},                       // Empty as per request
                                                                       {"ts", ""}                         // Empty as per request
                                                                   }})},
                                                          {"id", ""},                                   // Empty as per request
                                                          {"inTime", ""},                               // Empty as per request
                                                          {"msg", ""},                                  // Empty as per request
                                                          {"op", "order"},                              // Hardcoded to "order" as requested
                                                          {"outTime", ""},                              // Empty as per request
                                                          {"request_source", message["request_source"]} // Replaced with request_source from message
                                                      }}}})}};

                // Loop through the sessions and send the data
                auto redis_score = singular::utility::timestamp<std::chrono::seconds>();
                redis_helper_.save_to_sorted_set("okx_order_last_min_data", final_data, static_cast<double>(redis_score));
                for (const auto &session_id_value : session_map)
                {
                    auto it = singular::network::globalWebSocketChannels.find(session_id_value);

                    // Send the final data over the WebSocket if a session is found
                    if (it != singular::network::globalWebSocketChannels.end() && it->second)
                    {
                        singular::utility::log_event(log_service_name, singular::utility::OEMSEvent::WS_CONNECTION_DEBUG, "Sending to Global Websocket Server");
                        it->second->send(final_data.dump());
                    }
                }
            }
            
            void Gateway::send_algo_execution_status(const nlohmann::json &message)
            {
                nlohmann::json final_data = {
                    {"message", message["message"]},
                    {"is_initialized", message["is_initialized"]},
                    {"request_source", message["request_source"]},
                    {"algorithm_id", message["algorithm_id"]},
                    {"symbol", message["symbol"]},
                    {"is_completed", message["is_completed"]}
                };

        // Loop through the sessions and send the data
        for (const auto &session_id_value : session_map)
        {
          auto it = singular::network::globalWebSocketChannels.find(session_id_value);

          // Send the final data over the WebSocket if a session is found
          if (it != singular::network::globalWebSocketChannels.end() && it->second)
          {
            singular::utility::log_event(log_service_name, singular::utility::OEMSEvent::WS_CONNECTION_DEBUG, "Sending algo execution status Response to Global Websocket Server");
            it->second->send(final_data.dump());
          }
        }
            }
  

    } // namespace gateio
  } // namespace gateway
} // namespace singular
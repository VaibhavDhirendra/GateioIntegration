#pragma once

#include <string_view>
#include <thread>
#include <nlohmann/json.hpp>

#include <singular/network/network/include/WebsocketClient.h>
#include <singular/types/include/Order.h>
#include <singular/event/include/OrderbookUpdate.h>
#include <singular/event/include/Trade.h>
#include <singular/event/include/Event.h>
#include <singular/event/include/PlaceAck.h>
#include <singular/event/include/PlaceReject.h>
#include <singular/event/include/CancelAck.h>
#include <singular/event/include/CancelReject.h>
#include <singular/event/include/Fill.h>
#include <singular/event/include/GatewayDisconnect.h>
#include <singular/utility/include/Authentication.h>
#include <singular/utility/include/Time.h>
#include <singular/utility/include/LatencyLogger.h>
#include <singular/utility/include/EnvLoader.h>
#include <singular/network/include/GlobalWebsocket.h>
#include <singular/types/include/GlobalAlgorithmId.h>
#include <singular/utility/include/LatencyManager.h>
#include <hiredis/hiredis.h>
#include <gateway/include/AbstractGateway_V2.h>
#include <singular/utility/include/RedisHelper.h>
#include "ErrorCodes.h"

namespace singular {
namespace gateway {
namespace gateio {

class Gateway : public AbstractGateway_V2 {
public:
    Gateway(hv::EventLoopPtr& executor,
            bool authenticate,
            const std::string& name,
            const std::string& key,
            const std::string& secret,
            const std::string& passphrase,
            const std::string& mode);


    void initialize_callback_funcs();
    void do_place(singular::types::Symbol symbol, singular::types::InstrumentType type,
                  singular::types::OrderId order_id, singular::types::OrderType order_type,
                  singular::types::Side side, double price, double quantity, singular::types::RequestSource source,
                  std::string credential_id = "", std::string td_mode = "");
    void do_websocket_task_latency(singular::utility::TimePoint latency_info,long long internal_order_id, std::string credential_id = "");
    void do_send_native_order_latency(long long internal_order_id);
    void do_cancel(singular::types::OrderId order_id, singular::types::RequestSource source);
    void do_cancel(std::string exchange_order_id, singular::types::Instrument* instrument, singular::types::RequestSource source);
    void do_modify(singular::types::Order* order, double quantity, double price, singular::types::RequestSource source);
    void do_subscribe_positions();
    void do_subscribe_account();
    void do_unsubscribe_positions();
    void do_subscribe_orderbooks(std::vector<singular::types::Symbol>& symbols);
    void do_unsubscribe_orderbooks(std::vector<singular::types::Symbol>& symbols);
    void do_subscribe_tickers(std::vector<singular::types::Symbol>& symbols);
    void do_unsubscribe_tickers(std::vector<singular::types::Symbol>& symbols);
    void do_subscribe_top_of_book(std::vector<singular::types::Symbol>& symbols);
    void do_subscribe_trades(std::vector<singular::types::Symbol>& symbols);
    void do_unsubscribe_trades(std::vector<singular::types::Symbol>& symbols);
    void do_subscribe_funding(std::vector<singular::types::Symbol>& symbols);
    singular::types::GatewayStatus status();
    std::string getCurrentTimestamp();
    std::string iso_timestamp();
    std::vector<std::pair<std::string,std::string>> splitSymbols(std::vector<singular::types::Symbol>& symbols);
    bool is_btc(std::string& symbol);
    nlohmann::json get_open_orders();
    void logout();
    nlohmann::json get_account_data();
    nlohmann::json get_position_data();
    nlohmann::json get_order_data();
    void set_order_channel_status(std::string session_id, std::string credential_id = "");
    void unset_order_channel_status(std::string session_id);
    void set_order_execution_quality_channel_status(std::string session_id, std::string credential_id = "");
    void unset_order_execution_quality_channel_status(std::string session_id);
    nlohmann::json get_orderbook_data();
    nlohmann::json get_last_trades_data();
    void send_final_latency_info(singular::types::AlgorithmId algo_id, char* credential_id);
    void close_private_socket();
    void close_public_socket();
    void purge();
    bool is_purged() const { return is_purged_; }
private:
    void subscribe_fills();
    void unsubscribe_fills();
    void login_spot_private();
    void login_futures_private();
    void run_private_spot_ws();
    void run_private_futures_ws();
    void run_public_ws_spot();
    void run_public_ws_futures_btc();
    void run_public_ws_futures_usdt();
    void login_public();
    unsigned long long get_client_id(singular::types::OrderId order_id);
    void parse_websocket_private(const std::string& buffer);
    void stream_order_data(nlohmann::json message, const std::string order_state);
    void send_reject_response(const nlohmann::json& message) override;
    void send_algo_execution_status(const nlohmann::json& message) override;
    std::string generate_hmac_sha512_hex(const std::string &message, const std::string &secret_key);

    std::string log_service_name = "GATEIO";
    const char* public_futures_url;
    const char* private_futures_url;
    const char* public_spot_url;
    const char* private_spot_url;
    const char* env_mode;
    const char* public_futures_usdt_url;
    const char* public_futures_btc_url;
    bool sim_trading;

    std::unique_ptr<singular::network::WebsocketClient> public_client_;
    std::unique_ptr<singular::network::WebsocketClient> public_client_spot;
    std::unique_ptr<singular::network::WebsocketClient> public_client_futures_usdt;
    std::unique_ptr<singular::network::WebsocketClient> public_client_futures_btc;
    std::unique_ptr<singular::network::WebsocketClient> private_spot_client_;
    std::unique_ptr<singular::network::WebsocketClient> private_futures_client_;
    std::unique_ptr<singular::network::WebsocketClient> private_client_;
    std::string orderbook_channel_ = "futures.order_book_update";
    std::string ticker_channel_ = "futures.tickers";
    std::string trades_channel_ = "futures.trades";
    std::string book_ticker_channel_ = "futures.book_ticker";

    bool authenticate_;
    bool authenticated_ = {false};
    bool is_purged_ = { false };
    bool login_flag_= {false};

    std::string name_;
    std::string key_;
    std::string secret_;
    std::string passphrase_;
    std::string mode_;

    nlohmann::json account_info;
    nlohmann::json session_map = nlohmann::json::array();

    singular::types::GatewayStatus private_status_ = {singular::types::GatewayStatus::OFFLINE};
    singular::types::GatewayStatus public_status_ = {singular::types::GatewayStatus::OFFLINE};

    singular::types::GatewayStatus private_spot_status_ = {singular::types::GatewayStatus::OFFLINE};
    singular::types::GatewayStatus public_spot_status_ = {singular::types::GatewayStatus::OFFLINE};
    singular::types::GatewayStatus private_futures_status_ = {singular::types::GatewayStatus::OFFLINE};
    singular::types::GatewayStatus public_futures_status_ = {singular::types::GatewayStatus::OFFLINE};

    std::map<unsigned long int, singular::types::Symbol> client_id_to_symbol_map_;
    std::map<unsigned long int, singular::types::OrderId> client_to_internal_id_map_;
    std::map<unsigned long int, singular::types::RequestSource> client_id_to_source_map_;
    // nlohmann::json session_map = nlohmann::json::array();
    nlohmann::json order_execution_quality_session_map = nlohmann::json::array();
    std::map<unsigned long int, double> client_id_to_price_map_;
    std::map<unsigned long int, double> client_id_to_qty_map_;
    std::map<unsigned long int, singular::types::Side> client_id_to_side_map_;
    std::map<std::string, std::map<std::string, nlohmann::json>> position_data_;
    

    std::vector<unsigned long int> modify_order_vect;
    
    // These maps are used by do_cancel and do_modify (HOT PATH)
    // So changing them to unordered_map with LOAD_FACTOR and INITIAL_MAP_SIZE

    static constexpr size_t INITIAL_MAP_SIZE = 10000;  // Adjust based on expected load
    static constexpr float LOAD_FACTOR = 0.7f;        // Optimal load factor for performance

    std::unordered_map<singular::types::OrderId, unsigned long int> internal_to_client_id_map_;
    std::unordered_map<singular::types::OrderId, singular::types::Symbol> internal_id_symbol_map_;
    std::unordered_map<singular::types::OrderId, std::string> internal_to_credential_id_map_;
    //std::unordered_map<std::string, double> symbol_volume_map_;
    std::unordered_map<singular::types::Symbol, double> symbol_volume_map_;
    std::unordered_map<singular::types::OrderId, singular::types::InstrumentType> internal_to_type_map_;

    void initializeMaps();

    // Class constant for buffer size
    static constexpr size_t ESTIMATED_MESSAGE_SIZE = 256;
    
    // Utility method for message initialization
    std::string initializeMessage() const {
        std::string message;
        message.reserve(ESTIMATED_MESSAGE_SIZE);
        return message;
    }

    bool futures_login_status;
    bool spot_login_status;

    singular::utility::LatencyMeasure* latency_measure = nullptr;
    singular::utility::RedisHelper redis_helper_;

    
};

} // namespace gateio
} // namespace gateway
} // namespace singular
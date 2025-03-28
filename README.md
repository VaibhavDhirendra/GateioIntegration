# GateioIntegration
## Implementation of Gateio integration

### Introduction and Gateway Structure

Gateio is a global cryptocurrency exchange, that has over 3400 currencies. For the implementation of gateway with Gateio, we first fetch instruments through their REST api endpoints for spot, perpetual futures, deliver futures and options.
Since gateio provides a websocket connection for each of its instrument types, we create websocket clients for each websocket url available. In the current implementation, websocket connections to spot and perpetual futures are available, as they are the most widely used.

### Functionalities implemented:
- Orderbook
- Tickers
- Last trades
- Buy order
- Sell order
- Modify order
- Login
- Logout

Gateio provides different websockets for different instrument type unlike other exchanges.

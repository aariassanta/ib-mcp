# ib-mcp

MCP server for connecting to IBKR TWS/Gateway, providing real-time market data, positions, P&L, and option chain access via the [Model Context Protocol](https://github.com/modelcontextprotocol).

## Features

- **get_status**: Check connection status to TWS/Gateway
- **get_account**: Retrieve account summary, balance, buying power
- **get_positions**: List all open positions with unrealized P&L
- **get_pnl**: Get daily and unrealized P&L
- **get_market_data**: Real-time bid/ask/last/close/volume for stocks
- **get_option_chain**: Full option chain for a given symbol and expiry
- **get_option_prices**: ATM option prices with delta + Greeks/IV/OI

## Prerequisites

1. **IBKR TWS or IB Gateway** running in paper or live mode
   - Paper mode: `TRADING_MODE=paper` in Docker
   - API port (default: `4002`)
   - Enable ActiveX and Socket clients
   - Trusted IPs configured (127.0.0.1 or your host IP)

2. **TWS API configured**:
   - Set API port (e.g., `7497` for TWS live, `7498` for TWS paper, `4002` for Gateway)
   - Add `127.0.0.1` to trusted IPs
   - Enable ActiveX and Socket Clients

## Installation

```bash
# Clone the repository
git clone https://github.com/aariassanta/ib-mcp.git
cd ib-mcp

# Create and activate a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

## Usage

### HTTP Server (Recommended for remote access)

```bash
# Set required environment variables
export IB_HOST=127.0.0.1
export IB_PORT=4002
export IB_MCP_TOKEN=mi-token-secur0
export IB_MCP_DEBUG=0  # optional, set to 1 for debug output

# Start the HTTP server
python server_http2.py
```

The server listens on `0.0.0.0:8765` by default.

### Client Access

All requests require Bearer token authentication:

```bash
curl -H "Authorization: Bearer $IB_MCP_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"get_status","arguments":{}}}' \
  http://127.0.0.1:8765/mcp
```

### Available Tools

#### get_status
Check if the server is connected to TWS/Gateway.

#### get_account
Retrieve account summary information including balance, buying power, and margin.

#### get_positions
List all open positions with current market value and unrealized P&L.

#### get_pnl
Get profit and loss data (daily + unrealized).

#### get_market_data
Retrieve real-time market data for a symbol.
- **Parameters**: `symbol` (e.g., "NVDA", "SPY"), `sec_type` (default "STK")
- **Returns**: bid, ask, last, close, volume

#### get_option_chain
Get the full option chain for a symbol.
- **Parameters**: `symbol` (e.g., "NVDA"), `expiry` (YYYY-MM-DD), `sec_type` (default "OPT")
- **Returns**: All strikes (calls and puts) for the given expiry

#### get_option_prices
Get ATM option prices with Greeks, IV, and Open Interest.
- **Parameters**: `symbol` (e.g., "NVDA"), `expiry` (YYYY-MM-DD), `sec_type` (default "OPT")
- **Returns**: ATM call/put prices, delta, gamma, vega, theta, IV, OI

## Docker Setup

Example docker-compose for TWS headless + MCP server:

```yaml
version: '3.8'
services:
  ib-gateway:
    image: ghcr.io/gnzsnz/ib-gateway:stable
    container_name: ib-gateway
    ports:
      - "4002:4004"
    environment:
      - TRADING_MODE=paper
      - TWS_USERID=aariassanta-demo
      - TWS_PASSWORD=your-password
      - AUTO_RESTART_TIME=11:45 PM
      - TIME_ZONE=Europe/Madrid
      - ACCEPT_MKT_DATA_DIALOG=yes
      - ACCEPT_NON_BROKERAGE_ACCOUNT_WARNING=yes
      - READ_ONLY_API=no
      - RELOGIN_AFTER_TWOFA_TIMEOUT=yes
      - TWOFA_EXIT_INTERVAL=60

  ib-mcp:
    build: .
    container_name: ib-mcp
    ports:
      - "8765:8765"
    environment:
      - IB_HOST=host.docker.internal
      - IB_PORT=4002
      - IB_MCP_TOKEN=mi-token-secur0
    depends_on:
      - ib-gateway
    restart: unless-stopped
```

## Known Limitations

1. **Option market data**: `snapshot=True` doesn't work — requires streaming mode
2. **Historical data**: Use `timeout=60`, `useRTH=False`, MIDPOINT→TRADES fallback
3. **Open Interest**: Access via `callOpenInterest`/`putOpenInterest` tick types
4. **IV calculation**: Use `modelGreeks.impliedVol`, not `impliedVolatility`

## License

MIT
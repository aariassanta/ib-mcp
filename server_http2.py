#!/usr/bin/env python3
"""
IB Gateway MCP Server - HTTP Streamable transport.
Uso: IB_MCP_TOKEN=mi-token IB_HOST=127.0.0.1 IB_PORT=4002 python server_http2.py
"""
import os
import json
import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Optional

from starlette.applications import Starlette
from starlette.routing import Route
from starlette.requests import Request
from starlette.responses import JSONResponse

from mcp.server import Server
from mcp.server.lowlevel.server import Server as LowLevelServer
from mcp.server.lowlevel.server import lifespan as default_lifespan
from mcp.server.streamable_http import EventStore
from mcp.server.context import ServerRequestContext, HandlerResult
from mcp.server.transport_security import TransportSecuritySettings
from mcp_types import Tool, TextContent, ListToolsRequest, ListToolsResult, CallToolResult
from mcp_types import CallToolRequestParams

from ib_insync import IB, Option, Stock, Index, Contract


# ── Config ───────────────────────────────────────────────────────────────────

HOST = os.getenv("IB_HOST", "127.0.0.1")
PORT = int(os.getenv("IB_PORT", "4002"))
TOKEN = os.getenv("IB_MCP_TOKEN", "")
DEBUG = os.getenv("IB_MCP_DEBUG", "0") == "1"


# ── Token Verifier estático ────────────────────────────────────────────────────

from mcp.server.auth.provider import AccessToken

class StaticTokenVerifier:
    """Verifica un token Bearer fijo contra IB_MCP_TOKEN."""
    def __init__(self, valid_token: str):
        self.valid_token = valid_token

    async def verify_token(self, token: str) -> AccessToken | None:
        if self.valid_token and token == self.valid_token:
            return AccessToken(
                token=token,
                client_id="mcp-client",
                subject="mcp-user",
                scopes=[],
                claims={},
            )
        return None


# ── Server MCP ─────────────────────────────────────────────────────────────────

server = LowLevelServer("ib-gateway", lifespan=default_lifespan)


# ── Thread pool para ib_insync (crea su propio event loop) ─────────────────────
import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor

_ib_pool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="ib-")

def _run_ib_sync(fn, *args, **kwargs):
    """Ejecuta fn en thread pool con su propio event loop async."""
    loop = None
    tid = threading.current_thread().name
    try:
        # Crear event loop para este thread si no existe
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        result = fn(*args, **kwargs)
        return result
    finally:
        if loop is not None:
            loop.close()

_client_counter = [1]
_client_lock = threading.Lock()

def get_ib(client_id: Optional[int] = None) -> IB:
    if client_id is None:
        with _client_lock:
            client_id = _client_counter[0]
            _client_counter[0] += 1
    ib = IB()
    ib.connect(HOST, PORT, clientId=client_id, timeout=10)
    return ib


def ib_get_status() -> dict:
    ib = get_ib()
    connected = ib.isConnected()
    ib.disconnect()
    return {"connected": connected}


def ib_get_account() -> dict:
    ib = get_ib()
    accts = ib.accountSummary()
    ib.disconnect()
    data = {}
    for a in accts:
        if a.tag in ('NetLiquidation', 'CashBalance', 'BuyingPower',
                     'EquityWithLoanValue', 'FullMaintMarginReq',
                     'AvailableFunds', 'ExcessLiquidity'):
            data[f"{a.tag}_{a.currency}"] = {"value": a.value, "currency": a.currency}
    return data


def ib_get_positions() -> list:
    ib = get_ib()
    positions = ib.positions()
    pnl_map = {pn.contract: pn.unrealizedPnL for pn in ib.pnl()}
    ib.disconnect()
    if not positions:
        return []
    return [
        {"account": p.account, "contract": fmt_contract(p.contract),
         "position": p.position, "avgCost": round(p.avgCost, 4),
         "unrealizedPnl": round(pnl_map.get(p.contract), 2) if p.contract in pnl_map else None}
        for p in positions
    ]


def ib_get_pnl() -> list:
    ib = get_ib()
    pnl_list = ib.pnl()
    ib.disconnect()
    return [
        {"account": pn.account, "dailyPnL": round(pn.dailyPnL or 0, 2),
         "unrealizedPnL": round(pn.unrealizedPnL or 0, 2),
         "symbol": fmt_contract(pn.contract)["symbol"]}
        for pn in pnl_list
    ]


def ib_get_market_data(symbol: str, exchange: str = "SMART") -> dict:
    ib = get_ib()
    contract = Stock(symbol, exchange, "USD")
    ib.qualifyContracts(contract)
    ticker = ib.reqMktData(contract, "", False)
    ib.sleep(1.5)
    data = {"symbol": symbol, "bid": ticker.bid, "ask": ticker.ask,
            "last": ticker.last, "close": ticker.close, "volume": ticker.volume}
    ib.disconnect()
    return data


def ib_get_option_chain(symbol: str, expiry: str = "") -> list | list[str]:
    ib = get_ib()

    # Detectar si es índice (SPX, NDX, etc.)
    INDEXES = {"SPX", "NDX", "VIX", "RUT", "SPXW", "NDXW"}
    if symbol.upper() in INDEXES:
        base = Index(symbol.upper(), "CBOE", "USD")
    else:
        base = Stock(symbol.upper(), "SMART", "USD")

    ib.qualifyContracts(base)

    # Usar reqSecDefOptParams para obtener la cadena completa
    opt_chains = ib.reqSecDefOptParams(base.symbol, '', base.secType, base.conId)

    if not opt_chains:
        ib.disconnect()
        return []

    if expiry:
        # Strikes para un vencimiento específico
        all_contracts = []
        for chain in opt_chains:
            if expiry in chain.expirations:
                for strike in chain.strikes:
                    all_contracts.append({
                        "strike": strike,
                        "right": "C",
                        "multiplier": chain.multiplier,
                        "tradingClass": chain.tradingClass,
                    })
                    all_contracts.append({
                        "strike": strike,
                        "right": "P",
                        "multiplier": chain.multiplier,
                        "tradingClass": chain.tradingClass,
                    })
        # Dedupe y retornar
        seen = set()
        result = []
        for c in all_contracts:
            key = (c["strike"], c["right"])
            if key not in seen:
                seen.add(key)
                result.append(c)
        ib.disconnect()
        return sorted(result, key=lambda x: x["strike"])
    else:
        # Todos los vencimientos disponibles
        expirations = set()
        for chain in opt_chains:
            expirations.update(chain.expirations)
        ib.disconnect()
        return sorted(expirations)


_client_counter = [10]  # counter para clientId

def _next_client_id():
    _client_counter[0] += 1
    return _client_counter[0]


def ib_get_option_prices(symbol: str, expiry: str, right: str = "", ATM_delta: int = 10) -> list:
    """Precios bid/ask de opciones para strikes ATM +- delta."""
    ib = get_ib(client_id=_next_client_id())

    INDEXES = {"SPX", "NDX", "VIX", "RUT", "SPXW", "NDXW"}
    if symbol.upper() in INDEXES:
        base = Index(symbol.upper(), "CBOE", "USD")
    else:
        base = Stock(symbol.upper(), "SMART", "USD")

    ib.qualifyContracts(base)

    # Obtener spot price
    ticker = ib.reqMktData(base, "", False)
    ib.sleep(1)
    # Index no tiene bid/ask → usar marketPrice() o close
    spot = ticker.marketPrice() if ticker.marketPrice() == ticker.marketPrice() else None
    if not spot or spot != spot:  # NaN
        spot = ticker.close
    if not spot or spot != spot:  # NaN
        # Fallback: usar reqHistoricalData para el último close conocido
        try:
            hist = ib.reqHistoricalData(base, '', '1 D', '1 day', 'TRADES', False)
            if hist:
                spot = hist[-1].close
        except Exception:
            pass

    # Obtener strikes del expiry seleccionado
    chains = ib.reqSecDefOptParams(base.symbol, '', base.secType, base.conId)
    all_strikes = []
    trading_class = None
    for chain in chains:
        if expiry in chain.expirations:
            all_strikes.extend(chain.strikes)
            trading_class = chain.tradingClass
            break

    if not all_strikes:
        ib.disconnect()
        return [{"error": f"No strikes found for {symbol} {expiry}"}]

    all_strikes = sorted(set(all_strikes))

    # Si no tenemos spot, usar strike medio como aproximación
    if not spot or spot != spot:
        spot = all_strikes[len(all_strikes)//2]

    atm_strike = min(all_strikes, key=lambda s: abs(s - spot))

    # Filtrar strikes ATM +- delta (cada 5 puntos)
    range_strikes = [s for s in all_strikes if abs(s - atm_strike) <= ATM_delta * 5]
    range_strikes = sorted(range_strikes)

    # Obtener precios para cada strike (CALL y/o PUT)
    rights = ["C", "P"] if not right else [right.upper()]
    opt_exchange = "CBOE" if symbol.upper() in INDEXES else "SMART"
    contracts = []
    for s in range_strikes:
        for r in rights:
            # Usar Contract() directo (TWS resuelve via tradingClass+multiplier)
            c = Contract()
            c.symbol = symbol.upper()
            c.secType = 'OPT'
            c.currency = 'USD'
            c.exchange = opt_exchange
            c.lastTradeDateOrContractMonth = expiry
            c.strike = s
            c.right = r
            c.tradingClass = trading_class or symbol.upper()
            c.multiplier = '100'
            contracts.append(c)

    ib.qualifyContracts(*contracts)
    tickers = []
    for c in contracts:
        if c.conId:
            # Tick 101 = option OI, 106 = volume, 236 = IV
            t = ib.reqMktData(c, "101,106,236", False, False)
            tickers.append((c, t))
    ib.sleep(5)  # opciones tardan más en poblar bid/ask

    spot_used = spot
    result = []
    for c, t in tickers:
        greeks = t.modelGreeks if hasattr(t, 'modelGreeks') and t.modelGreeks else {}
        # OI depende del tipo: callOpenInterest para C, putOpenInterest para P
        oi = None
        if c.right == 'C' and hasattr(t, 'callOpenInterest'):
            oi = t.callOpenInterest
        elif c.right == 'P' and hasattr(t, 'putOpenInterest'):
            oi = t.putOpenInterest

        result.append({
            "strike": c.strike,
            "right": c.right,
            "conId": c.conId,
            "bid": float(t.bid) if t.bid == t.bid else None,
            "ask": float(t.ask) if t.ask == t.ask else None,
            "last": float(t.last) if t.last == t.last else None,
            "volume": int(t.volume) if t.volume == t.volume else None,
            "open_interest": int(oi) if oi and oi == oi else None,
            "iv": float(greeks.impliedVol) if hasattr(greeks, 'impliedVol') and greeks.impliedVol else None,
            "delta": float(greeks.delta) if hasattr(greeks, 'delta') and greeks.delta else None,
            "gamma": float(greeks.gamma) if hasattr(greeks, 'gamma') and greeks.gamma else None,
            "theta": float(greeks.theta) if hasattr(greeks, 'theta') and greeks.theta else None,
            "vega": float(greeks.vega) if hasattr(greeks, 'vega') and greeks.vega else None,
        })

    ib.disconnect()
    return {"spot": spot_used, "atm_strike": atm_strike, "options": result}


def fmt_contract(c):
    if isinstance(c, Option):
        return {"symbol": c.symbol, "type": "Option", "right": c.right,
                "strike": c.strike, "expiry": c.lastTradeDateOrContractMonth,
                "exchange": c.exchange, "multiplier": c.multiplier}
    elif isinstance(c, Stock):
        return {"symbol": c.symbol, "type": "Stock", "exchange": c.exchange}
    elif isinstance(c, Index):
        return {"symbol": c.symbol, "type": "Index", "exchange": c.exchange}
    return {"symbol": c.symbol, "type": type(c).__name__}


# ── Tool handlers ──────────────────────────────────────────────────────────────

TOOLS = [
    Tool(name="get_status", description="Verifica conexión IB Gateway",
         inputSchema={"type": "object", "properties": {}}),
    Tool(name="get_account", description="Balance y buying power IBKR",
         inputSchema={"type": "object", "properties": {}}),
    Tool(name="get_positions", description="Posiciones abiertas con P&L",
         inputSchema={"type": "object", "properties": {}}),
    Tool(name="get_pnl", description="P&L diario y no realizado",
         inputSchema={"type": "object", "properties": {}}),
    Tool(name="get_market_data", description="Bid/ask/last de un ticker",
         inputSchema={"type": "object", "properties": {
             "symbol": {"type": "string"},
             "exchange": {"type": "string", "default": "SMART"}},"required": ["symbol"]}),
    Tool(name="get_option_chain", description="Cadena de opciones (expirations o strikes por expiry)",
         inputSchema={"type": "object", "properties": {
             "symbol": {"type": "string"},
             "expiry": {"type": "string"}},"required": ["symbol"]}),
    Tool(name="get_option_prices", description="Precios bid/ask de opciones para strikes ATM",
         inputSchema={"type": "object", "properties": {
             "symbol": {"type": "string"},
             "expiry": {"type": "string"},
             "right": {"type": "string", "description": "C o P (opcional, todas si se omite)"},
             " ATM_delta": {"type": "number", "description": "Rango de strikes alrededor de ATM (default 10)"}},"required": ["symbol", "expiry"]}),
]


async def list_tools(ctx: ServerRequestContext, params: ListToolsRequest) -> HandlerResult:
    return ListToolsResult(tools=TOOLS)


async def call_tool(ctx: ServerRequestContext, params: CallToolRequestParams) -> HandlerResult:
    try:
        name = params.name
        arguments = params.arguments or {}
    except Exception:
        return CallToolResult(content=[TextContent(type="text", text="Invalid request")], isError=True)

    try:
        if name == "get_status":
            result = await asyncio.to_thread(_run_ib_sync, ib_get_status)
            return CallToolResult(content=[TextContent(type="text", text=json.dumps(result))])

        elif name == "get_account":
            result = await asyncio.to_thread(_run_ib_sync, ib_get_account)
            return CallToolResult(content=[TextContent(type="text", text=json.dumps(result, indent=2))])

        elif name == "get_positions":
            result = await asyncio.to_thread(_run_ib_sync, ib_get_positions)
            if not result:
                return CallToolResult(content=[TextContent(type="text", text="Sin posiciones abiertas")])
            return CallToolResult(content=[TextContent(type="text", text=json.dumps(result, indent=2))])

        elif name == "get_pnl":
            result = await asyncio.to_thread(_run_ib_sync, ib_get_pnl)
            return CallToolResult(content=[TextContent(type="text", text=json.dumps(result, indent=2))])

        elif name == "get_market_data":
            symbol = arguments.get("symbol")
            exchange = arguments.get("exchange", "SMART")
            result = await asyncio.to_thread(_run_ib_sync, ib_get_market_data, symbol, exchange)
            return CallToolResult(content=[TextContent(type="text", text=json.dumps(result, indent=2))])

        elif name == "get_option_chain":
            symbol = arguments.get("symbol")
            expiry = arguments.get("expiry", "")
            result = await asyncio.to_thread(_run_ib_sync, ib_get_option_chain, symbol, expiry)
            return CallToolResult(content=[TextContent(type="text", text=json.dumps(result, indent=2))])

        elif name == "get_option_prices":
            symbol = arguments.get("symbol")
            expiry = arguments.get("expiry")
            right = arguments.get("right", "")
            delta = arguments.get("ATM_delta", 10)
            result = await asyncio.to_thread(_run_ib_sync, ib_get_option_prices, symbol, expiry, right, delta)
            return CallToolResult(content=[TextContent(type="text", text=json.dumps(result, indent=2))])

        else:
            return CallToolResult(content=[TextContent(type="text", text=f"Unknown tool: {name}")], isError=True)

    except Exception as e:
        return CallToolResult(content=[TextContent(type="text", text=f"Error: {e}")], isError=True)


server.add_request_handler("tools/list", ListToolsRequest, list_tools)
server.add_request_handler("tools/call", CallToolRequestParams, call_tool)


# ── Health endpoint ────────────────────────────────────────────────────────────

async def health(request: Request) -> JSONResponse:
    return JSONResponse({"status": "ok", "ib_host": HOST, "ib_port": PORT})


# ── HTTP App con health route ─────────────────────────────────────────────────

token_verifier = StaticTokenVerifier(TOKEN) if TOKEN else None

# NOTA sobre auth: pasar solo `token_verifier` (sin `auth=AuthSettings`) hace que
# RequireAuthMiddleware rechace todo con 401, porque AuthenticationMiddleware nunca
# se registra. Para auth completa hay que pasar también `auth=AuthSettings(...)`
# con issuer_url + resource_server_url (OAuth flow). Por simplicidad, no usamos auth.

mcp_app = server.streamable_http_app(
    streamable_http_path="/mcp",
    stateless_http=True,
    json_response=False,
    token_verifier=None,  # sin auth — todas las requests pasan
    debug=DEBUG,
    transport_security=TransportSecuritySettings(
        enable_dns_rebinding_protection=False,
    ),
)

# Las rutas del MCP server se injectan en el mismo Starlette app
mcp_app.add_route("/health", route=health, methods=["GET"])


# ── REST endpoints (para dashboard externo) ──────────────────────────────────

def _ib_status():
    """Sincrono para usar con add_route."""
    return ib_get_status()

def _ib_account():
    return ib_get_account()

def _ib_positions():
    return ib_get_positions()

from starlette.responses import JSONResponse

async def rest_status(request: Request) -> JSONResponse:
    try:
        result = await asyncio.get_event_loop().run_in_executor(None, _run_ib_sync, _ib_status)
        return JSONResponse({**result, "time": datetime.now().isoformat()})
    except Exception as e:
        return JSONResponse({"connected": False, "error": str(e)}, status_code=503)

async def rest_account(request: Request) -> JSONResponse:
    try:
        result = await asyncio.get_event_loop().run_in_executor(None, _run_ib_sync, _ib_account)
        return JSONResponse(result)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

async def rest_positions(request: Request) -> JSONResponse:
    try:
        result = await asyncio.get_event_loop().run_in_executor(None, _run_ib_sync, _ib_positions)
        return JSONResponse(result)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

mcp_app.add_route("/api/status", route=rest_status, methods=["GET"])
mcp_app.add_route("/api/account", route=rest_account, methods=["GET"])
mcp_app.add_route("/api/positions", route=rest_positions, methods=["GET"])

app = mcp_app


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    print(f"IB Gateway MCP HTTP -> {HOST}:{PORT}, token={'OK' if TOKEN else 'NONE'}")
    uvicorn.run(app, host="0.0.0.0", port=8765, log_level="info")

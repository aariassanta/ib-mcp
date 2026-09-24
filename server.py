#!/usr/bin/env python3
"""
IB Gateway MCP Server — conexión persistente con reconnect automático.
"""
import asyncio
import json
import logging
from typing import Any

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp_types import (
    Tool, TextContent, ListToolsRequest, ListToolsResult,
    CallToolResult, CallToolRequestParams, ToolResultContent,
)

from ib_insync import IB, Option, Stock, Index

# ── Configuración ─────────────────────────────────────────────────────────────
IB_HOST = '127.0.0.1'
IB_PORT = 4002
IB_TIMEOUT = 30          # timeout para la conexión inicial
MAX_RECONNECT_ATTEMPTS = 5
RECONNECT_DELAY = 2.0    # segundos entre intentos de reconnect
# ──────────────────────────────────────────────────────────────────────────────

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger('ib-mcp')

# ── Server ─────────────────────────────────────────────────────────────────────
server = Server("ib-gateway")

# ── Connection cache (persistent connections, one per clientId) ──────────────

_connection_cache: dict[int, IB] = {}


def _ensure_connection(client_id: int = 1) -> IB:
    """
    Retorna una conexión IB lista para usar.
    - Si existe y está conectada → la reusa.
    - Si existe pero está desconectada → intenta reconnect (hasta MAX_RECONNECT_ATTEMPTS).
    - Si no existe → crea una nueva.
    """
    if client_id in _connection_cache:
        ib = _connection_cache[client_id]
        if ib.isConnected():
            return ib
        logger.info(f"Reconectando client_id={client_id} ...")
    else:
        logger.info(f"Creando nueva conexión para client_id={client_id} ...")

    ib = IB()
    for attempt in range(1, MAX_RECONNECT_ATTEMPTS + 1):
        try:
            ib.connect(IB_HOST, IB_PORT, clientId=client_id, timeout=IB_TIMEOUT)
            if ib.isConnected():
                _connection_cache[client_id] = ib
                logger.info(f"✅ Client {client_id} conectado (intento {attempt})")
                # Registrar callbacks de error
                ib._resetOnError()
                return ib
            logger.warning(f"Intento {attempt}: conexión como desconectada")
        except Exception as e:
            logger.warning(f"Intento {attempt} fallido: {e}")
        if attempt < MAX_RECONNECT_ATTEMPTS:
            asyncio.sleep(RECONNECT_DELAY)

    logger.error(f"❌ No se pudo conectar client_id={client_id} después de {MAX_RECONNECT_ATTEMPTS} intentos")
    raise ConnectionError(f"No se pudo conectar a IB Gateway en {IB_HOST}:{IB_PORT}")


def get_ib(client_id: int = 1) -> IB:
    """Alias público — reutiliza conexión o crea reconexión."""
    return _ensure_connection(client_id)


def disconnect_all():
    """Desconecta todas las conexiones al shutdown."""
    for client_id, ib in list(_connection_cache.items()):
        try:
            ib.disconnect()
            logger.info(f"Desconectado client_id={client_id}")
        except Exception as e:
            logger.warning(f"Error al desconectar client_id={client_id}: {e}")
    _connection_cache.clear()


# ── Error handler ─────────────────────────────────────────────────────────────

_pending_errors: list[dict] = []

def _on_error(reqId, errorCode, errorString, contract):
    """Callback de error de IB — accede sin bloquear."""
    _pending_errors.append({
        "reqId": reqId,
        "errorCode": errorCode,
        "errorString": errorString,
        "contract": fmt_contract(contract) if contract else None,
    })
    logger.warning(f"⚠️ IB error {errorCode}: {errorString} (reqId={reqId})")

def _on_warn(reqId, warnCode, warnString, contract):
    logger.info(f"ℹ️ IB warning {warnCode}: {warnString}")

def _on_echo(reqId, echo):
    logger.debug(f"Echo reqId={reqId}: {echo}")

def _on_server_version(version, connectionTime, secondaryBrokerConn):
    logger.info(f"IB versión: {version} · time={connectionTime}")

def _on_tick(res, tic):
    pass  # manejar si se necesita en el futuro

def _on_update_tickParams(tickerId, tickAttr, newValue):
    pass


def register_ib_callbacks(ib: IB):
    """Registra todos los callbacks de eventos de IB."""
    ib.errorEvent += _on_error
    ib.warningEvent += _on_warn
    ib.serverVersionEvent += _on_server_version
    ib.tickEvent += _on_tick
    ib.updateTickParamsEvent += _on_update_tickParams
    # Nota: echoEvent requiere Python ≥ 3.8 conectado
    try:
        ib.echoEvent += _on_echo
    except (AttributeError, TypeError):
        pass


# ── Helpers ───────────────────────────────────────────────────────────────────

def fmt_contract(c) -> dict:
    if isinstance(c, Option):
        return {
            "symbol":     c.symbol,
            "type":       "Option",
            "right":      c.right,
            "strike":     c.strike,
            "expiry":     c.lastTradeDateOrContractMonth,
            "exchange":   c.exchange,
            "multiplier": c.multiplier,
        }
    elif isinstance(c, Stock):
        return {"symbol": c.symbol, "type": "Stock", "exchange": c.exchange}
    elif isinstance(c, Index):
        return {"symbol": c.symbol, "type": "Index", "exchange": c.exchange}
    return {"symbol": c.symbol, "type": type(c).__name__}


def flush_pending_errors() -> list[dict]:
    """Retorna y limpia los errores pendientes acumulados desde el último flush."""
    errors = _pending_errors.copy()
    _pending_errors.clear()
    return errors


# ── Tool definitions ──────────────────────────────────────────────────────────

TOOLS = [
    Tool(
        name="get_status",
        description="Verifica si la conexión a IB Gateway está activa (usa conexión persistente)",
        inputSchema={"type": "object", "properties": {}},
    ),
    Tool(
        name="get_account",
        description="Obtiene balance, buying power y datos de la cuenta IBKR",
        inputSchema={"type": "object", "properties": {}},
    ),
    Tool(
        name="get_positions",
        description="Obtiene posiciones abiertas con P&L no realizado",
        inputSchema={"type": "object", "properties": {}},
    ),
    Tool(
        name="get_pnl",
        description="Obtiene P&L diario y no realizado de la cuenta",
        inputSchema={"type": "object", "properties": {}},
    ),
    Tool(
        name="get_market_data",
        description="Obtiene precio actual (bid/ask/last) de un ticker — usa conexión persistente",
        inputSchema={
            "type": "object",
            "properties": {
                "symbol":   {"type": "string", "description": "Símbolo (e.g. SPX, AAPL, NVDA)"},
                "exchange": {"type": "string", "description": "Exchange (SMART, CBOE, etc.)", "default": "SMART"},
            },
            "required": ["symbol"],
        },
    ),
    Tool(
        name="get_option_chain",
        description="Obtiene la cadena de opciones para un subyacente y fecha",
        inputSchema={
            "type": "object",
            "properties": {
                "symbol": {"type": "string", "description": "Símbolo (e.g. SPX, AAPL)"},
                "expiry": {"type": "string", "description": "Fecha vencimiento YYYYMMDD (opcional, todos si se omite)"},
            },
            "required": ["symbol"],
        },
    ),
]


# ── Request handlers ──────────────────────────────────────────────────────────

async def list_tools_handler(
    context,
    params: ListToolsRequest,
) -> ListToolsResult:
    return ListToolsResult(tools=TOOLS)


async def call_tool_handler(
    context,
    params: CallToolRequestParams,
) -> CallToolResult:
    try:
        name = params.name
        arguments = params.arguments or {}
    except Exception as e:
        return CallToolResult(content=[TextContent(type="text", text=f"Invalid request format: {type(e).__name__}: {e}")], is_error=True)

    errors_before = len(_pending_errors)

    try:
        if name == "get_status":
            ib = get_ib()
            connected = ib.isConnected()
            if connected:
                # Forzar reconexión no necesaria — ya está conectado
                return CallToolResult(content=[
                    TextContent(type="text", text=json.dumps({
                        "connected": True,
                        "pendingErrors": len(_pending_errors) - errors_before,
                    }))
                ])
            else:
                return CallToolResult(content=[
                    TextContent(type="text", text=json.dumps({
                        "connected": False,
                        "pendingErrors": len(_pending_errors) - errors_before,
                    }))
                ])

        elif name == "get_account":
            ib = get_ib()
            wanted = {'NetLiquidation', 'CashBalance', 'BuyingPower',
                      'EquityWithLoanValue', 'FullMaintMarginReq',
                      'AvailableFunds', 'ExcessLiquidity'}
            data = {}
            for v in ib.accountValues():
                if v.tag in wanted:
                    data[f"{v.tag}_{v.currency}"] = {"value": v.value, "currency": v.currency}
            if not data:
                raise ConnectionError("accountValues empty — connection too fresh, retry once")
            return CallToolResult(content=[
                TextContent(type="text", text=json.dumps(data, indent=2))
            ])

        elif name == "get_positions":
            ib = get_ib()
            positions = ib.positions()
            pnl_map = {pn.contract: pn.unrealizedPnL for pn in ib.pnl()}
            if not positions:
                return CallToolResult(content=["Sin posiciones abiertas"])

            result = [
                {
                    "account":         p.account,
                    "contract":        fmt_contract(p.contract),
                    "position":       p.position,
                    "avgCost":        round(p.avgCost, 4),
                    "unrealizedPnl":  round(pnl_map.get(p.contract), 2)
                                      if p.contract in pnl_map else None,
                }
                for p in positions
            ]
            return CallToolResult(content=[
                TextContent(type="text", text=json.dumps(result, indent=2))
            ])

        elif name == "get_pnl":
            ib = get_ib()
            pnl_list = ib.pnl()
            result = [
                {
                    "account":       pn.account,
                    "dailyPnL":     round(pn.dailyPnL, 2) if pn.dailyPnL else 0,
                    "unrealizedPnL": round(pn.unrealizedPnL, 2) if pn.unrealizedPnL else 0,
                    "symbol":        fmt_contract(pn.contract)["symbol"],
                }
                for pn in pnl_list
            ]
            return CallToolResult(content=[
                TextContent(type="text", text=json.dumps(result, indent=2))
            ])

        elif name == "get_market_data":
            symbol   = arguments.get("symbol")
            exchange = arguments.get("exchange", "SMART")

            ib = get_ib()
            contract = Stock(symbol, exchange, "USD")
            ib.qualifyContracts(contract)
            ticker = ib.reqMktData(contract, "", False, False)
            ib.sleep(1.5)

            data = {
                "symbol": symbol,
                "bid":    ticker.bid,
                "ask":    ticker.ask,
                "last":   ticker.last,
                "close":  ticker.close,
                "volume": ticker.volume,
                "hasPendingErrors": len(_pending_errors) > errors_before,
            }
            return CallToolResult(content=[
                TextContent(type="text", text=json.dumps(data, indent=2))
            ])

        elif name == "get_option_chain":
            symbol = arguments.get("symbol")
            expiry  = arguments.get("expiry", "")

            ib = get_ib()

            if expiry:
                contracts = ib.reqContractDetails(
                    Option(symbol, expiry, 0, "", "100", "USD")
                )
                chain = [
                    {
                        "strike":    c.contract.strike,
                        "right":     c.contract.right,
                        "multiplier": c.contract.multiplier,
                    }
                    for c in contracts
                ]
                return CallToolResult(content=[
                    TextContent(type="text", text=json.dumps(chain, indent=2))
                ])
            else:
                all_cds = ib.reqContractDetails(Stock(symbol, "SMART", "USD"))
                expirations = sorted(set(
                    cd.contract.lastTradeDateOrContractMonth
                    for cd in all_cds
                    if isinstance(cd.contract, Option) and cd.contract.strike == 0
                ))
                return CallToolResult(content=[
                    TextContent(type="text", text=json.dumps(expirations, indent=2))
                ])

        else:
            return CallToolResult(content=["Unknown tool: " + name], isError=True)

    except ConnectionError as e:
        return CallToolResult(
            content=[TextContent(type="text", text=f"❌ Conexión perdida: {e}")],
            isError=True,
        )
    except Exception as e:
        logger.exception(f"Tool {name} falló")
        return CallToolResult(
            content=[TextContent(type="text", text=f"Error: {type(e).__name__}: {e}")],
            isError=True,
        )


# ── Register handlers ─────────────────────────────────────────────────────────

server.add_request_handler("tools/list", ListToolsRequest, list_tools_handler)
server.add_request_handler("tools/call", CallToolRequestParams, call_tool_handler)


# ── Main ──────────────────────────────────────────────────────────────────────

async def main():
    # Inicializar conexión al inicio (solo para clientId=1, el predeterminado)
    try:
        ib_init = _ensure_connection(1)
        # Register callbacks sobre la conexión inicial
        register_ib_callbacks(ib_init)
        logger.info("✅ MCP server listo — conexión IB inicial establecida")
    except ConnectionError as e:
        logger.error(f"⚠️ No se pudo establecer conexión inicial: {e}")
        logger.warning("El server arrancará pero los tools fallarán hasta que se reconecte")

    async with stdio_server() as (read_stream, write_stream):
        try:
            await server.run(
                read_stream,
                write_stream,
                server.create_initialization_options(),
            )
        finally:
            disconnect_all()
            logger.info("MCP server shutdown — todas las conexiones cerradas")


if __name__ == "__main__":
    asyncio.run(main())

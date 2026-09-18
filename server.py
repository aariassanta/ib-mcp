#!/usr/bin/env python3
"""
IB Gateway MCP Server - stateless, one connection per tool call.
"""
import asyncio
import json
from typing import Any

from mcp.server import Server
from mcp.types import (
    Tool, TextContent, ListToolsRequest, CallToolRequest,
    ListToolsResult, CallToolResult, ToolResultContent,
)
from mcp.server.stdio import stdio_server

from ib_insync import IB, Option, Stock, Index

# ── Server ───────────────────────────────────────────────────────────────────

server = Server("ib-gateway")

# ── IB helpers ──────────────────────────────────────────────────────────────

def get_ib(client_id: int = 1) -> IB:
    ib = IB()
    ib.connect('127.0.0.1', 4002, clientId=client_id, timeout=10)
    return ib


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


# ── Tool definitions ────────────────────────────────────────────────────────

TOOLS = [
    Tool(
        name="get_status",
        description="Verifica si la conexión a IB Gateway está activa",
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
        name="get_market_data",
        description="Obtiene precio actual (bid/ask/last) de un ticker",
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
    Tool(
        name="get_pnl",
        description="Obtiene P&L diario y no realizado de la cuenta",
        inputSchema={"type": "object", "properties": {}},
    ),
]

# ── Request handlers ────────────────────────────────────────────────────────

async def list_tools_handler(
    request: ListToolsRequest,
    context=None,
) -> ListToolsResult:
    return ListToolsResult(tools=TOOLS)


async def call_tool_handler(
    request: CallToolRequest,
    context=None,
) -> CallToolResult:
    # El request ya viene validado por CallToolRequest
    try:
        name = request.params.name
        arguments = request.params.arguments or {}
    except Exception:
        # Fallback si la validación parseó de forma diferente
        return CallToolResult(content=[TextContent(type="text", text="Invalid request format")], isError=True)

    try:
        if name == "get_status":
            ib = get_ib()
            connected = ib.isConnected()
            ib.disconnect()
            return CallToolResult(content=[TextContent(type="text", text=json.dumps({"connected": connected}))])

        elif name == "get_account":
            ib = get_ib()
            accts = ib.accountSummary()
            ib.disconnect()
            data = {}
            for a in accts:
                if a.tag in ('NetLiquidation', 'CashBalance', 'BuyingPower',
                             'EquityWithLoanValue', 'FullMaintMarginReq',
                             'AvailableFunds', 'ExcessLiquidity'):
                    key = f"{a.tag}_{a.currency}"
                    data[key] = {"value": a.value, "currency": a.currency}
            return CallToolResult(content=[TextContent(type="text", text=json.dumps(data, indent=2))])

        elif name == "get_positions":
            ib = get_ib()
            positions = ib.positions()
            pnl_map = {pn.contract: pn.unrealizedPnL for pn in ib.pnl()}
            ib.disconnect()

            if not positions:
                return CallToolResult(content=[TextContent(type="text", text="Sin posiciones abiertas")])

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
            return CallToolResult(content=[TextContent(type="text", text=json.dumps(result, indent=2))])

        elif name == "get_pnl":
            ib = get_ib()
            pnl_list = ib.pnl()
            ib.disconnect()
            result = [
                {
                    "account":       pn.account,
                    "dailyPnL":     round(pn.dailyPnL, 2) if pn.dailyPnL else 0,
                    "unrealizedPnL": round(pn.unrealizedPnL, 2) if pn.unrealizedPnL else 0,
                    "symbol":        fmt_contract(pn.contract)["symbol"],
                }
                for pn in pnl_list
            ]
            return CallToolResult(content=[TextContent(type="text", text=json.dumps(result, indent=2))])

        elif name == "get_market_data":
            symbol   = arguments.get("symbol")
            exchange = arguments.get("exchange", "SMART")

            ib = get_ib()
            contract = Stock(symbol, exchange, "USD")
            ib.qualifyContracts(contract)
            ticker = ib.reqMktData(contract, "", False)
            ib.sleep(1.5)

            data = {
                "symbol": symbol,
                "bid":    ticker.bid,
                "ask":    ticker.ask,
                "last":   ticker.last,
                "close":  ticker.close,
                "volume": ticker.volume,
            }
            ib.disconnect()
            return CallToolResult(content=[TextContent(type="text", text=json.dumps(data, indent=2))])

        elif name == "get_option_chain":
            symbol = arguments.get("symbol")
            expiry  = arguments.get("expiry", "")

            ib = get_ib()

            if expiry:
                # Cadena para un vencimiento específico
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
                ib.disconnect()
                return CallToolResult(content=[TextContent(type="text", text=json.dumps(chain, indent=2))])
            else:
                # Lista todos los vencimientos disponibles
                all_cds = ib.reqContractDetails(Stock(symbol, "SMART", "USD"))
                expirations = sorted(set(
                    cd.contract.lastTradeDateOrContractMonth
                    for cd in all_cds
                    if isinstance(cd.contract, Option) and cd.contract.strike == 0
                ))
                ib.disconnect()
                return CallToolResult(content=[TextContent(type="text", text=json.dumps(expirations, indent=2))])

        else:
            return CallToolResult(content=[TextContent(type="text", text=f"Unknown tool: {name}")], isError=True)

    except Exception as e:
        return CallToolResult(
            content=[TextContent(type="text", text=f"Error: {e}")],
            isError=True,
        )


# ── Register handlers ───────────────────────────────────────────────────────

server.add_request_handler("tools/list", ListToolsRequest, list_tools_handler)
server.add_request_handler("tools/call", CallToolRequest, call_tool_handler)


# ── Main ───────────────────────────────────────────────────────────────────

async def main():
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options(),
        )


if __name__ == "__main__":
    asyncio.run(main())

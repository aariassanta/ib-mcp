#!/usr/bin/env python3
"""
IB Gateway MCP Server - HTTP Streamable transport.
Uso: IB_MCP_TOKEN=mi-token IB_HOST=127.0.0.1 IB_PORT=4002 python server_http2.py
"""
import os
import sys
import json
import logging
import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Optional
import threading

# ── Timeout para llamadas IB ──────────────────────────────────────────────────
def _ib_call(fn, timeout=8, *args, **kwargs):
    """Ejecuta fn en thread separado con timeout y event loop ib_insync."""
    result = [None]
    exc = [None]
    def target():
        try:
            # ib_insync requiere event loop por thread
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                result[0] = fn(*args, **kwargs)
            finally:
                loop.close()
        except Exception as e:
            exc[0] = e
    t = threading.Thread(target=target)
    t.daemon = True
    t.start()
    t.join(timeout=timeout)
    if t.is_alive():
        raise TimeoutError(f"IB call timed out after {timeout}s")
    if exc[0]:
        raise exc[0]
    return result[0]

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
)
logger = logging.getLogger('ib-mcp')

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

from ib_insync import IB, Option, Stock, Index, Contract, Order

# ── Shared combo code from eval/lib ──────────────────────────────────────────
sys.path.insert(0, '/root/eval/lib')
from spxw_bag_bracket import place_spxw_bag_bracket
from conid_cache import get_conid, put_conid

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

# ── Thread-local persistent connection ──────────────────────────────────────
# Cada thread del pool mantiene su propia conexión IB persistente.
# La conexión se crea al primer uso y se reaprovecha en tool calls posteriores.
# Si la conexión se cae, se reconecta automáticamente.

_thread_local = threading.local()

_client_counter = [10]  # counter para clientId
_client_lock = threading.Lock()


def _next_client_id():
    _client_counter[0] += 1
    return _client_counter[0]


def _get_thread_ib() -> IB:
    """
    Retorna la conexión IB persistente para el thread actual.
    - Si existe y está conectada → la reusa.
    - Si existe pero está desconectada → reconecta con el mismo clientId.
    - Si no existe → crea nueva conexión y registra callbacks.
    """
    ib = getattr(_thread_local, 'ib', None)
    client_id = getattr(_thread_local, 'client_id', None)

    if ib is not None and ib.isConnected():
        return ib

    # Reconectar o crear nueva
    if ib is not None:
        logger.info(f"Reconectando thread ib (clientId={client_id})...")
        try:
            ib.disconnect()
        except Exception:
            pass

    # Nuevo clientId si no tenemos uno
    if client_id is None:
        with _client_lock:
            client_id = _client_counter[0]
            _client_counter[0] += 1

    ib = IB()
    ib.connect(HOST, PORT, clientId=client_id, timeout=30)
    _thread_local.ib = ib
    _thread_local.client_id = client_id

    # Registrar callback de errores
    ib.errorEvent += _on_ib_error

    logger.info(f"✅ Thread ib conectado (clientId={client_id})")
    return ib


def _reset_thread_ib():
    """Forzar desconexión para el thread actual (próximo uso reconnectará)."""
    ib = getattr(_thread_local, 'ib', None)
    if ib is not None:
        try:
            ib.disconnect()
        except Exception:
            pass
        _thread_local.ib = None
        _thread_local.client_id = None


def _on_ib_error(reqId, errorCode, errorString, contract):
    logger.warning(f"⚠️ IB error {errorCode}: {errorString} (reqId={reqId})")


def get_ib(client_id: Optional[int] = None) -> IB:
    """
    Legacy: retorna conexión persistente para el thread actual.
    El parámetro client_id se ignora — cada thread tiene su propia conexión.
    """
    return _get_thread_ib()


def ib_get_status() -> dict:
    ib = get_ib()
    connected = ib.isConnected()
    return {"connected": connected}


def ib_get_account() -> dict:
    """Snapshot de valores clave de cuenta — usa ``ib.accountValues()`` que ya viene
    pre-poblado tras la conexión. Si está vacío (conexión muy reciente) lanza
    ``ConnectionError`` para que el caller haga fallback."""
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
    return data


def ib_get_positions() -> list:
    ib = get_ib()
    positions = ib.positions()
    pnl_map = {pn.contract: pn.unrealizedPnL for pn in ib.pnl()}
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
    return [
        {"account": pn.account, "dailyPnL": round(pn.dailyPnL or 0, 2),
         "unrealizedPnl": round(pn.unrealizedPnL or 0, 2),
         "symbol": fmt_contract(pn.contract)["symbol"]}
        for pn in pnl_list
    ]


def ib_get_market_data(symbol: str, exchange: str = "SMART") -> dict:
    """Snapshot de bid/ask/last/close. Streaming + wait corto; el ticker queda vivo
    para reuso pero se cancela explícitamente al final para evitar acumulación."""
    ib = get_ib()
    contract = Stock(symbol, exchange, "USD")
    ib.qualifyContracts(contract)
    ticker = ib.reqMktData(contract, "", False)
    # Esperar hasta 3s para que llegue un tick con datos reales (no NaN)
    for _ in range(30):
        if (ticker.bid is not None and not _is_nan(ticker.bid)) or \
           (ticker.last is not None and not _is_nan(ticker.last)):
            break
        ib.sleep(0.1)
    data = {"symbol": symbol, "bid": _clean(ticker.bid), "ask": _clean(ticker.ask),
            "last": _clean(ticker.last), "close": _clean(ticker.close),
            "volume": _clean(ticker.volume)}
    return data


def _is_nan(x):
    return x is None or (isinstance(x, float) and x != x)


def _clean(x):
    return None if _is_nan(x) else x


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
        return sorted(result, key=lambda x: x["strike"])
    else:
        # Todos los vencimientos disponibles
        expirations = set()
        for chain in opt_chains:
            expirations.update(chain.expirations)
        return sorted(expirations)


def ib_get_option_prices(symbol: str, expiry: str, right: str = "", ATM_delta: int = 10) -> dict:
    """Precios bid/ask de opciones para strikes ATM +- delta."""
    ib = get_ib()

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
        return {"error": f"No strikes found for {symbol} {expiry}"}

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

    return {"spot": spot_used, "atm_strike": atm_strike, "options": result}


def ib_get_metrics_underlying(symbol: str, exchange: str = "SMART") -> dict:
    """Métricas del subyacente: spot, spread, HV (vol histórica), 52w high/low.

    Vol implícita (IV) viene por ``get_option_prices`` / ``get_metrics_chain``.
    Aquí se calcula ``HV`` (Close-to-Close log-returns) sobre 20 y 50 días
    usando ``reqHistoricalData``. Útil para IV Rank cuando se compara contra HV.

    Args:
        symbol: ticker o índice (SPY, NVDA, SPX, ...).
        exchange: defaults SMART; CBOE para índices.
    """
    import math
    from ib_insync import Stock, Index

    ib = get_ib()
    sym = symbol.upper()
    INDEXES = {"SPX", "NDX", "VIX", "RUT", "SPXW", "NDXW"}
    if sym in INDEXES:
        base = Index(sym, "CBOE", "USD")
    else:
        base = Stock(sym, exchange, "USD")
    ib.qualifyContracts(base)
    if not base.conId:
        return {"error": f"No se pudo resolver contrato para {sym}"}

    # 1) Snapshot spot / bid / ask
    ticker = ib.reqMktData(base, "", False)
    for _ in range(20):
        if ticker.bid is not None and not _is_nan(ticker.bid):
            break
        ib.sleep(0.1)
    bid = _clean(ticker.bid)
    ask = _clean(ticker.ask)
    last = _clean(ticker.last)
    close = _clean(ticker.close)
    spot = ticker.marketPrice() if ticker.marketPrice() == ticker.marketPrice() else None
    if not spot or spot != spot:
        spot = last or close
    try:
        ib.cancelMktData(base)
    except Exception:
        pass

    bid_ask_spread = round((ask - bid), 4) if (bid is not None and ask is not None) else None
    spread_pct = round((bid_ask_spread / spot) * 100, 3) if (spot and bid_ask_spread is not None) else None

    # 2) Histórico 1 año para HV + 52w high/low
    bars = []
    try:
        bars = ib.reqHistoricalData(base, '', '1 Y', '1 day', 'TRADES', False)
    except Exception as e:
        return {
            "symbol": sym,
            "spot": spot, "bid": bid, "ask": ask, "last": last,
            "spread": bid_ask_spread, "spread_pct": spread_pct,
            "error": f"historical data failed: {e}",
        }

    closes = [b.close for b in bars if b.close == b.close]  # filtra NaN
    if len(closes) < 21:
        return {
            "symbol": sym, "spot": spot, "bid": bid, "ask": ask, "last": last,
            "spread": bid_ask_spread, "spread_pct": spread_pct,
            "bars_count": len(closes),
            "warning": "pocos históricos para HV — <21 barras",
        }

    def hv(window):
        if len(closes) < window + 1:
            return None
        recent = closes[-window - 1:]
        rets = [math.log(recent[i] / recent[i - 1]) for i in range(1, len(recent))]
        mean = sum(rets) / len(rets)
        var = sum((r - mean) ** 2 for r in rets) / (len(rets) - 1)
        return round(math.sqrt(var) * math.sqrt(252) * 100, 2)  # anualizada, %

    hv20 = hv(20)
    hv50 = hv(50)

    hi_52w = round(max(closes), 2)
    lo_52w = round(min(closes), 2)
    last_close = round(closes[-1], 2) if closes else None
    pct_off_high = round(((last_close - hi_52w) / hi_52w) * 100, 2) if last_close else None
    pct_off_low = round(((last_close - lo_52w) / lo_52w) * 100, 2) if last_close else None

    return {
        "symbol": sym,
        "spot": round(spot, 2) if spot else None,
        "bid": bid, "ask": ask, "last": last,
        "spread": bid_ask_spread,
        "spread_pct": spread_pct,  # % del spot
        "hv20_pct": hv20,          # vol histórica 20d anualizada
        "hv50_pct": hv50,          # vol histórica 50d anualizada
        "close_52w_high": hi_52w,
        "close_52w_low": lo_52w,
        "last_close": last_close,
        "pct_off_52w_high": pct_off_high,
        "pct_off_52w_low": pct_off_low,
        "bars_used": len(closes),
    }


def ib_get_metrics_spread(symbol: str, expiry: str, right: str,
                          short_strike: float, long_strike: float) -> dict:
    """Métricas de un spread 2-leg (PCS o CCS) ya colocado: credit, max loss,
    breakeven, IV/delta agregados, spread% de cada leg.

    Args:
        symbol: subyacente (SPX, SPY, NVDA, ...).
        expiry: YYYYMMDD.
        right: 'P' (put credit spread) o 'C' (call credit spread).
        short_strike: strike vendido (más cercano al spot).
        long_strike: strike comprado (protección, más lejos del spot).
    """
    ib = get_ib()
    sym = symbol.upper()
    r = right.upper()
    if r not in ("C", "P"):
        return {"error": "right debe ser 'C' o 'P'"}

    INDEXES = {"SPX", "NDX", "VIX", "RUT", "SPXW", "NDXW"}
    if sym in INDEXES:
        base = Index(sym, "CBOE", "USD")
        opt_exchange = "CBOE"
    else:
        base = Stock(sym, "SMART", "USD")
        opt_exchange = "SMART"
    ib.qualifyContracts(base)
    if not base.conId:
        return {"error": f"No se pudo resolver contrato para {sym}"}

    # Trading class del chain
    chains = ib.reqSecDefOptParams(base.symbol, '', base.secType, base.conId)
    trading_class = None
    for chain in chains:
        if expiry in chain.expirations:
            trading_class = chain.tradingClass
            break

    def make_leg(strike):
        c = Contract()
        c.symbol = sym
        c.secType = 'OPT'
        c.currency = 'USD'
        c.exchange = opt_exchange
        c.lastTradeDateOrContractMonth = expiry
        c.strike = strike
        c.right = r
        c.tradingClass = trading_class or sym
        c.multiplier = '100'
        return c

    short_c = make_leg(short_strike)
    long_c = make_leg(long_strike)
    ib.qualifyContracts(short_c, long_c)
    if not short_c.conId or not long_c.conId:
        return {"error": "no se pudo resolver conId para los strikes dados"}

    short_t = ib.reqMktData(short_c, "101,106,236", False, False)
    long_t = ib.reqMktData(long_c, "101,106,236", False, False)
    for _ in range(30):
        ready = (short_t.bid is not None and short_t.bid == short_t.bid and
                 long_t.bid is not None and long_t.bid == long_t.bid)
        if ready:
            break
        ib.sleep(0.2)

    def leg_data(c, t):
        bid = _clean(t.bid)
        ask = _clean(t.ask)
        last = _clean(t.last)
        mid = round((bid + ask) / 2, 2) if (bid is not None and ask is not None) else None
        spread = round((ask - bid), 2) if (bid is not None and ask is not None) else None
        oi = None
        if c.right == 'C' and hasattr(t, 'callOpenInterest'):
            oi = t.callOpenInterest
        elif c.right == 'P' and hasattr(t, 'putOpenInterest'):
            oi = t.putOpenInterest
        greeks = t.modelGreeks if (hasattr(t, 'modelGreeks') and t.modelGreeks) else None
        return {
            "strike": c.strike,
            "conId": c.conId,
            "bid": bid, "ask": ask, "last": last, "mid": mid,
            "spread": spread,
            "spread_pct": round((spread / mid) * 100, 2) if (spread and mid) else None,
            "volume": int(t.volume) if t.volume == t.volume else None,
            "open_interest": int(oi) if oi and oi == oi else None,
            "iv": float(greeks.impliedVol) if greeks and getattr(greeks, 'impliedVol', None) else None,
            "delta": float(greeks.delta) if greeks and getattr(greeks, 'delta', None) else None,
            "gamma": float(greeks.gamma) if greeks and getattr(greeks, 'gamma', None) else None,
            "theta": float(greeks.theta) if greeks and getattr(greeks, 'theta', None) else None,
            "vega": float(greeks.vega) if greeks and getattr(greeks, 'vega', None) else None,
        }

    short = leg_data(short_c, short_t)
    long_ = leg_data(long_c, long_t)

    try:
        ib.cancelMktData(short_c)
        ib.cancelMktData(long_c)
    except Exception:
        pass

    # Cálculos de spread (asumimos CREDIT spread — short más caro que long)
    width = abs(long_strike - short_strike)
    if r == "P":
        # PCS: short_strike > long_strike
        net_credit = round(short["bid"] - long_["ask"], 2) if (short["bid"] is not None and long_["ask"] is not None) else None
    else:
        # CCS: short_strike < long_strike
        net_credit = round(short["bid"] - long_["ask"], 2) if (short["bid"] is not None and long_["ask"] is not None) else None

    if net_credit is not None and width > 0:
        max_loss = round(width * 100 - net_credit * 100, 2)
        breakeven = round(short_strike - net_credit, 2) if r == "P" else round(short_strike + net_credit, 2)
        risk_reward = round(net_credit / (width - net_credit), 3) if width > net_credit else None
    else:
        max_loss = None
        breakeven = None
        risk_reward = None

    # Net debit (lo que pagas para CERRAR ahora): comprar short, vender long
    net_debit = round(long_["bid"] - short["ask"], 2) if (long_["bid"] is not None and short["ask"] is not None) else None
    current_pnl_per_contract = None
    if net_credit is not None and net_debit is not None:
        current_pnl_per_contract = round((net_credit - net_debit) * 100, 2)

    return {
        "symbol": sym, "expiry": expiry, "right": r,
        "short_leg": short,
        "long_leg": long_,
        "width": width,
        "net_credit": net_credit,        # credit recibido al abrir (por contrato, en $)
        "net_debit": net_debit,          # coste de cerrar ahora
        "max_loss": max_loss,            # máx pérdida al expiry (en $)
        "breakeven": breakeven,
        "risk_reward": risk_reward,      # credit / (width - credit), >0.3 ideal
        "current_pnl_per_contract": current_pnl_per_contract,
        "iv_short": short["iv"],
        "iv_long": long_["iv"],
        "delta_short": short["delta"],
        "delta_long": long_["delta"],
        "net_delta": round((short["delta"] or 0) - (long_["delta"] or 0), 3) if (short["delta"] is not None and long_["delta"] is not None) else None,
    }


def ib_get_metrics_chain(symbol: str, exchange: str = "SMART",
                         expiry: str = "", lookback_days: int = 252) -> dict:
    """Resumen agregado de la cadena ATM: IV atm, IV ±5/10, OI/vol ATM, **IV Rank**.

    IV Rank se calcula como ``(IV_atm_actual - IV_atm_min_252d) / (IV_atm_max_252d - IV_atm_min_252d) * 100``.
    IBKR no expone IV histórica; la aproximación estándar es usar **HV20 rolling** como
    proxy de IV histórica (el mercado asume que IV ≃ HV en el largo plazo). Se devuelve
    tanto ``iv_rank_hv_proxy`` (basado en HV20) como ``iv_current`` (IV actual del ATM).

    Args:
        symbol: subyacente (SPY, NVDA, ...).
        expiry: YYYYMMDD; si vacío, usa el expiry mensual más cercano.
        lookback_days: ventana para HV rolling (default 252 = 1 año).
    """
    import math

    ib = get_ib()
    sym = symbol.upper()
    INDEXES = {"SPX", "NDX", "VIX", "RUT", "SPXW", "NDXW"}
    if sym in INDEXES:
        base = Index(sym, "CBOE", "USD")
        opt_exchange = "CBOE"
    else:
        base = Stock(sym, exchange, "USD")
        opt_exchange = "SMART"
    ib.qualifyContracts(base)
    if not base.conId:
        return {"error": f"No se pudo resolver contrato para {sym}"}

    # Expiries
    chains = ib.reqSecDefOptParams(base.symbol, '', base.secType, base.conId)
    if not chains:
        return {"error": f"No chain para {sym}"}
    all_expiries = sorted({e for c in chains for e in c.expirations})
    if not all_expiries:
        return {"error": "no hay expiries"}
    if not expiry:
        # mensual más cercano: el primero con formato YYYYMM... pero IB devuelve YYYYMMDD o YYYYMM
        # Filtrar los que tienen 6+ dígitos y tomar el primero
        expiry = all_expiries[0] if len(all_expiries[0]) >= 6 else (all_expiries[0] + "01" if all_expiries else "")

    # Strikes del expiry elegido
    chain = next((c for c in chains if expiry in c.expirations), None)
    if not chain:
        return {"error": f"expiry {expiry} no encontrado en el chain"}
    strikes = sorted(set(chain.strikes))
    trading_class = chain.tradingClass

    # Spot
    spot_t = ib.reqMktData(base, "", False)
    for _ in range(15):
        if spot_t.last is not None and not _is_nan(spot_t.last):
            break
        ib.sleep(0.1)
    spot = spot_t.marketPrice() if spot_t.marketPrice() == spot_t.marketPrice() else None
    if not spot or spot != spot:
        spot = _clean(spot_t.close) or _clean(spot_t.last)
    try:
        ib.cancelMktData(base)
    except Exception:
        pass
    if not spot or spot != spot:
        # Fallback histórico
        try:
            hist = ib.reqHistoricalData(base, '', '1 D', '1 day', 'TRADES', False)
            spot = hist[-1].close if hist else None
        except Exception:
            return {"error": "no se pudo obtener spot"}

    atm_strike = min(strikes, key=lambda s: abs(s - spot))
    # Solo ATM ±1 strike para mantenerlo ligero; ±10 ya está cubierto por get_option_prices
    strikes_near = [s for s in strikes if abs(s - atm_strike) <= 1]

    # Subscribir ATM call + put para IV actual
    def make(strike, right):
        c = Contract()
        c.symbol = sym; c.secType = 'OPT'; c.currency = 'USD'
        c.exchange = opt_exchange; c.lastTradeDateOrContractMonth = expiry
        c.strike = strike; c.right = right
        c.tradingClass = trading_class or sym; c.multiplier = '100'
        return c

    atm_call = make(atm_strike, "C")
    atm_put = make(atm_strike, "P")
    ib.qualifyContracts(atm_call, atm_put)

    call_t = ib.reqMktData(atm_call, "101,106,236", False, False)
    put_t = ib.reqMktData(atm_put, "101,106,236", False, False)
    for _ in range(25):
        ok = False
        for t in (call_t, put_t):
            if hasattr(t, 'modelGreeks') and t.modelGreeks and t.modelGreeks.impliedVol:
                ok = True
        if ok:
            break
        ib.sleep(0.2)

    def g(t):
        if hasattr(t, 'modelGreeks') and t.modelGreeks:
            return t.modelGreeks
        return None

    cg = g(call_t); pg = g(put_t)
    iv_atm_call = cg.impliedVol if cg else None
    iv_atm_put = pg.impliedVol if pg else None
    iv_atm_avg = None
    if iv_atm_call and iv_atm_put:
        iv_atm_avg = round((iv_atm_call + iv_atm_put) / 2, 4)
    elif iv_atm_call:
        iv_atm_avg = round(iv_atm_call, 4)
    elif iv_atm_put:
        iv_atm_avg = round(iv_atm_put, 4)

    # OI / vol ATM
    oi_call = call_t.callOpenInterest if hasattr(call_t, 'callOpenInterest') else None
    oi_put = put_t.putOpenInterest if hasattr(put_t, 'putOpenInterest') else None
    vol_call = call_t.volume if hasattr(call_t, 'volume') else None
    vol_put = put_t.volume if hasattr(put_t, 'volume') else None

    # Total OI/vol ±10
    near_contracts = []
    for s in strikes_near:
        for r in ("C", "P"):
            near_contracts.append(make(s, r))
    ib.qualifyContracts(*near_contracts)
    near_tickers = []
    for c in near_contracts:
        if c.conId:
            nt = ib.reqMktData(c, "101,106,236", False, False)
            near_tickers.append((c, nt))
    ib.sleep(2)

    total_oi_near = 0; oi_near_count = 0
    total_vol_near = 0; vol_near_count = 0
    for c, t in near_tickers:
        oi = (t.callOpenInterest if c.right == 'C' else t.putOpenInterest) if hasattr(t, 'callOpenInterest') else None
        if oi and oi == oi:
            total_oi_near += oi; oi_near_count += 1
        if t.volume and t.volume == t.volume:
            total_vol_near += t.volume; vol_near_count += 1
        try:
            ib.cancelMktData(c)
        except Exception:
            pass
    try:
        ib.cancelMktData(atm_call); ib.cancelMktData(atm_put)
    except Exception:
        pass

    # Históricos para IV Rank (HV20 rolling como proxy)
    bars = []
    try:
        bars = ib.reqHistoricalData(base, '', f'{lookback_days} D', '1 day', 'TRADES', False)
    except Exception:
        pass
    closes = [b.close for b in bars if b.close == b.close]

    hv_series = []
    if len(closes) >= 21:
        for i in range(20, len(closes)):
            window = closes[i - 20:i + 1]
            rets = [math.log(window[j] / window[j - 1]) for j in range(1, len(window))]
            mean = sum(rets) / len(rets)
            var = sum((r - mean) ** 2 for r in rets) / (len(rets) - 1)
            hv_series.append(math.sqrt(var) * math.sqrt(252))

    hv20_current = round(hv_series[-1] * 100, 2) if hv_series else None
    hv_min = round(min(hv_series) * 100, 2) if hv_series else None
    hv_max = round(max(hv_series) * 100, 2) if hv_series else None

    # IV Rank: si no tenemos IV actual, no se puede calcular
    iv_rank = None
    iv_pct = None
    if iv_atm_avg and hv_min is not None and hv_max and hv_max > hv_min:
        # usamos HV como proxy de "rango IV histórico"; IV actual en %
        iv_current_pct = iv_atm_avg * 100
        iv_rank = round((iv_current_pct - hv_min) / (hv_max - hv_min) * 100, 1)
        iv_pct = iv_current_pct

    return {
        "symbol": sym,
        "expiry": expiry,
        "spot": round(spot, 2) if spot else None,
        "atm_strike": atm_strike,
        "strikes_in_range_pm1": len(strikes_near),
        "iv_atm_call": round(iv_atm_call, 4) if iv_atm_call else None,
        "iv_atm_put": round(iv_atm_put, 4) if iv_atm_put else None,
        "iv_atm_avg": iv_atm_avg,
        "iv_current_pct": iv_pct,
        "oi_call_atm": int(oi_call) if oi_call and oi_call == oi_call else None,
        "oi_put_atm": int(oi_put) if oi_put and oi_put == oi_put else None,
        "vol_call_atm": int(vol_call) if vol_call and vol_call == vol_call else None,
        "vol_put_atm": int(vol_put) if vol_put and vol_put == vol_put else None,
        "total_oi_pm1": total_oi_near,
        "total_vol_pm1": int(total_vol_near),
        "hv20_current_pct": hv20_current,
        "hv_min_252d_pct": hv_min,
        "hv_max_252d_pct": hv_max,
        "iv_rank_hv_proxy": iv_rank,   # 0-100 (puede exceder 100 si IV > HV max histórico)
        "iv_rank_capped": min(iv_rank, 100.0) if iv_rank is not None else None,
        "lookback_days": lookback_days,
        "hv_window_days": 20,
        "note": "iv_rank usa HV20 rolling como proxy de IV histórica (IBKR no expone IV histórica nativa)",
    }


def calculate_spread_strikes(symbol: str, spot: float, offset: float, width: float = 10.0) -> dict:
    """Calcula strikes de venta (PCS) y compra (CCS) para un spread de opciones.

    PCS (Put Credit Spread): vender PUT ATM-offset, comprar PUT ATM-offset-width
    CCS (Call Credit Spread): vender CALL ATM+offset, comprar CALL ATM+offset+width

    Returns: {"sell_strike": ..., "buy_strike": ..., "net_credit_max": ...}
    """
    ib = get_ib()

    INDEXES = {"SPX", "NDX", "VIX", "RUT", "SPXW", "NDXW"}
    if symbol.upper() in INDEXES:
        base = Index(symbol.upper(), "CBOE", "USD")
    else:
        base = Stock(symbol.upper(), "SMART", "USD")

    ib.qualifyContracts(base)

    # Obtener strikes disponibles alrededor del spot
    chains = ib.reqSecDefOptParams(base.symbol, '', base.secType, base.conId)

    all_strikes = []
    trading_class = None
    for chain in chains:
        all_strikes.extend(chain.strikes)
        trading_class = chain.tradingClass
        break

    if not all_strikes:
        return {"error": f"No strikes found for {symbol}"}

    all_strikes = sorted(set(all_strikes))

    # Encontrar el strike más cercano al spot (ATM)
    atm_strike = min(all_strikes, key=lambda s: abs(s - spot))

    # Strike de venta: el strike disponible más cercano al spot - offset
    sell_target = spot - offset
    sell_strike = min(all_strikes, key=lambda s: abs(s - sell_target))

    # Strike de compra: sell_strike - width (para PCS) o sell_strike + width (para CCS)
    # Por defecto calculamos PCS: vender put strike más bajo, comprar put strike más alto
    buy_strike = sell_strike + width

    return {
        "symbol": symbol.upper(),
        "spot": spot,
        "offset": offset,
        "width": width,
        "atm_strike": atm_strike,
        "sell_strike": sell_strike,
        "buy_strike": buy_strike,
        "spread_type": "PCS",  # Put Credit Spread
        "risk": width * 100,  # riesgo máximo por contrato en USD
        "trading_class": trading_class,
    }


def ib_combo_quote(short_strike: float, long_strike: float,
                    right: str, expiry: str) -> dict:
    """Obtiene bid/ask mid de ambos legs de un spread."""
    ib = get_ib()
    SYMBOL, EXCHANGE_OPT, TRADING_CLASS = 'SPX', 'CBOE', 'SPXW'

    def _opt(strike):
        c = Contract(secType='OPT', symbol=SYMBOL, exchange=EXCHANGE_OPT,
                     right=right, strike=float(strike),
                     lastTradeDateOrContractMonth=expiry,
                     currency='USD', multiplier='100', tradingClass=TRADING_CLASS)
        ib.qualifyContracts(c)
        return c

    short_opt = _opt(short_strike)
    long_opt  = _opt(long_strike)

    ts = ib.reqMktData(short_opt, '', False, False)
    tl = ib.reqMktData(long_opt,  '', False, False)
    for _ in range(10):
        ib.sleep(0.3)
        if ts.bid > 0 and tl.bid > 0:
            break

    ib.cancelMktData(short_opt)
    ib.cancelMktData(long_opt)

    s_bid, s_ask = ts.bid, ts.ask
    l_bid, l_ask = tl.bid, tl.ask
    credit_mid = None
    if all(x == x for x in [s_bid, s_ask, l_bid, l_ask]):
        credit_mid = round((s_bid + s_ask) / 2 - (l_bid + l_ask) / 2, 4)

    return {
        "short_bid": s_bid, "short_ask": s_ask,
        "long_bid":  l_bid,  "long_ask":  l_ask,
        "credit_mid": credit_mid,
        "short_strike": short_strike, "long_strike": long_strike,
        "right": right, "expiry": expiry,
    }


def ib_submit_combo(short_strike: float, long_strike: float,
                    right: str, expiry: str,
                    credit_est: Optional[float],
                    order_ref: str, qty: int) -> dict:
    """Coloca un BAG combo spread (2-leg PCS/CCS) con bracket TP/SL."""
    ib = get_ib()
    result = place_spxw_bag_bracket(
        ib=ib,
        short_strike=float(short_strike),
        long_strike=float(long_strike),
        expiry=expiry,
        right=right,
        credit_est=float(credit_est) if credit_est else None,
        order_ref=order_ref,
        oca_prefix=f"OCA_{order_ref}",
        qty=qty,
    )
    return {
        "status":      result.get("status"),
        "credit_real": result.get("credit_real"),
        "credit_mid":  result.get("credit_mid"),
        "limit_price": result.get("limit_price"),
        "tp_price":    result.get("tp_price"),
        "sl_trigger":  result.get("sl_trigger"),
        "trade_ids":   result.get("trade_ids", []),
    }


def submit_option_order(action: str, symbol: str, expiry: str, strike: float,
                        right: str, quantity: int, orderType: str = "LIMIT",
                        price: float = None, tif: str = "DAY") -> dict:
    """Enviar una orden de opción (BUY to open / SELL to close)."""
    ib = get_ib()
    client_id = _next_client_id()

    # Construir el contrato de opción
    c = Contract()
    c.symbol = symbol.upper()
    c.secType = 'OPT'
    c.currency = 'USD'
    c.exchange = 'CBOE' if symbol.upper() in {"SPX", "NDX", "VIX", "RUT", "SPXW", "NDXW"} else 'SMART'
    c.lastTradeDateOrContractMonth = expiry
    c.strike = strike
    c.right = right.upper()
    c.tradingClass = symbol.upper()
    c.multiplier = '100'

    ib.qualifyContracts(c)

    if not c.conId:
        return {"error": f"Contract not qualified: {symbol} {right} {strike} {expiry}"}

    # Construir la orden
    order = Order()
    order.action = action.upper()
    order.orderType = orderType.upper()
    order.totalQuantity = quantity
    if orderType.upper() == "LIMIT" and price is not None:
        order.lmtPrice = price
    elif orderType.upper() == "MKT":
        pass  # orden al mercado, no necesita precio
    order.tif = tif.upper()
    order.transmit = True
    order.clientId = client_id

    # Enviar la orden
    trade = ib.placeOrder(c, order)

    # Esperar confirmación
    ib.sleep(2)

    return {
        "status": "sent" if trade.isDone else "pending",
        "clientOrderId": str(order.clientId),
        "orderId": trade.orderId,
        "action": order.action,
        "symbol": symbol.upper(),
        "expiry": expiry,
        "strike": strike,
        "right": right.upper(),
        "quantity": quantity,
        "orderType": order.orderType,
        "price": order.lmtPrice if hasattr(order, 'lmtPrice') else None,
        "tif": order.tif,
        "contract": fmt_contract(c),
    }


def cancel_order(clientOrderId: str) -> dict:
    """Cancelar una orden por clientOrderId."""
    ib = get_ib()

    try:
        client_id = int(clientOrderId)
    except ValueError:
        return {"error": f"Invalid clientOrderId: {clientOrderId}"}

    # Buscar la orden
    orders = ib.openOrders()
    target_order = None
    for o in orders:
        if o.clientId == client_id:
            target_order = o
            break

    if target_order is None:
        return {"error": f"Order not found with clientOrderId: {clientOrderId}"}

    # Cancelar
    ib.cancelOrder(target_order)

    ib.sleep(1)

    return {
        "status": "cancelled",
        "clientOrderId": clientOrderId,
        "orderId": target_order.orderId,
        "action": target_order.action,
        "symbol": target_order.contract.symbol if target_order.contract else "unknown",
    }


def get_open_orders(symbol: str = None) -> list:
    """Lista órdenes pendientes o en ejecución."""
    ib = get_ib()

    orders = ib.openOrders()
    if not orders:
        return []

    result = []
    for o in orders:
        if symbol and o.contract and o.contract.symbol != symbol.upper():
            continue
        result.append({
            "orderId": o.orderId,
            "clientOrderId": o.clientId,
            "action": o.action,
            "orderType": o.orderType,
            "totalQuantity": o.totalQuantity,
            "lmtPrice": o.lmtPrice,
            "status": o.status,
            "tif": o.tif,
            "contract": fmt_contract(o.contract) if o.contract else {},
            "remaining": o.totalQuantity - (o.filledCount or 0),
        })

    return result


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
             "exchange": {"type": "string", "default": "SMART"}},
             "required": ["symbol"]}),
    Tool(name="get_option_chain", description="Cadena de opciones (expirations o strikes por expiry)",
         inputSchema={"type": "object", "properties": {
             "symbol": {"type": "string"},
             "expiry": {"type": "string"}},
             "required": ["symbol"]}),
    Tool(name="get_option_prices", description="Precios bid/ask de opciones para strikes ATM",
         inputSchema={"type": "object", "properties": {
             "symbol": {"type": "string"},
             "expiry": {"type": "string"},
             "right": {"type": "string", "description": "C o P (opcional, todas si se omite)"},
             " ATM_delta": {"type": "number", "description": "Rango de strikes alrededor de ATM (default 10)"}},
             "required": ["symbol", "expiry"]}),
    Tool(name="get_metrics_underlying", description="Métricas del subyacente: spot, spread, HV20/50, 52w high/low, % off high",
         inputSchema={"type": "object", "properties": {
             "symbol": {"type": "string"},
             "exchange": {"type": "string", "default": "SMART"}},
             "required": ["symbol"]}),
    Tool(name="get_metrics_spread", description="Métricas de un spread 2-leg: credit, max loss, breakeven, IV/delta agregados",
         inputSchema={"type": "object", "properties": {
             "symbol": {"type": "string"},
             "expiry": {"type": "string", "description": "YYYYMMDD"},
             "right": {"type": "string", "enum": ["C", "P"], "description": "C=CCS, P=PCS"},
             "short_strike": {"type": "number"},
             "long_strike": {"type": "number"}},
             "required": ["symbol", "expiry", "right", "short_strike", "long_strike"]}),
    Tool(name="get_metrics_chain", description="Resumen cadena ATM: IV atm, OI/Vol ATM, IV Rank (proxy HV20)",
         inputSchema={"type": "object", "properties": {
             "symbol": {"type": "string"},
             "exchange": {"type": "string", "default": "SMART"},
             "expiry": {"type": "string", "description": "YYYYMMDD; si vacío, primer expiry"},
             "lookback_days": {"type": "integer", "default": 252, "description": "Ventana para HV rolling"}},

             "required": ["symbol"]}),
    Tool(name="submit_option_order", description="Enviar orden de compra/venta de opción (BUY to open / SELL to close)",
         inputSchema={"type": "object", "properties": {
             "action": {"type": "string", "enum": ["BUY", "SELL"], "description": "BUY to open, SELL to close"},
             "symbol": {"type": "string"},
             "expiry": {"type": "string"},
             "strike": {"type": "number"},
             "right": {"type": "string", "enum": ["C", "P"]},
             "quantity": {"type": "integer", "description": "Número de contratos"},
             "orderType": {"type": "string", "enum": ["LIMIT", "MKT"], "default": "LIMIT"},
             "price": {"type": "number", "description": "Precio límite (solo LIMIT)"},
             "tif": {"type": "string", "enum": ["DAY", "GTC", "IOC", "FOK"], "default": "DAY"}},
             "required": ["action", "symbol", "expiry", "strike", "right", "quantity"]}),
    Tool(name="cancel_order", description="Cancelar una orden abierta por clientOrderId",
         inputSchema={"type": "object", "properties": {
             "clientOrderId": {"type": "string"}},
             "required": ["clientOrderId"]}),
    Tool(name="get_open_orders", description="Lista órdenes pendientes o en ejecución (con filtro opcional por symbol)",
         inputSchema={"type": "object", "properties": {
             "symbol": {"type": "string", "description": "Filtrar por symbol (opcional)"}},
             "required": []}),
    Tool(name="calculate_spread_strikes", description="Calcula strikes de venta y compra para un spread PCS/CCS dado el spot y el offset",
         inputSchema={"type": "object", "properties": {
             "symbol": {"type": "string"},
             "spot": {"type": "number"},
             "offset": {"type": "number", "description": "Puntos de offset desde el spot para el strike de venta"},
             "width": {"type": "number", "description": "Ancho del spread en puntos (default 10)"}},
             "required": ["symbol", "spot", "offset"]}),
    Tool(name="combo_quote", description="Obtiene bid/ask mid de ambos legs de un spread para calcular crédito — sin colocar orden",
         inputSchema={"type": "object", "properties": {
             "short_strike": {"type": "number"},
             "long_strike":  {"type": "number"},
             "right":        {"type": "string", "enum": ["P", "C"]},
             "expiry":       {"type": "string", "description": "YYYYMMDD"}},
             "required": ["short_strike", "long_strike", "right", "expiry"]}),
    Tool(name="submit_combo_order", description="Coloca un BAG combo spread (2-leg) — PCS o CCS",
         inputSchema={"type": "object", "properties": {
             "short_strike": {"type": "number"},
             "long_strike":  {"type": "number"},
             "right":        {"type": "string", "enum": ["P", "C"]},
             "expiry":       {"type": "string", "description": "YYYYMMDD"},
             "credit_est":   {"type": "number", "description": "Crédito estimado (opcional — obtiene mid de mercado si se omite)"},
             "order_ref":    {"type": "string", "description": "Tag para identificar la orden en TWS (default DASHCOMBO)"},
             "qty":          {"type": "integer", "description": "Contratos (default 1)"}},
             "required": ["short_strike", "long_strike", "right", "expiry"]}),
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
            delta = arguments.get(" ATM_delta", arguments.get("ATM_delta", 10))
            result = await asyncio.to_thread(_run_ib_sync, ib_get_option_prices, symbol, expiry, right, delta)
            return CallToolResult(content=[TextContent(type="text", text=json.dumps(result, indent=2))])

        elif name == "get_metrics_underlying":
            symbol = arguments.get("symbol")
            exchange = arguments.get("exchange", "SMART")
            result = await asyncio.to_thread(_run_ib_sync, ib_get_metrics_underlying, symbol, exchange)
            return CallToolResult(content=[TextContent(type="text", text=json.dumps(result, indent=2))])

        elif name == "get_metrics_spread":
            symbol = arguments.get("symbol")
            expiry = arguments.get("expiry")
            right = arguments.get("right")
            short_strike = arguments.get("short_strike")
            long_strike = arguments.get("long_strike")
            result = await asyncio.to_thread(
                _run_ib_sync, ib_get_metrics_spread, symbol, expiry, right, short_strike, long_strike
            )
            return CallToolResult(content=[TextContent(type="text", text=json.dumps(result, indent=2))])

        elif name == "get_metrics_chain":
            symbol = arguments.get("symbol")
            exchange = arguments.get("exchange", "SMART")
            expiry = arguments.get("expiry", "")
            lookback_days = arguments.get("lookback_days", 252)
            result = await asyncio.to_thread(
                _run_ib_sync, ib_get_metrics_chain, symbol, exchange, expiry, lookback_days
            )
            return CallToolResult(content=[TextContent(type="text", text=json.dumps(result, indent=2))])

        elif name == "submit_option_order":
            action = arguments.get("action")
            symbol = arguments.get("symbol")
            expiry = arguments.get("expiry")
            strike = arguments.get("strike")
            right = arguments.get("right")
            quantity = arguments.get("quantity")
            orderType = arguments.get("orderType", "LIMIT")
            price = arguments.get("price")
            tif = arguments.get("tif", "DAY")
            result = await asyncio.to_thread(
                _run_ib_sync, submit_option_order,
                action, symbol, expiry, strike, right, quantity, orderType, price, tif
            )
            return CallToolResult(content=[TextContent(type="text", text=json.dumps(result, indent=2))])

        elif name == "cancel_order":
            clientOrderId = arguments.get("clientOrderId")
            result = await asyncio.to_thread(_run_ib_sync, cancel_order, clientOrderId)
            return CallToolResult(content=[TextContent(type="text", text=json.dumps(result, indent=2))])

        elif name == "get_open_orders":
            symbol = arguments.get("symbol")
            result = await asyncio.to_thread(_run_ib_sync, get_open_orders, symbol)
            return CallToolResult(content=[TextContent(type="text", text=json.dumps(result, indent=2))])

        elif name == "calculate_spread_strikes":
            symbol = arguments.get("symbol")
            spot = arguments.get("spot")
            offset = arguments.get("offset")
            width = arguments.get("width", 10)
            result = await asyncio.to_thread(
                _run_ib_sync, calculate_spread_strikes, symbol, spot, offset, width
            )
            return CallToolResult(content=[TextContent(type="text", text=json.dumps(result, indent=2))])

        elif name == "combo_quote":
            short_strike = float(arguments["short_strike"])
            long_strike  = float(arguments["long_strike"])
            right        = arguments["right"]
            expiry       = arguments["expiry"]
            result = await asyncio.to_thread(
                _run_ib_sync, ib_combo_quote, short_strike, long_strike, right, expiry
            )
            return CallToolResult(content=[TextContent(type="text", text=json.dumps(result))])

        elif name == "submit_combo_order":
            short_strike = float(arguments["short_strike"])
            long_strike  = float(arguments["long_strike"])
            right        = arguments["right"]
            expiry       = arguments["expiry"]
            credit_est   = arguments.get("credit_est")
            order_ref    = arguments.get("order_ref", "DASHCOMBO")
            qty          = int(arguments.get("qty", 1))
            result = await asyncio.to_thread(
                _run_ib_sync, ib_submit_combo, short_strike, long_strike, right, expiry,
                credit_est, order_ref, qty
            )
            return CallToolResult(content=[TextContent(type="text", text=json.dumps(result))])

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

def _ib_combo_quote(short_strike: float, long_strike: float, right: str, expiry: str):
    return ib_combo_quote(short_strike, long_strike, right, expiry)

def _ib_submit_combo(short_strike: float, long_strike: float, right: str, expiry: str,
                     credit_est, order_ref: str, qty: int):
    return ib_submit_combo(short_strike, long_strike, right, expiry, credit_est, order_ref, qty)

from starlette.responses import JSONResponse

async def rest_status(request: Request) -> JSONResponse:
    try:
        result = await asyncio.get_event_loop().run_in_executor(None, _ib_call, _ib_status, 8)
        return JSONResponse({**result, "time": datetime.now().isoformat()})
    except TimeoutError as e:
        return JSONResponse({"connected": False, "error": str(e)}, status_code=503)
    except Exception as e:
        return JSONResponse({"connected": False, "error": str(e)}, status_code=503)

async def rest_account(request: Request) -> JSONResponse:
    try:
        result = await asyncio.get_event_loop().run_in_executor(None, _ib_call, _ib_account, 8)
        return JSONResponse(result)
    except TimeoutError as e:
        return JSONResponse({"error": f"timeout: {e}"}, status_code=503)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

async def rest_positions(request: Request) -> JSONResponse:
    try:
        result = await asyncio.get_event_loop().run_in_executor(None, _ib_call, _ib_positions, 8)
        return JSONResponse(result)
    except TimeoutError as e:
        return JSONResponse({"error": f"timeout: {e}"}, status_code=503)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

mcp_app.add_route("/api/status", route=rest_status, methods=["GET"])
mcp_app.add_route("/api/account", route=rest_account, methods=["GET"])
mcp_app.add_route("/api/positions", route=rest_positions, methods=["GET"])


async def rest_combo_quote(request: Request) -> JSONResponse:
    try:
        data = await request.json()
        result = await asyncio.get_event_loop().run_in_executor(
            None, _ib_call, _ib_combo_quote, 15,
            float(data["short_strike"]), float(data["long_strike"]),
            data["right"], data["expiry"]
        )
        return JSONResponse(result)
    except TimeoutError as e:
        return JSONResponse({"error": f"timeout: {e}"}, status_code=503)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


async def rest_combo_submit(request: Request) -> JSONResponse:
    try:
        data = await request.json()
        result = await asyncio.get_event_loop().run_in_executor(
            None, _ib_call, _ib_submit_combo, 20,
            float(data["short_strike"]), float(data["long_strike"]),
            data["right"], data["expiry"],
            data.get("credit_est"), data.get("order_ref", "DASHCOMBO"),
            int(data.get("qty", 1))
        )
        return JSONResponse(result)
    except TimeoutError as e:
        return JSONResponse({"error": f"timeout: {e}"}, status_code=503)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


mcp_app.add_route("/api/combo/quote",   route=rest_combo_quote,   methods=["POST"])
mcp_app.add_route("/api/combo/submit",  route=rest_combo_submit,  methods=["POST"])

app = mcp_app


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    print(f"IB Gateway MCP HTTP -> {HOST}:{PORT}, token={'OK' if TOKEN else 'NONE'}")
    uvicorn.run(app, host="0.0.0.0", port=8765, log_level="info")
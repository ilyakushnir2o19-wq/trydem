"""
OKX Liquidity Trap Bot v3.2 — Multi-Coin Scanner (READ-ONLY)
============================================================
Стратегия: снятие ликвидности (sweep свинг-экстремума) + хвост/подтверждение +
объём + закрытие обратно за снятым уровнем; EMA20/VWAP/тренд 1h — баллы.

НОВОЕ В 3.2:
  - ДВУХСВЕЧНАЯ ЛОВУШКА: свеча A прокалывает экстремум и закрывается за ним,
    свеча B возвращает цену обратно — теперь это валидный сетап (база 5),
    раньше такие уходили в отбраковку "закрылась ниже снятого лоу".
  - ФИКС ТОПА МОНЕТ: оборот считается по нативному полю OKX volCcy24h;
    палладий (XPD), платина (XPT) и прочие металлы — в чёрном списке.
  - ВРЕМЯ МСК во всех сообщениях (TZ_OFFSET_HOURS).
  - РАЗМЕР ВХОДА ПО КАЧЕСТВУ: B=1.5% (🤏 полпозиции), A=3% (💪),
    A+=5% (ЕБАШ!!! 🔥 — и это жёсткий потолок, не фулл банк).
  - БЛОК "ГРАНЬ" в каждом сигнале: красная линия (стоп), TP1=1R
    (фиксация 50% + стоп в безубыток), TP2=2R, трейлинг остатка
    до закрытия 5m за EMA20 против позиции, время-стоп 30 минут.
  - Почти-сигналы БОЛЬШЕ НЕ провоцируют входы: отправка выключена
    по умолчанию (SEND_NEAR_MISSES=False), в /status остаются.

НОВОЕ В 3.1: ядро = sweep + хвост + объём + возврат за уровень (6 баллов),
EMA/VWAP/тренд — бонусы; журнал почти-сигналов в /status.
НОВОЕ В 3.0: мультисканер топ-N ликвидных USDT-свопов OKX (auto/static),
кэш тренда 1h, пер-символьные кулдауны, /start /status /ping.

Бот НЕ торгует. Только сигналы в Telegram.

Зависимости (requirements.txt): pandas, ccxt, python-telegram-bot>=21

Переменные окружения:
  OKX_API_KEY, OKX_API_SECRET, OKX_API_PASSPHRASE (только чтение!)
  TG_BOT_TOKEN, TG_CHAT_ID
"""

import asyncio
import logging
import os
import time
from collections import deque
from datetime import datetime, timezone

import ccxt.async_support as ccxt
import pandas as pd
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# ============================ НАСТРОЙКИ ============================

CONFIG = {
    # --- Выбор монет ---
    "SYMBOLS_MODE": "auto",        # "auto" = топ по обороту | "static" = свой список
    "TOP_N_SYMBOLS": 30,           # сколько монет сканировать в режиме auto
    "STATIC_SYMBOLS": [            # используется в режиме static или как fallback
        "BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT", "XRP/USDT:USDT",
        "DOGE/USDT:USDT", "ADA/USDT:USDT", "AVAX/USDT:USDT", "LINK/USDT:USDT",
        "LTC/USDT:USDT", "DOT/USDT:USDT", "TON/USDT:USDT", "SUI/USDT:USDT",
        "OP/USDT:USDT", "ARB/USDT:USDT", "PEPE/USDT:USDT",
    ],
    # Не-криптовалютные и суррогатные инструменты — вне стратегии:
    "EXCLUDE_BASES": {"XPD", "XPT", "XAU", "XAG", "USDC", "DAI", "EURT"},
    "SYMBOL_REFRESH_MIN": 60,      # как часто обновлять топ-список (мин)
    "MIN_24H_VOLUME_USDT": 20_000_000,  # отсечка неликвида в режиме auto

    # --- Часовой пояс для сообщений ---
    "TZ_OFFSET_HOURS": 3,          # МСК = UTC+3
    "TZ_LABEL": "МСК",

    # --- Таймфреймы ---
    "TIMEFRAME": "5m",
    "HTF_TIMEFRAME": "1h",
    "CANDLES_LIMIT": 320,
    "HTF_CANDLES_LIMIT": 260,
    "HTF_CACHE_MIN": 10,           # кэш тренда 1h (мин)

    # --- Ядро стратегии ---
    "EMA_PERIOD": 20,
    "VOL_SMA_PERIOD": 20,
    "WICK_MIN_RATIO": 0.50,
    "VOL_MULTIPLIER": 1.3,         # мин. объём односвечной ловушки (было 1.5)
    "VOL_STRONG_MULT": 2.2,        # "аномальный" объём (+1 балл)
    "TREND_LOOKBACK": 3,
    "SWEEP_LOOKBACK": 20,
    "HTF_EMA_PERIOD": 200,

    # --- Двухсвечная ловушка (свеча 1 прокалывает, свеча 2 подтверждает) ---
    "TWO_CANDLE_ENABLED": True,
    "TWO_CANDLE_VOL_MULT": 1.3,    # объём хотя бы одной из двух свечей >= x1.3

    # --- Скоринг: 5-6 = базовые сетапы, 8 = сбалансированно, 11+ = элита ---
    "MIN_SCORE_TO_SEND": 6,
    # Отправка "👀 наблюдений" (почти-сигналов) в чат. ВЫКЛЮЧЕНО сознательно:
    # это отбраковка, а не сигналы — входить по ним нельзя. Включайте на свой риск.
    "SEND_NEAR_MISSES": False,

    # --- Стопы и цели ---
    "ATR_PERIOD": 14,
    "ATR_STOP_MULT": 0.25,
    "RR_RATIO": 2.0,               # TP2; TP1 всегда = 1R (фиксация 50% + стоп в БУ)

    # --- Риск и плечо ---
    # Размер входа зависит от КАЧЕСТВА сигнала (это и есть "заходить насколько"):
    "RISK_BY_GRADE": {
        "B": 0.015,                # 6-8 баллов: осторожно, 1.5% депозита
        "A": 0.03,                 # 9-10 баллов: уверенный вход, 3%
        "A+": 0.05,                # 11-12 баллов: максимальная агрессия, 5%
    },
    "MAX_RISK_PER_TRADE": 0.05,    # жёсткий потолок, выше бот не посчитает
    "LEVERAGE": 20,
    "MAX_MARGIN_SHARE": 0.90,
    "MMR": 0.005,

    # --- Фильтр времени (UTC), None = всегда ---
    "TRADING_HOURS_UTC": range(6, 22),

    # --- Цикл ---
    "CHECK_INTERVAL_SEC": 60,
    "SIGNAL_COOLDOWN_CANDLES": 3,

    # --- Ключи ---
    "OKX_API_KEY": os.getenv("OKX_API_KEY", "ff2096ce-c6c3-480f-8e9c-1967f40992ad"),
    "OKX_API_SECRET": os.getenv("OKX_API_SECRET", "B97AFC64D3DF87E796212C1594287A07"),
    "OKX_API_PASSPHRASE": os.getenv("OKX_API_PASSPHRASE", "Ataman4825!"),
    "TG_BOT_TOKEN": os.getenv("TG_BOT_TOKEN", "8484342686:AAF4Dr05pu2NHFqgDHAC0Iy2C1dBGee86r4"),
    "TG_CHAT_ID": os.getenv("TG_CHAT_ID", "7413242280"),
    "FALLBACK_BALANCE_USDT": 200.0,
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger("liq-trap-v3")

# Живое состояние для /status
STATE = {
    "started_at": None,
    "last_cycle": None,
    "cycle_sec": 0.0,
    "symbols": [],                 # активный список монет
    "per_symbol": {},              # symbol -> {"close":..,"trend":..,"vol_x":..}
    "signals_sent": 0,
    "setups_seen": 0,              # сколько раз ядро ловушки (sweep+хвост) замечено
    "near_misses": deque(maxlen=8),  # последние почти-сигналы с причинами отказа
    "last_signal_text": None,
    "last_error": None,
}


# ==================== ВСПОМОГАТЕЛЬНОЕ =============================

def fmt_time(dt_utc) -> str:
    """UTC -> локальное время сообщений (по умолчанию МСК = UTC+3)."""
    local = dt_utc + pd.Timedelta(hours=CONFIG["TZ_OFFSET_HOURS"])
    return f"{local.strftime('%d.%m %H:%M')} {CONFIG['TZ_LABEL']}"


def now_local_str() -> str:
    return fmt_time(pd.Timestamp.now(tz="UTC"))


def fmt_price(p: float) -> str:
    """Динамическая точность: BTC 65,432.10, SOL 145.2, PEPE 0.00001234."""
    p = float(p)
    if p >= 1000:
        return f"{p:,.2f}"
    if p >= 1:
        return f"{p:.4g}"
    # мелкие цены: фиксированная запись без научной нотации,
    # оставляем ~4 значащие цифры
    s = f"{p:.12f}".rstrip("0")
    frac = s.split(".")[1] if "." in s else ""
    lead_zeros = len(frac) - len(frac.lstrip("0"))
    return f"{p:.{min(lead_zeros + 4, 12)}f}"


# ======================= ЗАГРУЗКА ДАННЫХ ==========================

async def fetch_df(exchange: ccxt.okx, symbol: str, timeframe: str,
                   limit: int) -> pd.DataFrame:
    """Свечи -> DataFrame только с ЗАКРЫТЫМИ свечами."""
    ohlcv = await exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
    df = pd.DataFrame(
        ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"]
    )
    df["dt"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    return df.iloc[:-1].reset_index(drop=True)


async def fetch_top_symbols(exchange: ccxt.okx) -> list[str]:
    """Топ-N USDT-свопов OKX по обороту за 24ч (режим auto).

    ВАЖНО: оборот считаем по нативному полю OKX volCcy24h (объём в базовой
    монете) * last. Поле quoteVolume из ccxt для свопов OKX ненадёжно —
    из-за него в топ попадали палладий (XPD) и платина (XPT)."""
    if CONFIG["SYMBOLS_MODE"] != "auto":
        return list(CONFIG["STATIC_SYMBOLS"])
    try:
        markets = await exchange.load_markets(True)
        swap_symbols = [
            s for s, m in markets.items()
            if m.get("swap") and m.get("settle") == "USDT"
            and m.get("quote") == "USDT" and m.get("active", True)
            and m.get("base") not in CONFIG["EXCLUDE_BASES"]
        ]
        tickers = await exchange.fetch_tickers(swap_symbols)

        def turnover(t: dict) -> float:
            last = t.get("last") or 0.0
            info = t.get("info") or {}
            vol_base = info.get("volCcy24h")          # нативное поле OKX
            if vol_base and last:
                return float(vol_base) * float(last)
            qv = t.get("quoteVolume")
            if qv:
                return float(qv)
            bv = t.get("baseVolume")
            return float(bv) * float(last) if bv and last else 0.0

        ranked = sorted(tickers.values(), key=turnover, reverse=True)
        top = [(t["symbol"], turnover(t)) for t in ranked
               if turnover(t) >= CONFIG["MIN_24H_VOLUME_USDT"]]
        top = top[:CONFIG["TOP_N_SYMBOLS"]]
        if not top:
            raise ValueError("пустой топ-список")
        log.info("Топ по обороту 24ч: %s",
                 ", ".join(f"{s.split('/')[0]}({v/1e6:.0f}M)" for s, v in top[:10]))
        return [s for s, _ in top]
    except Exception as e:
        log.error("Не удалось получить топ монет (%s) — статический список.", e)
        return list(CONFIG["STATIC_SYMBOLS"])[:CONFIG["TOP_N_SYMBOLS"]]


async def fetch_usdt_balance(exchange: ccxt.okx) -> float:
    if not CONFIG["OKX_API_KEY"]:
        log.warning("OKX ключи не заданы — виртуальный баланс %.2f USDT",
                    CONFIG["FALLBACK_BALANCE_USDT"])
        return CONFIG["FALLBACK_BALANCE_USDT"]
    try:
        bal = await exchange.fetch_balance({"type": "trading"})
        usdt = bal.get("USDT", {}) or {}
        return float(usdt.get("total") or usdt.get("free") or 0.0)
    except Exception as e:
        log.error("Баланс недоступен (%s), fallback.", e)
        return CONFIG["FALLBACK_BALANCE_USDT"]


# ========================= ИНДИКАТОРЫ =============================
# Собственные реализации (pandas_ta недоступен для Python 3.11):

def ema(series: pd.Series, length: int) -> pd.Series:
    return series.ewm(span=length, adjust=False, min_periods=length).mean()


def sma(series: pd.Series, length: int) -> pd.Series:
    return series.rolling(length, min_periods=length).mean()


def atr(high: pd.Series, low: pd.Series, close: pd.Series,
        length: int) -> pd.Series:
    """ATR по Уайлдеру (RMA-сглаживание, как в TradingView)."""
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)
    return tr.ewm(alpha=1 / length, adjust=False, min_periods=length).mean()


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["ema20"] = ema(df["close"], CONFIG["EMA_PERIOD"])
    df["vol_sma"] = sma(df["volume"], CONFIG["VOL_SMA_PERIOD"])
    df["atr"] = atr(df["high"], df["low"], df["close"], CONFIG["ATR_PERIOD"])

    # Дневной VWAP: группировка по датам UTC, сброс в 00:00 UTC
    tp = (df["high"] + df["low"] + df["close"]) / 3.0
    df["_tpv"] = tp * df["volume"]
    df["_d"] = df["dt"].dt.date
    g = df.groupby("_d", sort=False)
    df["vwap"] = g["_tpv"].cumsum() / g["volume"].cumsum().replace(0, pd.NA)
    df.drop(columns=["_tpv", "_d"], inplace=True)

    # Экстремумы предыдущих N свечей (не включая текущую) — лужи ликвидности
    lb = CONFIG["SWEEP_LOOKBACK"]
    df["swing_high"] = df["high"].shift(1).rolling(lb).max()
    df["swing_low"] = df["low"].shift(1).rolling(lb).min()
    return df


def htf_trend(df_htf: pd.DataFrame) -> str:
    """'up' / 'down' / 'flat' по EMA200 старшего ТФ (мёртвая зона 0.15%)."""
    ema_series = ema(df_htf["close"], CONFIG["HTF_EMA_PERIOD"])
    if pd.isna(ema_series.iloc[-1]):
        return "flat"
    last_close = df_htf["close"].iloc[-1]
    last_ema = ema_series.iloc[-1]
    if last_close > last_ema * 1.0015:
        return "up"
    if last_close < last_ema * 0.9985:
        return "down"
    return "flat"


# ====================== ЛОГИКА СТРАТЕГИИ ==========================

_NEAR_SEEN: dict[str, int] = {}   # symbol -> ts свечи последнего учтённого почти-сигнала


def _log_near_miss(symbol: str, side: str, candle, reasons: list[str]) -> None:
    """Запоминаем 'почти-сигнал' для /status: ядро ловушки было, но что-то не добрало.
    Одна и та же свеча учитывается один раз (цикл опрашивает её многократно)."""
    ts = int(candle["timestamp"])
    if _NEAR_SEEN.get(symbol) == ts:
        return
    _NEAR_SEEN[symbol] = ts
    STATE["setups_seen"] += 1
    STATE["near_misses"].appendleft({
        "symbol": symbol, "side": side,
        "time": fmt_time(candle["dt"]),
        "reasons": ", ".join(reasons),
    })
    log.info("Почти-сигнал %s %s: %s", symbol, side, ", ".join(reasons))


def analyze(df: pd.DataFrame, trend_1h: str, symbol: str = "?") -> dict | None:
    """
    Скоринг последней закрытой свечи. v3.1: EMA/VWAP переведены из
    обязательных условий в баллы — раньше требование закрыться за
    EMA20 И VWAP одновременно отсекало почти все реальные ловушки
    (после снятия лоу дневной VWAP часто слишком далеко от цены).

    ОБЯЗАТЕЛЬНО (ядро ловушки, базовые 6 баллов):
      sweep свинг-экстремума ................. +3
      хвост >= WICK_MIN_RATIO ................ +2
      объём >= 1.5x .......................... +1
      закрытие ОБРАТНО за снятым уровнем ..... (фильтр, без баллов)
    БОНУСЫ (до 12):
      закрытие за EMA20 +1 | за VWAP +1 | объём >= 2.5x +1
      тренд 1h в сторону сделки +2 | контекст 5m +1
    Градации: 11-12 = A+, 9-10 = A, 6-8 = B.
    """
    lb = CONFIG["TREND_LOOKBACK"]
    need = CONFIG["SWEEP_LOOKBACK"] + CONFIG["EMA_PERIOD"] + lb + 2
    if len(df) < need:
        return None

    last = df.iloc[-1]
    prev = df.iloc[-1 - lb:-1]

    cols = ["ema20", "vwap", "vol_sma", "atr", "swing_high", "swing_low"]
    if last[cols].isna().any():
        return None

    rng = last["high"] - last["low"]
    if rng <= 0:
        return None

    body_top = max(last["open"], last["close"])
    body_bot = min(last["open"], last["close"])
    upper_wick = (last["high"] - body_top) / rng
    lower_wick = (body_bot - last["low"]) / rng
    vol_x = last["volume"] / last["vol_sma"] if last["vol_sma"] > 0 else 0.0
    vol_ok = vol_x >= CONFIG["VOL_MULTIPLIER"]
    vol_strong = vol_x >= CONFIG["VOL_STRONG_MULT"]

    # ------------------------- SHORT (1 свеча) ---------------
    sweep_hi = last["high"] > last["swing_high"]
    wick_up_ok = upper_wick >= CONFIG["WICK_MIN_RATIO"]

    if sweep_hi and (wick_up_ok or vol_ok):        # ядро замечено — диагностируем
        back_inside = last["close"] < last["swing_high"]   # отказ: закрытие под снятым хаем
        if wick_up_ok and vol_ok and back_inside:
            score = 6
            score += 1 if last["close"] < last["ema20"] else 0
            score += 1 if last["close"] < last["vwap"] else 0
            score += 1 if vol_strong else 0
            score += 2 if trend_1h == "down" else 0
            ctx_up = ((prev["close"] > prev["ema20"]) &
                      (prev["close"] > prev["vwap"])).all()
            score += 1 if ctx_up else 0
            return {"side": "SHORT", "candle": last, "vol_x": vol_x,
                    "score": score, "trend_1h": trend_1h,
                    "swept": float(last["swing_high"]),
                    "pattern": "1-свечная",
                    "stop_high": float(last["high"]),
                    "stop_low": float(last["low"])}
        two = _two_candle(df, trend_1h, symbol)
        if two:
            return two
        reasons = []
        if not wick_up_ok:
            reasons.append(f"хвост {upper_wick*100:.0f}%<{CONFIG['WICK_MIN_RATIO']*100:.0f}%")
        if not vol_ok:
            reasons.append(f"объём x{vol_x:.1f}<{CONFIG['VOL_MULTIPLIER']}")
        if not back_inside:
            reasons.append("закрылась выше снятого хая (жду свечу-подтверждение)")
        _log_near_miss(symbol, "SHORT", last, reasons)
        return None

    # ------------------------- LONG (1 свеча) ----------------
    sweep_lo = last["low"] < last["swing_low"]
    wick_dn_ok = lower_wick >= CONFIG["WICK_MIN_RATIO"]

    if sweep_lo and (wick_dn_ok or vol_ok):
        back_inside = last["close"] > last["swing_low"]    # отказ: закрытие над снятым лоу
        if wick_dn_ok and vol_ok and back_inside:
            score = 6
            score += 1 if last["close"] > last["ema20"] else 0
            score += 1 if last["close"] > last["vwap"] else 0
            score += 1 if vol_strong else 0
            score += 2 if trend_1h == "up" else 0
            ctx_dn = ((prev["close"] < prev["ema20"]) &
                      (prev["close"] < prev["vwap"])).all()
            score += 1 if ctx_dn else 0
            return {"side": "LONG", "candle": last, "vol_x": vol_x,
                    "score": score, "trend_1h": trend_1h,
                    "swept": float(last["swing_low"]),
                    "pattern": "1-свечная",
                    "stop_high": float(last["high"]),
                    "stop_low": float(last["low"])}
        two = _two_candle(df, trend_1h, symbol)
        if two:
            return two
        reasons = []
        if not wick_dn_ok:
            reasons.append(f"хвост {lower_wick*100:.0f}%<{CONFIG['WICK_MIN_RATIO']*100:.0f}%")
        if not vol_ok:
            reasons.append(f"объём x{vol_x:.1f}<{CONFIG['VOL_MULTIPLIER']}")
        if not back_inside:
            reasons.append("закрылась ниже снятого лоу (жду свечу-подтверждение)")
        _log_near_miss(symbol, "LONG", last, reasons)
        return None

    # Односвечного ядра нет — проверяем двухсвечный вариант
    return _two_candle(df, trend_1h, symbol)


def _two_candle(df: pd.DataFrame, trend_1h: str, symbol: str) -> dict | None:
    """
    ДВУХСВЕЧНАЯ ЛОВУШКА: свеча A прокалывает экстремум и закрывается ЗА ним
    (ложный пробой без мгновенного отказа), свеча B (последняя) закрывается
    ОБРАТНО внутри диапазона — отказ пришёл со второй свечой.
    Это тот самый случай "закрылась ниже снятого лоу" из журнала почти-сигналов.

    Обязательно: прокол A + возврат B в сторону сделки + объём (A или B) >= x1.3.
    База 5 баллов (чуть слабее односвечной), бонусы те же, максимум 11.
    """
    if not CONFIG["TWO_CANDLE_ENABLED"] or len(df) < CONFIG["SWEEP_LOOKBACK"] + 25:
        return None

    a = df.iloc[-2]
    b = df.iloc[-1]
    lb = CONFIG["TREND_LOOKBACK"]
    prev = df.iloc[-2 - lb:-2]          # контекст ДО свечи-прокола

    cols = ["ema20", "vwap", "vol_sma", "atr", "swing_high", "swing_low"]
    if a[cols].isna().any() or b[cols].isna().any():
        return None
    if a["vol_sma"] <= 0:
        return None

    vol_a = a["volume"] / a["vol_sma"]
    vol_b = b["volume"] / b["vol_sma"] if b["vol_sma"] > 0 else 0.0
    vol_pair_ok = max(vol_a, vol_b) >= CONFIG["TWO_CANDLE_VOL_MULT"]
    vol_x = max(vol_a, vol_b)
    vol_strong = vol_x >= CONFIG["VOL_STRONG_MULT"]

    # ---- SHORT: A проколола хай и закрылась выше, B закрылась обратно ниже
    level_hi = float(a["swing_high"])
    if (a["high"] > level_hi and a["close"] > level_hi
            and b["close"] < level_hi and b["close"] < b["open"]
            and vol_pair_ok):
        score = 5
        score += 1 if b["close"] < b["ema20"] else 0
        score += 1 if b["close"] < b["vwap"] else 0
        score += 1 if vol_strong else 0
        score += 2 if trend_1h == "down" else 0
        ctx_up = ((prev["close"] > prev["ema20"]) &
                  (prev["close"] > prev["vwap"])).all()
        score += 1 if ctx_up else 0
        return {"side": "SHORT", "candle": b, "vol_x": vol_x,
                "score": score, "trend_1h": trend_1h, "swept": level_hi,
                "pattern": "2-свечная",
                "stop_high": float(max(a["high"], b["high"])),
                "stop_low": float(min(a["low"], b["low"]))}

    # ---- LONG: A проколола лоу и закрылась ниже, B закрылась обратно выше
    level_lo = float(a["swing_low"])
    if (a["low"] < level_lo and a["close"] < level_lo
            and b["close"] > level_lo and b["close"] > b["open"]
            and vol_pair_ok):
        score = 5
        score += 1 if b["close"] > b["ema20"] else 0
        score += 1 if b["close"] > b["vwap"] else 0
        score += 1 if vol_strong else 0
        score += 2 if trend_1h == "up" else 0
        ctx_dn = ((prev["close"] < prev["ema20"]) &
                  (prev["close"] < prev["vwap"])).all()
        score += 1 if ctx_dn else 0
        return {"side": "LONG", "candle": b, "vol_x": vol_x,
                "score": score, "trend_1h": trend_1h, "swept": level_lo,
                "pattern": "2-свечная",
                "stop_high": float(max(a["high"], b["high"])),
                "stop_low": float(min(a["low"], b["low"]))}

    return None


# ==================== РИСК, ПЛЕЧО, ЛИКВИДАЦИЯ =====================

def liquidation_price(entry: float, side: str, leverage: int) -> float:
    """Оценка цены ликвидации (изолированная маржа)."""
    mmr = CONFIG["MMR"]
    if side == "LONG":
        return entry * (1 - 1 / leverage + mmr)
    return entry * (1 + 1 / leverage - mmr)


def build_trade_plan(signal: dict, balance: float) -> dict:
    c = signal["candle"]
    entry = float(c["close"])
    atr_buf = float(c["atr"]) * CONFIG["ATR_STOP_MULT"]
    rr = CONFIG["RR_RATIO"]
    lev = CONFIG["LEVERAGE"]

    stop_high = signal.get("stop_high", float(c["high"]))
    stop_low = signal.get("stop_low", float(c["low"]))

    if signal["side"] == "SHORT":
        stop = stop_high + atr_buf
        risk_per_unit = stop - entry
        tp1 = entry - risk_per_unit           # 1R: фиксация 50%, стоп в БУ
        take = entry - risk_per_unit * rr     # 2R: основная цель
    else:
        stop = stop_low - atr_buf
        risk_per_unit = entry - stop
        tp1 = entry + risk_per_unit
        take = entry + risk_per_unit * rr

    stop_pct = risk_per_unit / entry
    if stop_pct <= 0:
        raise ValueError("Стоп <= 0")

    # Риск зависит от качества сигнала: B=1.5%, A=3%, A+=5% (потолок жёсткий)
    g = grade_key(signal["score"])
    risk = min(CONFIG["RISK_BY_GRADE"].get(g, 0.015),
               CONFIG["MAX_RISK_PER_TRADE"])
    position_usdt = (balance * risk) / stop_pct
    margin = position_usdt / lev

    capped = False
    max_margin = balance * CONFIG["MAX_MARGIN_SHARE"]
    if margin > max_margin:
        margin = max_margin
        position_usdt = margin * lev
        capped = True
    actual_risk_pct = position_usdt * stop_pct / balance

    liq = liquidation_price(entry, signal["side"], lev)
    liq_safe = liq < stop if signal["side"] == "LONG" else liq > stop

    return {
        "entry": entry, "stop": stop, "tp1": tp1, "take": take,
        "stop_pct": stop_pct, "position_usdt": position_usdt,
        "margin": margin, "leverage": lev, "liq": liq,
        "liq_safe": liq_safe, "capped": capped,
        "actual_risk_pct": actual_risk_pct, "balance": balance,
    }


# ==================== TELEGRAM: СООБЩЕНИЯ =========================

def grade_key(score: int) -> str:
    if score >= 11:
        return "A+"
    if score >= 9:
        return "A"
    return "B"


def grade(score: int) -> str:
    return {"A+": "A+ 🔥", "A": "A", "B": "B"}[grade_key(score)]


def entry_directive(score: int) -> str:
    """Насколько заходить — по качеству сигнала."""
    g = grade_key(score)
    if g == "A+":
        return ("ЕБАШ!!! 🔥🔥🔥 Элитный сетап — максимальная агрессия: "
                f"риск {CONFIG['RISK_BY_GRADE']['A+']*100:.0f}% депозита. "
                "Но объём ниже — это и есть максимум, фулл банк = ликвидация.")
    if g == "A":
        return ("💪 Уверенный вход — полный размер: "
                f"риск {CONFIG['RISK_BY_GRADE']['A']*100:.0f}% депозита.")
    return ("🤏 Осторожно, сетап базовый — полпозиции: "
            f"риск {CONFIG['RISK_BY_GRADE']['B']*100:.1f}% депозита. "
            "Или пропусти и жди сигнал пожирнее.")


def format_message(symbol: str, signal: dict, plan: dict) -> str:
    c = signal["candle"]
    head = "🔴 СЕТАП В ШОРТ" if signal["side"] == "SHORT" else "🟢 СЕТАП В ЛОНГ"
    coin = symbol.split("/")[0]
    trail_side = "лоу" if signal["side"] == "LONG" else "хаёв"
    lines = [
        f"{head}  |  {grade(signal['score'])} ({signal['score']}/12) | "
        f"{signal.get('pattern', '1-свечная')}",
        f"Монета: {coin} ({symbol}, ТФ {CONFIG['TIMEFRAME']})",
        f"Снята ликвидность у {fmt_price(signal['swept'])} | "
        f"Тренд 1h: {signal['trend_1h']} | Объём x{signal['vol_x']:.2f}",
        "",
        entry_directive(signal["score"]),
        "",
        f"✅ Вход: {fmt_price(plan['entry'])}",
        f"🛑 Стоп: {fmt_price(plan['stop'])} (−{plan['stop_pct']*100:.2f}%)",
        f"🎯 TP1: {fmt_price(plan['tp1'])} (1R) | "
        f"TP2: {fmt_price(plan['take'])} ({CONFIG['RR_RATIO']:.0f}R)",
        f"📊 Объем: {plan['position_usdt']:.2f} USDT "
        f"(риск {plan['actual_risk_pct']*100:.1f}% = "
        f"{plan['balance']*plan['actual_risk_pct']:.2f} USDT)",
        f"💼 Маржа при {plan['leverage']}x: {plan['margin']:.2f} USDT | "
        f"Ликвидация ~{fmt_price(plan['liq'])}",
        "",
        "⚖️ ГРАНЬ (план удержания):",
        f"1. Красная линия — стоп {fmt_price(plan['stop'])}. Задет = вышел. "
        "Двигать стоп ВНИЗ по убытку нельзя никогда.",
        f"2. На TP1 ({fmt_price(plan['tp1'])}) закрой 50% и переставь стоп в "
        f"безубыток ({fmt_price(plan['entry'])}). С этого момента обосраться "
        "уже невозможно — худший исход 0.",
        f"3. На TP2 ({fmt_price(plan['take'])}) закрой ещё 25%.",
        f"4. Фуллхаус: последние 25% держи, пока цена не закроет 5m-свечу "
        f"за EMA20 против тебя — тогда забирай всё. Трейль стоп за "
        f"{trail_side} последних 3 свечей.",
        "5. Время-стоп: если за 6 свечей (30 мин) цена не дошла до TP1 и "
        "болтается — выходи по рынку, сетап протух.",
        "",
        f"🕒 Свеча: {fmt_time(c['dt'])}",
    ]
    if plan["capped"]:
        lines.append("❗ Позиция урезана лимитом маржи (90% депозита).")
    if not plan["liq_safe"]:
        lines.append("🚨 НЕ ВХОДИТЬ с этим плечом: ликвидация ближе стопа. "
                     "Снизь плечо или объём.")
    return "\n".join(lines)


async def send_signal(app: Application | None, text: str) -> None:
    if app is None or not CONFIG["TG_CHAT_ID"]:
        log.warning("Telegram не настроен. Сообщение:\n%s", text)
        return
    try:
        await app.bot.send_message(chat_id=CONFIG["TG_CHAT_ID"], text=text)
        log.info("Сигнал отправлен в Telegram.")
    except Exception as e:
        log.error("Ошибка Telegram: %s", e)


# ==================== TELEGRAM: КОМАНДЫ ===========================

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    syms = STATE["symbols"] or CONFIG["STATIC_SYMBOLS"]
    coins = ", ".join(s.split("/")[0] for s in syms)
    r = CONFIG["RISK_BY_GRADE"]
    await update.message.reply_text(
        "🤖 Liquidity Trap Bot v3.2 — мультисканер (read-only)\n\n"
        f"Монет в работе: {len(syms)} ({CONFIG['SYMBOLS_MODE']})\n"
        f"{coins}\n\n"
        f"ТФ: {CONFIG['TIMEFRAME']} + 2-свечная ловушка | Фильтр тренда: "
        f"EMA{CONFIG['HTF_EMA_PERIOD']} {CONFIG['HTF_TIMEFRAME']}\n"
        f"Порог сигнала: {CONFIG['MIN_SCORE_TO_SEND']}/12 | "
        f"Время в сообщениях: {CONFIG['TZ_LABEL']}\n\n"
        "Насколько заходить (пишется в каждом сигнале):\n"
        f"• B (6-8): 🤏 полпозиции, риск {r['B']*100:.1f}%\n"
        f"• A (9-10): 💪 полный размер, риск {r['A']*100:.0f}%\n"
        f"• A+ (11-12): ЕБАШ!!! 🔥 риск {r['A+']*100:.0f}% — и это потолок\n\n"
        "В каждом сигнале — блок ГРАНЬ: стоп, где фиксировать 50% и уходить "
        "в безубыток, и как трейлить остаток до фуллхауса.\n\n"
        "Команды:\n/status — сводка по всем монетам\n/ping — проверка связи\n\n"
        "Бот сам пришлёт сообщение при сетапе. Он НЕ торгует. "
        "👀 наблюдения и почти-сигналы — НЕ входы."
    )


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    up = "-"
    if STATE["started_at"]:
        d = datetime.now(timezone.utc) - STATE["started_at"]
        up = f"{d.days}д {d.seconds // 3600}ч {(d.seconds % 3600) // 60}м"
    lines = [
        "📡 Статус мультисканера",
        f"Аптайм: {up} | Сигналов отправлено: {STATE['signals_sent']}",
        f"Ядро ловушки замечено: {STATE['setups_seen']} раз "
        f"(порог отправки: {CONFIG['MIN_SCORE_TO_SEND']}/12)",
        f"Последний цикл: {STATE['last_cycle'] or 'ещё не было'} "
        f"({STATE['cycle_sec']:.0f} сек, монет: {len(STATE['symbols'])})",
    ]
    if STATE["near_misses"]:
        lines.append("\nПочти-сигналы (что не добрало):")
        for nm in list(STATE["near_misses"])[:5]:
            coin = nm["symbol"].split("/")[0]
            lines.append(f"• {nm['time']} {coin} {nm['side']}: {nm['reasons']}")
    if STATE["last_error"]:
        lines.append(f"⚠️ Последняя ошибка: {STATE['last_error']}")
    if STATE["per_symbol"]:
        lines.append("\nМонета | Цена | 1h | Vol")
        for sym, s in STATE["per_symbol"].items():
            coin = sym.split("/")[0]
            lines.append(f"{coin}: {fmt_price(s['close'])} | "
                         f"{s['trend']} | x{s['vol_x']:.1f}")
    if STATE["last_signal_text"]:
        lines.append(f"\nПоследний сигнал:\n{STATE['last_signal_text']}")
    await update.message.reply_text("\n".join(lines)[:4000])


async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("🏓 pong — бот жив.")


# ======================== СКАНЕР ==================================

def in_trading_hours(dt_utc) -> bool:
    hours = CONFIG["TRADING_HOURS_UTC"]
    return True if hours is None else dt_utc.hour in hours


class HtfCache:
    """Кэш тренда 1h: EMA200 меняется медленно, незачем дёргать биржу каждый цикл."""

    def __init__(self, exchange: ccxt.okx):
        self.exchange = exchange
        self.cache: dict[str, tuple[float, str]] = {}   # symbol -> (ts, trend)

    async def get(self, symbol: str) -> str:
        now = time.time()
        cached = self.cache.get(symbol)
        if cached and now - cached[0] < CONFIG["HTF_CACHE_MIN"] * 60:
            return cached[1]
        df_htf = await fetch_df(self.exchange, symbol,
                                CONFIG["HTF_TIMEFRAME"],
                                CONFIG["HTF_CANDLES_LIMIT"])
        trend = htf_trend(df_htf)
        self.cache[symbol] = (now, trend)
        return trend


async def scan_symbol(exchange: ccxt.okx, htf: HtfCache, symbol: str,
                      app: Application | None, runtime: dict) -> None:
    """Полная проверка одной монеты: данные -> индикаторы -> сигнал -> отправка."""
    df = await fetch_df(exchange, symbol, CONFIG["TIMEFRAME"],
                        CONFIG["CANDLES_LIMIT"])
    df = add_indicators(df)
    trend_1h = await htf.get(symbol)
    last = df.iloc[-1]

    STATE["per_symbol"][symbol] = {
        "close": float(last["close"]),
        "trend": trend_1h,
        "vol_x": (float(last["volume"] / last["vol_sma"])
                  if pd.notna(last["vol_sma"]) and last["vol_sma"] > 0 else 0.0),
    }

    if int(last["timestamp"]) < runtime["cooldown"].get(symbol, 0):
        return

    signal = analyze(df, trend_1h, symbol)
    if signal is None:
        # Уведомление о почти-сигнале (если включено). ЭТО НЕ ВХОД.
        ts = int(last["timestamp"])
        if (CONFIG["SEND_NEAR_MISSES"] and _NEAR_SEEN.get(symbol) == ts
                and runtime["near_notified"].get(symbol) != ts
                and STATE["near_misses"]):
            nm = STATE["near_misses"][0]
            if nm["symbol"] == symbol:
                runtime["near_notified"][symbol] = ts
                await send_signal(app,
                    f"👀 Наблюдение (НЕ вход!): {symbol.split('/')[0]} "
                    f"{nm['side']} — ядро ловушки было, но отбраковано: "
                    f"{nm['reasons']}. Входить по этому НЕЛЬЗЯ.")
        return
    if signal["score"] < CONFIG["MIN_SCORE_TO_SEND"]:
        log.info("%s: сетап %s (%d/12) слабее порога — пропуск.",
                 symbol, signal["side"], signal["score"])
        return
    if int(signal["candle"]["timestamp"]) == runtime["last_sig"].get(symbol, 0):
        return

    balance = await fetch_usdt_balance(exchange)
    plan = build_trade_plan(signal, balance)
    msg = format_message(symbol, signal, plan)
    log.info("СИГНАЛ %s %s (%d/12):\n%s",
             symbol, signal["side"], signal["score"], msg)
    await send_signal(app, msg)
    STATE["signals_sent"] += 1
    STATE["last_signal_text"] = msg
    runtime["last_sig"][symbol] = int(signal["candle"]["timestamp"])
    runtime["cooldown"][symbol] = (
        int(signal["candle"]["timestamp"])
        + CONFIG["SIGNAL_COOLDOWN_CANDLES"] * runtime["tf_ms"]
    )


async def scanner_loop(exchange: ccxt.okx, app: Application | None) -> None:
    htf = HtfCache(exchange)
    runtime = {
        "cooldown": {},        # symbol -> ts, до которого молчим
        "last_sig": {},        # symbol -> ts свечи последнего сигнала
        "near_notified": {},   # symbol -> ts почти-сигнала, о котором уже сообщили
        "tf_ms": exchange.parse_timeframe(CONFIG["TIMEFRAME"]) * 1000,
    }
    symbols: list[str] = []
    symbols_refreshed = 0.0

    while True:
        cycle_start = time.time()
        try:
            # Обновление списка монет раз в SYMBOL_REFRESH_MIN
            if (not symbols or
                    time.time() - symbols_refreshed >
                    CONFIG["SYMBOL_REFRESH_MIN"] * 60):
                symbols = await fetch_top_symbols(exchange)
                symbols_refreshed = time.time()
                STATE["symbols"] = symbols
                # выкидываем из статуса монеты, выпавшие из топа
                for gone in set(STATE["per_symbol"]) - set(symbols):
                    STATE["per_symbol"].pop(gone, None)
                log.info("Сканируем %d монет: %s", len(symbols),
                         ", ".join(s.split("/")[0] for s in symbols))

            now_utc = datetime.now(timezone.utc)
            if not in_trading_hours(now_utc):
                log.info("Вне торговых часов (%02d UTC) — пропуск цикла.",
                         now_utc.hour)
            else:
                # Последовательно: rate-limiter ccxt сам выдерживает паузы
                for sym in symbols:
                    try:
                        await scan_symbol(exchange, htf, sym, app, runtime)
                    except ccxt.BadSymbol:
                        log.warning("%s: символ недоступен, пропуск.", sym)
                    except (ccxt.NetworkError, ccxt.ExchangeError) as e:
                        log.error("%s: ошибка биржи: %s", sym, e)
                        STATE["last_error"] = f"{sym}: {e}"

            STATE["last_cycle"] = now_local_str()
            STATE["cycle_sec"] = time.time() - cycle_start
            log.info("Цикл завершён за %.1f сек (%d монет).",
                     STATE["cycle_sec"], len(symbols))

        except Exception as e:
            STATE["last_error"] = str(e)
            log.exception("Непредвиденная ошибка цикла: %s", e)

        elapsed = time.time() - cycle_start
        await asyncio.sleep(max(5.0, CONFIG["CHECK_INTERVAL_SEC"] - elapsed))


# ============================ MAIN ================================

async def main() -> None:
    STATE["started_at"] = datetime.now(timezone.utc)

    exchange = ccxt.okx({
        "apiKey": CONFIG["OKX_API_KEY"],
        "secret": CONFIG["OKX_API_SECRET"],
        "password": CONFIG["OKX_API_PASSPHRASE"],
        "enableRateLimit": True,
        "options": {"defaultType": "swap"},
    })

    app: Application | None = None
    if CONFIG["TG_BOT_TOKEN"]:
        app = Application.builder().token(CONFIG["TG_BOT_TOKEN"]).build()
        app.add_handler(CommandHandler("start", cmd_start))
        app.add_handler(CommandHandler("status", cmd_status))
        app.add_handler(CommandHandler("ping", cmd_ping))
    else:
        log.warning("TG_BOT_TOKEN не задан — Telegram отключён, только логи.")

    log.info("Бот v3.0 запущен: режим %s, топ-%d монет, ТФ %s",
             CONFIG["SYMBOLS_MODE"], CONFIG["TOP_N_SYMBOLS"], CONFIG["TIMEFRAME"])

    try:
        if app is not None:
            await app.initialize()
            await app.start()
            await app.updater.start_polling(drop_pending_updates=True)
            await send_signal(app,
                f"🤖 Liquidity Trap v3.2 (мультисканер) запущен\n"
                f"Монет: топ-{CONFIG['TOP_N_SYMBOLS']} по обороту OKX | "
                f"ТФ {CONFIG['TIMEFRAME']} + 2-свечная | порог "
                f"{CONFIG['MIN_SCORE_TO_SEND']}/12 | время {CONFIG['TZ_LABEL']}\n"
                f"Вход по качеству: B 🤏 {CONFIG['RISK_BY_GRADE']['B']*100:.1f}% | "
                f"A 💪 {CONFIG['RISK_BY_GRADE']['A']*100:.0f}% | "
                f"A+ ЕБАШ!!! 🔥 {CONFIG['RISK_BY_GRADE']['A+']*100:.0f}%\n"
                f"Команды: /start /status /ping")
        await scanner_loop(exchange, app)

    except (KeyboardInterrupt, asyncio.CancelledError):
        log.info("Остановка...")
    finally:
        if app is not None:
            try:
                await app.updater.stop()
                await app.stop()
                await app.shutdown()
            except Exception:
                pass
        await exchange.close()
        log.info("Бот остановлен.")


if __name__ == "__main__":
    asyncio.run(main())

"""
OKX Liquidity Trap Bot v3.0 — Multi-Coin Scanner (READ-ONLY)
============================================================
Стратегия: снятие ликвидности (sweep свинг-экстремума) + хвост >= 50% +
закрытие обратно за EMA20/VWAP + объём + фильтр тренда 1h + скоринг.

НОВОЕ В 3.0:
  - МУЛЬТИСКАНЕР: бот следит не за одной парой, а за TOP_N_SYMBOLS
    самых ликвидных USDT-свопов OKX (по обороту за 24ч).
    Список обновляется автоматически раз в SYMBOL_REFRESH_MIN минут.
  - Режим STATIC: можно задать свой фиксированный список монет.
  - Кэш тренда 1h на 10 минут (EMA200 меняется медленно) — меньше
    запросов к бирже, нет упора в rate-limit.
  - Пер-символьные кулдауны и защита от дублей.
  - /status показывает сводку по всем монетам.
  - Динамическое форматирование цен (BTC 65432.10, PEPE 0.00001234).

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
from datetime import datetime, timezone

import ccxt.async_support as ccxt
import pandas as pd
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# ============================ НАСТРОЙКИ ============================

CONFIG = {
    # --- Выбор монет ---
    "SYMBOLS_MODE": "auto",        # "auto" = топ по обороту | "static" = свой список
    "TOP_N_SYMBOLS": 15,           # сколько монет сканировать в режиме auto
    "STATIC_SYMBOLS": [            # используется в режиме static или как fallback
        "BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT", "XRP/USDT:USDT",
        "DOGE/USDT:USDT", "ADA/USDT:USDT", "AVAX/USDT:USDT", "LINK/USDT:USDT",
        "LTC/USDT:USDT", "DOT/USDT:USDT", "TON/USDT:USDT", "SUI/USDT:USDT",
        "OP/USDT:USDT", "ARB/USDT:USDT", "PEPE/USDT:USDT",
    ],
    "SYMBOL_REFRESH_MIN": 60,      # как часто обновлять топ-список (мин)
    "MIN_24H_VOLUME_USDT": 20_000_000,  # отсечка неликвида в режиме auto

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
    "VOL_MULTIPLIER": 1.5,
    "VOL_STRONG_MULT": 2.5,
    "TREND_LOOKBACK": 3,
    "SWEEP_LOOKBACK": 20,
    "HTF_EMA_PERIOD": 200,

    # --- Скоринг (обязательный минимум 8, максимум 12) ---
    "MIN_SCORE_TO_SEND": 9,

    # --- Стопы ---
    "ATR_PERIOD": 14,
    "ATR_STOP_MULT": 0.25,
    "RR_RATIO": 2.0,

    # --- Риск и плечо ---
    "RISK_PER_TRADE": 0.03,
    "MAX_RISK_PER_TRADE": 0.05,
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
    "last_signal_text": None,
    "last_error": None,
}


# ==================== ВСПОМОГАТЕЛЬНОЕ =============================

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
    """Топ-N USDT-свопов OKX по обороту за 24ч (режим auto)."""
    if CONFIG["SYMBOLS_MODE"] != "auto":
        return list(CONFIG["STATIC_SYMBOLS"])
    try:
        markets = await exchange.load_markets(True)
        swap_symbols = [
            s for s, m in markets.items()
            if m.get("swap") and m.get("settle") == "USDT"
            and m.get("quote") == "USDT" and m.get("active", True)
        ]
        tickers = await exchange.fetch_tickers(swap_symbols)

        def turnover(t: dict) -> float:
            qv = t.get("quoteVolume")
            if qv:
                return float(qv)
            bv, lp = t.get("baseVolume"), t.get("last")
            return float(bv) * float(lp) if bv and lp else 0.0

        ranked = sorted(tickers.values(), key=turnover, reverse=True)
        top = [t["symbol"] for t in ranked
               if turnover(t) >= CONFIG["MIN_24H_VOLUME_USDT"]]
        top = top[:CONFIG["TOP_N_SYMBOLS"]]
        if not top:
            raise ValueError("пустой топ-список")
        return top
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

def analyze(df: pd.DataFrame, trend_1h: str) -> dict | None:
    """
    Скоринг последней закрытой свечи (макс. 12):
      sweep свинг-экстремума +3 | хвост >=50% +2 | закрытие за EMA и VWAP +2
      объём >=1.5x +1 (>=2.5x +2) | тренд 1h в сторону сделки +2 | контекст 5m +1
    Первые четыре условия обязательны.
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

    def volume_score() -> int:
        if vol_x >= CONFIG["VOL_STRONG_MULT"]:
            return 2
        if vol_x >= CONFIG["VOL_MULTIPLIER"]:
            return 1
        return 0

    # ------------------------- SHORT -------------------------
    sweep_hi = last["high"] > last["swing_high"]
    reject_dn = last["close"] < last["vwap"] and last["close"] < last["ema20"]
    wick_up_ok = upper_wick >= CONFIG["WICK_MIN_RATIO"]
    ctx_up = ((prev["close"] > prev["ema20"]) & (prev["close"] > prev["vwap"])).all()

    if sweep_hi and wick_up_ok and reject_dn and volume_score() >= 1:
        score = 3 + 2 + 2 + volume_score()
        score += 2 if trend_1h == "down" else 0
        score += 1 if ctx_up else 0
        return {"side": "SHORT", "candle": last, "vol_x": vol_x,
                "score": score, "trend_1h": trend_1h,
                "swept": float(last["swing_high"])}

    # ------------------------- LONG --------------------------
    sweep_lo = last["low"] < last["swing_low"]
    reject_up = last["close"] > last["vwap"] and last["close"] > last["ema20"]
    wick_dn_ok = lower_wick >= CONFIG["WICK_MIN_RATIO"]
    ctx_dn = ((prev["close"] < prev["ema20"]) & (prev["close"] < prev["vwap"])).all()

    if sweep_lo and wick_dn_ok and reject_up and volume_score() >= 1:
        score = 3 + 2 + 2 + volume_score()
        score += 2 if trend_1h == "up" else 0
        score += 1 if ctx_dn else 0
        return {"side": "LONG", "candle": last, "vol_x": vol_x,
                "score": score, "trend_1h": trend_1h,
                "swept": float(last["swing_low"])}

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

    if signal["side"] == "SHORT":
        stop = float(c["high"]) + atr_buf
        risk_per_unit = stop - entry
        take = entry - risk_per_unit * rr
    else:
        stop = float(c["low"]) - atr_buf
        risk_per_unit = entry - stop
        take = entry + risk_per_unit * rr

    stop_pct = risk_per_unit / entry
    if stop_pct <= 0:
        raise ValueError("Стоп <= 0")

    risk = min(CONFIG["RISK_PER_TRADE"], CONFIG["MAX_RISK_PER_TRADE"])
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
        "entry": entry, "stop": stop, "take": take,
        "stop_pct": stop_pct, "position_usdt": position_usdt,
        "margin": margin, "leverage": lev, "liq": liq,
        "liq_safe": liq_safe, "capped": capped,
        "actual_risk_pct": actual_risk_pct, "balance": balance,
    }


# ==================== TELEGRAM: СООБЩЕНИЯ =========================

def grade(score: int) -> str:
    return "A+ 🔥" if score >= 11 else ("A" if score >= 9 else "B")


def format_message(symbol: str, signal: dict, plan: dict) -> str:
    c = signal["candle"]
    head = "🔴 СЕТАП В ШОРТ" if signal["side"] == "SHORT" else "🟢 СЕТАП В ЛОНГ"
    lines = [
        f"{head}  |  Качество: {grade(signal['score'])} ({signal['score']}/12)",
        f"Монета: {symbol} (ТФ: {CONFIG['TIMEFRAME']})",
        f"Снята ликвидность у: {fmt_price(signal['swept'])} | "
        f"Тренд 1h: {signal['trend_1h']}",
        f"✅ Вход: {fmt_price(plan['entry'])}",
        f"🛑 Стоп-лосс: {fmt_price(plan['stop'])} ({plan['stop_pct']*100:.2f}%)",
        f"🎯 Тейк-профит: {fmt_price(plan['take'])} (RR 1:{CONFIG['RR_RATIO']:.0f})",
        f"📊 Рекомендуемый объем: {plan['position_usdt']:.2f} USDT "
        f"(Риск {plan['actual_risk_pct']*100:.1f}% от депозита)",
        f"💼 Маржа при {plan['leverage']}x: {plan['margin']:.2f} USDT "
        f"из {plan['balance']:.2f}",
        f"⚠️ Ликвидация ~{fmt_price(plan['liq'])} (изолир., оценка)",
        f"📈 Объем свечи: x{signal['vol_x']:.2f} от среднего",
        f"🕒 Свеча: {c['dt'].strftime('%Y-%m-%d %H:%M UTC')}",
    ]
    if plan["capped"]:
        lines.append("❗ Позиция урезана лимитом маржи (90% депозита) — "
                     "фактический риск ниже заданного.")
    if not plan["liq_safe"]:
        lines.append("🚨 ОПАСНО: ликвидация БЛИЖЕ стоп-лосса. "
                     "Уменьшите объём или плечо — сделку так брать нельзя.")
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
    await update.message.reply_text(
        "🤖 Liquidity Trap Bot v3.0 — мультисканер (read-only)\n\n"
        f"Монет в работе: {len(syms)} ({CONFIG['SYMBOLS_MODE']})\n"
        f"{coins}\n\n"
        f"ТФ: {CONFIG['TIMEFRAME']} | Фильтр тренда: "
        f"EMA{CONFIG['HTF_EMA_PERIOD']} {CONFIG['HTF_TIMEFRAME']}\n"
        f"Мин. качество сигнала: {CONFIG['MIN_SCORE_TO_SEND']}/12\n"
        f"Риск: {CONFIG['RISK_PER_TRADE']*100:.0f}% | "
        f"Плечо для расчётов: {CONFIG['LEVERAGE']}x\n\n"
        "Команды:\n/status — сводка по всем монетам\n/ping — проверка связи\n\n"
        "Бот сам пришлёт сообщение при сетапе. Он НЕ торгует."
    )


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    up = "-"
    if STATE["started_at"]:
        d = datetime.now(timezone.utc) - STATE["started_at"]
        up = f"{d.days}д {d.seconds // 3600}ч {(d.seconds % 3600) // 60}м"
    lines = [
        "📡 Статус мультисканера",
        f"Аптайм: {up} | Сигналов: {STATE['signals_sent']}",
        f"Последний цикл: {STATE['last_cycle'] or 'ещё не было'} "
        f"({STATE['cycle_sec']:.0f} сек, монет: {len(STATE['symbols'])})",
    ]
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

    signal = analyze(df, trend_1h)
    if signal is None:
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
        "cooldown": {},   # symbol -> ts, до которого молчим
        "last_sig": {},   # symbol -> ts свечи последнего сигнала
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

            STATE["last_cycle"] = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
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
                f"🤖 Liquidity Trap v3.0 (мультисканер) запущен\n"
                f"Монет: топ-{CONFIG['TOP_N_SYMBOLS']} по обороту OKX | "
                f"ТФ {CONFIG['TIMEFRAME']} | мин. качество "
                f"{CONFIG['MIN_SCORE_TO_SEND']}/12\n"
                f"Команды: /status /ping")
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

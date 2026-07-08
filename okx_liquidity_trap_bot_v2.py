"""
OKX Liquidity Trap Bot v2.1 — "Sniper Edition" (READ-ONLY)
==========================================================
Стратегия: снятие ликвидности (sweep свинг-экстремума) + хвост >= 50% +
закрытие обратно за EMA20/VWAP + объём + фильтр тренда 1h + скоринг сигналов.

Бот НЕ торгует. Только сигналы в Telegram.

НОВОЕ В 2.2:
  - pandas_ta УДАЛЁН: на PyPI не осталось версий для Python 3.11
    (0.4.x требует Python >= 3.12), поэтому EMA/SMA/ATR считаются
    чистым pandas — те же формулы, ноль лишних зависимостей.
  - Команды в Telegram: /start, /status, /ping (добавлены в 2.1).

Зависимости (requirements.txt):
  pandas, ccxt, python-telegram-bot>=21

Переменные окружения:
  OKX_API_KEY, OKX_API_SECRET, OKX_API_PASSPHRASE (только чтение!)
  TG_BOT_TOKEN, TG_CHAT_ID
"""

import asyncio
import logging
import os
from datetime import datetime, timezone

import ccxt.async_support as ccxt
import pandas as pd
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# ============================ НАСТРОЙКИ ============================

CONFIG = {
    # --- Рынок ---
    "SYMBOL": "BTC/USDT:USDT",
    "TIMEFRAME": "5m",
    "HTF_TIMEFRAME": "1h",
    "CANDLES_LIMIT": 320,
    "HTF_CANDLES_LIMIT": 260,

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
    "RISK_PER_TRADE": 0.03,        # 3% на сделку (агрессивно)
    "MAX_RISK_PER_TRADE": 0.05,    # жёсткий потолок
    "LEVERAGE": 20,
    "MAX_MARGIN_SHARE": 0.90,
    "MMR": 0.005,

    # --- Фильтр времени (UTC), None = всегда ---
    "TRADING_HOURS_UTC": range(6, 22),

    # --- Цикл ---
    "CHECK_INTERVAL_SEC": 60,
    "SIGNAL_COOLDOWN_CANDLES": 3,

    # --- Ключи ---
    "OKX_API_KEY": os.getenv("OKX_API_KEY", ""),
    "OKX_API_SECRET": os.getenv("OKX_API_SECRET", ""),
    "OKX_API_PASSPHRASE": os.getenv("OKX_API_PASSPHRASE", ""),
    "TG_BOT_TOKEN": os.getenv("TG_BOT_TOKEN", ""),
    "TG_CHAT_ID": os.getenv("TG_CHAT_ID", ""),
    "FALLBACK_BALANCE_USDT": 200.0,
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
logging.getLogger("httpx").setLevel(logging.WARNING)  # тише логи PTB
log = logging.getLogger("liq-trap-v2")

# Живое состояние для /status
STATE = {
    "started_at": None,
    "last_check": None,
    "last_close": None,
    "trend_1h": "?",
    "vol_x": 0.0,
    "signals_sent": 0,
    "last_signal_text": None,
    "last_error": None,
}


# ======================= ЗАГРУЗКА ДАННЫХ ==========================

async def fetch_df(exchange: ccxt.okx, timeframe: str, limit: int) -> pd.DataFrame:
    """Свечи -> DataFrame только с ЗАКРЫТЫМИ свечами."""
    ohlcv = await exchange.fetch_ohlcv(CONFIG["SYMBOL"], timeframe, limit=limit)
    df = pd.DataFrame(
        ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"]
    )
    df["dt"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    return df.iloc[:-1].reset_index(drop=True)


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
# Собственные реализации вместо pandas_ta (формулы идентичны):

def ema(series: pd.Series, length: int) -> pd.Series:
    """Экспоненциальная скользящая средняя."""
    return series.ewm(span=length, adjust=False, min_periods=length).mean()


def sma(series: pd.Series, length: int) -> pd.Series:
    """Простая скользящая средняя."""
    return series.rolling(length, min_periods=length).mean()


def atr(high: pd.Series, low: pd.Series, close: pd.Series,
        length: int) -> pd.Series:
    """Average True Range по Уайлдеру (RMA-сглаживание, как в pandas_ta/TradingView)."""
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


def format_message(signal: dict, plan: dict) -> str:
    c = signal["candle"]
    head = "🔴 СЕТАП В ШОРТ" if signal["side"] == "SHORT" else "🟢 СЕТАП В ЛОНГ"
    lines = [
        f"{head}  |  Качество: {grade(signal['score'])} ({signal['score']}/12)",
        f"Монета: {CONFIG['SYMBOL']} (ТФ: {CONFIG['TIMEFRAME']})",
        f"Снята ликвидность у: {signal['swept']:.2f} | Тренд 1h: {signal['trend_1h']}",
        f"✅ Вход: {plan['entry']:.2f}",
        f"🛑 Стоп-лосс: {plan['stop']:.2f} ({plan['stop_pct']*100:.2f}%)",
        f"🎯 Тейк-профит: {plan['take']:.2f} (RR 1:{CONFIG['RR_RATIO']:.0f})",
        f"📊 Рекомендуемый объем: {plan['position_usdt']:.2f} USDT "
        f"(Риск {plan['actual_risk_pct']*100:.1f}% от депозита)",
        f"💼 Маржа при {plan['leverage']}x: {plan['margin']:.2f} USDT "
        f"из {plan['balance']:.2f}",
        f"⚠️ Ликвидация ~{plan['liq']:.2f} (изолир., оценка)",
        f"📈 Объем свечи: {float(c['volume']):.2f} (x{signal['vol_x']:.2f} от среднего)",
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
    await update.message.reply_text(
        "🤖 Liquidity Trap Bot v2.1 (read-only сканер)\n\n"
        f"Пара: {CONFIG['SYMBOL']} | ТФ: {CONFIG['TIMEFRAME']}\n"
        f"Фильтр тренда: EMA{CONFIG['HTF_EMA_PERIOD']} на {CONFIG['HTF_TIMEFRAME']}\n"
        f"Мин. качество сигнала: {CONFIG['MIN_SCORE_TO_SEND']}/12\n"
        f"Риск на сделку: {CONFIG['RISK_PER_TRADE']*100:.0f}% | "
        f"Плечо для расчётов: {CONFIG['LEVERAGE']}x\n\n"
        "Команды:\n/status — что бот видит сейчас\n/ping — проверка связи\n\n"
        "Бот сам пришлёт сообщение, когда найдёт сетап. Он НЕ торгует."
    )


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    up = "-"
    if STATE["started_at"]:
        delta = datetime.now(timezone.utc) - STATE["started_at"]
        up = f"{delta.days}д {delta.seconds // 3600}ч {(delta.seconds % 3600) // 60}м"
    txt = (
        "📡 Статус сканера\n"
        f"Аптайм: {up}\n"
        f"Последняя проверка: {STATE['last_check'] or 'ещё не было'}\n"
        f"Цена (close): {STATE['last_close'] or '-'}\n"
        f"Тренд 1h: {STATE['trend_1h']}\n"
        f"Объём последней свечи: x{STATE['vol_x']:.2f} от среднего\n"
        f"Сигналов отправлено: {STATE['signals_sent']}\n"
    )
    if STATE["last_error"]:
        txt += f"⚠️ Последняя ошибка: {STATE['last_error']}\n"
    if STATE["last_signal_text"]:
        txt += f"\nПоследний сигнал:\n{STATE['last_signal_text']}"
    await update.message.reply_text(txt)


async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("🏓 pong — бот жив.")


# ======================== СКАНЕР (ФОНОВЫЙ ЦИКЛ) ===================

def in_trading_hours(dt_utc) -> bool:
    hours = CONFIG["TRADING_HOURS_UTC"]
    return True if hours is None else dt_utc.hour in hours


async def scanner_loop(exchange: ccxt.okx, app: Application | None) -> None:
    last_signal_ts = 0
    cooldown_until_ts = 0
    tf_ms = exchange.parse_timeframe(CONFIG["TIMEFRAME"]) * 1000

    while True:
        try:
            df, df_htf = await asyncio.gather(
                fetch_df(exchange, CONFIG["TIMEFRAME"], CONFIG["CANDLES_LIMIT"]),
                fetch_df(exchange, CONFIG["HTF_TIMEFRAME"], CONFIG["HTF_CANDLES_LIMIT"]),
            )
            df = add_indicators(df)
            trend_1h = htf_trend(df_htf)
            last = df.iloc[-1]

            STATE["last_check"] = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
            STATE["last_close"] = f"{last['close']:.2f}"
            STATE["trend_1h"] = trend_1h
            STATE["vol_x"] = (last["volume"] / last["vol_sma"]
                              if last["vol_sma"] and last["vol_sma"] > 0 else 0.0)
            STATE["last_error"] = None

            if not in_trading_hours(last["dt"]):
                log.info("Вне торговых часов (%s UTC) — пропуск.", last["dt"].hour)
            elif int(last["timestamp"]) < cooldown_until_ts:
                log.info("Кулдаун после сигнала — пропуск.")
            else:
                signal = analyze(df, trend_1h)
                if signal is None:
                    log.info("Сигнала нет. Close=%.2f | 1h: %s | Vol x%.2f",
                             last["close"], trend_1h, STATE["vol_x"])
                elif signal["score"] < CONFIG["MIN_SCORE_TO_SEND"]:
                    log.info("Сетап %s (%d/12) слабее порога %d — пропуск.",
                             signal["side"], signal["score"],
                             CONFIG["MIN_SCORE_TO_SEND"])
                elif int(signal["candle"]["timestamp"]) == last_signal_ts:
                    pass
                else:
                    balance = await fetch_usdt_balance(exchange)
                    plan = build_trade_plan(signal, balance)
                    msg = format_message(signal, plan)
                    log.info("СИГНАЛ %s (%d/12):\n%s",
                             signal["side"], signal["score"], msg)
                    await send_signal(app, msg)
                    STATE["signals_sent"] += 1
                    STATE["last_signal_text"] = msg
                    last_signal_ts = int(signal["candle"]["timestamp"])
                    cooldown_until_ts = last_signal_ts + \
                        CONFIG["SIGNAL_COOLDOWN_CANDLES"] * tf_ms

        except ccxt.NetworkError as e:
            STATE["last_error"] = f"NetworkError: {e}"
            log.error("Сетевая ошибка: %s", e)
        except ccxt.ExchangeError as e:
            STATE["last_error"] = f"ExchangeError: {e}"
            log.error("Ошибка биржи: %s", e)
        except Exception as e:
            STATE["last_error"] = str(e)
            log.exception("Непредвиденная ошибка: %s", e)

        await asyncio.sleep(CONFIG["CHECK_INTERVAL_SEC"])


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

    log.info("Бот v2.1 запущен: %s %s | плечо %dx | риск %.0f%%",
             CONFIG["SYMBOL"], CONFIG["TIMEFRAME"],
             CONFIG["LEVERAGE"], CONFIG["RISK_PER_TRADE"] * 100)

    try:
        if app is not None:
            # Ручной жизненный цикл PTB, чтобы сканер работал параллельно с polling
            await app.initialize()
            await app.start()
            await app.updater.start_polling(drop_pending_updates=True)
            await send_signal(app,
                f"🤖 Liquidity Trap v2.1 запущен\n"
                f"{CONFIG['SYMBOL']} | {CONFIG['TIMEFRAME']} | "
                f"мин. качество: {CONFIG['MIN_SCORE_TO_SEND']}/12\n"
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

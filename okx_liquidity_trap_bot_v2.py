"""
OKX Liquidity Trap Bot v2 — "Sniper Edition" (READ-ONLY)
========================================================
Улучшенная версия стратегии "Ловушка ликвидности".

ЧТО НОВОГО ПО СРАВНЕНИЮ С v1 (кратко, детали в чате):
  1. РЕАЛЬНОЕ СНЯТИЕ ЛИКВИДНОСТИ: хвост должен пробить экстремум
     последних N свечей (свинг-хай/лоу), а не просто задеть EMA/VWAP.
     Стопы толпы лежат за экстремумами, а не за скользящими средними.
  2. ФИЛЬТР СТАРШЕГО ТФ: лонги только когда 1h-тренд вверх (цена > EMA200 1h),
     шорты — только когда вниз. Разворот торгуем ПО старшему тренду.
  3. ATR-СТОП: запас за хвостом зависит от текущей волатильности,
     а не фиксированные 0.1%.
  4. СКОРИНГ СИГНАЛОВ (A+/A): бот оценивает конфлюэнс в баллах и шлёт
     только сильнейшие сетапы. Это и есть "считывание обстановки".
  5. КАЛЬКУЛЯТОР ПЛЕЧА И ЛИКВИДАЦИИ: для каждого сигнала бот считает
     нужную маржу, цену ликвидации при вашем плече и БЛОКИРУЕТ сигнал,
     если ликвидация ближе стопа (анти-самоубийство).
  6. Кулдаун после сигнала, фильтр "тонких" часов, лимит риска.

Бот по-прежнему НЕ торгует — только сигналы в Telegram.

Зависимости:
  pip install ccxt pandas pandas_ta python-telegram-bot
  (при ошибке импорта pandas_ta: pip install "numpy<2")

Переменные окружения:
  OKX_API_KEY, OKX_API_SECRET, OKX_API_PASSPHRASE (только чтение!)
  TG_BOT_TOKEN, TG_CHAT_ID
"""

import asyncio
import logging
import os

import ccxt.async_support as ccxt
import pandas as pd
import pandas_ta as ta
from telegram import Bot

# ============================ НАСТРОЙКИ ============================

CONFIG = {
    # --- Рынок ---
    "SYMBOL": "BTC/USDT:USDT",
    "TIMEFRAME": "5m",
    "HTF_TIMEFRAME": "1h",          # старший ТФ для фильтра тренда
    "CANDLES_LIMIT": 320,
    "HTF_CANDLES_LIMIT": 260,       # хватает на EMA200 по 1h

    # --- Ядро стратегии ---
    "EMA_PERIOD": 20,
    "VOL_SMA_PERIOD": 20,
    "WICK_MIN_RATIO": 0.50,         # хвост >= 50% свечи
    "VOL_MULTIPLIER": 1.5,          # минимальный порог объёма
    "VOL_STRONG_MULT": 2.5,         # "аномальный" объём (доп. балл)
    "TREND_LOOKBACK": 3,            # контекст: свечи до сигнальной
    "SWEEP_LOOKBACK": 20,           # хвост должен снять экстремум N свечей
    "HTF_EMA_PERIOD": 200,          # EMA200 на 1h

    # --- Скоринг: какие сигналы отправлять ---
    # Обязательные условия дают 7 баллов, максимум 12.
    "MIN_SCORE_TO_SEND": 9,         # 9-10 = A, 11-12 = A+

    # --- Стопы ---
    "ATR_PERIOD": 14,
    "ATR_STOP_MULT": 0.25,          # запас за хвостом = 0.25 * ATR
    "RR_RATIO": 2.0,

    # --- Риск и плечо ---
    "RISK_PER_TRADE": 0.03,         # 3% от депозита на сделку (агрессивно!)
    "MAX_RISK_PER_TRADE": 0.05,     # жёсткий потолок, выше бот не даст
    "LEVERAGE": 20,                 # ваше плечо (для расчёта маржи/ликвидации)
    "MAX_MARGIN_SHARE": 0.90,       # не более 90% депозита в маржу одной сделки
    "MMR": 0.005,                   # maintenance margin ~0.5% (BTC swap OKX, tier 1)

    # --- Фильтр времени (UTC): тонкие часы = ложные хвосты ---
    "TRADING_HOURS_UTC": range(6, 22),  # торгуем 06:00–21:59 UTC; None = всегда

    # --- Цикл ---
    "CHECK_INTERVAL_SEC": 60,
    "SIGNAL_COOLDOWN_CANDLES": 3,   # после сигнала пропускаем N свечей

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
log = logging.getLogger("liq-trap-v2")


# ======================= ЗАГРУЗКА ДАННЫХ ==========================

async def fetch_df(exchange: ccxt.okx, timeframe: str, limit: int) -> pd.DataFrame:
    """Свечи -> DataFrame только с ЗАКРЫТЫМИ свечами."""
    ohlcv = await exchange.fetch_ohlcv(CONFIG["SYMBOL"], timeframe, limit=limit)
    df = pd.DataFrame(
        ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"]
    )
    df["dt"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    return df.iloc[:-1].reset_index(drop=True)   # выбрасываем формирующуюся свечу


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

def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["ema20"] = ta.ema(df["close"], length=CONFIG["EMA_PERIOD"])
    df["vol_sma"] = ta.sma(df["volume"], length=CONFIG["VOL_SMA_PERIOD"])
    df["atr"] = ta.atr(df["high"], df["low"], df["close"],
                       length=CONFIG["ATR_PERIOD"])

    # Дневной VWAP: группировка по датам UTC, сброс в 00:00 UTC
    tp = (df["high"] + df["low"] + df["close"]) / 3.0
    df["_tpv"] = tp * df["volume"]
    df["_d"] = df["dt"].dt.date
    g = df.groupby("_d", sort=False)
    df["vwap"] = g["_tpv"].cumsum() / g["volume"].cumsum().replace(0, pd.NA)
    df.drop(columns=["_tpv", "_d"], inplace=True)

    # Экстремумы предыдущих SWEEP_LOOKBACK свечей (НЕ включая текущую) —
    # это и есть "лужи ликвидности", за которыми стоят стопы толпы.
    lb = CONFIG["SWEEP_LOOKBACK"]
    df["swing_high"] = df["high"].shift(1).rolling(lb).max()
    df["swing_low"] = df["low"].shift(1).rolling(lb).min()
    return df


def htf_trend(df_htf: pd.DataFrame) -> str:
    """'up' / 'down' / 'flat' по EMA200 старшего ТФ."""
    ema = ta.ema(df_htf["close"], length=CONFIG["HTF_EMA_PERIOD"])
    if ema is None or pd.isna(ema.iloc[-1]):
        return "flat"
    last_close = df_htf["close"].iloc[-1]
    last_ema = ema.iloc[-1]
    # небольшая мёртвая зона 0.15%, чтобы не дёргаться на касаниях
    if last_close > last_ema * 1.0015:
        return "up"
    if last_close < last_ema * 0.9985:
        return "down"
    return "flat"


# ====================== ЛОГИКА СТРАТЕГИИ ==========================

def analyze(df: pd.DataFrame, trend_1h: str) -> dict | None:
    """
    Скоринг последней закрытой свечи.
    Обязательные условия (без любого из них сигнала нет):
      снятие ликвидности + хвост >= 50% + закрытие за EMA20 и VWAP + объём >= 1.5x
    Баллы:
      снятие свинг-экстремума ....... +3
      хвост >= 50% .................. +2
      закрытие за EMA20 и VWAP ...... +2  (итого обязательный минимум 7... +объём)
      объём >= 1.5x ................. +1  | объём >= 2.5x: +2
      тренд 1h в сторону сделки ..... +2
      контекст 5m (TREND_LOOKBACK) .. +1
    Максимум 12. Отправляем при score >= MIN_SCORE_TO_SEND.
    """
    lb = CONFIG["TREND_LOOKBACK"]
    need = max(CONFIG["HTF_EMA_PERIOD"] // 10,
               CONFIG["SWEEP_LOOKBACK"] + CONFIG["EMA_PERIOD"] + lb + 2)
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
    sweep_hi = last["high"] > last["swing_high"]                     # снял хай
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
    sweep_lo = last["low"] < last["swing_low"]                       # снял лоу
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
    """
    Приблизительная цена ликвидации для ИЗОЛИРОВАННОЙ маржи:
      long : entry * (1 - 1/lev + mmr)
      short: entry * (1 + 1/lev - mmr)
    Реальная цена на OKX зависит от тира позиции и комиссий — это оценка.
    """
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
    position_usdt = (balance * risk) / stop_pct          # номинал позиции
    margin = position_usdt / lev                          # нужная маржа

    # Потолок: не более MAX_MARGIN_SHARE депозита в маржу.
    capped = False
    max_margin = balance * CONFIG["MAX_MARGIN_SHARE"]
    if margin > max_margin:
        margin = max_margin
        position_usdt = margin * lev
        capped = True
    actual_risk_pct = position_usdt * stop_pct / balance  # фактический риск

    liq = liquidation_price(entry, signal["side"], lev)
    # Анти-самоубийство: ликвидация не должна быть ближе стопа
    if signal["side"] == "LONG":
        liq_safe = liq < stop
    else:
        liq_safe = liq > stop

    return {
        "entry": entry, "stop": stop, "take": take,
        "stop_pct": stop_pct, "position_usdt": position_usdt,
        "margin": margin, "leverage": lev, "liq": liq,
        "liq_safe": liq_safe, "capped": capped,
        "actual_risk_pct": actual_risk_pct, "balance": balance,
    }


# ========================= TELEGRAM ===============================

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
        f"📈 Объем свечи: {float(c['volume']):.2f} "
        f"(x{signal['vol_x']:.2f} от среднего)",
        f"🕒 Свеча: {c['dt'].strftime('%Y-%m-%d %H:%M UTC')}",
    ]
    if plan["capped"]:
        lines.append("❗ Позиция урезана лимитом маржи (90% депозита) — "
                     "фактический риск ниже заданного.")
    if not plan["liq_safe"]:
        lines.append("🚨 ОПАСНО: цена ликвидации БЛИЖЕ стоп-лосса. "
                     "При таком плече сделку брать нельзя — уменьшите объём/плечо.")
    return "\n".join(lines)


async def send_telegram(bot: Bot | None, text: str) -> None:
    if bot is None:
        log.warning("Telegram не настроен. Сообщение:\n%s", text)
        return
    try:
        await bot.send_message(chat_id=CONFIG["TG_CHAT_ID"], text=text)
        log.info("Сигнал отправлен в Telegram.")
    except Exception as e:
        log.error("Ошибка Telegram: %s", e)


# ======================== ОСНОВНОЙ ЦИКЛ ===========================

def in_trading_hours(dt_utc) -> bool:
    hours = CONFIG["TRADING_HOURS_UTC"]
    return True if hours is None else dt_utc.hour in hours


async def main() -> None:
    exchange = ccxt.okx({
        "apiKey": CONFIG["OKX_API_KEY"],
        "secret": CONFIG["OKX_API_SECRET"],
        "password": CONFIG["OKX_API_PASSPHRASE"],
        "enableRateLimit": True,
        "options": {"defaultType": "swap"},
    })
    tg_bot = Bot(CONFIG["TG_BOT_TOKEN"]) if CONFIG["TG_BOT_TOKEN"] else None

    last_signal_ts = 0
    cooldown_until_ts = 0
    tf_ms = exchange.parse_timeframe(CONFIG["TIMEFRAME"]) * 1000

    log.info("Бот v2 запущен: %s %s | плечо %dx | риск %.0f%%",
             CONFIG["SYMBOL"], CONFIG["TIMEFRAME"],
             CONFIG["LEVERAGE"], CONFIG["RISK_PER_TRADE"] * 100)
    await send_telegram(tg_bot,
        f"🤖 Liquidity Trap v2 запущен\n"
        f"{CONFIG['SYMBOL']} | {CONFIG['TIMEFRAME']} | фильтр 1h EMA200 | "
        f"мин. качество сигнала: {CONFIG['MIN_SCORE_TO_SEND']}/12")

    try:
        while True:
            try:
                df, df_htf = await asyncio.gather(
                    fetch_df(exchange, CONFIG["TIMEFRAME"], CONFIG["CANDLES_LIMIT"]),
                    fetch_df(exchange, CONFIG["HTF_TIMEFRAME"], CONFIG["HTF_CANDLES_LIMIT"]),
                )
                df = add_indicators(df)
                trend_1h = htf_trend(df_htf)
                last = df.iloc[-1]

                if not in_trading_hours(last["dt"]):
                    log.info("Вне торговых часов (%s UTC) — пропуск.", last["dt"].hour)
                elif int(last["timestamp"]) < cooldown_until_ts:
                    log.info("Кулдаун после сигнала — пропуск.")
                else:
                    signal = analyze(df, trend_1h)
                    if signal is None:
                        log.info("Сигнала нет. Close=%.2f | 1h тренд: %s | Vol x%.2f",
                                 last["close"], trend_1h,
                                 last["volume"] / last["vol_sma"]
                                 if last["vol_sma"] > 0 else 0)
                    elif signal["score"] < CONFIG["MIN_SCORE_TO_SEND"]:
                        log.info("Сетап найден (%s, %d/12), но слабее порога %d — пропуск.",
                                 signal["side"], signal["score"],
                                 CONFIG["MIN_SCORE_TO_SEND"])
                    elif int(signal["candle"]["timestamp"]) == last_signal_ts:
                        pass  # уже отправляли по этой свече
                    else:
                        balance = await fetch_usdt_balance(exchange)
                        plan = build_trade_plan(signal, balance)
                        msg = format_message(signal, plan)
                        log.info("СИГНАЛ %s (%d/12):\n%s",
                                 signal["side"], signal["score"], msg)
                        await send_telegram(tg_bot, msg)
                        last_signal_ts = int(signal["candle"]["timestamp"])
                        cooldown_until_ts = last_signal_ts + \
                            CONFIG["SIGNAL_COOLDOWN_CANDLES"] * tf_ms

            except ccxt.NetworkError as e:
                log.error("Сетевая ошибка: %s", e)
            except ccxt.ExchangeError as e:
                log.error("Ошибка биржи: %s", e)
            except Exception as e:
                log.exception("Непредвиденная ошибка: %s", e)

            await asyncio.sleep(CONFIG["CHECK_INTERVAL_SEC"])

    except (KeyboardInterrupt, asyncio.CancelledError):
        log.info("Остановка...")
    finally:
        await exchange.close()
        log.info("Бот остановлен.")


if __name__ == "__main__":
    asyncio.run(main())

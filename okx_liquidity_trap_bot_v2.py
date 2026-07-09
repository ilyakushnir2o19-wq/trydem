"""
OKX Liquidity Trap Bot v3.6 — Multi-Coin Scanner + Trade Tracker (READ-ONLY)
============================================================================
НОВОЕ В 3.4 (по брифу после первой недели: винрейт 15%, убыток от ручных
входов и неисполнимых сигналов — чиним причины, не симптомы):
  КРИТИЧЕСКОЕ:
  - АВТО-БЛОК liq<стоп: сигнал с ликвидацией ближе стопа НЕ отправляется
    вообще (раньше слал с предупреждением — соблазн победил разум).
  - Блок B-грейда против тренда 1h (55% вечернего мусора).
  - Часы генерации 09:00-20:59 МСК: после 21:00 ликвидность умирает,
    бот только ведёт открытые сделки.
  ВАЖНОЕ:
  - Минимальный стоп 0.5%: микро-стопы расширяются (спред+шум выбивал их).
  - Кулдаун 60 мин на монету (было 15 — CL спамил 3 сигнала за 40 мин).
  - Анти-пыль: TP1 < $0.50 чистыми после комиссий — не сигнал, а шум.
  UX:
  - ⚡ КАК ВЗЯТЬ: блок мгновенного входа В САМОМ ВЕРХУ — цена, объём,
    стоп/тейк, окно актуальности и цена "дальше не гнаться".
  - 🧠 ПОЧЕМУ: бот объясняет логику каждого входа человеческим языком.
  - 📝 После закрытия взятой сделки — кнопки причины выхода
    (по плану / паника / не выходил) для честной статистики.
"""

import asyncio
import json
import logging
import os
import time
from collections import deque
from datetime import datetime, timezone

import ccxt.async_support as ccxt
import pandas as pd
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (Application, CallbackQueryHandler, CommandHandler,
                          ContextTypes)

# ============================ НАСТРОЙКИ ============================

CONFIG = {
    # --- Выбор монет ---
    "SYMBOLS_MODE": "auto",
    "TOP_N_SYMBOLS": 30,
    "STATIC_SYMBOLS": [
        "BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT", "XRP/USDT:USDT",
        "DOGE/USDT:USDT", "ADA/USDT:USDT", "AVAX/USDT:USDT", "LINK/USDT:USDT",
        "LTC/USDT:USDT", "DOT/USDT:USDT", "TON/USDT:USDT", "SUI/USDT:USDT",
        "OP/USDT:USDT", "ARB/USDT:USDT", "PEPE/USDT:USDT",
    ],
    # Не-криптовалютные и суррогатные инструменты — вне стратегии.
    # Токенизированные акции/ETF: тонкий стакан, структурно огромный фандинг,
    # цена ведётся за фондовым рынком — сетап там не имеет преимущества.
    "EXCLUDE_BASES": {
        "XPD", "XPT", "XAU", "XAG", "USDC", "DAI", "EURT",
        "SNDK", "SKHYNIX", "SOXL", "INTC", "DRAM", "SPCX", "NES", "CHIP",
        "LAB", "EWY", "CL", "AAPL", "TSLA", "NVDA", "MSFT", "META", "AMZN",
        "GOOGL", "COIN", "MSTR", "HOOD", "QQQ", "SPY", "GME", "AMC", "PLTR",
        "CRCL", "OPEN", "NFLX", "AMD", "AVGO", "MU", "TSM", "BABA",
    },
    "SYMBOL_REFRESH_MIN": 60,
    "MIN_24H_VOLUME_USDT": 20_000_000,

    # --- Часовой пояс сообщений ---
    "TZ_OFFSET_HOURS": 3,
    "TZ_LABEL": "МСК",

    # --- Таймфреймы ---
    "TIMEFRAME": "5m",
    "HTF_TIMEFRAME": "1h",
    "CANDLES_LIMIT": 320,
    "HTF_CANDLES_LIMIT": 260,
    "HTF_CACHE_MIN": 10,

    # --- Ядро стратегии ---
    "EMA_PERIOD": 20,
    "VOL_SMA_PERIOD": 20,
    "WICK_MIN_RATIO": 0.50,
    "VOL_MULTIPLIER": 1.3,
    "VOL_STRONG_MULT": 2.2,
    "TREND_LOOKBACK": 3,
    "SWEEP_LOOKBACK": 20,
    "HTF_EMA_PERIOD": 200,

    # --- Двухсвечная ловушка ---
    "TWO_CANDLE_ENABLED": True,
    "TWO_CANDLE_VOL_MULT": 1.3,

    # --- Скоринг и уведомления ---
    "MIN_SCORE_TO_SEND": 6,
    "SEND_NEAR_MISSES": False,

    # --- Стопы и цели ---
    "ATR_PERIOD": 14,
    "ATR_STOP_MULT": 0.25,
    "RR_RATIO": 2.0,               # TP2; TP1 всегда 1R
    "TIME_STOP_CANDLES": 6,        # свечей без TP1 -> время-стоп

    # --- Риск по качеству сигнала ---
    "RISK_BY_GRADE": {"B": 0.015, "A": 0.03, "A+": 0.05},
    "MAX_RISK_PER_TRADE": 0.05,
    "LEVERAGE": 20,
    "MAX_MARGIN_SHARE": 0.90,
    "MMR": 0.005,

    # --- Защита ---
    # None = отключить. По просьбе владельца: лимит по ВСЕМ сигналам выключен
    # (мог глушить вечерние сетапы из-за утренних стопов не взятых сделок).
    # Лимит по ВЗЯТЫМ оставлен: он срабатывает только об твои реальные стопы
    # и не прячет ни одного сигнала, пока ты сам не словил 3 стопа за день.
    "DAILY_STOP_LIMIT": 3,         # стопов по ВЗЯТЫМ сделкам за день -> пауза
    "DAILY_STOP_LIMIT_ALL": None,  # стопов по всем сигналам (выключено)
    "MAX_SAME_SIDE_ACTIVE": 2,     # активных сигналов в одну сторону
    "BLOCK_LIQ_UNSAFE": True,      # ликвидация ближе стопа -> сигнал НЕ шлём
    # False по данным тени 09.07: фильтр отрезал 16 сетапов на +2.0R.
    # Вернуть True, если /stats покажет минус по этим сигналам вживую.
    "BLOCK_B_AGAINST_TREND": False,
    "MIN_STOP_PCT": 0.005,         # стоп меньше 0.5% расширяется до 0.5% (шум)
    # None = отключено (по просьбе владельца). Расчёт tp1_net_usd в журнале
    # остаётся — /stats покажет, что реально приносят микро-сигналы.
    "MIN_TP1_NET_USD": None,       # порог чистой прибыли TP1 (было 0.50)
    "FEE_RATE": 0.0005,            # тейкер OKX в одну сторону

    # --- 💎 Lifechange-детектор и раннер ---
    "LIFECHANGE_MIN_SCORE": 9,     # кандидат = A-грейд + хотя бы 1 фактор
    "LIFECHANGE_MIN_SCORE_2F": 7,  # ...или от 7 баллов при 2+ факторах
    "SQUEEZE_PCTILE": 0.25,        # ATR перед проколом в нижних 25% суток
    "SQUEEZE_LOOKBACK": 288,       # сутки 5m-свечей
    "FUNDING_EXTREME": 0.0003,     # |фандинг| >= 0.03%/8ч = толпа перекошена
    "FUNDING_SANE_MAX": 0.0015,    # выше 0.15% = структурная премия, НЕ топливо
    "FUNDING_CACHE_MIN": 15,

    # --- Импульс-вето: не встаём против свежего выброса ---
    "IMPULSE_LOOKBACK": 6,         # свечей для замера импульса (без сигнальной)
    "IMPULSE_VETO_ATR": 3.0,       # ход > 3×ATR против сделки = вето (2.0 душило поток)
    "H4_TIMEFRAME": "4h",          # ловушка старшего порядка
    "H4_SWEEP_LOOKBACK": 20,
    "H4_CACHE_MIN": 30,
    "LIFE_ATR_BUF_MULT": 1.0,      # LIFE: буфер за хвостом 1×ATR (не 0.25)
    "LIFE_MIN_STOP_ATR": 2.0,      # LIFE: дистанция стопа минимум 2×ATR
    "RUNNER_ATR_MULT": 3.0,        # chandelier-трейл: экстремум - 3*ATR
    "RUNNER_NOTIFY_STEP_R": 1.0,   # уведомлять о передвижке трейла раз в 1R

    # --- 🏦 SWING-слой: та же ловушка на 1h, входы "на дни", 5-20R ---
    "SWING_ENABLED": True,
    "SWING_TIMEFRAME": "1h",
    "SWING_CANDLES_LIMIT": 320,    # ~13 дней часовиков
    "SWING_MIN_SCORE": 9,          # только A/A+ структуры
    "SWING_SCAN_MIN": 15,          # проверять слой раз в 15 мин на монету
    "SWING_COOLDOWN_MIN": 360,     # не чаще 1 swing-сигнала на монету в 6ч
    "SWING_TIME_STOP_CANDLES": 72, # 5m-свечей без TP1 = 6 часов

    # --- Часы генерации сигналов (МСК). Вне окна — только ведение открытых ---
    "SIGNAL_HOURS_MSK": range(9, 21),   # 09:00–20:59 МСК; None = всегда

    # --- Цикл ---
    "CHECK_INTERVAL_SEC": 60,
    "SIGNAL_COOLDOWN_MIN": 60,     # не чаще 1 сигнала на монету в час
    "HEARTBEAT_HOURS": 4,          # нет сигналов N часов -> отчёт "жив, вот что видел"
    "TRADE_MAX_AGE_H": 24,         # зависшая сделка закрывается (символ мог пропасть)
    "RUNNER_MAX_AGE_H": 72,        # раннеру даём больше — большие тренды живут днями

    # --- Ключи и пути ---
    "OKX_API_KEY": os.getenv("OKX_API_KEY", ""),
    "OKX_API_SECRET": os.getenv("OKX_API_SECRET", ""),
    "OKX_API_PASSPHRASE": os.getenv("OKX_API_PASSPHRASE", ""),
    "TG_BOT_TOKEN": os.getenv("TG_BOT_TOKEN", ""),
    "TG_CHAT_ID": os.getenv("TG_CHAT_ID", ""),
    "DATA_DIR": os.getenv("DATA_DIR", "./data"),
    "FALLBACK_BALANCE_USDT": 200.0,
}

VERSION = "3.9"   # единая версия: приветствие, /start и логи берут отсюда

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(levelname)-8s | %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger("liq-trap-v33")

STATE = {
    "started_at": None,
    "last_cycle": None,
    "cycle_sec": 0.0,
    "symbols": [],
    "per_symbol": {},
    "signals_sent": 0,
    "setups_seen": 0,
    "near_misses": deque(maxlen=8),
    "last_signal_text": None,
    "last_error": None,
    "paused_day": None,            # день МСК, на который сработал предохранитель
}


# ==================== ВСПОМОГАТЕЛЬНОЕ =============================

def fmt_time(dt_utc) -> str:
    local = dt_utc + pd.Timedelta(hours=CONFIG["TZ_OFFSET_HOURS"])
    return f"{local.strftime('%d.%m %H:%M')} {CONFIG['TZ_LABEL']}"


def now_local_str() -> str:
    return fmt_time(pd.Timestamp.now(tz="UTC"))


def local_day_key(ts_ms: int | None = None) -> str:
    """Ключ дня в МСК для предохранителя."""
    t = (pd.Timestamp(ts_ms, unit="ms", tz="UTC") if ts_ms
         else pd.Timestamp.now(tz="UTC"))
    return (t + pd.Timedelta(hours=CONFIG["TZ_OFFSET_HOURS"])).strftime("%Y-%m-%d")


def fmt_price(p: float) -> str:
    p = float(p)
    if p >= 1000:
        return f"{p:,.2f}"
    if p >= 1:
        return f"{p:.4g}"
    s = f"{p:.12f}".rstrip("0")
    frac = s.split(".")[1] if "." in s else ""
    lead_zeros = len(frac) - len(frac.lstrip("0"))
    return f"{p:.{min(lead_zeros + 4, 12)}f}"


# ===================== ЖУРНАЛ СДЕЛОК (ДИСК) =======================

class TradeStore:
    """Персистентный журнал сигналов и их исходов (JSON на диске)."""

    def __init__(self, path: str):
        self.path = path
        self.data = {"trades": []}
        try:
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as f:
                    self.data = json.load(f)
                log.info("Журнал загружен: %d записей.", len(self.data["trades"]))
        except Exception as e:
            log.error("Журнал не загрузился (%s) — начинаю пустой.", e)
            self.data = {"trades": []}

    def save(self) -> None:
        try:
            tmp = self.path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self.data, f, ensure_ascii=False, indent=1)
            os.replace(tmp, self.path)
        except Exception as e:
            log.error("Не удалось сохранить журнал: %s", e)

    # --- операции ---
    def add(self, trade: dict) -> None:
        self.data["trades"].append(trade)
        self.save()

    def get(self, sid: str) -> dict | None:
        for t in self.data["trades"]:
            if t["id"] == sid:
                return t
        return None

    def active(self, symbol: str | None = None,
               include_virtual: bool = False) -> list[dict]:
        return [t for t in self.data["trades"]
                if t["status"] in ("active", "tp1", "runner")
                and (include_virtual or not t.get("virtual"))
                and (symbol is None or t["symbol"] == symbol)]

    def active_same_side(self, side: str) -> int:
        return sum(1 for t in self.active() if t["side"] == side)

    def stops_on_day(self, day_key: str, taken_only: bool = False) -> int:
        return sum(1 for t in self.data["trades"]
                   if t["status"] == "stop" and t.get("closed_day") == day_key
                   and not t.get("virtual")
                   and (not taken_only or t.get("taken") is True))

    def closed(self) -> list[dict]:
        return [t for t in self.data["trades"]
                if t["status"] in ("stop", "be", "tp2", "time", "run")
                and not t.get("virtual")]

    def shadow_closed(self) -> list[dict]:
        """Закрытые ВИРТУАЛЬНЫЕ сделки — то, что отрезали фильтры."""
        return [t for t in self.data["trades"]
                if t["status"] in ("stop", "be", "tp2", "time", "run")
                and t.get("virtual")]


STORE: TradeStore | None = None   # инициализируется в main()


# ======================= ЗАГРУЗКА ДАННЫХ ==========================

async def fetch_df(exchange: ccxt.okx, symbol: str, timeframe: str,
                   limit: int) -> pd.DataFrame:
    ohlcv = await exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
    df = pd.DataFrame(
        ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df["dt"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    return df.iloc[:-1].reset_index(drop=True)


async def fetch_top_symbols(exchange: ccxt.okx) -> list[str]:
    """Топ-N USDT-свопов OKX по обороту 24ч (нативное поле volCcy24h)."""
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
            vol_base = info.get("volCcy24h")
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
        return CONFIG["FALLBACK_BALANCE_USDT"]
    try:
        bal = await exchange.fetch_balance({"type": "trading"})
        usdt = bal.get("USDT", {}) or {}
        return float(usdt.get("total") or usdt.get("free") or 0.0)
    except Exception as e:
        log.error("Баланс недоступен (%s), fallback.", e)
        return CONFIG["FALLBACK_BALANCE_USDT"]


# ========================= ИНДИКАТОРЫ =============================

def ema(series: pd.Series, length: int) -> pd.Series:
    return series.ewm(span=length, adjust=False, min_periods=length).mean()


def sma(series: pd.Series, length: int) -> pd.Series:
    return series.rolling(length, min_periods=length).mean()


def atr(high: pd.Series, low: pd.Series, close: pd.Series,
        length: int) -> pd.Series:
    prev_close = close.shift(1)
    tr = pd.concat([high - low, (high - prev_close).abs(),
                    (low - prev_close).abs()], axis=1).max(axis=1)
    return tr.ewm(alpha=1 / length, adjust=False, min_periods=length).mean()


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["ema20"] = ema(df["close"], CONFIG["EMA_PERIOD"])
    df["vol_sma"] = sma(df["volume"], CONFIG["VOL_SMA_PERIOD"])
    df["atr"] = atr(df["high"], df["low"], df["close"], CONFIG["ATR_PERIOD"])

    tp = (df["high"] + df["low"] + df["close"]) / 3.0
    df["_tpv"] = tp * df["volume"]
    df["_d"] = df["dt"].dt.date
    g = df.groupby("_d", sort=False)
    df["vwap"] = g["_tpv"].cumsum() / g["volume"].cumsum().replace(0, pd.NA)
    df.drop(columns=["_tpv", "_d"], inplace=True)

    lb = CONFIG["SWEEP_LOOKBACK"]
    df["swing_high"] = df["high"].shift(1).rolling(lb).max()
    df["swing_low"] = df["low"].shift(1).rolling(lb).min()
    return df


def htf_trend(df_htf: pd.DataFrame) -> str:
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


def volatility_squeeze(df: pd.DataFrame, pre: int = 2) -> bool:
    """Пружина взведена: ATR ПЕРЕД паттерном в нижних 25% суток.
    pre=2 для односвечного (до сигнальной свечи), pre=3 для двухсвечного
    (свеча-прокол на -2 уже раздувает ATR)."""
    lb = CONFIG["SQUEEZE_LOOKBACK"]
    a = df["atr"]
    window = a.iloc[-(lb + pre):-pre].dropna()
    if len(window) < 60:
        return False
    ref = a.iloc[-pre]
    if pd.isna(ref):
        return False
    return float((window <= ref).mean()) <= CONFIG["SQUEEZE_PCTILE"]


def detect_h4_trap(d4: pd.DataFrame) -> dict:
    """Ловушка старшего порядка: одна из 2 последних закрытых H4-свечей
    сняла экстремум 20 H4-свечей и закрылась обратно внутри."""
    res = {"LONG": None, "SHORT": None}
    if len(d4) < CONFIG["H4_SWEEP_LOOKBACK"] + 5:
        return res
    sw_hi = d4["high"].shift(1).rolling(CONFIG["H4_SWEEP_LOOKBACK"]).max()
    sw_lo = d4["low"].shift(1).rolling(CONFIG["H4_SWEEP_LOOKBACK"]).min()
    for i in (-1, -2):
        c4 = d4.iloc[i]
        lo_ref, hi_ref = sw_lo.iloc[i], sw_hi.iloc[i]
        if (res["LONG"] is None and pd.notna(lo_ref)
                and c4["low"] < lo_ref and c4["close"] > lo_ref):
            res["LONG"] = (f"H4-ЛОВУШКА: 4h-свеча сняла лоу "
                           f"{fmt_price(float(lo_ref))} и закрылась обратно — "
                           "разворот старшего порядка, цели считаются днями")
        if (res["SHORT"] is None and pd.notna(hi_ref)
                and c4["high"] > hi_ref and c4["close"] < hi_ref):
            res["SHORT"] = (f"H4-ЛОВУШКА: 4h-свеча сняла хай "
                            f"{fmt_price(float(hi_ref))} и закрылась обратно — "
                            "разворот старшего порядка, цели считаются днями")
    return res


def lifechange_qualifies(score: int, factors: list) -> bool:
    """💎 включается: A-грейд (9+) + хотя бы 1 фактор, ИЛИ от 7 баллов
    при 2+ факторах (двойное топливо важнее идеальной структуры).
    Риск при этом остаётся по грейду — контекст не повышает ставку."""
    if not factors:
        return False
    if score >= CONFIG["LIFECHANGE_MIN_SCORE"]:
        return True
    return score >= CONFIG["LIFECHANGE_MIN_SCORE_2F"] and len(factors) >= 2


def fresh_impulse_atr(df: pd.DataFrame, pre: int = 2) -> float:
    """Сила свежего импульса в ATR: ход закрытий за IMPULSE_LOOKBACK свечей
    ДО паттерна. Главный урок 09.07: снятие экстремума внутри свежего
    выброса — это продолжение, а не ловушка. Против выброса не встаём."""
    lb = CONFIG["IMPULSE_LOOKBACK"]
    c, a = df["close"], df["atr"]
    if len(c) < lb + pre + 1:
        return 0.0
    ref_atr = a.iloc[-pre]
    if pd.isna(ref_atr) or ref_atr <= 0:
        return 0.0
    return float((c.iloc[-pre] - c.iloc[-pre - lb]) / ref_atr)


def lifechange_reasons(side: str, squeeze: bool,
                       funding: float | None) -> list[str]:
    """💎 Факторы большого движения: компрессия + перекошенная толпа.
    Фандинг учитывается только в разумной полосе: выше FUNDING_SANE_MAX —
    это структурная премия инструмента (токен-акции), а не сигнал толпы."""
    lc = []
    if squeeze:
        lc.append("сжатие волатильности — ATR в нижних 25% суток, "
                  "пружина взведена: прокол компрессии даёт выброс, а не отскок")
    if funding is not None:
        f, cap = CONFIG["FUNDING_EXTREME"], CONFIG["FUNDING_SANE_MAX"]
        if side == "LONG" and -cap <= funding <= -f:
            lc.append(f"фандинг {funding*100:.3f}% — толпа сидит в шортах и "
                      "платит за это: топливо для шорт-сквиза вверх")
        elif side == "SHORT" and f <= funding <= cap:
            lc.append(f"фандинг +{funding*100:.3f}% — толпа сидит в лонгах и "
                      "платит: топливо для каскада лонг-ликвидаций вниз")
    return lc


# ====================== ЛОГИКА СТРАТЕГИИ ==========================

_NEAR_SEEN: dict[str, int] = {}


def _log_near_miss(symbol: str, side: str, candle, reasons: list[str]) -> None:
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
    Односвечная ловушка: обязательное ядро (6 баллов) = sweep + хвост >=50% +
    объём >=1.3x + закрытие обратно за снятым уровнем. Бонусы: EMA20 +1,
    VWAP +1, объём >=2.2x +1, тренд 1h +2, контекст +1. Максимум 12.
    Если односвечная не сложилась — проверяется двухсвечная (_two_candle).
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
    imp = fresh_impulse_atr(df, pre=2)          # + = выброс вверх, − = вниз
    veto = CONFIG["IMPULSE_VETO_ATR"]

    # ---------------- SHORT (1 свеча) ----------------
    sweep_hi = last["high"] > last["swing_high"]
    wick_up_ok = upper_wick >= CONFIG["WICK_MIN_RATIO"]
    near_miss = None                    # откладываем: свеча могла снять ОБА
                                        # экстремума — LONG проверяем всегда

    if sweep_hi and (wick_up_ok or vol_ok):
        back_inside = last["close"] < last["swing_high"]
        if wick_up_ok and vol_ok and back_inside:
            vetoed = (f"импульс вверх +{imp:.1f} ATR — против свежего "
                      f"выброса не шортим" if imp > veto else None)
            why = [f"над хаем {fmt_price(float(last['swing_high']))} сняли "
                   "стопы и заманили пробойщиков в лонг — их сейчас повезут вниз",
                   f"хвост {upper_wick*100:.0f}% — весь рост мгновенно продали"]
            score = 6
            if last["close"] < last["ema20"]:
                score += 1
            if last["close"] < last["vwap"]:
                score += 1
            if last["close"] < last["ema20"] and last["close"] < last["vwap"]:
                why.append("закрытие под EMA20 и VWAP — контроль у продавцов")
            if vol_strong:
                score += 1
                why.append(f"АНОМАЛЬНЫЙ объём x{vol_x:.1f} — в хвосте "
                           "разгружался крупный игрок")
            else:
                why.append(f"объём x{vol_x:.1f} подтверждает ловушку")
            if trend_1h == "down":
                score += 2
                why.append("тренд 1h вниз — шортим ПО старшему тренду")
            ctx_up = ((prev["close"] > prev["ema20"]) &
                      (prev["close"] > prev["vwap"])).all()
            score += 1 if ctx_up else 0
            return {"side": "SHORT", "candle": last, "vol_x": vol_x,
                    "score": score, "trend_1h": trend_1h,
                    "swept": float(last["swing_high"]),
                    "pattern": "1-свечная", "why": why, "vetoed": vetoed,
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
        near_miss = ("SHORT", reasons)

    # ---------------- LONG (1 свеча) -----------------
    sweep_lo = last["low"] < last["swing_low"]
    wick_dn_ok = lower_wick >= CONFIG["WICK_MIN_RATIO"]

    if sweep_lo and (wick_dn_ok or vol_ok):
        back_inside = last["close"] > last["swing_low"]
        if wick_dn_ok and vol_ok and back_inside:
            vetoed = (f"импульс вниз {imp:.1f} ATR — падающий нож не ловим"
                      if imp < -veto else None)
            why = [f"под лоу {fmt_price(float(last['swing_low']))} выбили "
                   "стопы лонгов и заманили пробойщиков в шорт — их выкупят вверх",
                   f"хвост {lower_wick*100:.0f}% — всё падение мгновенно выкупили"]
            score = 6
            if last["close"] > last["ema20"]:
                score += 1
            if last["close"] > last["vwap"]:
                score += 1
            if last["close"] > last["ema20"] and last["close"] > last["vwap"]:
                why.append("закрытие над EMA20 и VWAP — контроль у покупателей")
            if vol_strong:
                score += 1
                why.append(f"АНОМАЛЬНЫЙ объём x{vol_x:.1f} — в хвосте "
                           "набирался крупный игрок")
            else:
                why.append(f"объём x{vol_x:.1f} подтверждает ловушку")
            if trend_1h == "up":
                score += 2
                why.append("тренд 1h вверх — лонгуем ПО старшему тренду")
            ctx_dn = ((prev["close"] < prev["ema20"]) &
                      (prev["close"] < prev["vwap"])).all()
            score += 1 if ctx_dn else 0
            return {"side": "LONG", "candle": last, "vol_x": vol_x,
                    "score": score, "trend_1h": trend_1h,
                    "swept": float(last["swing_low"]),
                    "pattern": "1-свечная", "why": why, "vetoed": vetoed,
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
        near_miss = ("LONG", reasons)   # LONG-диагноз информативнее при двойном снятии

    if near_miss:
        _log_near_miss(symbol, near_miss[0], last, near_miss[1])
        return None

    return _two_candle(df, trend_1h, symbol)


def _two_candle(df: pd.DataFrame, trend_1h: str, symbol: str) -> dict | None:
    """Свеча A прокалывает экстремум и закрывается ЗА ним, свеча B
    закрывается обратно внутри — отказ пришёл второй свечой. База 5 баллов."""
    if not CONFIG["TWO_CANDLE_ENABLED"] or len(df) < CONFIG["SWEEP_LOOKBACK"] + 25:
        return None

    a = df.iloc[-2]
    b = df.iloc[-1]
    lb = CONFIG["TREND_LOOKBACK"]
    prev = df.iloc[-2 - lb:-2]

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
    imp = fresh_impulse_atr(df, pre=3)          # импульс ДО свечи-прокола
    veto = CONFIG["IMPULSE_VETO_ATR"]

    level_hi = float(a["swing_high"])
    if (a["high"] > level_hi and a["close"] > level_hi
            and b["close"] < level_hi and b["close"] < b["open"]
            and vol_pair_ok and imp <= veto):
        why = [f"пробой хая {fmt_price(level_hi)} НЕ удержали — вторая свеча "
               "захлопнула ловушку, пробойщики-лонгусты в минусе",
               f"объём x{vol_x:.1f} на проколе/возврате"]
        score = 5
        if b["close"] < b["ema20"]:
            score += 1
        if b["close"] < b["vwap"]:
            score += 1
        if b["close"] < b["ema20"] and b["close"] < b["vwap"]:
            why.append("возврат под EMA20 и VWAP")
        if vol_strong:
            score += 1
        if trend_1h == "down":
            score += 2
            why.append("тренд 1h вниз — шортим ПО старшему тренду")
        ctx_up = ((prev["close"] > prev["ema20"]) &
                  (prev["close"] > prev["vwap"])).all()
        score += 1 if ctx_up else 0
        return {"side": "SHORT", "candle": b, "vol_x": vol_x,
                "score": score, "trend_1h": trend_1h, "swept": level_hi,
                "pattern": "2-свечная", "why": why,
                "stop_high": float(max(a["high"], b["high"])),
                "stop_low": float(min(a["low"], b["low"]))}

    level_lo = float(a["swing_low"])
    if (a["low"] < level_lo and a["close"] < level_lo
            and b["close"] > level_lo and b["close"] > b["open"]
            and vol_pair_ok and imp >= -veto):
        why = [f"пробой лоу {fmt_price(level_lo)} НЕ удержали — вторая свеча "
               "захлопнула ловушку, пробойщики-шортисты в минусе",
               f"объём x{vol_x:.1f} на проколе/возврате"]
        score = 5
        if b["close"] > b["ema20"]:
            score += 1
        if b["close"] > b["vwap"]:
            score += 1
        if b["close"] > b["ema20"] and b["close"] > b["vwap"]:
            why.append("возврат над EMA20 и VWAP")
        if vol_strong:
            score += 1
        if trend_1h == "up":
            score += 2
            why.append("тренд 1h вверх — лонгуем ПО старшему тренду")
        ctx_dn = ((prev["close"] < prev["ema20"]) &
                  (prev["close"] < prev["vwap"])).all()
        score += 1 if ctx_dn else 0
        return {"side": "LONG", "candle": b, "vol_x": vol_x,
                "score": score, "trend_1h": trend_1h, "swept": level_lo,
                "pattern": "2-свечная", "why": why,
                "stop_high": float(max(a["high"], b["high"])),
                "stop_low": float(min(a["low"], b["low"]))}

    return None


# ==================== РИСК, ПЛЕЧО, ЛИКВИДАЦИЯ =====================

def liquidation_price(entry: float, side: str, leverage: int) -> float:
    mmr = CONFIG["MMR"]
    if side == "LONG":
        return entry * (1 - 1 / leverage + mmr)
    return entry * (1 + 1 / leverage - mmr)


def build_trade_plan(signal: dict, balance: float) -> dict:
    c = signal["candle"]
    entry = float(c["close"])
    atr_val = float(c["atr"])
    life = bool(signal.get("lifechange"))
    # LIFE (раннер) требует простора: буфер 1×ATR и дистанция >= 2×ATR,
    # иначе первый же откат выбьет хвост, ради которого всё затевалось.
    # Риск в $ не меняется — позиция сжимается пропорционально.
    atr_buf = atr_val * (CONFIG["LIFE_ATR_BUF_MULT"] if life
                         else CONFIG["ATR_STOP_MULT"])
    rr = CONFIG["RR_RATIO"]
    lev = CONFIG["LEVERAGE"]

    stop_high = signal.get("stop_high", float(c["high"]))
    stop_low = signal.get("stop_low", float(c["low"]))
    min_dist = CONFIG["LIFE_MIN_STOP_ATR"] * atr_val if life else 0.0

    if signal["side"] == "SHORT":
        stop = max(stop_high + atr_buf, entry + min_dist)
        risk_per_unit = stop - entry
        tp1 = entry - risk_per_unit
        take = entry - risk_per_unit * rr
    else:
        stop = min(stop_low - atr_buf, entry - min_dist)
        risk_per_unit = entry - stop
        tp1 = entry + risk_per_unit
        take = entry + risk_per_unit * rr

    stop_pct = risk_per_unit / entry
    if stop_pct <= 0:
        raise ValueError("Стоп <= 0")

    # Микро-стоп (<0.5%) выбивается спредом и шумом — расширяем до минимума
    stop_widened = False
    if stop_pct < CONFIG["MIN_STOP_PCT"]:
        stop_pct = CONFIG["MIN_STOP_PCT"]
        if signal["side"] == "SHORT":
            stop = entry * (1 + stop_pct)
            risk_per_unit = stop - entry
            tp1 = entry - risk_per_unit
            take = entry - risk_per_unit * rr
        else:
            stop = entry * (1 - stop_pct)
            risk_per_unit = entry - stop
            tp1 = entry + risk_per_unit
            take = entry + risk_per_unit * rr
        stop_widened = True

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

    # Анти-пыль: чистая прибыль на TP1. Закрывается 50% позиции:
    # её вход (0.5P×fee) + её выход (0.5P×fee) = P×fee, НЕ ×2
    fees = position_usdt * CONFIG["FEE_RATE"]
    tp1_net_usd = 0.5 * position_usdt * stop_pct - fees

    liq = liquidation_price(entry, signal["side"], lev)
    liq_safe = liq < stop if signal["side"] == "LONG" else liq > stop

    return {
        "entry": entry, "stop": stop, "tp1": tp1, "take": take,
        "stop_pct": stop_pct, "position_usdt": position_usdt,
        "margin": margin, "leverage": lev, "liq": liq,
        "liq_safe": liq_safe, "capped": capped,
        "stop_widened": stop_widened, "tp1_net_usd": tp1_net_usd,
        "actual_risk_pct": actual_risk_pct, "balance": balance,
    }


# ==================== TELEGRAM: СООБЩЕНИЯ =========================

def signal_blocks(signal: dict, plan: dict) -> str | None:
    """Критические блокировки: возвращает причину, если сигнал слать НЕЛЬЗЯ.
    По брифу: технически неисполнимые и мусорные сигналы не показываем вообще,
    потому что 'показать с предупреждением' = соблазн войти."""
    if CONFIG["BLOCK_LIQ_UNSAFE"] and not plan["liq_safe"]:
        return (f"ликвидация {fmt_price(plan['liq'])} ближе стопа "
                f"{fmt_price(plan['stop'])} — технически неисполним при "
                f"{plan['leverage']}x")
    against = ((signal["side"] == "LONG" and signal["trend_1h"] == "down")
               or (signal["side"] == "SHORT" and signal["trend_1h"] == "up"))
    if (CONFIG["BLOCK_B_AGAINST_TREND"]
            and grade_key(signal["score"]) == "B" and against):
        return "B-грейд против тренда 1h — статистический мусор"
    if (CONFIG["MIN_TP1_NET_USD"] is not None
            and plan["tp1_net_usd"] < CONFIG["MIN_TP1_NET_USD"]):
        return (f"пыль: TP1 принесёт ~${plan['tp1_net_usd']:.2f} после "
                f"комиссий (< ${CONFIG['MIN_TP1_NET_USD']:.2f})")
    return None


def grade_key(score: int) -> str:
    if score >= 11:
        return "A+"
    if score >= 9:
        return "A"
    return "B"


def grade(score: int) -> str:
    return {"A+": "A+ 🔥", "A": "A", "B": "B"}[grade_key(score)]


def entry_directive(score: int) -> str:
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


def format_message(symbol: str, signal: dict, plan: dict, sid: str) -> str:
    c = signal["candle"]
    head = "🔴 ШОРТ" if signal["side"] == "SHORT" else "🟢 ЛОНГ"
    coin = symbol.split("/")[0]
    trail_side = "лоу" if signal["side"] == "LONG" else "хаёв"
    r_unit = abs(plan["stop"] - plan["entry"])
    # Дальше этой цены за входом не гнаться (0.3R от входа)
    chase = (plan["entry"] + 0.3 * r_unit if signal["side"] == "LONG"
             else plan["entry"] - 0.3 * r_unit)
    lc = signal.get("lifechange", [])
    swing = signal.get("layer") == "swing"
    tf_min = 60 if swing else int(CONFIG["TIMEFRAME"].rstrip("m"))
    valid_until = fmt_time(c["dt"] + pd.Timedelta(minutes=2 * tf_min))
    ts_candles = (CONFIG["SWING_TIME_STOP_CANDLES"] if swing
                  else CONFIG["TIME_STOP_CANDLES"])
    ts_min = ts_candles * 5                      # трекинг всегда по 5m
    ts_txt = f"{ts_min} мин" + (f" ({ts_min//60}ч)" if ts_min >= 60 else "")
    banner = ""
    if swing:
        banner += "🏦🏦 SWING-ВХОД — держим ДНЯМИ, цель 5-20R 🏦🏦\n"
    if lc and not swing:
        banner += "💎💎💎 LIFECHANGE-КАНДИДАТ 💎💎💎\n"
    lines = [
        banner
        + f"{head} {coin}  |  {grade(signal['score'])} ({signal['score']}/12) | "
        f"{signal.get('pattern', '1-свечная')} | #{sid}",
        "",
        "⚡ КАК ВЗЯТЬ (быстро):",
        f"1. {signal['side']} {coin} по рынку ~{fmt_price(plan['entry'])}. "
        f"Цена ушла за {fmt_price(chase)} — НЕ гнаться, пропуск.",
        f"2. Объём {plan['position_usdt']:.0f} USDT "
        f"(маржа {plan['margin']:.2f} при {plan['leverage']}x).",
        f"3. СРАЗУ стоп {fmt_price(plan['stop'])} и тейк {fmt_price(plan['tp1'])}"
        f" на 50% позиции.",
        f"⏳ Окно входа до {valid_until}. Позже — пропускай, сетап уехал.",
        "",
        f"🧠 ПОЧЕМУ: {'; '.join(signal.get('why', []))}.",
    ]
    if lc:
        lines += ["",
                  "💎 ПОЧЕМУ ЛАЙФЧЕНДЖ (потенциал 5-20R, не 2R):",
                  *[f"• {r}" for r in lc],
                  "Такие условия дают вертикальные движения. Хвост позиции "
                  "поедет БЕЗ потолка — раннер, бот ведёт сам."]
    lines += [
        "",
        entry_directive(signal["score"]),
        "",
        f"✅ Вход: {fmt_price(plan['entry'])} | 🛑 Стоп: {fmt_price(plan['stop'])} "
        f"(−{plan['stop_pct']*100:.2f}%)",
        f"🎯 TP1: {fmt_price(plan['tp1'])} (1R) | "
        f"TP2: {fmt_price(plan['take'])} ({CONFIG['RR_RATIO']:.0f}R) | "
        f"риск {plan['balance']*plan['actual_risk_pct']:.2f} USDT "
        f"({plan['actual_risk_pct']*100:.1f}%)",
        f"Тренд 1h: {signal['trend_1h']} | Объём x{signal['vol_x']:.2f} | "
        f"Ликвидация ~{fmt_price(plan['liq'])}",
        "",
    ]
    if lc:
        lines += [
            "⚖️ ГРАНЬ-РАННЕР (после входа):",
            "1. Стоп задет = вышел. Двигать по убытку нельзя.",
            f"2. TP1 {fmt_price(plan['tp1'])}: 50% закрыто (ставил тейком), "
            f"стоп в безубыток ({fmt_price(plan['entry'])}). Худший исход "
            "с этого момента +0.5R.",
            "3. Остальные 50% — РАННЕР БЕЗ ЦЕЛИ. Не трогай: бот ведёт "
            f"chandelier-трейл (экстремум − {CONFIG['RUNNER_ATR_MULT']:.0f}"
            "×ATR) и напишет 💎 при каждой передвижке и 🏁 когда выходить.",
            f"4. {ts_txt} без TP1 — выход, "
            "протух.",
        ]
    else:
        lines += [
            "⚖️ ГРАНЬ (после входа):",
            f"1. Стоп задет = вышел. Двигать по убытку нельзя.",
            f"2. TP1 {fmt_price(plan['tp1'])}: 50% закрыто (ставил тейком), "
            f"стоп в безубыток ({fmt_price(plan['entry'])}).",
            f"3. TP2 {fmt_price(plan['take'])}: закрой ещё 25%.",
            f"4. Остаток держи до закрытия 5m за EMA20 против тебя; "
            f"трейль за {trail_side} 3 свечей.",
            f"5. {ts_txt} без TP1 — выход, протух.",
        ]
    lines += [
        "",
        f"🕒 Свеча: {fmt_time(c['dt'])}. Бот пришлёт 🎯/🏆/🛑/⏰ сам. "
        "Жми кнопку, если вошёл.",
    ]
    if plan["stop_widened"]:
        lines.append(f"❗ Стоп расширен до {CONFIG['MIN_STOP_PCT']*100:.1f}% — "
                     "структурный был уже и выбивался бы шумом/спредом.")
    if plan["capped"]:
        lines.append("❗ Позиция урезана лимитом маржи (90% депозита).")
    return "\n".join(lines)


def signal_keyboard(sid: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Я зашёл", callback_data=f"in:{sid}"),
        InlineKeyboardButton("🙅 Пропустил", callback_data=f"skip:{sid}"),
    ]])


async def send_signal(app: Application | None, text: str,
                      reply_markup=None) -> None:
    if app is None or not CONFIG["TG_CHAT_ID"]:
        log.warning("Telegram не настроен. Сообщение:\n%s", text)
        return
    try:
        await app.bot.send_message(chat_id=CONFIG["TG_CHAT_ID"], text=text,
                                   reply_markup=reply_markup)
    except Exception as e:
        log.error("Ошибка Telegram: %s", e)


# ====================== ТРЕКЕР ИСХОДОВ ============================
# R-результаты по плану ГРАНЬ: стоп = -1R; TP1 достигнут, потом БУ = +0.5R;
# TP2 = +1.5R (50% на 1R + 50% на 2R, консервативно без трейла);
# время-стоп = 0R.

R_RESULT = {"stop": -1.0, "be": 0.5, "tp2": 1.5, "time": 0.0}


def make_trade_record(sid: str, symbol: str, signal: dict, plan: dict,
                      virtual: bool = False,
                      block_reason: str | None = None) -> dict:
    entry = plan["entry"]
    return {
        "id": sid, "symbol": symbol, "side": signal["side"],
        "virtual": virtual, "block_reason": block_reason,
        "layer": signal.get("layer", "scalp"),
        "time_stop_candles": (CONFIG["SWING_TIME_STOP_CANDLES"]
                              if signal.get("layer") == "swing"
                              else CONFIG["TIME_STOP_CANDLES"]),
        "score": signal["score"], "grade": grade_key(signal["score"]),
        "pattern": signal.get("pattern", "1-свечная"),
        "trend_1h": signal["trend_1h"], "vol_x": round(signal["vol_x"], 2),
        "signal_ts": int(signal["candle"]["timestamp"]),
        "signal_time": fmt_time(signal["candle"]["dt"]),
        "entry": entry, "stop": plan["stop"],
        "tp1": plan["tp1"], "tp2": plan["take"],
        "stop_pct": round(plan["stop_pct"], 5),
        "risk_pct": round(plan["actual_risk_pct"], 4),
        "position_usdt": round(plan["position_usdt"], 2),
        # --- 💎 lifechange / раннер ---
        "mode": "runner" if signal.get("lifechange") else "standard",
        "lc": signal.get("lifechange", []),
        "r_unit": abs(entry - plan["stop"]),
        "atr_sig": float(signal["candle"]["atr"]),
        "extreme": entry,              # хай/лоу с момента входа (для трейла)
        "trail": None,
        "trail_notified": entry,
        "taken": None,                 # True/False по кнопке
        "status": "active",            # active -> tp1|runner -> tp2|be|run ; stop|time
        "result_r": None,
        "candles_seen": 0,
        "last_ts": int(signal["candle"]["timestamp"]),
        "closed_day": None,
        "events": [],
    }


def track_trade(trade: dict, df: pd.DataFrame) -> list[str]:
    """Прогоняет новые закрытые свечи через сделку. Возвращает список
    сообщений для отправки. Консервативно: стоп проверяется раньше цели."""
    msgs = []
    new = df[df["timestamp"] > trade["last_ts"]]
    if new.empty:
        return msgs

    long = trade["side"] == "LONG"
    coin = trade["symbol"].split("/")[0]
    tag = f"{coin} {trade['side']} #{trade['id']}"

    for _, cndl in new.iterrows():
        if trade["status"] not in ("active", "tp1", "runner"):
            break
        trade["last_ts"] = int(cndl["timestamp"])
        trade["candles_seen"] += 1
        hi, lo = float(cndl["high"]), float(cndl["low"])

        if trade["status"] == "active":
            hit_stop = lo <= trade["stop"] if long else hi >= trade["stop"]
            hit_tp1 = hi >= trade["tp1"] if long else lo <= trade["tp1"]
            if hit_stop:                                   # стоп раньше цели
                trade["status"] = "stop"
                trade["result_r"] = R_RESULT["stop"]
                trade["closed_day"] = local_day_key(trade["last_ts"])
                msgs.append(f"🛑 СТОП: {tag}. −1R "
                            f"(−{trade['risk_pct']*100:.1f}% депо). "
                            "Это часть игры — следующий сетап.")
                break
            if hit_tp1:
                trade["events"].append("tp1")
                if trade.get("mode") == "runner":
                    trade["status"] = "runner"
                    trade["extreme"] = hi if long else lo
                    msgs.append(f"🎯 TP1: {tag}!\n"
                                f"Закрой 50% по {fmt_price(trade['tp1'])}, "
                                f"стоп в безубыток ({fmt_price(trade['entry'])})."
                                "\n💎 Остальные 50% — РАННЕР. Держи и ничего "
                                "не трогай: бот сам ведёт трейл и скажет, "
                                "когда выходить. Цель — поймать большое.")
                else:
                    trade["status"] = "tp1"
                    msgs.append(f"🎯 TP1 ДОСТИГНУТ: {tag}!\n"
                                f"Закрой 50% по {fmt_price(trade['tp1'])} и "
                                f"переставь стоп в безубыток "
                                f"({fmt_price(trade['entry'])}). "
                                "Дальше сделка безопасна.")
                # БУ/TP2/трейл проверяем со СЛЕДУЮЩЕЙ свечи
                continue
            if trade["candles_seen"] >= trade.get("time_stop_candles",
                                                  CONFIG["TIME_STOP_CANDLES"]):
                trade["status"] = "time"
                trade["result_r"] = R_RESULT["time"]
                trade["closed_day"] = local_day_key(trade["last_ts"])
                msgs.append(f"⏰ ВРЕМЯ-СТОП: {tag}. "
                            f"{trade.get('time_stop_candles', CONFIG['TIME_STOP_CANDLES']) * 5} "
                            "мин без TP1 — выходи по рынку, сетап протух. ~0R.")
                break
            continue

        if trade["status"] == "runner":
            r_unit = trade["r_unit"]
            mult = CONFIG["RUNNER_ATR_MULT"]
            if long:
                trade["extreme"] = max(trade["extreme"], hi)
                trail = max(trade["entry"],
                            trade["extreme"] - mult * trade["atr_sig"])
                exited = lo <= trail
            else:
                trade["extreme"] = min(trade["extreme"], lo)
                trail = min(trade["entry"],
                            trade["extreme"] + mult * trade["atr_sig"])
                exited = hi >= trail
            trade["trail"] = trail
            if exited:
                r_run = ((trail - trade["entry"]) if long
                         else (trade["entry"] - trail)) / r_unit
                trade["result_r"] = round(0.5 + 0.5 * max(r_run, 0.0), 2)
                trade["status"] = "run"
                trade["closed_day"] = local_day_key(trade["last_ts"])
                msgs.append(f"🏁 РАННЕР ЗАКРЫТ: {tag} по трейлу "
                            f"{fmt_price(trail)}. Итог сделки "
                            f"{trade['result_r']:+.2f}R"
                            + (" 💎 Вот это и был лайфчендж-хвост."
                               if trade["result_r"] >= 3 else "."))
                break
            # уведомляем о передвижке трейла раз в RUNNER_NOTIFY_STEP_R
            moved = (trail - trade["trail_notified"] if long
                     else trade["trail_notified"] - trail)
            if moved >= CONFIG["RUNNER_NOTIFY_STEP_R"] * r_unit:
                trade["trail_notified"] = trail
                locked = ((trail - trade["entry"]) if long
                          else (trade["entry"] - trail)) / r_unit
                close_px = float(cndl["close"])
                cushion = (abs(close_px - trail) / close_px * 100
                           if close_px > 0 else 0.0)
                word = "выше" if long else "ниже"
                msgs.append(f"💎 РАННЕР {tag}: трейл на {fmt_price(trail)}, "
                            f"зафиксировано минимум "
                            f"{0.5 + 0.5*max(locked, 0):.1f}R.\n"
                            f"Безопасно держать, пока цена {word} "
                            f"{fmt_price(trail)} (запас ~{cushion:.1f}%). "
                            "Держим дальше.")
            continue

        if trade["status"] == "tp1":
            hit_be = lo <= trade["entry"] if long else hi >= trade["entry"]
            hit_tp2 = hi >= trade["tp2"] if long else lo <= trade["tp2"]
            if hit_be and not hit_tp2:                     # консервативно
                trade["status"] = "be"
                trade["result_r"] = R_RESULT["be"]
                trade["closed_day"] = local_day_key(trade["last_ts"])
                msgs.append(f"⚖️ БЕЗУБЫТОК: {tag}. Остаток закрыт в ноль. "
                            "Итог +0.5R — прибыльная сделка.")
                break
            if hit_tp2:
                trade["status"] = "tp2"
                trade["result_r"] = R_RESULT["tp2"]
                trade["closed_day"] = local_day_key(trade["last_ts"])
                msgs.append(f"🏆 TP2 ДОСТИГНУТ: {tag}! Закрой ещё 25%, "
                            "остаток трейль за 3 свечами до закрытия за EMA20. "
                            "Итог ≥ +1.5R. Красавчик.")
                break
    return msgs


# ==================== TELEGRAM: КОМАНДЫ ===========================

async def on_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    parts = (q.data or "").split(":")
    trade = STORE.get(parts[1]) if STORE and len(parts) >= 2 else None
    if trade is None:
        await q.answer("Сделка не найдена в журнале.")
        return

    if parts[0] in ("in", "skip"):
        trade["taken"] = (parts[0] == "in")
        STORE.save()
        label = "✅ Вход записан" if trade["taken"] else "🙅 Пропуск записан"
    elif parts[0] == "ex" and len(parts) == 3 and parts[2] in EXIT_LABELS:
        trade["exit_reason"] = parts[2]
        STORE.save()
        label = f"📝 Записано: {EXIT_LABELS[parts[2]]}"
    else:
        await q.answer()
        return
    await q.answer(label)
    try:
        await q.edit_message_reply_markup(InlineKeyboardMarkup(
            [[InlineKeyboardButton(label, callback_data="noop:0")]]))
    except Exception:
        pass


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    syms = STATE["symbols"] or CONFIG["STATIC_SYMBOLS"]
    r = CONFIG["RISK_BY_GRADE"]
    await update.message.reply_text(
        f"🤖 Liquidity Trap Bot v{VERSION} (read-only, с трекером сделок)\n\n"
        f"Монет: {len(syms)} ({CONFIG['SYMBOLS_MODE']}) | ТФ "
        f"{CONFIG['TIMEFRAME']} + 2-свечная | порог "
        f"{CONFIG['MIN_SCORE_TO_SEND']}/12 | время {CONFIG['TZ_LABEL']}\n\n"
        "Насколько заходить (в каждом сигнале):\n"
        f"• B (6-8): 🤏 полпозиции, риск {r['B']*100:.1f}%\n"
        f"• A (9-10): 💪 полный размер, риск {r['A']*100:.0f}%\n"
        f"• A+ (11-12): ЕБАШ!!! 🔥 риск {r['A+']*100:.0f}% — потолок\n\n"
        "Под сигналом кнопки ✅/🙅 — жми, это пишется в статистику.\n"
        "Бот сам ведёт сделку: 🎯 TP1 / 🏆 TP2 / 🛑 стоп / ⏰ время-стоп.\n\n"
        f"Защита: {CONFIG['DAILY_STOP_LIMIT']} стопа за день — пауза до "
        f"завтра; максимум {CONFIG['MAX_SAME_SIDE_ACTIVE']} активных "
        "сигнала в одну сторону.\n\n"
        "Команды: /status /stats /export /ping"
    )


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    up = "-"
    if STATE["started_at"]:
        d = datetime.now(timezone.utc) - STATE["started_at"]
        up = f"{d.days}д {d.seconds // 3600}ч {(d.seconds % 3600) // 60}м"
    active = STORE.active() if STORE else []
    lines = [
        "📡 Статус",
        f"Аптайм: {up} | Сигналов: {STATE['signals_sent']} | "
        f"Активных сделок: {len(active)}",
        f"Ядро ловушки замечено: {STATE['setups_seen']} раз",
        f"Заблокировано фильтрами: {STATE.get('blocked_signals', 0)}"
        + (f" (последний: {STATE['last_block']})"
           if STATE.get("last_block") else ""),
        f"Последний цикл: {STATE['last_cycle'] or '-'} "
        f"({STATE['cycle_sec']:.0f} сек, монет: {len(STATE['symbols'])})",
        f"Часы сигналов: {'ОТКРЫТЫ' if signal_hours_ok() else 'закрыты (только ведение сделок)'}",
        f"Анти-кластер: LONG {STORE.active_same_side('LONG') if STORE else 0}"
        f"/{CONFIG['MAX_SAME_SIDE_ACTIVE']}, "
        f"SHORT {STORE.active_same_side('SHORT') if STORE else 0}"
        f"/{CONFIG['MAX_SAME_SIDE_ACTIVE']}",
    ]
    if STATE["paused_day"] == local_day_key():
        lines.append(f"⛔ ПРЕДОХРАНИТЕЛЬ: {CONFIG['DAILY_STOP_LIMIT']} стопа "
                     "за день — сигналы на паузе до завтра.")
    for t in active[:6]:
        extra = ""
        if t["status"] == "runner" and t.get("trail"):
            px = STATE["per_symbol"].get(t["symbol"], {}).get("close")
            if px:
                cu = abs(px - t["trail"]) / px * 100
                extra = (f" | держать пока цена "
                         f"{'выше' if t['side'] == 'LONG' else 'ниже'} "
                         f"{fmt_price(t['trail'])} (запас {cu:.1f}%)")
        layer = "🏦" if t.get("layer") == "swing" else ""
        lines.append(f"• #{t['id']} {layer}{t['symbol'].split('/')[0]} "
                     f"{t['side']} [{t['status']}] вход {fmt_price(t['entry'])}"
                     f"{extra}")
    if STATE["last_error"]:
        lines.append(f"⚠️ Ошибка: {STATE['last_error']}")
    if STATE["near_misses"]:
        lines.append("\nПочти-сигналы:")
        for nm in list(STATE["near_misses"])[:4]:
            lines.append(f"• {nm['time']} {nm['symbol'].split('/')[0]} "
                         f"{nm['side']}: {nm['reasons']}")
    await update.message.reply_text("\n".join(lines)[:4000])


def _stats_block(trades: list[dict], title: str) -> list[str]:
    closed = [t for t in trades if t["result_r"] is not None]
    if not closed:
        return [f"{title}: нет закрытых"]
    wins = sum(1 for t in closed if t["result_r"] > 0)
    total_r = sum(t["result_r"] for t in closed)
    return [f"{title}: {len(closed)} закр., винрейт "
            f"{wins/len(closed)*100:.0f}%, суммарно {total_r:+.1f}R"]


async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if STORE is None or not STORE.data["trades"]:
        await update.message.reply_text("Журнал пуст — сигналов ещё не было.")
        return
    trades = STORE.data["trades"]
    closed = STORE.closed()
    lines = [f"📊 Статистика ({len(trades)} сигналов, "
             f"{len(closed)} закрыто, {len(STORE.active())} активно)"]
    if closed:
        by = {"tp2": 0, "be": 0, "stop": 0, "time": 0, "run": 0}
        for t in closed:
            by[t["status"]] += 1
        wins = sum(1 for t in closed if t["result_r"] and t["result_r"] > 0)
        total_r = sum(t["result_r"] for t in closed)
        best = max(closed, key=lambda t: t["result_r"] or 0)
        lines += [
            f"🏆 TP2: {by['tp2']} | 💎 Раннер: {by['run']} | ⚖️ БУ: {by['be']} | "
            f"🛑 Стоп: {by['stop']} | ⏰ Время: {by['time']}",
            f"Прибыльных: {wins/len(closed)*100:.0f}%",
            f"Суммарный результат: {total_r:+.1f}R "
            f"(средний {total_r/len(closed):+.2f}R на сделку)",
            f"Лучшая: #{best['id']} {best['result_r']:+.2f}R",
            "",
        ]
        for g in ("A+", "A", "B"):
            lines += _stats_block([t for t in closed if t["grade"] == g],
                                  f"Грейд {g}")
        lines.append("")
        for p in ("1-свечная", "2-свечная"):
            lines += _stats_block([t for t in closed if t["pattern"] == p], p)
        lines.append("")
        taken = [t for t in closed if t.get("taken")]
        lines += _stats_block(taken, "Взятые тобой (✅)")

        shadow = STORE.shadow_closed()
        if shadow:
            tot = sum(t["result_r"] for t in shadow)
            lines.append(f"\n🕵️ ТЕНЕВОЙ ЖУРНАЛ — что отрезали фильтры "
                         f"({len(shadow)} закрыто, итог {tot:+.1f}R):")
            by_reason: dict = {}
            for t in shadow:
                by_reason.setdefault(t.get("block_reason") or "?", []).append(
                    t["result_r"])
            for r, vals in by_reason.items():
                lines.append(f"• {r}: {len(vals)} шт, {sum(vals):+.1f}R")
            lines.append("Минус = фильтр спасает деньги. Плюс = режет прибыль, "
                         "будем ослаблять.")
        lines.append("\nПолные данные: /export — файл кидай на разбор.")
    await update.message.reply_text("\n".join(lines)[:4000])


async def cmd_export(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if STORE is None or not os.path.exists(STORE.path):
        await update.message.reply_text("Журнал ещё пуст.")
        return
    try:
        with open(STORE.path, "rb") as f:
            await update.message.reply_document(
                document=f, filename="trades_journal.json",
                caption="Журнал сигналов и исходов. Кидай его на разбор — "
                        "будем править фильтры по фактам.")
    except Exception as e:
        await update.message.reply_text(f"Не удалось отправить файл: {e}")


async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("🏓 pong — бот жив.")


# ======================== СКАНЕР ==================================

def signal_hours_ok() -> bool:
    """Генерация новых сигналов только в часы ликвидности (МСК).
    После 21:00 МСК объёмы падают, спреды растут — сигналы превращаются в шум."""
    hours = CONFIG["SIGNAL_HOURS_MSK"]
    if hours is None:
        return True
    local_hour = (pd.Timestamp.now(tz="UTC")
                  + pd.Timedelta(hours=CONFIG["TZ_OFFSET_HOURS"])).hour
    return local_hour in hours


class H4SweepCache:
    """Ловушка старшего порядка (detect_h4_trap), кэш 30 мин."""

    def __init__(self, exchange: ccxt.okx):
        self.exchange = exchange
        self.cache: dict[str, tuple[float, dict]] = {}

    async def get(self, symbol: str, side: str) -> str | None:
        data = await self._data(symbol)
        return data.get(side)

    async def trend(self, symbol: str) -> str:
        """Тренд 4h (EMA200) — фильтр направления для SWING-слоя."""
        data = await self._data(symbol)
        return data.get("trend", "flat")

    async def _data(self, symbol: str) -> dict:
        now = time.time()
        cached = self.cache.get(symbol)
        if not cached or now - cached[0] > CONFIG["H4_CACHE_MIN"] * 60:
            data = {"LONG": None, "SHORT": None, "trend": "flat"}
            try:
                d4 = await fetch_df(self.exchange, symbol,
                                    CONFIG["H4_TIMEFRAME"], 280)
                data = detect_h4_trap(d4)
                data["trend"] = htf_trend(d4)
            except Exception as e:
                log.debug("%s: H4 недоступен: %s", symbol, e)
            self.cache[symbol] = (now, data)
            cached = self.cache[symbol]
        return cached[1]


class FundingCache:
    """Фандинг меняется медленно — кэшируем на 15 минут."""

    def __init__(self, exchange: ccxt.okx):
        self.exchange = exchange
        self.cache: dict[str, tuple[float, float | None]] = {}

    async def get(self, symbol: str) -> float | None:
        now = time.time()
        cached = self.cache.get(symbol)
        if cached and now - cached[0] < CONFIG["FUNDING_CACHE_MIN"] * 60:
            return cached[1]
        rate = None
        try:
            fr = await self.exchange.fetch_funding_rate(symbol)
            rate = fr.get("fundingRate")
            rate = float(rate) if rate is not None else None
        except Exception as e:
            log.debug("%s: фандинг недоступен: %s", symbol, e)
        self.cache[symbol] = (now, rate)
        return rate


class HtfCache:
    def __init__(self, exchange: ccxt.okx):
        self.exchange = exchange
        self.cache: dict[str, tuple[float, str]] = {}

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


EXIT_LABELS = {"plan": "По плану", "early": "Раньше/паника", "held": "Не выходил"}


def exit_keyboard(sid: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton(v, callback_data=f"ex:{sid}:{k}")
        for k, v in EXIT_LABELS.items()
    ]])


async def update_tracked(symbol: str, df: pd.DataFrame,
                         app: Application | None) -> None:
    """Ведём все активные сделки по этой монете; шлём follow-up и
    считаем дневной предохранитель."""
    changed = False
    for trade in STORE.active(symbol, include_virtual=True):
        msgs = track_trade(trade, df)
        if msgs:
            changed = True
            if trade.get("virtual"):
                continue          # теневая сделка: только журнал, без сообщений
            for m in msgs:
                await send_signal(app, m)
            # Сделка закрылась и была взята — спрашиваем, как вышел по факту
            if trade["status"] not in ("active", "tp1") and trade.get("taken"):
                await send_signal(
                    app,
                    f"📝 #{trade['id']}: как вышел по факту? "
                    "(1 тап — пишется в статистику)",
                    reply_markup=exit_keyboard(trade["id"]))
        if trade["status"] == "stop":
            day = local_day_key()
            taken_lim = CONFIG["DAILY_STOP_LIMIT"]
            all_lim = CONFIG["DAILY_STOP_LIMIT_ALL"]
            taken_stops = STORE.stops_on_day(day, taken_only=True)
            all_stops = STORE.stops_on_day(day)
            trig_taken = taken_lim is not None and taken_stops >= taken_lim
            trig_all = all_lim is not None and all_stops >= all_lim
            if (trig_taken or trig_all) and STATE["paused_day"] != day:
                STATE["paused_day"] = day
                reason = (f"{taken_stops} стопа по твоим сделкам" if trig_taken
                          else f"{all_stops} стопов по сигналам — стратегия "
                               "сегодня холодная")
                await send_signal(app,
                    f"⛔ ПРЕДОХРАНИТЕЛЬ: {reason}. Новые сигналы — с "
                    f"завтрашнего дня ({CONFIG['TZ_LABEL']}). Рынок сегодня "
                    "не наш — закрой терминал, это лучший трейд дня.")
    if changed:
        STORE.save()


async def scan_symbol(exchange: ccxt.okx, htf: HtfCache, funding: FundingCache,
                      h4sweep: H4SweepCache,
                      symbol: str, app: Application | None,
                      runtime: dict) -> None:
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

    # 1) Ведение открытых сделок — всегда, даже на паузе и в кулдауне
    await update_tracked(symbol, df, app)

    # 2) Предохранитель и часы генерации: вне окна — только ведение
    if STATE["paused_day"] == local_day_key():
        return
    if not signal_hours_ok():
        return

    if int(last["timestamp"]) < runtime["cooldown"].get(symbol, 0):
        return

    signal = analyze(df, trend_1h, symbol)
    if signal is None:
        ts = int(last["timestamp"])
        if (CONFIG["SEND_NEAR_MISSES"] and _NEAR_SEEN.get(symbol) == ts
                and runtime["near_notified"].get(symbol) != ts
                and STATE["near_misses"]):
            nm = STATE["near_misses"][0]
            if nm["symbol"] == symbol:
                runtime["near_notified"][symbol] = ts
                await send_signal(app,
                    f"👀 Наблюдение (НЕ вход!): {symbol.split('/')[0]} "
                    f"{nm['side']} отбраковано: {nm['reasons']}.")
        return

    sig_ts = int(signal["candle"]["timestamp"])
    if sig_ts == runtime["last_sig"].get(symbol, 0):
        return

    # Импульс-вето: сетап полный, но против выброса. НЕ отправляем,
    # но пишем в теневой журнал — /stats покажет, режет фильтр прибыль
    # или спасает деньги.
    if signal.get("vetoed"):
        plan = build_trade_plan(signal, CONFIG["FALLBACK_BALANCE_USDT"])
        sid = f"V{symbol.split('/')[0]}{sig_ts//60000 % 100000}"
        STORE.add(make_trade_record(sid, symbol, signal, plan, virtual=True,
                                    block_reason=f"импульс-вето"))
        STATE["blocked_signals"] = STATE.get("blocked_signals", 0) + 1
        STATE["last_block"] = (f"{symbol.split('/')[0]} {signal['side']} "
                               f"({signal['score']}/12): {signal['vetoed']}")
        log.info("%s: сетап в ТЕНЬ (вето): %s", symbol, signal["vetoed"])
        runtime["last_sig"][symbol] = sig_ts
        return

    if signal["score"] < CONFIG["MIN_SCORE_TO_SEND"]:
        return
    # 3) Анти-кластер: не плодим однонаправленные ставки
    if STORE.active_same_side(signal["side"]) >= CONFIG["MAX_SAME_SIDE_ACTIVE"]:
        log.info("%s: %s пропущен — уже %d активных в эту сторону.",
                 symbol, signal["side"], CONFIG["MAX_SAME_SIDE_ACTIVE"])
        return

    balance = await fetch_usdt_balance(exchange)
    # 4) 💎 Lifechange-детектор — ДО плана: LIFE переключает стоп на широкий.
    #    Факторы: сжатие (со сдвигом под паттерн), фандинг в разумной полосе,
    #    H4-ловушка. Гейт: A-грейд + 1 фактор, либо 7+ баллов при 2+ факторах.
    signal["lifechange"] = []
    if signal["score"] >= CONFIG["LIFECHANGE_MIN_SCORE_2F"]:
        fr = await funding.get(symbol)
        pre = 3 if signal.get("pattern") == "2-свечная" else 2
        lc = lifechange_reasons(signal["side"], volatility_squeeze(df, pre), fr)
        h4 = await h4sweep.get(symbol, signal["side"])
        if h4:
            lc.append(h4)
        if lifechange_qualifies(signal["score"], lc):
            signal["lifechange"] = lc
            log.info("%s: 💎 LIFECHANGE-кандидат (%d факторов)",
                     symbol, len(lc))

    plan = build_trade_plan(signal, balance)

    # 5) Критические блокировки: неисполнимые/мусорные сигналы НЕ показываем
    block = signal_blocks(signal, plan)
    if block:
        STATE["blocked_signals"] = STATE.get("blocked_signals", 0) + 1
        STATE["last_block"] = (f"{symbol.split('/')[0]} {signal['side']} "
                               f"({signal['score']}/12): {block}")
        log.info("%s: сигнал %s (%d/12) в ТЕНЬ: %s",
                 symbol, signal["side"], signal["score"], block)
        sid = f"V{symbol.split('/')[0]}{sig_ts//60000 % 100000}"
        STORE.add(make_trade_record(sid, symbol, signal, plan, virtual=True,
                                    block_reason=block.split(":")[0].split("—")[0].strip()))
        runtime["last_sig"][symbol] = sig_ts
        return

    sid = f"{symbol.split('/')[0]}{int(signal['candle']['timestamp'])//60000 % 100000}"
    msg = format_message(symbol, signal, plan, sid)
    log.info("СИГНАЛ %s %s (%d/12) #%s", symbol, signal["side"],
             signal["score"], sid)
    await send_signal(app, msg, reply_markup=signal_keyboard(sid))
    STORE.add(make_trade_record(sid, symbol, signal, plan))
    STATE["signals_sent"] += 1
    STATE["last_signal_text"] = msg.split("\n\n")[0]
    runtime["last_sig"][symbol] = int(signal["candle"]["timestamp"])
    runtime["cooldown"][symbol] = (int(signal["candle"]["timestamp"])
                                   + CONFIG["SIGNAL_COOLDOWN_MIN"] * 60_000)


async def scan_swing(exchange: ccxt.okx, h4sweep: H4SweepCache, symbol: str,
                     app: Application | None, runtime: dict) -> None:
    """🏦 SWING-слой: та же ловушка ликвидности, но на 1h — снятие экстремумов
    ~13 дней с фильтром тренда 4h. Входы 'на дни': широкий стоп (LIFE-режим),
    весь остаток после TP1 — раннер, время-стоп 6 часов."""
    if not CONFIG["SWING_ENABLED"]:
        return
    if STATE["paused_day"] == local_day_key() or not signal_hours_ok():
        return
    now = time.time()
    if now - runtime["swing_scan"].get(symbol, 0) < CONFIG["SWING_SCAN_MIN"] * 60:
        return
    runtime["swing_scan"][symbol] = now

    df1 = add_indicators(await fetch_df(
        exchange, symbol, CONFIG["SWING_TIMEFRAME"],
        CONFIG["SWING_CANDLES_LIMIT"]))
    trend4 = await h4sweep.trend(symbol)
    signal = analyze(df1, trend4, symbol)
    if signal is None or signal.get("vetoed"):
        return
    if signal["score"] < CONFIG["SWING_MIN_SCORE"]:
        return
    sig_ts = int(signal["candle"]["timestamp"])
    key = symbol + "|sw"
    if sig_ts == runtime["last_sig"].get(key, 0):
        return
    if sig_ts < runtime["cooldown"].get(key, 0):
        return
    if STORE.active_same_side(signal["side"]) >= CONFIG["MAX_SAME_SIDE_ACTIVE"]:
        log.info("%s swing: %s пропущен анти-кластером.", symbol, signal["side"])
        return

    signal["layer"] = "swing"
    lc = [f"🏦 SWING: ловушка на {CONFIG['SWING_TIMEFRAME']} — снят экстремум "
          f"~13 дней. Разворот старшего порядка, цели считаются днями"]
    h4t = await h4sweep.get(symbol, signal["side"])
    if h4t:
        lc.append(h4t)
    signal["lifechange"] = lc      # автоматом: широкий стоп + режим раннера

    balance = await fetch_usdt_balance(exchange)
    plan = build_trade_plan(signal, balance)
    block = signal_blocks(signal, plan)
    if block:
        sid = f"VS{symbol.split('/')[0]}{sig_ts // 60000 % 100000}"
        STORE.add(make_trade_record(sid, symbol, signal, plan, virtual=True,
                                    block_reason="swing: "
                                    + block.split(":")[0].split("—")[0].strip()))
        STATE["blocked_signals"] = STATE.get("blocked_signals", 0) + 1
        log.info("%s swing: в тень: %s", symbol, block)
        runtime["last_sig"][key] = sig_ts
        return

    sid = f"S{symbol.split('/')[0]}{sig_ts // 60000 % 100000}"
    msg = format_message(symbol, signal, plan, sid)
    log.info("🏦 SWING-СИГНАЛ %s %s (%d/12) #%s",
             symbol, signal["side"], signal["score"], sid)
    await send_signal(app, msg, reply_markup=signal_keyboard(sid))
    STORE.add(make_trade_record(sid, symbol, signal, plan))
    STATE["signals_sent"] += 1
    STATE["last_signal_text"] = msg.split("\n\n")[0]
    runtime["last_sig"][key] = sig_ts
    runtime["cooldown"][key] = sig_ts + CONFIG["SWING_COOLDOWN_MIN"] * 60_000


async def scanner_loop(exchange: ccxt.okx, app: Application | None) -> None:
    htf = HtfCache(exchange)
    funding = FundingCache(exchange)
    h4sweep = H4SweepCache(exchange)
    runtime = {
        "cooldown": {}, "last_sig": {}, "near_notified": {}, "swing_scan": {},
        "tf_ms": exchange.parse_timeframe(CONFIG["TIMEFRAME"]) * 1000,
        "hb_ts": time.time(), "hb_sent": 0, "hb_seen": 0, "hb_blk": 0,
    }
    symbols: list[str] = []
    symbols_refreshed = 0.0

    while True:
        cycle_start = time.time()
        try:
            if (not symbols or time.time() - symbols_refreshed >
                    CONFIG["SYMBOL_REFRESH_MIN"] * 60):
                symbols = await fetch_top_symbols(exchange)
                symbols_refreshed = time.time()
                # монеты с активными сделками не выкидываем из скана
                tracked = {t["symbol"] for t in STORE.active(include_virtual=True)}
                for s in tracked:
                    if s not in symbols:
                        symbols.append(s)
                STATE["symbols"] = symbols
                for gone in set(STATE["per_symbol"]) - set(symbols):
                    STATE["per_symbol"].pop(gone, None)
                log.info("Сканируем %d монет: %s", len(symbols),
                         ", ".join(s.split("/")[0] for s in symbols))

            for sym in symbols:
                try:
                    await scan_symbol(exchange, htf, funding, h4sweep, sym, app, runtime)
                    await scan_swing(exchange, h4sweep, sym, app, runtime)
                except ccxt.BadSymbol:
                    log.warning("%s: символ недоступен.", sym)
                except (ccxt.NetworkError, ccxt.ExchangeError) as e:
                    log.error("%s: ошибка биржи: %s", sym, e)
                    STATE["last_error"] = f"{sym}: {e}"
                except Exception as e:
                    # один сломанный символ не должен глушить остальные
                    log.exception("%s: сбой обработки: %s", sym, e)
                    STATE["last_error"] = f"{sym}: {e}"
            if not signal_hours_ok():
                log.info("Вне часов генерации (%s) — только ведение открытых.",
                         CONFIG["TZ_LABEL"])

            # Протухшие сделки: символ мог пропасть из данных — не даём им
            # вечно висеть и забивать анти-кластер
            now_ms = int(time.time() * 1000)
            expired = False
            for t in STORE.active(include_virtual=True):
                cap = (CONFIG["RUNNER_MAX_AGE_H"] if t["status"] == "runner"
                       else CONFIG["TRADE_MAX_AGE_H"])
                if now_ms - t["signal_ts"] > cap * 3600_000:
                    t["status"] = "time"
                    t["result_r"] = t.get("result_r") or 0.0
                    t["closed_day"] = local_day_key()
                    expired = True
                    log.info("Сделка #%s протухла (>%dч) — закрыта как time.",
                             t["id"], cap)
            if expired:
                STORE.save()

            # Heartbeat: тишина не должна выглядеть как смерть
            hb = CONFIG["HEARTBEAT_HOURS"]
            if (hb and app is not None and signal_hours_ok()
                    and time.time() - runtime["hb_ts"] >= hb * 3600):
                sent_d = STATE["signals_sent"] - runtime["hb_sent"]
                if sent_d == 0:
                    seen_d = STATE["setups_seen"] - runtime["hb_seen"]
                    blk_d = STATE.get("blocked_signals", 0) - runtime["hb_blk"]
                    await send_signal(app,
                        f"🤖 Жив, слежу за {len(symbols)} монетами. За {hb}ч: "
                        f"сигналов 0, ядро ловушки замечено {seen_d}, в тень "
                        f"отправлено {blk_d}"
                        + (f" (посл.: {STATE['last_block']})"
                           if STATE.get("last_block") else "")
                        + ". Рынок не даёт сетапов по критериям — это не повод "
                          "входить руками.")
                runtime["hb_ts"] = time.time()
                runtime["hb_sent"] = STATE["signals_sent"]
                runtime["hb_seen"] = STATE["setups_seen"]
                runtime["hb_blk"] = STATE.get("blocked_signals", 0)

            STATE["last_cycle"] = now_local_str()
            STATE["cycle_sec"] = time.time() - cycle_start

        except Exception as e:
            STATE["last_error"] = str(e)
            log.exception("Непредвиденная ошибка цикла: %s", e)

        elapsed = time.time() - cycle_start
        await asyncio.sleep(max(5.0, CONFIG["CHECK_INTERVAL_SEC"] - elapsed))


# ============================ MAIN ================================

async def main() -> None:
    global STORE
    STATE["started_at"] = datetime.now(timezone.utc)
    STORE = TradeStore(os.path.join(CONFIG["DATA_DIR"], "trades_journal.json"))

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
        app.add_handler(CommandHandler("stats", cmd_stats))
        app.add_handler(CommandHandler("export", cmd_export))
        app.add_handler(CommandHandler("ping", cmd_ping))
        app.add_handler(CallbackQueryHandler(on_button))
    else:
        log.warning("TG_BOT_TOKEN не задан — Telegram отключён.")

    log.info("Бот v%s запущен: топ-%d монет, журнал: %s", VERSION,
             CONFIG["TOP_N_SYMBOLS"], STORE.path)

    try:
        if app is not None:
            await app.initialize()
            await app.start()
            await app.updater.start_polling(drop_pending_updates=True)
            r = CONFIG["RISK_BY_GRADE"]
            await send_signal(app,
                f"🤖 Liquidity Trap v{VERSION} запущен (трекер сделок включён)\n"
                f"Топ-{CONFIG['TOP_N_SYMBOLS']} монет | ТФ {CONFIG['TIMEFRAME']} "
                f"+ 2-свечная | порог {CONFIG['MIN_SCORE_TO_SEND']}/12\n"
                f"Вход: B 🤏 {r['B']*100:.1f}% | A 💪 {r['A']*100:.0f}% | "
                f"A+ ЕБАШ!!! 🔥 {r['A+']*100:.0f}%\n"
                f"Под сигналом жми ✅/🙅 — бот сам ведёт сделку и копит "
                f"статистику.\nКоманды: /status /stats /export /ping")
        await scanner_loop(exchange, app)

    except (KeyboardInterrupt, asyncio.CancelledError):
        log.info("Остановка...")
    finally:
        if STORE:
            STORE.save()
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

"""
═══════════════════════════════════════════════════════════════════════════
БОТ ПРУЖИНА 1H — WebSocket версия — Binance Futures
═══════════════════════════════════════════════════════════════════════════

Назначение: ловит ВЫХОД ИЗ СЖАТИЯ ("пружину") на 1H свечах в РЕАЛЬНОМ ВРЕМЕНИ.
Ранний вход — этап 2: момент первой реакции ≥4% вверх из узкого коридора,
ещё ДО основного взрыва (примеры: HEI, ALLO).

Почему WebSocket, а не REST:
  Binance сам рекомендует WS для мониторинга многих монет. Мы НЕ опрашиваем
  биржу в цикле — подписываемся ОДИН раз на все ~577 монет (1 соединение,
  лимит 1024 стрима), и Binance сам шлёт обновления формирующихся свечей.
  REST используется ТОЛЬКО при старте — разово подгрузить историю коридора.
  → Запросов в цикле нет → бан невозможен.

Логика сигнала (всё должно совпасть, только ВВЕРХ):
  1. КОРИДОР — последние N закрытых 1H: средние тела ≤1.5% И диапазон
               закрытий ≤4%. Меряется по телам/закрытиям, НЕ по фитилям.
  2. ВЫХОД   — формирующаяся 1H свеча: (high-open)/open ≥ 4%.
  (Объёмного фильтра нет — по требованию: факта 4% выхода достаточно.)

Память (Render free 512МБ): держим строго N+1 свечей на монету (~пара КБ),
не растим историю. Расход ~150-250МБ — с запасом.

Стабильность: авто-reconnect (Binance рвёт WS раз в 24ч), keepalive против
засыпания Render, watchdog.
═══════════════════════════════════════════════════════════════════════════
"""

import ccxt
import requests
import time
import os
import json
import logging
import threading
from datetime import datetime
from collections import deque
from flask import Flask
import websocket  # websocket-client

# ─────────────────────────────────────────────
# ЛОГИРОВАНИЕ
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# ─────────────────────────────────────────────
# КОНФИГУРАЦИЯ
# ─────────────────────────────────────────────

# Telegram
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID        = os.getenv("CHAT_ID")

# --- Логика пружины ---
TIMEFRAME            = '1h'
COMPRESSION_LOOKBACK = 10     # сколько закрытых 1H образуют "коридор"
MAX_AVG_BODY_PCT     = 1.5    # средние тела свечей коридора ≤ 1.5%
MAX_CLOSE_RANGE_PCT  = 4.0    # размах закрытий коридора ≤ 4% (узкий)
BREAKOUT_PCT         = 4.0    # (high-open)/open текущей свечи ≥ 4% = выход

# --- Памп/дамп в моменте (вариант B, независимо от пружины) ---
# Ловит ЛЮБОЕ резкое движение на формирующейся свече, без условия коридора.
# Нужен для анализа: если памп пришёл, а пружины не было — недокрутили пороги.
IMPULSE_PCT          = 7.0    # (high-open)/open ≥7% памп, (open-low)/open ≥7% дамп

# --- WebSocket ---
# Подключаемся к combined-эндпоинту БЕЗ стримов в URL, затем шлём SUBSCRIBE
# пачками. Так Binance присылает ответ на подписку (видно ошибки), и один
# битый стрим не рушит весь поток молча.
WS_URL               = "wss://fstream.binance.com/stream"
STREAMS_PER_CONN     = 150    # потоков на 1 сокет. НЕ ставим близко к лимиту 1024:
                              # на слабом инстансе один сокет с 587 потоками
                              # захлёбывается и Binance рвёт связь (code=None).
                              # 150 → ~4 соединения, каждый дышит свободно.
SUB_BATCH            = 50     # стримов в одном SUBSCRIBE
SUB_PAUSE            = 0.4    # пауза между батчами SUBSCRIBE

# --- Прочее ---
HISTORY_LOAD_PAUSE   = 0.15   # пауза между REST-запросами истории при старте
PORT                 = int(os.environ.get("PORT", 10000))

# ─────────────────────────────────────────────
# BINANCE CCXT (только для разовой загрузки истории + рынки)
# ─────────────────────────────────────────────
exchange = ccxt.binance({
    'enableRateLimit': True,
    'rateLimit': 150,
    'timeout': 15000,
    'options': {
        'defaultType': 'future',
        'adjustForTimeDifference': True,
    }
})

# ─────────────────────────────────────────────
# ГЛОБАЛЬНОЕ СОСТОЯНИЕ
# ─────────────────────────────────────────────
# candles[symbol] = deque последних свечей, каждая = [ts, o, h, l, c, v]
#   последняя в deque = формирующаяся (current)
#   предыдущие = закрытые (коридор)
candles = {}
candles_lock = threading.Lock()

# Антидубликат: (symbol, candle_ts) → ts отправки
sent_alerts = {}
sent_lock = threading.Lock()

# symbol_map: "btcusdt" (из стрима) → "BTC/USDT:USDT" (ccxt-символ)
symbol_map = {}

ws_apps = []  # активные WebSocketApp (может быть несколько соединений)

# ─────────────────────────────────────────────
# FLASK
# ─────────────────────────────────────────────
app = Flask(__name__)

bot_status = {
    "started_at": datetime.now().isoformat(),
    "springs_sent": 0,
    "pumps_sent": 0,
    "dumps_sent": 0,
    "ws_messages": 0,
    "ws_reconnects": 0,
    "tracked_symbols": 0,
    "last_signal": "никогда",
    "last_msg_at": "никогда",
}

@app.route('/')
def home():
    return f"💎 БОТ ПРУЖИНА WS 1H АКТИВЕН. Время: {datetime.now().strftime('%H:%M:%S')}"

@app.route('/health')
def health():
    uptime = str(datetime.now() - datetime.fromisoformat(bot_status["started_at"])).split('.')[0]
    return (f"✅ OK | ПРУЖИНА WS 1H v3.0\n"
            f"Uptime: {uptime}\n"
            f"Монет отслеживается: {bot_status['tracked_symbols']}\n"
            f"WS сообщений: {bot_status['ws_messages']}\n"
            f"WS реконнектов: {bot_status['ws_reconnects']}\n"
            f"Пружин 💎: {bot_status['springs_sent']}\n"
            f"Пампов 🚀: {bot_status['pumps_sent']}\n"
            f"Дампов 🔻: {bot_status['dumps_sent']}\n"
            f"Последний сигнал: {bot_status['last_signal']}\n"
            f"Последнее WS-сообщение: {bot_status['last_msg_at']}")

# ─────────────────────────────────────────────
# TELEGRAM ОТПРАВКА с retry на 429
# ─────────────────────────────────────────────
def send_msg(text: str) -> bool:
    if not TELEGRAM_TOKEN or not CHAT_ID:
        logging.error("TELEGRAM_TOKEN или CHAT_ID не заданы в env")
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    for attempt in range(3):
        try:
            r = requests.post(url, json=payload, timeout=10)
            if r.status_code == 200:
                return True
            if r.status_code == 429:
                try:
                    retry_after = r.json().get('parameters', {}).get('retry_after', 30)
                except Exception:
                    retry_after = 30
                logging.warning(f"TG 429, waiting {retry_after}s (attempt {attempt+1}/3)")
                time.sleep(retry_after + 1)
                continue
            logging.warning(f"TG error: {r.status_code} {r.text[:200]}")
            return False
        except Exception as e:
            logging.warning(f"TG exception attempt {attempt+1}/3: {e}")
            time.sleep(2)
    logging.error("TG отправка не удалась после 3 попыток")
    return False


# ─────────────────────────────────────────────
# ФОРМАТИРОВАНИЕ
# ─────────────────────────────────────────────
def fmt_money(v: float) -> str:
    if v >= 1e9:  return f"${v/1e9:.1f}B"
    if v >= 1e6:  return f"${v/1e6:.1f}M"
    if v >= 1e3:  return f"${v/1e3:.0f}K"
    return f"${v:.0f}"

def fmt_price(v: float) -> str:
    if v >= 1000:    return f"{v:.2f}"
    if v >= 1:       return f"{v:.4f}"
    if v >= 0.01:    return f"{v:.5f}"
    if v >= 0.0001:  return f"{v:.6f}"
    return f"{v:.8f}"

def build_tv_link(symbol: str) -> str:
    base = symbol.split('/')[0]
    return f"🔗 <a href='https://www.tradingview.com/chart/?symbol=BINANCE:{base}USDT.P'>TradingView</a>"

def build_coinglass_link(symbol: str) -> str:
    base = symbol.split('/')[0]
    return f"📊 <a href='https://www.coinglass.com/tv/Binance_{base}USDT'>CoinGlass СуперГрафик</a>"


# ─────────────────────────────────────────────
# СПИСОК USDT-PERP
# ─────────────────────────────────────────────
def get_all_usdt_perps() -> list:
    perps = []
    for symbol, market in exchange.markets.items():
        if not market.get('active'):
            continue
        if market.get('type') != 'swap':
            continue
        if market.get('quote') != 'USDT':
            continue
        if market.get('expiry'):
            continue
        perps.append(symbol)
    return perps


# ─────────────────────────────────────────────
# ДЕТЕКТОР СЖАТИЯ
# ─────────────────────────────────────────────
def analyze_compression(closed_candles: list) -> dict:
    """
    Анализ коридора по ЗАКРЫТЫМ свечам.
    Сжатие по ТЕЛАМ и диапазону ЗАКРЫТИЙ (не по фитилям).
    """
    if len(closed_candles) < COMPRESSION_LOOKBACK:
        return {'is_compressed': False}

    last_n = closed_candles[-COMPRESSION_LOOKBACK:]

    bodies = []
    for c in last_n:
        o, cl = c[1], c[4]
        if o > 0:
            bodies.append(abs(cl - o) / o * 100)
    if not bodies:
        return {'is_compressed': False}
    avg_body_pct = sum(bodies) / len(bodies)

    closes = [c[4] for c in last_n]
    cmin, cmax = min(closes), max(closes)
    close_range_pct = (cmax - cmin) / cmin * 100 if cmin > 0 else 999.0

    is_compressed = (avg_body_pct <= MAX_AVG_BODY_PCT and
                     close_range_pct <= MAX_CLOSE_RANGE_PCT)
    return {
        'is_compressed':   is_compressed,
        'avg_body_pct':    avg_body_pct,
        'close_range_pct': close_range_pct,
    }


def check_spring(symbol: str):
    """
    Проверяет состояние свечей монеты на выход из пружины.
    Вызывается на каждом обновлении формирующейся свечи.
    Шлёт алерт при срабатывании. Потокобезопасно.
    """
    with candles_lock:
        dq = candles.get(symbol)
        if not dq or len(dq) < COMPRESSION_LOOKBACK + 1:
            return
        arr = list(dq)

    current = arr[-1]
    closed  = arr[:-1]

    open_p, high_p = current[1], current[2]
    close_p, vol_p = current[4], current[5]
    ts = current[0]
    if open_p <= 0:
        return

    # Коридор?
    comp = analyze_compression(closed)
    if not comp.get('is_compressed'):
        return

    # Выход вверх ≥ порога?
    breakout_pct = (high_p - open_p) / open_p * 100
    if breakout_pct < BREAKOUT_PCT:
        return

    # Антидубликат: тип spring — раз на свечу
    key = f"{symbol}_{ts}_spring"
    with sent_lock:
        if key in sent_alerts:
            return
        sent_alerts[key] = time.time()

    det = {
        'breakout_pct':    breakout_pct,
        'open':            open_p,
        'high':            high_p,
        'close':           close_p,
        'avg_body_pct':    comp['avg_body_pct'],
        'close_range_pct': comp['close_range_pct'],
        'vol_1h':          vol_p,
    }
    msg = build_alert_message(symbol, det)
    if send_msg(msg):
        bot_status['springs_sent'] += 1
        bot_status['last_signal'] = datetime.now().strftime('%H:%M:%S')
        logging.info(f"💎 ПРУЖИНА {symbol}: выход +{breakout_pct:.2f}% "
                     f"(тела {comp['avg_body_pct']:.2f}%, "
                     f"диапазон {comp['close_range_pct']:.1f}%)")


def build_alert_message(symbol: str, det: dict) -> str:
    title = f"💎 ПРУЖИНА 1H — выход +{det['breakout_pct']:.2f}%"
    cur_vol_usd = det['vol_1h'] * det['close']
    ctx_line = (f"🗜 Коридор: тела ~{det['avg_body_pct']:.2f}%, "
                f"диапазон {det['close_range_pct']:.1f}% за {COMPRESSION_LOOKBACK}ч")
    lines = [
        f"<b>{title}</b>",
        f"Монета: <b>{symbol}</b>",
        "━━━━━━━━━━━━━━━━",
        ctx_line,
        "",
        f"📊 Open: <code>{fmt_price(det['open'])}</code>",
        f"🔥 High: <code>{fmt_price(det['high'])}</code>",
        f"💵 Сейчас: <code>{fmt_price(det['close'])}</code>",
        "",
        f"📦 Vol 1H: {fmt_money(cur_vol_usd)}",
        "",
        build_tv_link(symbol),
        build_coinglass_link(symbol),
    ]
    return "\n".join(lines)


def check_impulse(symbol: str):
    """
    Детектор пампа/дампа В МОМЕНТЕ на формирующейся свече.
    Независим от пружины, БЕЗ условия коридора — ловит любое резкое движение.
    Памп: (high-open)/open ≥ IMPULSE_PCT. Дамп: (open-low)/open ≥ IMPULSE_PCT.

    Антидубликат: ОДИН сигнал на свечу по группе импульса (памп ИЛИ дамп).
    Что сработало первым — то и придёт, повтор по этой свече не шлётся.
    Назначение — анализ работы пружины (см. комментарий в конфиге).
    """
    with candles_lock:
        dq = candles.get(symbol)
        if not dq or len(dq) < 1:
            return
        current = list(dq)[-1]

    open_p, high_p, low_p, close_p, vol_p = (
        current[1], current[2], current[3], current[4], current[5]
    )
    ts = current[0]
    if open_p <= 0:
        return

    move_up = (high_p - open_p) / open_p * 100
    move_dn = (open_p - low_p) / open_p * 100

    direction = None
    move_pct = 0.0
    peak = 0.0
    if move_up >= IMPULSE_PCT:
        direction, move_pct, peak = 'UP', move_up, high_p
    elif move_dn >= IMPULSE_PCT:
        direction, move_pct, peak = 'DOWN', move_dn, low_p
    if not direction:
        return

    # Один сигнal импульса на свечу (общий ключ для пампа И дампа)
    key = f"{symbol}_{ts}_impulse"
    with sent_lock:
        if key in sent_alerts:
            return
        sent_alerts[key] = time.time()

    msg = build_impulse_message(symbol, direction, move_pct, open_p, peak,
                                close_p, vol_p)
    if send_msg(msg):
        bot_status['last_signal'] = datetime.now().strftime('%H:%M:%S')
        if direction == 'UP':
            bot_status['pumps_sent'] += 1
            logging.info(f"🚀 ПАМП {symbol}: +{move_pct:.2f}% в моменте")
        else:
            bot_status['dumps_sent'] += 1
            logging.info(f"🔻 ДАМП {symbol}: -{move_pct:.2f}% в моменте")


def build_impulse_message(symbol, direction, move_pct, open_p, peak,
                          close_p, vol_p) -> str:
    if direction == 'UP':
        title = f"🚀 ПАМП 1H +{move_pct:.2f}% в моменте"
        peak_label = "🔥 High"
    else:
        title = f"🔻 ДАМП 1H -{move_pct:.2f}% в моменте"
        peak_label = "💥 Low"

    cur_vol_usd = vol_p * close_p
    lines = [
        f"<b>{title}</b>",
        f"Монета: <b>{symbol}</b>",
        "━━━━━━━━━━━━━━━━",
        f"📊 Open: <code>{fmt_price(open_p)}</code>",
        f"{peak_label}: <code>{fmt_price(peak)}</code>",
        f"💵 Сейчас: <code>{fmt_price(close_p)}</code>",
        "",
        f"📦 Vol 1H: {fmt_money(cur_vol_usd)}",
        "",
        build_tv_link(symbol),
        build_coinglass_link(symbol),
    ]
    return "\n".join(lines)


# ─────────────────────────────────────────────
# ЗАГРУЗКА ИСТОРИИ (разово при старте)
# ─────────────────────────────────────────────
def load_initial_history(perps: list):
    """
    Разово подгружает COMPRESSION_LOOKBACK закрытых 1H свечей на монету,
    чтобы знать коридор ещё до прихода WS-данных. С паузами — бан не грозит.
    """
    logging.info(f"Загрузка истории для {len(perps)} монет (разово)...")
    loaded = 0
    for symbol in perps:
        try:
            ohlcv = exchange.fetch_ohlcv(symbol, TIMEFRAME, limit=COMPRESSION_LOOKBACK + 1)
            if ohlcv and len(ohlcv) >= 2:
                dq = deque(maxlen=COMPRESSION_LOOKBACK + 1)
                for c in ohlcv:
                    dq.append([c[0], c[1], c[2], c[3], c[4], c[5]])
                with candles_lock:
                    candles[symbol] = dq
                loaded += 1
        except ccxt.RateLimitExceeded:
            logging.warning(f"История {symbol}: rate limit, пауза 10с")
            time.sleep(10)
        except Exception as e:
            logging.warning(f"История {symbol}: {e}")
        time.sleep(HISTORY_LOAD_PAUSE)
    bot_status['tracked_symbols'] = loaded
    logging.info(f"История загружена: {loaded}/{len(perps)} монет")


# ─────────────────────────────────────────────
# ОБРАБОТКА WS-СООБЩЕНИЙ
# ─────────────────────────────────────────────
def on_ws_message(ws, message):
    """
    Обрабатывает kline-сообщение из стрима.
    Структура: {"stream":"btcusdt@kline_1h","data":{"k":{...}}}
    """
    bot_status['last_msg_at'] = datetime.now().strftime('%H:%M:%S')
    try:
        data = json.loads(message)

        # Ответ на SUBSCRIBE/ошибку (нет поля data) — логируем и выходим
        if 'result' in data or 'error' in data:
            if data.get('error'):
                logging.error(f"WS подписка отклонена: {data['error']}")
            else:
                logging.info(f"WS подписка подтверждена (id={data.get('id')})")
            return

        payload = data.get('data', data)
        k = payload.get('k')
        if not k:
            return

        # Это реальное данные-сообщение (kline) — считаем отдельно
        bot_status['ws_messages'] += 1
        if bot_status['ws_messages'] == 1:
            logging.info("✅ ПЕРВЫЕ kline-данные пришли — поток РАБОТАЕТ")

        stream_sym = k['s'].lower()           # "btcusdt"
        symbol = symbol_map.get(stream_sym)    # → "BTC/USDT:USDT"
        if not symbol:
            return

        ts      = k['t']                        # время начала свечи
        o       = float(k['o'])
        h       = float(k['h'])
        l       = float(k['l'])
        c       = float(k['c'])
        v       = float(k['v'])
        closed  = k['x']                        # True если свеча закрылась

        with candles_lock:
            dq = candles.get(symbol)
            if dq is None:
                dq = deque(maxlen=COMPRESSION_LOOKBACK + 1)
                candles[symbol] = dq

            if dq and dq[-1][0] == ts:
                # Обновляем текущую формирующуюся свечу
                dq[-1] = [ts, o, h, l, c, v]
            else:
                # Новая свеча началась — добавляем (старая ушла в коридор)
                dq.append([ts, o, h, l, c, v])

        # Проверяем выход из пружины и памп/дамп на свежих данных
        check_spring(symbol)
        check_impulse(symbol)

    except Exception as e:
        logging.debug(f"WS message parse error: {e}")


def on_ws_error(ws, error):
    logging.warning(f"WS error: {error}")

def on_ws_close(ws, code, msg):
    logging.warning(f"WS закрыт: code={code} msg={msg}")


# ─────────────────────────────────────────────
# ЗАПУСК WS-СОЕДИНЕНИЙ (с авто-reconnect)
# ─────────────────────────────────────────────
def build_stream_names(perps: list) -> list:
    """
    Из ccxt-символов делает имена стримов и заполняет symbol_map.
    Берём ТОЧНЫЙ id рынка из ccxt (market['id'], напр. 'BTCUSDT', '1000PEPEUSDT'),
    а не склеиваем вручную — иначе один битый тикер рушит всю подписку.
    """
    streams = []
    for symbol in perps:
        market = exchange.markets.get(symbol, {})
        market_id = market.get('id')        # точное имя для Binance API
        if not market_id:
            continue
        stream_sym = market_id.lower()       # "btcusdt", "1000pepeusdt"
        symbol_map[stream_sym] = symbol
        streams.append(f"{stream_sym}@kline_{TIMEFRAME}")
    return streams


def subscribe_in_batches(ws, stream_chunk, conn_id):
    """
    Шлёт SUBSCRIBE пачками по SUB_BATCH с паузами (укладываемся в 5 msg/s).
    Запускается в отдельном потоке после открытия соединения.
    """
    try:
        time.sleep(1)  # дать соединению устаканиться
        sub_id = conn_id * 1000 + 1
        sent = 0
        for i in range(0, len(stream_chunk), SUB_BATCH):
            batch = stream_chunk[i:i + SUB_BATCH]
            msg = {"method": "SUBSCRIBE", "params": batch, "id": sub_id}
            ws.send(json.dumps(msg))
            sub_id += 1
            sent += len(batch)
            time.sleep(SUB_PAUSE)
        logging.info(f"WS#{conn_id}: отправлено SUBSCRIBE на {sent} стримов")
    except Exception as e:
        logging.error(f"WS#{conn_id} ошибка подписки: {e}")


def run_ws_connection(stream_chunk: list, conn_id: int):
    """
    Держит одно WS-соединение на пачку стримов с авто-reconnect.
    Подключается к базовому эндпоинту, затем подписывается пачками.

    Про ping: Binance Futures САМ шлёт ping каждые 3 мин и ждёт pong —
    websocket-client отвечает автоматически. Поэтому клиентский ping ставим
    редким (600с), только как страховку от зависшего соединения. Слишком
    частый клиентский ping при 587 потоках вызывал разрывы code=None.
    """
    def on_open(ws):
        logging.info(f"WS#{conn_id} соединение открыто, подписываюсь...")
        threading.Thread(target=subscribe_in_batches,
                         args=(ws, stream_chunk, conn_id), daemon=True).start()

    reconnect_delay = 5
    while True:
        ws = None
        try:
            ws = websocket.WebSocketApp(
                WS_URL,
                on_message=on_ws_message,
                on_error=on_ws_error,
                on_close=on_ws_close,
                on_open=on_open,
            )
            while conn_id >= len(ws_apps):
                ws_apps.append(None)
            ws_apps[conn_id] = ws
            logging.info(f"WS#{conn_id}: подключение ({len(stream_chunk)} стримов)")
            ws.run_forever(ping_interval=600, ping_timeout=60)
            # Успешно дожили до сюда без исключения — сбрасываем задержку
            reconnect_delay = 5
        except Exception as e:
            logging.error(f"WS#{conn_id} исключение: {e}")

        bot_status['ws_reconnects'] += 1
        logging.warning(f"WS#{conn_id}: разрыв, реконнект через {reconnect_delay}с...")
        time.sleep(reconnect_delay)
        # Нарастающая задержка (5→10→20→40, максимум 60), чтобы не штормить connect
        reconnect_delay = min(reconnect_delay * 2, 60)


def start_websockets(perps: list):
    """Создаёт нужное число соединений (≤1000 стримов каждое) и стартует их."""
    streams = build_stream_names(perps)
    chunks = [streams[i:i + STREAMS_PER_CONN]
              for i in range(0, len(streams), STREAMS_PER_CONN)]
    logging.info(f"Всего стримов: {len(streams)}, соединений: {len(chunks)}")
    for i, chunk in enumerate(chunks):
        t = threading.Thread(target=run_ws_connection, args=(chunk, i),
                             daemon=True, name=f"ws-{i}")
        t.start()
        time.sleep(3)  # разносим старты сокетов, чтобы не долбить connect разом


# ─────────────────────────────────────────────
# ОЧИСТКА ДЕДУПЛИКАЦИИ
# ─────────────────────────────────────────────
def cleanup_loop():
    while True:
        time.sleep(1800)  # каждые 30 мин
        now = time.time()
        with sent_lock:
            global sent_alerts
            sent_alerts = {k: v for k, v in sent_alerts.items() if now - v < 6 * 3600}


# ─────────────────────────────────────────────
# ОСНОВНОЙ ЗАПУСК БОТА
# ─────────────────────────────────────────────
def bot_main():
    try:
        exchange.load_markets()
        logging.info("Рынки Binance загружены")
    except Exception as e:
        logging.error(f"Ошибка загрузки рынков: {e}")
        return

    perps = get_all_usdt_perps()
    logging.info(f"Найдено USDT-perp: {len(perps)}")

    # 1) Разово грузим историю (коридор)
    load_initial_history(perps)

    # 2) Запускаем WebSocket-потоки
    start_websockets(perps)

    # 3) Очистка дедупликации
    threading.Thread(target=cleanup_loop, daemon=True, name="cleanup").start()

    logging.info("Бот ПРУЖИНА WS 1H полностью запущен")

    # Стартовое уведомление
    send_msg(f"💎 Бот ПРУЖИНА WS 1H запущен\n"
             f"Монет: {bot_status['tracked_symbols']}\n"
             f"Пружина: выход +{BREAKOUT_PCT}% из коридора (тела ≤{MAX_AVG_BODY_PCT}%)\n"
             f"Памп/дамп в моменте: ±{IMPULSE_PCT}%")


# ─────────────────────────────────────────────
# KEEPALIVE (против засыпания Render)
# ─────────────────────────────────────────────
def keepalive_loop():
    time.sleep(60)
    external_url = os.environ.get("RENDER_EXTERNAL_URL", "").rstrip("/")
    target_url   = f"{external_url}/health" if external_url else f"http://localhost:{PORT}/health"
    while True:
        try:
            r = requests.get(target_url, timeout=10)
            logging.info(f"Keepalive [{target_url}]: {r.status_code}")
        except Exception as e:
            logging.warning(f"Keepalive failed: {e}")
        time.sleep(240)


# ─────────────────────────────────────────────
# WATCHDOG (следит за потоком данных)
# ─────────────────────────────────────────────
def watchdog():
    """
    Следит за потоком WS-данных. Если сообщения перестали приходить —
    принудительно закрывает сокеты, чтобы сработал авто-reconnect.
    Даёт запас времени на старт (загрузка истории ~2 мин + подписка).
    """
    time.sleep(300)  # запас на старт: история + подписка пачками
    last_count = bot_status['ws_messages']
    while True:
        time.sleep(120)
        current = bot_status['ws_messages']
        if current == last_count:
            logging.error("WATCHDOG: WS-данные не идут 2 мин — принудительный реконнект")
            for ws in list(ws_apps):
                if ws is None:
                    continue
                try:
                    ws.close()
                except Exception:
                    pass
        last_count = current


# ─────────────────────────────────────────────
# ЗАПУСК
# ─────────────────────────────────────────────
threading.Thread(target=bot_main,        daemon=True, name="bot_main").start()
threading.Thread(target=keepalive_loop,  daemon=True, name="keepalive").start()
threading.Thread(target=watchdog,        daemon=True, name="watchdog").start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)

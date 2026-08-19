import json
import os
import threading
import time
import urllib.parse
import urllib.request
import urllib.error
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

API_KEY = os.getenv("TWELVE_DATA_API_KEY", "").strip()
SYMBOL = os.getenv("GOLD_SYMBOL", "XAU/USD")
INTERVAL = os.getenv("GOLD_INTERVAL", "5min")

QUOTE_SECONDS = 180
CANDLE_SECONDS = 300
ANALYSIS_SECONDS = 15
QUOTE_STALE_AFTER = 300
CANDLE_STALE_AFTER = 600
MIN_SCORE = max(50, min(99, int(os.getenv("MIN_SCORE", "80"))))
PORT = int(os.getenv("PORT", "10000"))

QUOTE_URL = "https://api.twelvedata.com/quote"
TIME_SERIES_URL = "https://api.twelvedata.com/time_series"

BASE = Path(__file__).resolve().parent
INDEX = BASE / "index.html"
WORKER = BASE / "OneSignalSDKWorker.js"

lock = threading.Lock()

live_quote = None
candles = []
last_quote_ok = 0.0
last_candle_ok = 0.0

state = {
    "service": "Gold Sentinel",
    "mode": "STARTING",
    "symbol": SYMBOL,
    "interval": INTERVAL,
    "last_update_utc": None,
    "last_price": None,
    "trend": None,
    "rsi": None,
    "atr": None,
    "score": None,
    "decision": "STARTING",
    "signal": None,
    "error": None,
    "healthy": False,
    "data_source": None,
    "last_quote_fetch_utc": None,
    "last_candle_fetch_utc": None,
    "request_budget_estimate_per_day": 768,
}

def utc_now():
    return datetime.now(timezone.utc).isoformat()

def api_json(url, params):
    if not API_KEY:
        raise RuntimeError("API_KEY_MISSING")
    p = dict(params)
    p["apikey"] = API_KEY
    req = urllib.request.Request(
        url + "?" + urllib.parse.urlencode(p),
        headers={"User-Agent": "Gold-Sentinel/3.0"},
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            payload = json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        if e.code == 429:
            raise RuntimeError("RATE_LIMIT")
        raise RuntimeError(f"HTTP_{e.code}")
    except Exception as e:
        raise RuntimeError(f"NETWORK_ERROR: {e}")
    if isinstance(payload, dict) and payload.get("status") == "error":
        msg = str(payload.get("message", "Twelve Data error"))
        low = msg.lower()
        if "limit" in low or "credit" in low or "too many" in low:
            raise RuntimeError("RATE_LIMIT")
        raise RuntimeError(msg)
    return payload

def fetch_quote():
    payload = api_json(QUOTE_URL, {"symbol": SYMBOL, "format": "JSON"})
    for key in ("close", "price"):
        value = payload.get(key)
        if value not in (None, ""):
            return float(value)
    raise RuntimeError("NO_QUOTE_PRICE")

def fetch_candles():
    payload = api_json(
        TIME_SERIES_URL,
        {"symbol": SYMBOL, "interval": INTERVAL, "outputsize": 120, "format": "JSON"},
    )
    values = payload.get("values") if isinstance(payload, dict) else None
    if not values:
        raise RuntimeError("NO_CANDLE_DATA")
    return [
        {"t": x.get("datetime"), "o": float(x["open"]), "h": float(x["high"]), "l": float(x["low"]), "c": float(x["close"])}
        for x in reversed(values)
    ]

def ema(values, period):
    if len(values) < period:
        return None
    e = sum(values[:period]) / period
    k = 2.0 / (period + 1.0)
    for x in values[period:]:
        e = x * k + e * (1.0 - k)
    return e

def rsi(values, period=14):
    if len(values) <= period:
        return None
    gains = 0.0
    losses = 0.0
    for i in range(len(values) - period, len(values)):
        d = values[i] - values[i - 1]
        if d > 0:
            gains += d
        else:
            losses -= d
    if losses == 0:
        return 100.0
    rs = (gains / period) / (losses / period)
    return 100.0 - (100.0 / (1.0 + rs))

def atr(cs, period=14):
    if len(cs) <= period:
        return None
    trs = []
    for i in range(len(cs) - period, len(cs)):
        x = cs[i]
        pc = cs[i - 1]["c"]
        trs.append(max(x["h"] - x["l"], abs(x["h"] - pc), abs(x["l"] - pc)))
    return sum(trs) / len(trs)

def analyze(cs, quote):
    working = [dict(x) for x in cs]
    last = working[-1]
    last["c"] = quote
    last["h"] = max(last["h"], quote)
    last["l"] = min(last["l"], quote)
    closes = [x["c"] for x in working]
    e20 = ema(closes, 20)
    e50 = ema(closes, 50)
    rv = rsi(closes)
    av = atr(working)
    if None in (e20, e50, rv, av) or av <= 0:
        return {"price": round(quote, 2), "trend": None, "rsi": None, "atr": None, "score": None, "decision": "WAIT", "signal": None}
    bullish = e20 > e50 and 52 <= rv <= 70
    bearish = e20 < e50 and 30 <= rv <= 48
    score = int(round(min(99, 50 + min(25, abs(e20 - e50) / av * 10) + min(25, abs(rv - 50)))))
    result = {"price": round(quote, 2), "trend": "BULLISH" if e20 > e50 else "BEARISH", "rsi": round(rv, 2), "atr": round(av, 3), "score": score, "decision": "WAIT", "signal": None}
    if score < MIN_SCORE or not (bullish or bearish):
        return result
    side = "BUY" if bullish else "SELL"
    risk = max(av * 1.5, 0.01)
    if side == "BUY":
        sl, tp1, tp2 = quote - risk, quote + risk * 1.8, quote + risk * 2.5
    else:
        sl, tp1, tp2 = quote + risk, quote - risk * 1.8, quote - risk * 2.5
    result["decision"] = side
    result["signal"] = {"side": side, "entry": round(quote, 2), "stop_loss": round(sl, 2), "take_profit_1": round(tp1, 2), "take_profit_2": round(tp2, 2), "score": score, "interval": INTERVAL}
    return result

def quote_worker():
    global live_quote, last_quote_ok
    while True:
        started = time.monotonic()
        try:
            q = fetch_quote()
            with lock:
                live_quote = q
                last_quote_ok = time.monotonic()
                state["last_quote_fetch_utc"] = utc_now()
        except Exception as e:
            with lock:
                state["error"] = "Twelve Data rate/credit limit reached." if str(e) == "RATE_LIMIT" else f"Quote error: {e}"
        elapsed = time.monotonic() - started
        time.sleep(max(5, QUOTE_SECONDS - elapsed))

def candle_worker():
    global candles, last_candle_ok
    while True:
        started = time.monotonic()
        try:
            cs = fetch_candles()
            with lock:
                candles = cs
                last_candle_ok = time.monotonic()
                state["last_candle_fetch_utc"] = utc_now()
        except Exception as e:
            with lock:
                state["error"] = "Twelve Data rate/credit limit reached." if str(e) == "RATE_LIMIT" else f"Candle error: {e}"
        elapsed = time.monotonic() - started
        time.sleep(max(5, CANDLE_SECONDS - elapsed))

def analysis_worker():
    while True:
        now = time.monotonic()
        with lock:
            q = live_quote
            cs = [dict(x) for x in candles]
            q_age = (now - last_quote_ok) if last_quote_ok else None
            c_age = (now - last_candle_ok) if last_candle_ok else None
            current_error = state.get("error")
        quote_fresh = q is not None and q_age is not None and q_age <= QUOTE_STALE_AFTER
        candles_fresh = bool(cs) and c_age is not None and c_age <= CANDLE_STALE_AFTER
        if not quote_fresh or not candles_fresh:
            reasons = []
            if not quote_fresh:
                reasons.append("live quote stale/unavailable")
            if not candles_fresh:
                reasons.append("5-minute candles stale/unavailable")
            with lock:
                state.update({"mode": "STALE", "last_update_utc": utc_now(), "last_price": None, "trend": None, "rsi": None, "atr": None, "score": None, "decision": "WAIT", "signal": None, "healthy": False, "data_source": None, "error": current_error or "; ".join(reasons)})
            time.sleep(ANALYSIS_SECONDS)
            continue
        result = analyze(cs, q)
        with lock:
            state.update({"mode": "LIVE", "last_update_utc": utc_now(), "last_price": result["price"], "trend": result["trend"], "rsi": result["rsi"], "atr": result["atr"], "score": result["score"], "decision": result["decision"], "signal": result["signal"], "healthy": True, "data_source": "fresh_quote + 5m_candles", "error": None})
        time.sleep(ANALYSIS_SECONDS)

class Handler(BaseHTTPRequestHandler):
    def send_bytes(self, body, content_type, code=200):
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def send_json(self, obj, code=200):
        self.send_bytes(json.dumps(obj, indent=2).encode(), "application/json; charset=utf-8", code)

    def do_GET(self):
        path = self.path.split("?", 1)[0]
        if path == "/":
            if not INDEX.exists():
                return self.send_json({"error": "index.html not found"}, 500)
            return self.send_bytes(INDEX.read_bytes(), "text/html; charset=utf-8")
        if path == "/status":
            now = time.monotonic()
            with lock:
                snapshot = dict(state)
                q_age = now - last_quote_ok if last_quote_ok else None
                c_age = now - last_candle_ok if last_candle_ok else None
            snapshot["quote_age_seconds"] = round(q_age, 1) if q_age is not None else None
            snapshot["candle_age_seconds"] = round(c_age, 1) if c_age is not None else None
            snapshot["polling"] = {"quote_seconds": QUOTE_SECONDS, "candle_seconds": CANDLE_SECONDS, "analysis_seconds": ANALYSIS_SECONDS}
            snapshot["note"] = "Displayed price uses Twelve Data quote endpoint. Indicators use 5-minute candles. Signals are disabled whenever either source is stale."
            return self.send_json(snapshot)
        if path == "/OneSignalSDKWorker.js":
            if not WORKER.exists():
                return self.send_json({"error": "OneSignalSDKWorker.js not found"}, 404)
            return self.send_bytes(WORKER.read_bytes(), "application/javascript; charset=utf-8")
        if path == "/health":
            return self.send_json({"ok": True})
        return self.send_json({"error": "Not found"}, 404)

    def log_message(self, *args):
        pass

if __name__ == "__main__":
    threading.Thread(target=quote_worker, daemon=True).start()
    threading.Thread(target=candle_worker, daemon=True).start()
    threading.Thread(target=analysis_worker, daemon=True).start()
    print("Gold Sentinel live-quote backend running (~768 Twelve Data requests/day)", flush=True)
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()

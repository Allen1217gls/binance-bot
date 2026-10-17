import os, re, time, math, hmac, base64, hashlib, logging, secrets, sqlite3, asyncio, threading
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, Tuple, List
from urllib.parse import urlencode
import requests
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("Hull_Turtle_Bot")

BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "").strip()
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "").strip()
WEBHOOK_PASSPHRASE = os.getenv("WEBHOOK_PASSPHRASE", "Apple").strip()
USE_TESTNET = os.getenv("USE_TESTNET", "false").lower() in ("true", "1", "yes")
FUTURES_URL = "https://testnet.binancefuture.com" if USE_TESTNET else "https://fapi.binance.com"

LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "").strip()
LINE_CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET", "").strip()
LINE_USER_ID = os.getenv("LINE_USER_ID", "").strip()
DEFAULT_SYMBOL = os.getenv("DEFAULT_SYMBOL", "SOLUSDT").strip().upper()

DAILY_MAX_DD_PCT = float(os.getenv("DAILY_MAX_DD_PCT", "15.0"))
EMERGENCY_FLOOR_USDT = float(os.getenv("EMERGENCY_FLOOR_USDT", "0.0"))
MARGIN_ALERT_RATIO = float(os.getenv("MARGIN_ALERT_RATIO", "60.0"))
MAX_ENTRY_SLIPPAGE_PCT = float(os.getenv("MAX_ENTRY_SLIPPAGE_PCT", "0.8"))
SLIPPAGE_WARN_PCT = float(os.getenv("SLIPPAGE_WARN_PCT", "0.5"))
BE_BUFFER_PCT = float(os.getenv("BE_BUFFER_PCT", "0.12"))
SIGNAL_DEDUP_SEC = float(os.getenv("SIGNAL_DEDUP_SEC", "10.0"))
EMERGENCY_SL_FALLBACK_PCT = float(os.getenv("EMERGENCY_SL_FALLBACK_PCT", "2.5"))
DB_PATH = os.getenv("DB_PATH", "/tmp/trades_history.db")
TW_TZ = timezone(timedelta(hours=8))
webhook_lock = asyncio.Lock()

bot_state = {
    "trading_enabled": True,
    "circuit_breaker_tripped": False,
    "emergency_brake_tripped": False,
    "max_daily_dd_pct": DAILY_MAX_DD_PCT,
    "emergency_floor_usdt": EMERGENCY_FLOOR_USDT,
    "max_entry_slippage_pct": MAX_ENTRY_SLIPPAGE_PCT,
    "hourly_push_enabled": False,
    "bound_user_id": LINE_USER_ID,
    "last_symbol": DEFAULT_SYMBOL,
    "last_margin_alert_ts": 0.0,
    "consecutive_api_failures": 0,
    "api_disconnected_alerted": False,
}

recent_signals_cache: Dict[str, float] = {}
trade_logs_mem: List[Tuple[str, str, str, float]] = []

def is_duplicate_signal(sym: str, action: str, stop_loss: Optional[float]) -> bool:
    now_t = time.time()
    key = f"{sym.upper()}:{action.upper()}:{f'{stop_loss:.6f}' if stop_loss else 'none'}"
    if now_t - recent_signals_cache.get(key, 0.0) < SIGNAL_DEDUP_SEC:
        return True
    recent_signals_cache[key] = now_t
    if len(recent_signals_cache) > 200:
        for k in [k for k, t in recent_signals_cache.items() if now_t - t > 60.0]:
            recent_signals_cache.pop(k, None)
    return False

def init_db():
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("PRAGMA journal_mode=MEMORY;")
            conn.execute("CREATE TABLE IF NOT EXISTS trade_logs (id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, symbol TEXT, stage TEXT, side TEXT, qty REAL, entry_price REAL, exit_price REAL, pnl_usdt REAL, pnl_pct REAL)")
            conn.commit()
    except Exception as e:
        logger.warning(f"SQLite 備援模式: {e}")

def record_trade_log(symbol: str, stage: str, pos_amt: float, close_qty: float, entry_price: float, exit_price: float) -> float:
    if close_qty <= 0 or entry_price <= 0 or exit_price <= 0:
        return 0.0
    is_long = pos_amt > 0
    raw_pnl = (exit_price - entry_price) * close_qty if is_long else (entry_price - exit_price) * close_qty
    net_pnl = raw_pnl - ((entry_price + exit_price) * close_qty * 0.0005)
    pnl_pct = ((exit_price - entry_price) / entry_price * 100.0) if is_long else ((entry_price - exit_price) / entry_price * 100.0)
    now_str = datetime.now(TW_TZ).strftime("%Y-%m-%d %H:%M:%S")
    trade_logs_mem.append((now_str, symbol.upper(), stage, float(net_pnl)))
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("PRAGMA journal_mode=MEMORY;")
            conn.execute("INSERT INTO trade_logs (ts, symbol, stage, side, qty, entry_price, exit_price, pnl_usdt, pnl_pct) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                         (now_str, symbol.upper(), stage, "LONG" if is_long else "SHORT", float(close_qty), float(entry_price), float(exit_price), float(net_pnl), float(pnl_pct)))
            conn.commit()
    except Exception:
        pass
    return net_pnl

def build_trade_stats_report(days: int = 7) -> str:
    now_tw = datetime.now(TW_TZ)
    cutoff = (now_tw - timedelta(days=days)).strftime("%Y-%m-%d 00:00:00")
    today_p = now_tw.strftime("%Y-%m-%d")
    rows = []
    try:
        with sqlite3.connect(DB_PATH) as conn:
            rows = conn.execute("SELECT ts, symbol, stage, pnl_usdt FROM trade_logs WHERE ts >= ? ORDER BY id DESC", (cutoff,)).fetchall()
    except Exception:
        rows = [r for r in reversed(trade_logs_mem) if r[0] >= cutoff]
    if not rows and trade_logs_mem:
        rows = [r for r in reversed(trade_logs_mem) if r[0] >= cutoff]
    if not rows:
        return f"📊 【近 {days} 日交易統計週報】\n🕒 統計時間：{now_tw.strftime('%Y-%m-%d %H:%M:%S')}\n📭 尚無紀錄：目前尚無已平倉或減倉結算紀錄"

    wins = [r[3] for r in rows if r[3] > 0]
    losses = [abs(r[3]) for r in rows if r[3] <= 0]
    total_net = sum(r[3] for r in rows)
    wr = len(wins) / len(rows) * 100.0
    avg_w = sum(wins) / len(wins) if wins else 0.0
    avg_l = sum(losses) / len(losses) if losses else 0.0
    rr = (avg_w / avg_l) if avg_l > 0 else (99.9 if avg_w > 0 else 0.0)
    td_rows = [r for r in rows if r[0].startswith(today_p)]
    td_net = sum(r[3] for r in td_rows)
    td_wr = (len([r for r in td_rows if r[3] > 0]) / len(td_rows) * 100.0) if td_rows else 0.0
    return (
        f"📊 【近 {days} 日交易統計週報】\n"
        f"🕒 統計時間：{now_tw.strftime('%Y-%m-%d %H:%M')}\n"
        f"🔢 結算筆數：{len(rows)} 筆 (含減倉與平倉)\n"
        f"🏆 週總勝率：{wr:.1f}% ({len(wins)}勝 / {len(losses)}敗)\n"
        f"💰 週總淨利：{'🟢' if total_net >= 0 else '🔴'} {total_net:+.2f} USDT\n"
        f"⚖️ 均盈虧比：{rr:.2f} (均賺 {avg_w:.2f}U / 均賠 {avg_l:.2f}U)\n"
        f"🚀 最大單筆：+{max(wins) if wins else 0.0:.2f} USDT\n\n"
        f"📅 今日結算：{len(td_rows)} 筆 (勝率 {td_wr:.1f}%)\n"
        f"💵 今日實現：{'🟢' if td_net >= 0 else '🔴'} {td_net:+.2f} USDT"
    )

def send_line_push(text: str, to_user_id: Optional[str] = None):
    token = LINE_CHANNEL_ACCESS_TOKEN
    uid = (to_user_id or bot_state.get("bound_user_id") or LINE_USER_ID).strip()
    if not token or not uid:
        return
    def _push():
        try:
            requests.post("https://api.line.me/v2/bot/message/push",
                          headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
                          json={"to": uid, "messages": [{"type": "text", "text": text[:4900]}]}, timeout=8)
        except Exception as e:
            logger.error(f"LINE 推播異常: {e}")
    threading.Thread(target=_push, daemon=True).start()

def send_line_reply(reply_token: str, text: str):
    if not LINE_CHANNEL_ACCESS_TOKEN or not reply_token:
        return
    try:
        requests.post("https://api.line.me/v2/bot/message/reply",
                      headers={"Content-Type": "application/json", "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}"},
                      json={"replyToken": reply_token, "messages": [{"type": "text", "text": text[:4900]}]}, timeout=8)
    except Exception as e:
        logger.error(f"LINE 回覆異常: {e}")

class BinanceClient:
    def __init__(self, key: str, sec: str):
        self.key, self.sec, self.time_offset = key, sec, 0
        self._precisions: Dict[str, Tuple[float, int, int, float]] = {}
        self._leverage_cache: Dict[str, int] = {}
        self._current_sl: Dict[str, float] = {}
        try:
            self._sync()
            self._load_exchange_info()
        except Exception:
            pass

    def ping(self) -> bool:
        try:
            return requests.get(f"{FUTURES_URL}/fapi/v1/ping", timeout=5).status_code == 200
        except Exception:
            return False

    def _sync(self):
        try:
            res = requests.get(f"{FUTURES_URL}/fapi/v1/time", timeout=5).json()
            self.time_offset = int(res.get("serverTime", time.time() * 1000)) - int(time.time() * 1000)
        except Exception:
            self.time_offset = 0

    def _load_exchange_info(self):
        try:
            res = requests.get(f"{FUTURES_URL}/fapi/v1/exchangeInfo", timeout=10).json()
            for s in res.get("symbols", []):
                sym, price_prec, step, dec, min_notional = s.get("symbol"), int(s.get("pricePrecision", 2)), 0.001, 3, 5.0
                for f in s.get("filters", []):
                    ft = f.get("filterType")
                    if ft == "LOT_SIZE":
                        step = float(f.get("stepSize", "0.001"))
                        dec = max(0, -int(round(math.log10(step)))) if step < 1 else 0
                    elif ft == "PRICE_FILTER":
                        tick = float(f.get("tickSize", "0.01"))
                        if tick < 1:
                            price_prec = max(0, -int(round(math.log10(tick))))
                    elif ft in ("MIN_NOTIONAL", "NOTIONAL"):
                        min_notional = float(f.get("notional", f.get("minNotional", "5.0")))
                self._precisions[sym] = (step, dec, price_prec, min_notional)
        except Exception as e:
            logger.error(f"載入 exchangeInfo 失敗: {e}")

    def _sign(self, p: Dict[str, Any]) -> str:
        return hmac.new(self.sec.encode("utf-8"), urlencode(p).encode("utf-8"), hashlib.sha256).hexdigest()

    def _safe_request(self, method: str, endpoint: str, params: dict = None, data: dict = None, retries: int = 3) -> Any:
        url = f"{FUTURES_URL}{endpoint}"
        for attempt in range(retries):
            try:
                headers = {"X-MBX-APIKEY": self.key}
                if method == "GET":
                    res = requests.get(url, headers=headers, params=params, timeout=10)
                elif method == "DELETE":
                    res = requests.delete(url, headers=headers, params=params, timeout=10)
                else:
                    headers["Content-Type"] = "application/x-www-form-urlencoded"
                    res = requests.post(url, headers=headers, data=data, timeout=10)
                dj = res.json()
                if isinstance(dj, dict):
                    code = dj.get("code")
                    if code == -1003 and attempt < retries - 1:
                        time.sleep(1.5 * (2 ** attempt))
                        continue
                    if code == -1021 and attempt < retries - 1:
                        self._sync()
                        target = params if params and "timestamp" in params else (data if data and "timestamp" in data else None)
                        if target:
                            target["timestamp"] = int(time.time() * 1000) + self.time_offset
                            target["signature"] = self._sign({k: v for k, v in target.items() if k != "signature"})
                        continue
                return dj
            except Exception:
                if attempt == retries - 1:
                    return None
                time.sleep(1.2 * (attempt + 1))
        return None

    def get_equity_and_margin_ratio(self) -> Tuple[float, float, float, Any]:
        self._sync()
        p = {"timestamp": int(time.time() * 1000) + self.time_offset, "recvWindow": 60000}
        p["signature"] = self._sign(p)
        r2 = self._safe_request("GET", "/fapi/v2/account", params=p)
        if isinstance(r2, dict) and "code" not in r2:
            eq = float(r2.get("totalMarginBalance", r2.get("totalWalletBalance", 0.0)))
            avail = float(r2.get("availableBalance", 0.0))
            maint = float(r2.get("totalMaintMargin", 0.0))
            total_eq = max(eq, avail)
            return (total_eq, avail, (maint / total_eq * 100.0) if total_eq > 0 else 0.0, r2)
        res = self._safe_request("GET", "/fapi/v2/balance", params=p)
        if isinstance(res, list):
            for b in res:
                if b.get("asset") == "USDT":
                    eq, avail = float(b.get("crossWalletBalance", b.get("balance", 0.0))), float(b.get("availableBalance", 0.0))
                    return (max(eq, avail), avail, 0.0, res)
        return (0.0, 0.0, 0.0, r2)

    def get_equity(self) -> Tuple[float, float, Any]:
        eq, avail, _, raw = self.get_equity_and_margin_ratio()
        return (eq, avail, raw)

    def get_price(self, sym: str) -> float:
        try:
            return float(requests.get(f"{FUTURES_URL}/fapi/v1/ticker/price", params={"symbol": sym.upper()}, timeout=5).json().get("price", 0.0))
        except Exception:
            return 0.0

    def get_min_notional(self, sym: str) -> float:
        if sym.upper() not in self._precisions:
            self._load_exchange_info()
        return self._precisions.get(sym.upper(), (0.001, 3, 2, 5.0))[3]

    def format_qty(self, sym: str, raw_qty: float, round_up: bool = False) -> float:
        if sym.upper() not in self._precisions:
            self._load_exchange_info()
        step, dec, _, _ = self._precisions.get(sym.upper(), (0.001, 3, 2, 5.0))
        sc = round(raw_qty / step, 8)
        return round((math.ceil(sc) if round_up else math.floor(sc)) * step, dec)

    def format_price(self, sym: str, raw_price: float) -> float:
        if sym.upper() not in self._precisions:
            self._load_exchange_info()
        return round(raw_price, self._precisions.get(sym.upper(), (0.001, 3, 2, 5.0))[2])

    def set_leverage(self, sym: str, lev: int):
        sym = sym.upper()
        if self._leverage_cache.get(sym) == lev:
            return
        p = {"symbol": sym, "leverage": lev, "timestamp": int(time.time() * 1000) + self.time_offset, "recvWindow": 60000}
        p["signature"] = self._sign(p)
        res = self._safe_request("POST", "/fapi/v1/leverage", data=p)
        if isinstance(res, dict) and res.get("leverage") == lev:
            self._leverage_cache[sym] = lev

    def order(self, sym: str, side: str, qty: float, reduce_only: bool = False):
        fq = self.format_qty(sym, qty)
        if fq <= 0:
            return None
        p = {"symbol": sym.upper(), "side": side.upper(), "type": "MARKET", "quantity": fq, "timestamp": int(time.time() * 1000) + self.time_offset, "recvWindow": 60000}
        if reduce_only:
            p["reduceOnly"] = "true"
        p["signature"] = self._sign(p)
        return self._safe_request("POST", "/fapi/v1/order", data=p, retries=3)

    def set_sl(self, sym: str, side: str, price: float) -> bool:
        fp = self.format_price(sym, price)
        for attempt in range(3):
            p = {"algoType": "CONDITIONAL", "symbol": sym.upper(), "side": side.upper(), "type": "STOP_MARKET", "triggerPrice": fp,
                 "closePosition": "true", "workingType": "MARK_PRICE", "timestamp": int(time.time() * 1000) + self.time_offset, "recvWindow": 60000}
            p["signature"] = self._sign(p)
            res = self._safe_request("POST", "/fapi/v1/algoOrder", data=p, retries=2)
            if isinstance(res, dict) and "algoId" in res:
                self._current_sl[sym.upper()] = fp
                return True
            time.sleep(0.35 * (attempt + 1))
        return False

    def get_existing_sl_price(self, sym: str) -> Optional[float]:
        sym = sym.upper()
        p1 = {"symbol": sym, "timestamp": int(time.time() * 1000) + self.time_offset, "recvWindow": 60000}
        p1["signature"] = self._sign(p1)
        r1 = self._safe_request("GET", "/fapi/v1/openAlgoOrders", params=p1, retries=2)
        orders = r1.get("orders", []) if isinstance(r1, dict) else (r1 if isinstance(r1, list) else [])
        for o in orders:
            if o.get("symbol") == sym and o.get("orderType", o.get("type", "")) in ("STOP_MARKET", "STOP"):
                tp = float(o.get("triggerPrice", o.get("stopPrice", 0.0)))
                if tp > 0:
                    self._current_sl[sym] = tp
                    return tp
        return None

    def cancel_all(self, sym: str):
        sym = sym.upper()
        for ep in ("/fapi/v1/allOpenOrders", "/fapi/v1/algoOpenOrders"):
            p = {"symbol": sym, "timestamp": int(time.time() * 1000) + self.time_offset, "recvWindow": 60000}
            p["signature"] = self._sign(p)
            self._safe_request("DELETE", ep, params=p, retries=2)
        self._current_sl.pop(sym, None)

    def get_position_info(self, sym: str) -> Tuple[float, float]:
        p = {"symbol": sym.upper(), "timestamp": int(time.time() * 1000) + self.time_offset, "recvWindow": 60000}
        p["signature"] = self._sign(p)
        res = self._safe_request("GET", "/fapi/v2/positionRisk", params=p)
        if isinstance(res, list):
            for pos in res:
                if pos.get("symbol") == sym.upper():
                    return (float(pos.get("positionAmt", 0.0)), float(pos.get("entryPrice", 0.0)))
        return (0.0, 0.0)

    def verify_position_opened(self, sym: str, expected_action: str, retries: int = 3) -> Tuple[float, float]:
        for i in range(retries):
            amt, ep = self.get_position_info(sym)
            if (expected_action == "LONG" and amt > 0) or (expected_action == "SHORT" and amt < 0):
                return (amt, ep)
            time.sleep(0.35 * (i + 1))
        return (0.0, 0.0)

    def get_all_active_positions(self) -> List[Dict[str, Any]]:
        p = {"timestamp": int(time.time() * 1000) + self.time_offset, "recvWindow": 60000}
        p["signature"] = self._sign(p)
        res = self._safe_request("GET", "/fapi/v2/positionRisk", params=p)
        active = []
        if isinstance(res, list):
            for pos in res:
                amt = float(pos.get("positionAmt", 0.0))
                if abs(amt) > 0:
                    ep, mp, upnl = float(pos.get("entryPrice", 0.0)), float(pos.get("markPrice", 0.0)), float(pos.get("unRealizedProfit", 0.0))
                    notional = abs(amt) * (mp if mp > 0 else ep)
                    if notional >= 1.0:
                        active.append({"symbol": pos.get("symbol", ""), "amt": amt, "entry_price": ep, "mark_price": mp, "upnl": upnl, "notional": notional})
        return active

init_db()
client = BinanceClient(BINANCE_API_KEY, BINANCE_API_SECRET)
daily_start_balance = 0.0
last_report_date = None
last_hourly_key = None

def reconcile_state_with_binance(notify_if_repaired: bool = True) -> str:
    try:
        positions = client.get_all_active_positions()
        if not positions:
            return "📭 重啟檢查：目前無未平倉部位"
        synced, repaired = [], []
        for pos in positions:
            sym, amt, ep = pos["symbol"], pos["amt"], pos["entry_price"]
            mp = pos["mark_price"] if pos["mark_price"] > 0 else ep
            bot_state["last_symbol"] = sym
            ex_sl = client.get_existing_sl_price(sym)
            if ex_sl and ex_sl > 0:
                synced.append(f"{sym}(止損 {ex_sl})")
            else:
                sl_side = "SELL" if amt > 0 else "BUY"
                fb = EMERGENCY_SL_FALLBACK_PCT / 100.0
                if amt > 0:
                    safe_sl = ep * (1.0 + BE_BUFFER_PCT / 100.0) if mp >= ep * 1.01 else min(ep, mp) * (1.0 - fb)
                else:
                    safe_sl = ep * (1.0 - BE_BUFFER_PCT / 100.0) if mp <= ep * 0.99 else max(ep, mp) * (1.0 + fb)
                ok = client.set_sl(sym, sl_side, safe_sl)
                repaired.append(f"{sym}(已自動補掛止損 {client.format_price(sym, safe_sl)})" if ok else f"{sym}(⚠️補掛失敗)")
        msg = f"🔄 狀態對齊：已接管 {len(positions)} 筆持倉"
        if synced:
            msg += f"\n🛡️ 既有止損：{', '.join(synced)}"
        if repaired:
            msg += f"\n🔧 自動補掛：{', '.join(repaired)}"
            if notify_if_repaired:
                send_line_push(f"🛡️ 【重啟狀態對齊・自動補掛止損】\n📦 接管持倉：{len(positions)} 筆\n🔧 自動修復：{', '.join(repaired)}")
        return msg
    except Exception as e:
        return f"⚠️ 狀態對齊異常: {e}"

def check_emergency_hard_brake(current_eq: float) -> bool:
    floor_u = bot_state["emergency_floor_usdt"]
    if floor_u <= 0 or current_eq <= 0:
        return False
    if current_eq <= floor_u and not bot_state["emergency_brake_tripped"]:
        bot_state["emergency_brake_tripped"] = True
        bot_state["trading_enabled"] = False
        closed = []
        for pos in client.get_all_active_positions():
            sym, amt = pos["symbol"], pos["amt"]
            client.cancel_all(sym)
            client.order(sym, "SELL" if amt > 0 else "BUY", abs(amt), reduce_only=True)
            record_trade_log(sym, "EMERGENCY_BRAKE", amt, abs(amt), pos["entry_price"], pos["mark_price"])
            closed.append(f"{sym}({abs(amt)}顆)")
        send_line_push(
            f"🛑 【最高警報：總權益極限斷路器觸發】\n"
            f"💰 當前權益：{current_eq:.2f} USDT\n"
            f"🚨 觸發底線：{floor_u:.2f} USDT\n"
            f"✂️ 強制清倉：{', '.join(closed) if closed else '無持倉'}\n"
            f"🔒 系統狀態：已強制全平並停機，保住剩餘本金！"
        )
        return True
    return bot_state["emergency_brake_tripped"]

def check_daily_circuit_breaker(current_eq: float) -> bool:
    global daily_start_balance
    if check_emergency_hard_brake(current_eq):
        return True
    if daily_start_balance <= 0 or current_eq <= 0:
        return False
    dd_pct = (daily_start_balance - current_eq) / daily_start_balance * 100.0
    max_dd = bot_state["max_daily_dd_pct"]
    if dd_pct >= max_dd and not bot_state["circuit_breaker_tripped"]:
        bot_state["circuit_breaker_tripped"] = True
        bot_state["trading_enabled"] = False
        send_line_push(
            f"🚨 【每日最大回撤熔斷啟動】\n"
            f"📊 今日基準：{daily_start_balance:.2f} USDT\n"
            f"💰 當前權益：{current_eq:.2f} USDT\n"
            f"📉 當日回撤：-{dd_pct:.2f}% (達上限 -{max_dd:.1f}%)\n"
            f"🛡️ 防禦動作：已暫停開新倉，明日 08:00 自動重置"
        )
        return True
    return bot_state["circuit_breaker_tripped"]

def build_status_report(title: str = "📊 【即時帳戶與持倉查詢】") -> str:
    global daily_start_balance
    now_str = datetime.now(TW_TZ).strftime("%Y-%m-%d %H:%M:%S")
    total_eq, avail, margin_ratio, _ = client.get_equity_and_margin_ratio()
    if daily_start_balance <= 0 and total_eq > 0:
        daily_start_balance = total_eq
    if bot_state["emergency_floor_usdt"] <= 0 and total_eq > 0:
        bot_state["emergency_floor_usdt"] = round(total_eq * 0.5, 1)
    positions = client.get_all_active_positions()

    if bot_state["emergency_brake_tripped"]:
        mode_str = f"🛑 極限斷路停機 (底線 {bot_state['emergency_floor_usdt']:.1f}U)"
    elif bot_state["circuit_breaker_tripped"]:
        mode_str = f"🚨 熔斷防禦中 (滿 -{bot_state['max_daily_dd_pct']:.0f}%)"
    elif bot_state["trading_enabled"]:
        mode_str = "🟢 正常自動接單中"
    else:
        mode_str = "⏸️ 已暫停新開倉"

    if daily_start_balance > 0 and total_eq > 0:
        diff = total_eq - daily_start_balance
        pct = (diff / daily_start_balance) * 100.0
        daily_pnl_str = f"{'🟢' if diff >= 0 else '🔴'} {diff:+.2f} USDT ({pct:+.2f}%)"
    else:
        daily_pnl_str = "⚪ 0.00 USDT (0.00%)"

    lines = [
        f"{title}",
        f"🕒 查詢時間：{now_str}",
        f"🤖 系統狀態：{mode_str}",
        f"💰 帳戶權益：{total_eq:.2f} USDT",
        f"💵 可用保證：{avail:.2f} USDT",
        f"📈 今日損益：{daily_pnl_str}",
        f"🛡️ 防護底線：日熔斷 -{bot_state['max_daily_dd_pct']:.0f}% / 強平底線 {bot_state['emergency_floor_usdt']:.1f}U",
    ]
    if not positions:
        sym = bot_state.get("last_symbol", DEFAULT_SYMBOL)
        lines.append("📭 目前持倉：空手觀望中\n")
        lines.append(f"🔍 最近幣種：{sym} (現價 {client.get_price(sym)})")
    else:
        lines.append(f"📦 目前持倉：共 {len(positions)} 筆部位 (保證金率 {margin_ratio:.1f}%)")
        for pos in positions:
            sym, amt, upnl = pos["symbol"], pos["amt"], pos["upnl"]
            sl_val = client._current_sl.get(sym) or client.get_existing_sl_price(sym) or "依幣安條件單"
            lines.append(
                f"\n🔸 【{sym}】{'🟢 做多 LONG' if amt > 0 else '🔴 做空 SHORT'}\n"
                f"   持倉顆數：{abs(amt)} 顆 (約 {pos['notional']:.1f}U)\n"
                f"   開倉均價：{pos['entry_price']}\n"
                f"   標記現價：{pos['mark_price']}\n"
                f"   浮動損益：{'🟢' if upnl >= 0 else '🔴'} {upnl:+.2f} USDT\n"
                f"   當前止損：{sl_val}"
            )
    return "\n".join(lines)

def background_scheduler():
    global daily_start_balance, last_report_date, last_hourly_key
    try:
        init_eq, _, _, _ = client.get_equity_and_margin_ratio()
        if init_eq > 0:
            daily_start_balance = init_eq
            if bot_state["emergency_floor_usdt"] <= 0:
                bot_state["emergency_floor_usdt"] = round(init_eq * 0.5, 1)
    except Exception:
        pass
    while True:
        try:
            if not client.ping():
                bot_state["consecutive_api_failures"] += 1
                if bot_state["consecutive_api_failures"] >= 3 and not bot_state["api_disconnected_alerted"]:
                    bot_state["api_disconnected_alerted"] = True
                    send_line_push("⚠️ 【警告：機器人與幣安交易所連線異常】\n🚨 狀態說明：已連續 3 次無法連上幣安 API\n💡 建議操作：請立即開啟幣安 App 檢查倉位！")
                time.sleep(20)
                continue
            elif bot_state["api_disconnected_alerted"]:
                bot_state["api_disconnected_alerted"] = False
                bot_state["consecutive_api_failures"] = 0
                send_line_push(f"🟢 【幣安交易所連線已恢復】\n{reconcile_state_with_binance(notify_if_repaired=False)}")
            bot_state["consecutive_api_failures"] = 0

            now_tw = datetime.now(TW_TZ)
            today_date, hour_key = now_tw.date(), (now_tw.date(), now_tw.hour)
            total_eq, avail, margin_ratio, _ = client.get_equity_and_margin_ratio()
            if daily_start_balance <= 0 and total_eq > 0:
                daily_start_balance = total_eq
            if bot_state["emergency_floor_usdt"] <= 0 and total_eq > 0:
                bot_state["emergency_floor_usdt"] = round(total_eq * 0.5, 1)
            if total_eq > 0:
                check_daily_circuit_breaker(total_eq)

            now_ts = time.time()
            if margin_ratio >= MARGIN_ALERT_RATIO and (now_ts - bot_state["last_margin_alert_ts"] > 1800):
                bot_state["last_margin_alert_ts"] = now_ts
                send_line_push(f"⚠️ 【高風險！保證金率強平預警】\n🚨 保證金率：{margin_ratio:.1f}%\n💰 帳戶權益：{total_eq:.2f} USDT\n💡 請立即輸入「3」減倉或「2」全平降低風險！")

            if now_tw.hour == 8 and now_tw.minute < 5 and last_report_date != today_date:
                was_tripped = bot_state["circuit_breaker_tripped"]
                if was_tripped and not bot_state["emergency_brake_tripped"]:
                    bot_state["circuit_breaker_tripped"] = False
                    bot_state["trading_enabled"] = True
                if daily_start_balance > 0 and total_eq > 0:
                    diff = total_eq - daily_start_balance
                    pct = (diff / daily_start_balance) * 100.0
                    reset_note = "\n🔄 熔斷重置：已恢復自動接單" if was_tripped else ""
                    daily_msg = f"🌅 【每日 08:00 帳戶餘額損益日報】\n📅 結算日期：{today_date}\n💰 今日權益：{total_eq:.2f} USDT\n💵 可用保證：{avail:.2f} USDT\n📊 昨日基準：{daily_start_balance:.2f} USDT\n🏆 單日損益：{'🟢' if diff >= 0 else '🔴'} {diff:+.2f} USDT ({pct:+.2f}%){reset_note}"
                else:
                    daily_msg = f"🌅 【每日 08:00 帳戶餘額損益日報】\n📅 結算日期：{today_date}\n💰 今日權益：{total_eq:.2f} USDT"
                send_line_push(daily_msg)
                if total_eq > 0:
                    daily_start_balance = total_eq
                last_report_date, last_hourly_key = today_date, hour_key
            elif bot_state["hourly_push_enabled"] and now_tw.minute < 5 and last_hourly_key != hour_key:
                send_line_push(build_status_report(title=f"⏰ 【每小時整點巡檢報告 ({now_tw.hour:02d}:00)】"))
                last_hourly_key = hour_key
            time.sleep(25)
        except Exception:
            time.sleep(60)

@asynccontextmanager
async def lifespan(app: FastAPI):
    reconcile_summary = reconcile_state_with_binance(notify_if_repaired=False)
    threading.Thread(target=background_scheduler, daemon=True).start()
    if LINE_CHANNEL_ACCESS_TOKEN and (LINE_USER_ID or bot_state.get("bound_user_id")):
        send_line_push(f"🤖 【幣安合約機器人 v4.1 終極裝甲版】已上線！\n{reconcile_summary}\n💡 輸入「0」查看選單，輸入「9」查看統計週報。")
    yield

app = FastAPI(lifespan=lifespan)

class Signal(BaseModel):
    passphrase: str
    symbol: str
    action: str
    risk_pct: Optional[float] = 1.5
    stop_loss: Optional[float] = None
    leverage: Optional[int] = 10
    close_pct: Optional[float] = None
    signal_price: Optional[float] = None

@app.get("/")
def index():
    return {"status": "online", "version": "v4.1 Ultimate Armor", "trading_enabled": bot_state["trading_enabled"]}

@app.get("/test-binance")
def test_binance():
    total_eq, avail, margin_ratio, raw_debug = client.get_equity_and_margin_ratio()
    return {"status": "success", "total_equity_usdt": total_eq, "available_balance_usdt": avail, "margin_ratio_pct": round(margin_ratio, 2), "binance_raw_response": raw_debug}

def execute_line_command(cmd_text: str, user_id: str) -> str:
    global daily_start_balance
    text, upper_text = cmd_text.strip(), cmd_text.strip().upper()
    if not bot_state.get("bound_user_id") and user_id:
        bot_state["bound_user_id"] = user_id
    if LINE_USER_ID and user_id != LINE_USER_ID:
        return "⛔ 權限不足：僅限本人帳號操控。"

    if text in ("1", "查詢", "狀態", "倉位", "餘額", "查帳"):
        return build_status_report(title="📊 【即時帳戶與持倉查詢】")

    if text in ("2", "平倉", "全平", "一鍵平倉", "緊急平倉") or upper_text.startswith("平倉 "):
        parts = upper_text.split()
        target_sym = parts[1] if len(parts) > 1 else None
        positions = [p for p in client.get_all_active_positions() if (not target_sym or p["symbol"] == target_sym)]
        if not positions:
            sym_clean = target_sym or bot_state.get("last_symbol", DEFAULT_SYMBOL)
            client.cancel_all(sym_clean)
            return f"📭 目前無開倉部位（已清理 {sym_clean} 掛單）。"
        closed = []
        for pos in positions:
            sym, amt = pos["symbol"], pos["amt"]
            client.cancel_all(sym)
            client.order(sym, "SELL" if amt > 0 else "BUY", abs(amt), reduce_only=True)
            record_trade_log(sym, "MANUAL_CLOSE", amt, abs(amt), pos["entry_price"], pos["mark_price"])
            closed.append(f"🔸 {sym}：全平 {abs(amt)} 顆 ({pos['upnl']:+.2f}U)")
        total_eq, _, _ = client.get_equity()
        return "🚨 【LINE 一鍵全平倉完畢】\n" + "\n".join(closed) + f"\n💰 最新權益：{total_eq:.2f} USDT"

    if text in ("3", "減半", "平半", "減倉"):
        positions = client.get_all_active_positions()
        if not positions:
            return "📭 目前無持倉可供減半。"
        res = []
        for pos in positions:
            sym, amt, min_not = pos["symbol"], pos["amt"], client.get_min_notional(pos["symbol"])
            hq = client.format_qty(sym, abs(amt) * 0.5)
            rq = client.format_qty(sym, abs(amt) - hq)
            if hq <= 0 or (rq * pos["mark_price"] < min_not):
                res.append(f"🔸 {sym}：倉位過小無法拆半")
            else:
                client.order(sym, "SELL" if amt > 0 else "BUY", hq, reduce_only=True)
                pnl_est = record_trade_log(sym, "MANUAL_HALF", amt, hq, pos["entry_price"], pos["mark_price"])
                res.append(f"🔸 {sym}：已減倉 {hq} 顆 ({pnl_est:+.2f}U)，剩 {rq} 顆")
        return "✂️ 【LINE 手動減倉 50%】\n" + "\n".join(res)

    if text in ("4", "保本", "移保本"):
        positions = client.get_all_active_positions()
        if not positions:
            return "📭 目前無持倉可設定保本。"
        res = []
        for pos in positions:
            sym, amt, ep, mp = pos["symbol"], pos["amt"], pos["entry_price"], pos["mark_price"]
            be_p = ep * (1.0 + BE_BUFFER_PCT / 100.0) if amt > 0 else ep * (1.0 - BE_BUFFER_PCT / 100.0)
            if (amt > 0 and mp <= be_p) or (amt < 0 and mp >= be_p):
                res.append(f"🔸 {sym}：現價 ({mp}) 尚未超過保本價 ({client.format_price(sym, be_p)})")
            else:
                client.cancel_all(sym)
                ok = client.set_sl(sym, "SELL" if amt > 0 else "BUY", be_p)
                res.append(f"🔸 {sym}：{'✅ 已移至保本價 ' + str(client.format_price(sym, be_p)) if ok else '❌ 掛單失敗'}")
        return "🛡️ 【LINE 手動移保本】\n" + "\n".join(res)

    m_sl = re.match(r"^(?:5|止損|改止損|SL)\s+([0-9.]+)$", upper_text)
    if m_sl:
        new_sl = float(m_sl.group(1))
        positions = client.get_all_active_positions()
        if not positions:
            return "📭 目前無持倉，無法設定止損。"
        sym, amt = positions[0]["symbol"], positions[0]["amt"]
        client.cancel_all(sym)
        ok = client.set_sl(sym, "SELL" if amt > 0 else "BUY", new_sl)
        return f"✅ 已將 {sym} 止損更新為：{client.format_price(sym, new_sl)}" if ok else f"❌ 更新 {sym} 止損失敗。"

    if text in ("6", "暫停", "停止", "暫停開倉"):
        bot_state["trading_enabled"] = False
        return "⏸️ 【已暫停自動開新倉】\n既有持倉止損與止盈保護仍照常運作。輸入「7」可恢復。"

    if text in ("7", "恢復", "啟動", "開始", "恢復開倉"):
        bot_state["trading_enabled"] = True
        bot_state["circuit_breaker_tripped"] = False
        bot_state["emergency_brake_tripped"] = False
        eq, _, _ = client.get_equity()
        if eq > 0:
            daily_start_balance = eq
            if bot_state["emergency_floor_usdt"] >= eq:
                bot_state["emergency_floor_usdt"] = round(eq * 0.5, 1)
        return "🟢 【已恢復自動交易】\n機器人已重新開始接收所有進出場訊號！"

    if text in ("8", "整點推播", "每小時推播"):
        bot_state["hourly_push_enabled"] = not bot_state["hourly_push_enabled"]
        return f"⏰ 【每小時整點推播】：{'🟢 已開啟' if bot_state['hourly_push_enabled'] else '⏸️ 已關閉'}"

    if text in ("9", "統計", "週報", "戰績", "報表", "歷史"):
        return build_trade_stats_report(days=7)

    if text in ("10", "對齊", "巡檢", "檢查止損"):
        return reconcile_state_with_binance(notify_if_repaired=False)

    m_cb = re.match(r"^(?:熔斷|回撤|DD)\s+([0-9.]+)$", upper_text)
    if m_cb:
        bot_state["max_daily_dd_pct"] = max(1.0, min(95.0, float(m_cb.group(1))))
        return f"🛡️ 【每日最大回撤熔斷】已設為：-{bot_state['max_daily_dd_pct']:.1f}%"

    m_fl = re.match(r"^(?:底線|斷路|FLOOR)\s+([0-9.]+)$", upper_text)
    if m_fl:
        bot_state["emergency_floor_usdt"] = max(0.0, float(m_fl.group(1)))
        bot_state["emergency_brake_tripped"] = False
        return f"🛑 【總權益極限斷路底線】已設為：{bot_state['emergency_floor_usdt']:.2f} USDT"

    m_slp = re.match(r"^(?:滑點|SLIP)\s+([0-9.]+)$", upper_text)
    if m_slp:
        bot_state["max_entry_slippage_pct"] = max(0.1, min(10.0, float(m_slp.group(1))))
        return f"🛡️ 【開倉前滑點攔截上限】已設為：{bot_state['max_entry_slippage_pct']:.2f}%"

    if upper_text in ("ID", "USERID", "綁定"):
        return f"🔑 你的 LINE_USER_ID：\n{user_id}"

    return (
        "🤖 【幣安機器人 v4.1 遙控選單】\n"
        "🔹 輸入「1」查詢權益、今日損益與持倉\n"
        "🔹 輸入「2」🚨 一鍵市價全平所有持倉\n"
        "🔹 輸入「3」減倉 50% / 輸入「4」移保本\n"
        "🔹 輸入「止損 價格」修改止損價\n"
        "🔹 輸入「6」暫停 / 輸入「7」恢復開倉\n"
        "🔹 輸入「9」📊 查看近 7 日戰績統計週報\n"
        "🔹 輸入「10」🔄 巡檢倉位並自動補掛止損\n"
        f"🔹 輸入「熔斷 數字」(目前 -{bot_state['max_daily_dd_pct']:.0f}%)\n"
        f"🔹 輸入「底線 金額」(目前 {bot_state['emergency_floor_usdt']:.1f}U)\n"
        f"🔹 輸入「滑點 數字」(目前 {bot_state['max_entry_slippage_pct']:.1f}%)"
    )

@app.post("/line-webhook")
async def line_webhook(request: Request):
    raw_body = await request.body()
    if LINE_CHANNEL_SECRET:
        sig = request.headers.get("X-Line-Signature", "")
        exp = base64.b64encode(hmac.new(LINE_CHANNEL_SECRET.encode("utf-8"), raw_body, hashlib.sha256).digest()).decode("utf-8")
        if not secrets.compare_digest(sig, exp):
            raise HTTPException(status_code=400, detail="Invalid LINE signature")
    try:
        events = (await request.json()).get("events", [])
    except Exception:
        return {"status": "ok"}
    for ev in events:
        if ev.get("type") == "message" and ev.get("message", {}).get("type") == "text":
            async with webhook_lock:
                reply_msg = execute_line_command(ev["message"]["text"], ev.get("source", {}).get("userId", ""))
            send_line_reply(ev.get("replyToken", ""), reply_msg)
    return {"status": "ok"}

@app.post("/webhook")
async def handle_webhook(request: Request):
    try:
        sig = Signal(**(await request.json()))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid payload: {e}")
    if not secrets.compare_digest(sig.passphrase, WEBHOOK_PASSPHRASE):
        raise HTTPException(status_code=401, detail="密碼錯誤")

    sym, action = sig.symbol.upper(), sig.action.upper()
    bot_state["last_symbol"] = sym

    async with webhook_lock:
        if is_duplicate_signal(sym, action, sig.stop_loss):
            return {"success": True, "msg": "重複訊號已防呆略過"}

        if action in ("TP1_LONG", "TP1_SHORT"):
            pos_amt, entry_price = client.get_position_info(sym)
            if abs(pos_amt) <= 0 or ("LONG" in action and pos_amt < 0) or ("SHORT" in action and pos_amt > 0):
                return {"success": True, "msg": "無對應持倉，略過TP1"}
            side, curr_p, min_not = ("SELL" if "LONG" in action else "BUY"), client.get_price(sym), client.get_min_notional(sym)
            pct = (sig.close_pct if (sig.close_pct and 0 < sig.close_pct < 100) else 40.0) / 100.0
            close_qty = client.format_qty(sym, abs(pos_amt) * pct)
            rem_qty = client.format_qty(sym, abs(pos_amt) - close_qty)
            realized_note = ""
            if 0 < close_qty < abs(pos_amt) and (rem_qty * curr_p >= min_not):
                client.order(sym, side, close_qty, reduce_only=True)
                net_u = record_trade_log(sym, "TP1", pos_amt, close_qty, entry_price, curr_p)
                tp_note = f"已減倉 {int(round(pct*100))}% ({close_qty} 顆)，尾倉 {rem_qty} 顆"
                realized_note = f"💵 落袋損益：🟢 {net_u:+.2f} USDT\n"
            else:
                tp_note = f"尾倉拆減低於 {min_not}U，保留 {abs(pos_amt)} 顆直接移保本"

            be_buf = entry_price * (1.0 + BE_BUFFER_PCT / 100.0) if "LONG" in action else entry_price * (1.0 - BE_BUFFER_PCT / 100.0)
            target_sl = (max(be_buf, sig.stop_loss) if "LONG" in action else min(be_buf, sig.stop_loss)) if (sig.stop_loss and sig.stop_loss > 0) else be_buf
            client.cancel_all(sym)
            if not client.set_sl(sym, side, target_sl):
                rem_amt, _ = client.get_position_info(sym)
                if abs(rem_amt) > 0:
                    client.order(sym, side, abs(rem_amt), reduce_only=True)
                    record_trade_log(sym, "TP1_PROTECT", pos_amt, abs(rem_amt), entry_price, curr_p)
                send_line_push(f"🛡️ 【TP1 回落保護全平】\n🪙 交易幣種：{sym}\n⚠️ 現價已穿過保本價，自動市價全平鎖利！")
                return {"success": True, "msg": "已全平保護"}
            send_line_push(f"🎯 【1.5R TP1 一階止盈 & 保本】\n🪙 交易幣種：{sym}\n✂️ 執行動作：{tp_note}\n{realized_note}🏷️ 開倉均價：{entry_price}\n🛡️ 保本止損：{client.format_price(sym, target_sl)}")
            return {"success": True, "msg": "TP1完成"}

        if action in ("TP2_LONG", "TP2_SHORT"):
            pos_amt, entry_price = client.get_position_info(sym)
            if abs(pos_amt) <= 0 or ("LONG" in action and pos_amt < 0) or ("SHORT" in action and pos_amt > 0):
                return {"success": True, "msg": "無對應持倉，略過TP2"}
            side, curr_p, min_not = ("SELL" if "LONG" in action else "BUY"), client.get_price(sym), client.get_min_notional(sym)
            pct = (sig.close_pct if (sig.close_pct and 0 < sig.close_pct < 100) else 66.67) / 100.0
            close_qty = client.format_qty(sym, abs(pos_amt) * pct)
            rem_qty = client.format_qty(sym, abs(pos_amt) - close_qty)
            if rem_qty * curr_p < min_not:
                min_keep = client.format_qty(sym, (min_not * 1.05) / curr_p, round_up=True)
                close_qty = client.format_qty(sym, abs(pos_amt) - min_keep) if abs(pos_amt) > min_keep else 0.0
                rem_qty = client.format_qty(sym, abs(pos_amt) - close_qty)
            realized_note = ""
            if close_qty > 0 and (close_qty * curr_p >= min_not):
                client.order(sym, side, close_qty, reduce_only=True)
                net_u = record_trade_log(sym, "TP2", pos_amt, close_qty, entry_price, curr_p)
                tp2_note = f"再減倉 {close_qty} 顆，留尾倉 {rem_qty} 顆衝刺"
                realized_note = f"💵 落袋損益：🟢 {net_u:+.2f} USDT\n"
            else:
                tp2_note = f"保留尾倉 {abs(pos_amt)} 顆跟著海龜線衝刺"
            if sig.stop_loss and sig.stop_loss > 0:
                client.cancel_all(sym)
                if not client.set_sl(sym, side, sig.stop_loss):
                    rem_amt, _ = client.get_position_info(sym)
                    if abs(rem_amt) > 0:
                        client.order(sym, side, abs(rem_amt), reduce_only=True)
            send_line_push(f"🚀 【3.0R TP2 二階止盈成功】\n🪙 交易幣種：{sym}\n✂️ 執行動作：{tp2_note}\n{realized_note}🏷️ 開倉均價：{entry_price} | 現價：{curr_p}\n🛡️ 上推止損：{client._current_sl.get(sym, '保本止損')}")
            return {"success": True, "msg": "TP2完成"}

        if action == "UPDATE_SL":
            if not sig.stop_loss:
                return {"success": False, "msg": "缺少 stop_loss"}
            pos_amt, entry_price = client.get_position_info(sym)
            if abs(pos_amt) <= 0:
                return {"success": True, "msg": "無持倉"}
            sl_side, curr_p = ("SELL" if pos_amt > 0 else "BUY"), client.get_price(sym)
            client.cancel_all(sym)
            if not client.set_sl(sym, sl_side, sig.stop_loss):
                rem_amt, _ = client.get_position_info(sym)
                net_u = 0.0
                if abs(rem_amt) > 0:
                    client.order(sym, sl_side, abs(rem_amt), reduce_only=True)
                    net_u = record_trade_log(sym, "TRAIL_SL", pos_amt, abs(rem_amt), entry_price, curr_p)
                send_line_push(f"🛡️ 【海龜移動止損觸發出場】\n🪙 交易幣種：{sym}\n🏷️ 出場價格：{curr_p}\n💵 結算損益：{net_u:+.2f} USDT")
                return {"success": True, "msg": "已市價出場"}
            fmt_sl = client.format_price(sym, sig.stop_loss)
            send_line_push(f"🐢 【海龜移動止損上推更新】\n🪙 交易幣種：{sym} ({'做多 LONG' if pos_amt > 0 else '做空 SHORT'})\n📦 當前持倉：{abs(pos_amt)} 顆 (均價 {entry_price})\n🛡️ 最新止損：{fmt_sl}")
            return {"success": True, "msg": f"止損已更新至 {fmt_sl}"}

        if action in ("CLOSE_LONG", "CLOSE_SHORT"):
            pos_amt, entry_price = client.get_position_info(sym)
            if abs(pos_amt) > 0:
                if ("LONG" in action and pos_amt < 0) or ("SHORT" in action and pos_amt > 0):
                    return {"success": True, "msg": "方向已變"}
                client.cancel_all(sym)
                curr_p = client.get_price(sym)
                client.order(sym, "SELL" if pos_amt > 0 else "BUY", abs(pos_amt), reduce_only=True)
                net_u = record_trade_log(sym, "STOP_EXIT", pos_amt, abs(pos_amt), entry_price, curr_p)
                total_eq, _, _ = client.get_equity()
                check_daily_circuit_breaker(total_eq)
                send_line_push(f"🏁 【止損 / 趨勢出場平倉完成】\n🪙 交易幣種：{sym} ({'多單平倉' if pos_amt > 0 else '空單平倉'})\n📦 平倉顆數：{abs(pos_amt)} 顆\n🏷️ 進場均價：{entry_price} | 出場：{curr_p}\n💵 本筆損益：{'🟢' if net_u >= 0 else '🔴'} {net_u:+.2f} USDT\n💰 帳戶權益：{total_eq:.2f} USDT")
                return {"success": True, "msg": "已平倉"}
            client.cancel_all(sym)
            return {"success": True, "msg": "已清理掛單"}

        if action in ("LONG", "SHORT"):
            pos_amt, old_entry = client.get_position_info(sym)
            curr_p, min_not = client.get_price(sym), client.get_min_notional(sym)
            if ((action == "LONG" and pos_amt > 0) or (action == "SHORT" and pos_amt < 0)) and (abs(pos_amt) * curr_p >= min_not):
                return {"success": True, "msg": "已有同向持倉"}
            reversed_note = ""
            if abs(pos_amt) > 0:
                client.cancel_all(sym)
                client.order(sym, "BUY" if pos_amt < 0 else "SELL", abs(pos_amt), reduce_only=True)
                rev_pnl = record_trade_log(sym, "REVERSE_CLOSE", pos_amt, abs(pos_amt), old_entry, curr_p)
                reversed_note = f"🔄 反手平舊倉：{abs(pos_amt)} 顆 ({rev_pnl:+.2f}U)\n"
                time.sleep(0.4)

            total_eq, avail, _ = client.get_equity()
            account_base = max(total_eq, avail)
            if check_daily_circuit_breaker(account_base) or not bot_state["trading_enabled"]:
                reason = "已觸發極限斷路器" if bot_state["emergency_brake_tripped"] else ("今日回撤達熔斷上限" if bot_state["circuit_breaker_tripped"] else "目前為暫停開倉模式")
                send_line_push(f"⏸️ 【收到 {sym} {action} 訊號，已略過新進場】\n{reversed_note}🛡️ 略過原因：{reason}\n💡 輸入「7」可恢復開倉")
                return {"success": True, "msg": reason}

            if not sig.stop_loss:
                raise HTTPException(status_code=400, detail="缺少止損價")
            lev = sig.leverage or 10
            client.set_leverage(sym, lev)
            if curr_p <= 0:
                curr_p = client.get_price(sym)

            if sig.signal_price and sig.signal_price > 0:
                adv_slip = ((curr_p - sig.signal_price) / sig.signal_price * 100.0) if action == "LONG" else ((sig.signal_price - curr_p) / sig.signal_price * 100.0)
                if adv_slip > bot_state["max_entry_slippage_pct"]:
                    send_line_push(f"🛡️ 【滑點保護攔截・放棄追價】\n{reversed_note}🪙 交易幣種：{sym} ({action})\n🏷️ 訊號價格：{sig.signal_price} | 現價：{curr_p} (+{adv_slip:.2f}%)\n💡 滑點超過上限 ({bot_state['max_entry_slippage_pct']:.1f}%)，放棄進場")
                    return {"success": False, "msg": "滑點過大已攔截"}

            if (action == "LONG" and curr_p <= sig.stop_loss) or (action == "SHORT" and curr_p >= sig.stop_loss):
                return {"success": False, "msg": "現價已越過止損價"}

            risk_usd = account_base * ((sig.risk_pct or 1.5) / 100.0)
            safe_dist = max(abs(curr_p - sig.stop_loss), curr_p * 0.002)
            raw_qty = max(risk_usd / safe_dist, client.format_qty(sym, (min_not * 1.05) / curr_p, round_up=True) if (risk_usd / safe_dist * curr_p < min_not) else 0.0)
            target_qty = client.format_qty(sym, min(raw_qty, ((avail if avail > 0 else account_base) * lev * 0.95) / curr_p))
            if target_qty <= 0 or (target_qty * curr_p) < min_not:
                send_line_push(f"⚠️ 【{sym} {action} 保證金不足略過】\n💵 可用保證：{avail:.2f} USDT")
                return {"success": False, "msg": "可用保證金不足"}

            side, sl_side = ("BUY" if action == "LONG" else "SELL"), ("SELL" if action == "LONG" else "BUY")
            client.cancel_all(sym)
            order_res = client.order(sym, side, target_qty)
            if not isinstance(order_res, dict) or "orderId" not in order_res:
                send_line_push(f"❌ 【{sym} {action} 開倉失敗】\n⚠️ 回應：{order_res}")
                raise HTTPException(status_code=400, detail=f"開倉失敗: {order_res}")

            actual_amt, actual_entry = client.verify_position_opened(sym, action, retries=3)
            if abs(actual_amt) <= 0:
                send_line_push(f"🚨 【{sym} {action} 異常警報】市價單送出但未偵測到持倉！")
                return {"success": False, "msg": "未偵測到持倉"}

            base_entry = actual_entry if actual_entry > 0 else curr_p
            if not client.set_sl(sym, sl_side, sig.stop_loss):
                client.order(sym, sl_side, abs(actual_amt), reduce_only=True)
                send_line_push(f"🚨 【{sym} {action} 止損掛單失敗・已緊急平倉保護】")
                raise HTTPException(status_code=400, detail="止損設定失敗已平倉")

            ref_p = sig.signal_price if (sig.signal_price and sig.signal_price > 0) else curr_p
            slip_pct = abs(base_entry - ref_p) / ref_p * 100.0 if ref_p > 0 else 0.0
            slip_note = f"⚠️ 滑點提醒：實際成交滑點 {slip_pct:.2f}%\n" if slip_pct >= SLIPPAGE_WARN_PCT else ""
            fmt_sl = client.format_price(sym, sig.stop_loss)
            send_line_push(
                f"{'🟢 【做多 LONG 開倉成功】' if action == 'LONG' else '🔴 【做空 SHORT 開倉成功】'}\n"
                f"{reversed_note}{slip_note}"
                f"🪙 交易幣種：{sym} ({lev}x)\n"
                f"📦 開倉顆數：{abs(actual_amt)} 顆 (約 {abs(actual_amt) * base_entry:.1f}U)\n"
                f"🏷️ 成交均價：{base_entry}\n"
                f"🛡️ 標記止損：{fmt_sl}\n"
                f"🎯 多階止盈：1.5R減40%保本 / 3R減40%\n"
                f"💰 帳戶權益：{account_base:.2f} USDT"
            )
            return {"success": True, "qty": abs(actual_amt), "stop_loss": fmt_sl}
        return {"success": False, "msg": f"未知動作: {action}"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))

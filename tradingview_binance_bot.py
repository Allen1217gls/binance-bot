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

# 預設風控參數 (全幣種智慧雙軌制：單筆保證金上限 20% + 單筆名義倉位上限 3.2倍本金 / 熔斷 40% / 底線 30U / 滑點 1.5%)
DAILY_MAX_DD_PCT = float(os.getenv("DAILY_MAX_DD_PCT", "40.0"))
EMERGENCY_FLOOR_USDT = float(os.getenv("EMERGENCY_FLOOR_USDT", "30.0"))
MARGIN_ALERT_RATIO = float(os.getenv("MARGIN_ALERT_RATIO", "60.0"))
MAX_ENTRY_SLIPPAGE_PCT = float(os.getenv("MAX_ENTRY_SLIPPAGE_PCT", "1.5"))
MAX_MARGIN_PCT_PER_TRADE = float(os.getenv("MAX_MARGIN_PCT_PER_TRADE", "20.0"))
MAX_NOTIONAL_MULT = float(os.getenv("MAX_NOTIONAL_MULT", "3.2"))
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
    "max_margin_pct": MAX_MARGIN_PCT_PER_TRADE,
    "max_notional_mult": MAX_NOTIONAL_MULT,
    "hourly_push_enabled": False,
    "bound_user_id": LINE_USER_ID,
    "last_symbol": DEFAULT_SYMBOL,
    "last_margin_alert_ts": 0.0,
    "consecutive_api_failures": 0,
    "api_disconnected_alerted": False,
}

recent_signals_cache: Dict[str, float] = {}
trade_logs_mem: List[Tuple[str, str, str, float]] = []

def _disp_sym(sym: str) -> str:
    """若幣種名稱過長 (>9字元如 1000PEPEUSDT)，自動簡寫尾綴避免手機換行"""
    return sym[:-4] if (len(sym) > 9 and sym.endswith("USDT")) else sym

def _fmt_qty(val: float) -> str:
    """去除整數顆數多餘的 .0 (例如 41.0 -> 41，0.057 -> 0.057)"""
    if abs(val - round(val)) < 1e-8:
        return str(int(round(val)))
    return f"{val:.6f}".rstrip("0").rstrip(".")

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
        return (
            f"📊 【近 {days} 日交易統計週報】\n"
            f"🕒 統計時間：{now_tw.strftime('%m-%d %H:%M')}\n"
            f"📭 目前紀錄：尚無已結算紀錄"
        )

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
        f"🕒 統計時間：{now_tw.strftime('%m-%d %H:%M')}\n"
        f"🔢 結算筆數：{len(rows)} 筆 (含減倉)\n"
        f"🏆 週總勝率：{wr:.1f}% ({len(wins)}勝/{len(losses)}敗)\n"
        f"💰 週總淨利：{'🟢' if total_net >= 0 else '🔴'} {total_net:+.2f} U\n"
        f"⚖️ 均盈虧比：{rr:.2f}\n"
        f"📈 均賺均賠：+{avg_w:.1f}U / -{avg_l:.1f}U\n"
        f"🚀 最大單筆：+{max(wins) if wins else 0.0:.2f} USDT\n\n"
        f"📅 今日結算：{len(td_rows)} 筆 (勝率 {td_wr:.0f}%)\n"
        f"💵 今日實現：{'🟢' if td_net >= 0 else '🔴'} {td_net:+.2f} U"
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
        self.key, self.sec, self.time_offset, self._last_info_load = key, sec, 0, 0.0
        self._precisions: Dict[str, Tuple[float, int, int, float]] = {}
        self._leverage_cache: Dict[Tuple[str, int], int] = {}
        self._sym_leverage: Dict[str, int] = {}
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
        if time.time() - self._last_info_load < 60.0:
            return
        self._last_info_load = time.time()
        try:
            res = requests.get(f"{FUTURES_URL}/fapi/v1/exchangeInfo", timeout=10).json()
            for s in res.get("symbols", []):
                sym, price_prec, step, dec, min_notional = s.get("symbol"), int(s.get("pricePrecision", 2)), 0.001, 3, 5.0
                for f in s.get("filters", []):
                    ft = f.get("filterType")
                    if ft == "LOT_SIZE":
                        step_str = str(f.get("stepSize", "0.001")).rstrip("0")
                        step = float(f.get("stepSize", "0.001"))
                        dec = len(step_str.split(".")[1]) if ("." in step_str and len(step_str.split(".")[1]) > 0) else 0
                    elif ft == "PRICE_FILTER":
                        tick_str = str(f.get("tickSize", "0.01")).rstrip("0")
                        if "." in tick_str and len(tick_str.split(".")[1]) > 0:
                            price_prec = len(tick_str.split(".")[1])
                        elif float(f.get("tickSize", "0.01")) >= 1:
                            price_prec = 0
                    elif ft in ("MIN_NOTIONAL", "NOTIONAL"):
                        min_notional = float(f.get("notional", f.get("minNotional", "5.0")))
                self._precisions[sym] = (step, dec, price_prec, min_notional)
        except Exception as e:
            logger.error(f"載入 exchangeInfo 失敗: {e}")

    def _get_price_dec(self, sym: str, ref_price: float = 0.0) -> int:
        sym = sym.upper()
        if sym not in self._precisions:
            self._load_exchange_info()
        if sym in self._precisions:
            return self._precisions[sym][2]
        p = abs(ref_price)
        if p >= 100:
            return 2
        if p >= 10:
            return 3
        if p >= 1:
            return 4
        if p >= 0.001:
            return 6
        return 7

    def fmt_p(self, sym: str, raw_price: Optional[float]) -> str:
        """將價格格式化為與幣安 App 倉位顯示 100% 一致的精度字串，消除浮點數長尾"""
        if raw_price is None:
            return "--"
        val = float(raw_price)
        if val <= 0:
            return "--"
        dec = self._get_price_dec(sym, val)
        return f"{round(val, dec):.{dec}f}"

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
            raw_p = float(requests.get(f"{FUTURES_URL}/fapi/v1/ticker/price", params={"symbol": sym.upper()}, timeout=5).json().get("price", 0.0))
            return self.format_price(sym, raw_p) if raw_p > 0 else 0.0
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
        dec = self._get_price_dec(sym, raw_price)
        return round(float(raw_price), dec)

    def set_leverage(self, sym: str, lev: int) -> int:
        sym = sym.upper()
        if (sym, lev) in self._leverage_cache:
            actual = self._leverage_cache[(sym, lev)]
            self._sym_leverage[sym] = actual
            return actual
        candidates = [lev] + [x for x in (50, 40, 30, 25, 20, 15, 10) if x < lev]
        for try_lev in candidates:
            p = {"symbol": sym, "leverage": try_lev, "timestamp": int(time.time() * 1000) + self.time_offset, "recvWindow": 60000}
            p["signature"] = self._sign(p)
            res = self._safe_request("POST", "/fapi/v1/leverage", data=p, retries=1)
            if isinstance(res, dict) and res.get("leverage") == try_lev:
                self._leverage_cache[(sym, lev)] = try_lev
                self._sym_leverage[sym] = try_lev
                return try_lev
        self._sym_leverage[sym] = lev
        return lev

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
        fp_str = self.fmt_p(sym, price)
        fp_val = self.format_price(sym, price)
        for attempt in range(3):
            p = {"algoType": "CONDITIONAL", "symbol": sym.upper(), "side": side.upper(), "type": "STOP_MARKET", "triggerPrice": fp_str,
                 "closePosition": "true", "workingType": "MARK_PRICE", "timestamp": int(time.time() * 1000) + self.time_offset, "recvWindow": 60000}
            p["signature"] = self._sign(p)
            res = self._safe_request("POST", "/fapi/v1/algoOrder", data=p, retries=2)
            if isinstance(res, dict) and "algoId" in res:
                self._current_sl[sym.upper()] = fp_val
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
                    fp = self.format_price(sym, tp)
                    self._current_sl[sym] = fp
                    return fp
        return None

    def cancel_all(self, sym: str):
        sym = sym.upper()
        for ep in ("/fapi/v1/allOpenOrders", "/fapi/v1/algoOpenOrders"):
            p = {"symbol": sym, "timestamp": int(time.time() * 1000) + self.time_offset, "recvWindow": 60000}
            p["signature"] = self._sign(p)
            self._safe_request("DELETE", ep, params=p, retries=2)
        self._current_sl.pop(sym, None)

    def get_position_info(self, sym: str) -> Tuple[float, float]:
        sym = sym.upper()
        p = {"symbol": sym, "timestamp": int(time.time() * 1000) + self.time_offset, "recvWindow": 60000}
        p["signature"] = self._sign(p)
        res = self._safe_request("GET", "/fapi/v2/positionRisk", params=p)
        if isinstance(res, list):
            for pos in res:
                if pos.get("symbol") == sym:
                    amt = float(pos.get("positionAmt", 0.0))
                    ep = self.format_price(sym, float(pos.get("entryPrice", 0.0)))
                    lev_val = int(float(pos.get("leverage", self._sym_leverage.get(sym, 40)) or 40))
                    self._sym_leverage[sym] = lev_val
                    return (amt, ep)
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
                    sym = pos.get("symbol", "").upper()
                    raw_ep = float(pos.get("entryPrice", 0.0))
                    raw_mp = float(pos.get("markPrice", 0.0))
                    ep = self.format_price(sym, raw_ep)
                    mp = self.format_price(sym, raw_mp)
                    upnl = float(pos.get("unRealizedProfit", 0.0))
                    lev = int(float(pos.get("leverage", self._sym_leverage.get(sym, 40)) or 40))
                    self._sym_leverage[sym] = lev
                    notional = abs(amt) * (raw_mp if raw_mp > 0 else raw_ep)
                    margin = notional / max(lev, 1)
                    roi_pct = (upnl / margin * 100.0) if margin > 0 else 0.0
                    if notional >= 1.0:
                        active.append({
                            "symbol": sym,
                            "amt": amt,
                            "entry_price": ep,
                            "mark_price": mp,
                            "upnl": upnl,
                            "notional": notional,
                            "leverage": lev,
                            "margin": margin,
                            "roi_pct": roi_pct,
                        })
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
            return "📭 重啟檢查：目前無未平倉位"
        lines = [f"🔄 狀態對齊：接管 {len(positions)} 筆持倉"]
        repaired_lines = []
        for pos in positions:
            sym, amt, ep = pos["symbol"], pos["amt"], pos["entry_price"]
            dsym = _disp_sym(sym)
            mp = pos["mark_price"] if pos["mark_price"] > 0 else ep
            bot_state["last_symbol"] = sym
            ex_sl = client.get_existing_sl_price(sym)
            if ex_sl and ex_sl > 0:
                lines.append(f"🛡️ 既有止損：{dsym} ({client.fmt_p(sym, ex_sl)})")
            else:
                sl_side = "SELL" if amt > 0 else "BUY"
                fb = EMERGENCY_SL_FALLBACK_PCT / 100.0
                if amt > 0:
                    safe_sl = ep * (1.0 + BE_BUFFER_PCT / 100.0) if mp >= ep * 1.01 else min(ep, mp) * (1.0 - fb)
                else:
                    safe_sl = ep * (1.0 - BE_BUFFER_PCT / 100.0) if mp <= ep * 0.99 else max(ep, mp) * (1.0 + fb)
                ok = client.set_sl(sym, sl_side, safe_sl)
                rep_str = f"🔧 自動補掛：{dsym} ({client.fmt_p(sym, safe_sl)})" if ok else f"🔧 自動補掛：{dsym} (失敗)"
                lines.append(rep_str)
                repaired_lines.append(rep_str)
        if repaired_lines and notify_if_repaired:
            send_line_push(
                f"🛡️ 【重啟狀態自動對齊】\n"
                f"📦 接管持倉：{len(positions)} 筆\n"
                + "\n".join(repaired_lines)
            )
        return "\n".join(lines)
    except Exception as e:
        return f"⚠️ 對齊異常：{e}"

def check_emergency_hard_brake(current_eq: float) -> bool:
    floor_u = bot_state["emergency_floor_usdt"]
    if floor_u <= 0 or current_eq <= 0:
        return False
    if current_eq <= floor_u and not bot_state["emergency_brake_tripped"]:
        bot_state["emergency_brake_tripped"] = True
        bot_state["trading_enabled"] = False
        closed_lines = []
        for pos in client.get_all_active_positions():
            sym, amt = pos["symbol"], pos["amt"]
            client.cancel_all(sym)
            client.order(sym, "SELL" if amt > 0 else "BUY", abs(amt), reduce_only=True)
            record_trade_log(sym, "EMERGENCY_BRAKE", amt, abs(amt), pos["entry_price"], pos["mark_price"])
            closed_lines.append(f"✂️ 強制清倉：{_disp_sym(sym)} ({_fmt_qty(abs(amt))}顆)")
        closed_block = "\n".join(closed_lines) if closed_lines else "✂️ 強制清倉：目前無持倉"
        send_line_push(
            f"🛑 【總權益極限斷路觸發】\n"
            f"💰 當前權益：{current_eq:.2f} USDT\n"
            f"🚨 觸發底線：{floor_u:.2f} USDT\n"
            f"{closed_block}\n"
            f"🔒 系統狀態：已全平並停機"
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
            f"🚨 【每日最大回撤熔斷】\n"
            f"📊 今日基準：{daily_start_balance:.2f} USDT\n"
            f"💰 當前權益：{current_eq:.2f} USDT\n"
            f"📉 當日回撤：-{dd_pct:.1f}% (限-{max_dd:.0f}%)\n"
            f"🛡️ 防禦動作：暫停開倉至明早"
        )
        return True
    return bot_state["circuit_breaker_tripped"]

def build_status_report(title: str = "📊 【即時帳戶與持倉查詢】") -> str:
    global daily_start_balance
    now_str = datetime.now(TW_TZ).strftime("%m-%d %H:%M:%S")
    total_eq, avail, margin_ratio, _ = client.get_equity_and_margin_ratio()
    if daily_start_balance <= 0 and total_eq > 0:
        daily_start_balance = total_eq
    positions = client.get_all_active_positions()

    if bot_state["emergency_brake_tripped"]:
        mode_str = f"🛑 斷路停機 ({bot_state['emergency_floor_usdt']:.0f}U)"
    elif bot_state["circuit_breaker_tripped"]:
        mode_str = f"🚨 熔斷防禦 (-{bot_state['max_daily_dd_pct']:.0f}%)"
    elif bot_state["trading_enabled"]:
        mode_str = "🟢 正常自動接單"
    else:
        mode_str = "⏸️ 已暫停新開倉"

    if daily_start_balance > 0 and total_eq > 0:
        diff = total_eq - daily_start_balance
        pct = (diff / daily_start_balance) * 100.0
        daily_pnl_str = f"{'🟢' if diff >= 0 else '🔴'} {diff:+.2f}U ({pct:+.1f}%)"
    else:
        daily_pnl_str = "⚪ 0.00U (0.0%)"

    lines = [
        f"{title}",
        f"🕒 查詢時間：{now_str}",
        f"🤖 系統狀態：{mode_str}",
        f"💰 帳戶權益：{total_eq:.2f} USDT",
        f"💵 可用保證：{avail:.2f} USDT",
        f"📈 今日損益：{daily_pnl_str}",
        f"🛡️ 倉位上限：保證{bot_state['max_margin_pct']:.0f}% / {bot_state['max_notional_mult']:.1f}倍",
        f"🛑 防護底線：熔斷-{bot_state['max_daily_dd_pct']:.0f}% / 底{bot_state['emergency_floor_usdt']:.0f}U",
    ]
    if not positions:
        sym = bot_state.get("last_symbol", DEFAULT_SYMBOL)
        lines.append("📭 目前持倉：空手觀望中\n")
        lines.append(f"🔍 最近幣種：{sym} ({client.fmt_p(sym, client.get_price(sym))})")
    else:
        lines.append(f"📦 目前持倉：{len(positions)} 筆 (維持 {margin_ratio:.1f}%)")
        for pos in positions:
            sym, amt, upnl = pos["symbol"], pos["amt"], pos["upnl"]
            lev = pos.get("leverage", client._sym_leverage.get(sym, 40))
            margin_u = pos.get("margin", pos["notional"] / max(lev, 1))
            roi_pct = pos.get("roi_pct", (upnl / margin_u * 100.0) if margin_u > 0 else 0.0)
            roi_str = f"{roi_pct:+.0f}%" if abs(roi_pct) >= 10.0 else f"{roi_pct:+.1f}%"
            raw_sl = client._current_sl.get(sym) or client.get_existing_sl_price(sym)
            sl_str = client.fmt_p(sym, raw_sl) if raw_sl else "依條件單"
            ep_str = client.fmt_p(sym, pos["entry_price"])
            mp_str = client.fmt_p(sym, pos["mark_price"])
            qty_str = _fmt_qty(abs(amt))
            lines.append(
                f"\n🔸 【{sym}】{'🟢 做多' if amt > 0 else '🔴 做空'} {lev}x\n"
                f"💵 未實損益：{'🟢' if upnl >= 0 else '🔴'} {upnl:+.2f}U ({roi_str})\n"
                f"📦 持倉數量：{pos['notional']:.2f}U ({qty_str}顆)\n"
                f"🔒 倉位保證：{margin_u:.2f} USDT\n"
                f"🏷️ 開倉價格：{ep_str}\n"
                f"📍 標記價格：{mp_str}\n"
                f"🛡️ 止損價格：{sl_str}"
            )
    return "\n".join(lines)

def background_scheduler():
    global daily_start_balance, last_report_date, last_hourly_key
    try:
        init_eq, _, _, _ = client.get_equity_and_margin_ratio()
        if init_eq > 0:
            daily_start_balance = init_eq
    except Exception:
        pass
    while True:
        try:
            if not client.ping():
                bot_state["consecutive_api_failures"] += 1
                if bot_state["consecutive_api_failures"] >= 3 and not bot_state["api_disconnected_alerted"]:
                    bot_state["api_disconnected_alerted"] = True
                    send_line_push("⚠️ 【幣安 API 連線異常】\n🚨 連續失敗：已達 3 次\n💡 建議操作：請開 App 檢查倉位")
                time.sleep(20)
                continue
            elif bot_state["api_disconnected_alerted"]:
                bot_state["api_disconnected_alerted"] = False
                bot_state["consecutive_api_failures"] = 0
                send_line_push(f"🟢 【幣安連線已恢復】\n{reconcile_state_with_binance(notify_if_repaired=False)}")
            bot_state["consecutive_api_failures"] = 0

            now_tw = datetime.now(TW_TZ)
            today_date, hour_key = now_tw.date(), (now_tw.date(), now_tw.hour)
            total_eq, avail, margin_ratio, _ = client.get_equity_and_margin_ratio()
            if daily_start_balance <= 0 and total_eq > 0:
                daily_start_balance = total_eq
            if total_eq > 0:
                check_daily_circuit_breaker(total_eq)

            now_ts = time.time()
            if margin_ratio >= MARGIN_ALERT_RATIO and (now_ts - bot_state["last_margin_alert_ts"] > 1800):
                bot_state["last_margin_alert_ts"] = now_ts
                send_line_push(f"⚠️ 【保證金率強平預警】\n🚨 保證金率：{margin_ratio:.1f}%\n💰 帳戶權益：{total_eq:.2f} USDT\n💡 建議操作：輸入 3 減倉或 2 全平")

            if now_tw.hour == 8 and now_tw.minute < 5 and last_report_date != today_date:
                was_tripped = bot_state["circuit_breaker_tripped"]
                if was_tripped and not bot_state["emergency_brake_tripped"]:
                    bot_state["circuit_breaker_tripped"] = False
                    bot_state["trading_enabled"] = True
                if daily_start_balance > 0 and total_eq > 0:
                    diff = total_eq - daily_start_balance
                    pct = (diff / daily_start_balance) * 100.0
                    reset_note = "\n🔄 熔斷重置：已恢復自動接單" if was_tripped else ""
                    daily_msg = f"🌅 【每日 08:00 損益日報】\n📅 結算日期：{today_date}\n💰 今日權益：{total_eq:.2f} USDT\n💵 可用保證：{avail:.2f} USDT\n📊 昨日基準：{daily_start_balance:.2f} USDT\n🏆 單日損益：{'🟢' if diff >= 0 else '🔴'} {diff:+.2f}U ({pct:+.1f}%){reset_note}"
                else:
                    daily_msg = f"🌅 【每日 08:00 損益日報】\n📅 結算日期：{today_date}\n💰 今日權益：{total_eq:.2f} USDT"
                send_line_push(daily_msg)
                if total_eq > 0:
                    daily_start_balance = total_eq
                last_report_date, last_hourly_key = today_date, hour_key
            elif bot_state["hourly_push_enabled"] and now_tw.minute < 5 and last_hourly_key != hour_key:
                send_line_push(build_status_report(title=f"⏰ 【整點巡檢報告 ({now_tw.hour:02d}:00)】"))
                last_hourly_key = hour_key
            time.sleep(25)
        except Exception:
            time.sleep(60)

@asynccontextmanager
async def lifespan(app: FastAPI):
    reconcile_summary = reconcile_state_with_binance(notify_if_repaired=False)
    threading.Thread(target=background_scheduler, daemon=True).start()
    if LINE_CHANNEL_ACCESS_TOKEN and (LINE_USER_ID or bot_state.get("bound_user_id")):
        send_line_push(
            f"🤖 【幣安機器人 v4.4】已上線\n"
            f"{reconcile_summary}\n"
            f"💡 快捷指令：輸入 0 查看選單"
        )
    yield

app = FastAPI(lifespan=lifespan)

class Signal(BaseModel):
    passphrase: str
    symbol: str
    action: str
    risk_pct: Optional[float] = 1.5
    stop_loss: Optional[float] = None
    leverage: Optional[int] = 40
    close_pct: Optional[float] = None
    signal_price: Optional[float] = None
    use_tp2: Optional[bool] = False

@app.get("/")
def index():
    return {"status": "online", "version": "v4.4 Ultimate Armor", "trading_enabled": bot_state["trading_enabled"]}

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
        return "⛔ 權限不足：僅限本人操控"

    if text in ("1", "查詢", "狀態", "倉位", "餘額", "查帳"):
        return build_status_report(title="📊 【即時帳戶與持倉查詢】")

    if text in ("2", "平倉", "全平", "一鍵平倉", "緊急平倉") or upper_text.startswith("平倉 "):
        parts = upper_text.split()
        target_sym = parts[1] if len(parts) > 1 else None
        positions = [p for p in client.get_all_active_positions() if (not target_sym or p["symbol"] == target_sym)]
        if not positions:
            sym_clean = target_sym or bot_state.get("last_symbol", DEFAULT_SYMBOL)
            client.cancel_all(sym_clean)
            return f"📭 目前持倉：無 ({_disp_sym(sym_clean)} 已撤單)"
        closed = []
        for pos in positions:
            sym, amt = pos["symbol"], pos["amt"]
            client.cancel_all(sym)
            client.order(sym, "SELL" if amt > 0 else "BUY", abs(amt), reduce_only=True)
            record_trade_log(sym, "MANUAL_CLOSE", amt, abs(amt), pos["entry_price"], pos["mark_price"])
            closed.append(f"✂️ 已平倉位：{_disp_sym(sym)} ({pos['upnl']:+.2f}U)")
        total_eq, _, _ = client.get_equity()
        return "🚨 【一鍵全平倉執行完畢】\n" + "\n".join(closed) + f"\n💰 最新權益：{total_eq:.2f} USDT"

    if text in ("3", "減半", "平半", "減倉"):
        positions = client.get_all_active_positions()
        if not positions:
            return "📭 目前持倉：無持倉可供減半"
        res = []
        for pos in positions:
            sym, amt, min_not = pos["symbol"], pos["amt"], client.get_min_notional(pos["symbol"])
            hq = client.format_qty(sym, abs(amt) * 0.5)
            rq = client.format_qty(sym, abs(amt) - hq)
            if hq <= 0 or (rq * pos["mark_price"] < min_not):
                res.append(f"⚠️ 無法拆半：{_disp_sym(sym)} (倉位過小)")
            else:
                client.order(sym, "SELL" if amt > 0 else "BUY", hq, reduce_only=True)
                pnl_est = record_trade_log(sym, "MANUAL_HALF", amt, hq, pos["entry_price"], pos["mark_price"])
                res.append(f"✂️ 已減半倉：{_disp_sym(sym)} ({pnl_est:+.2f}U)")
        return "✂️ 【手動減倉 50% 結果】\n" + "\n".join(res)

    if text in ("4", "保本", "移保本"):
        positions = client.get_all_active_positions()
        if not positions:
            return "📭 目前持倉：無持倉可設保本"
        res = []
        for pos in positions:
            sym, amt, ep, mp = pos["symbol"], pos["amt"], pos["entry_price"], pos["mark_price"]
            be_p = ep * (1.0 + BE_BUFFER_PCT / 100.0) if amt > 0 else ep * (1.0 - BE_BUFFER_PCT / 100.0)
            if (amt > 0 and mp <= be_p) or (amt < 0 and mp >= be_p):
                res.append(f"⚠️ 未達保本：{_disp_sym(sym)} (現價未過)")
            else:
                client.cancel_all(sym)
                ok = client.set_sl(sym, "SELL" if amt > 0 else "BUY", be_p)
                if ok:
                    res.append(f"🛡️ 已移保本：{_disp_sym(sym)} ({client.fmt_p(sym, be_p)})")
                else:
                    res.append(f"❌ 掛單失敗：{_disp_sym(sym)} (請查現價)")
        return "🛡️ 【手動移至保本結果】\n" + "\n".join(res)

    m_sl = re.match(r"^(?:5|止損|改止損|SL)\s+([0-9.]+)$", upper_text)
    if m_sl:
        new_sl = float(m_sl.group(1))
        positions = client.get_all_active_positions()
        if not positions:
            return "📭 目前持倉：無持倉可改止損"
        sym, amt = positions[0]["symbol"], positions[0]["amt"]
        client.cancel_all(sym)
        ok = client.set_sl(sym, "SELL" if amt > 0 else "BUY", new_sl)
        return f"🛡️ 止損更新：{_disp_sym(sym)} ({client.fmt_p(sym, new_sl)})" if ok else f"❌ 止損失敗：{_disp_sym(sym)}"

    if text in ("6", "暫停", "停止", "暫停開倉"):
        bot_state["trading_enabled"] = False
        return "⏸️ 【已暫停自動開新倉】\n🛡️ 既有止損：照常運作\n💡 恢復指令：輸入 7 即可恢復"

    if text in ("7", "恢復", "啟動", "開始", "恢復開倉"):
        bot_state["trading_enabled"] = True
        bot_state["circuit_breaker_tripped"] = False
        bot_state["emergency_brake_tripped"] = False
        eq, _, _ = client.get_equity()
        if eq > 0:
            daily_start_balance = eq
        return "🟢 【已恢復自動開新倉】\n🤖 系統狀態：正常接收訊號中"

    if text in ("8", "整點推播", "每小時推播"):
        bot_state["hourly_push_enabled"] = not bot_state["hourly_push_enabled"]
        return f"⏰ 整點推播：{'🟢 已開啟' if bot_state['hourly_push_enabled'] else '⏸️ 已關閉'}"

    if text in ("9", "統計", "週報", "戰績", "報表", "歷史"):
        return build_trade_stats_report(days=7)

    if text in ("10", "對齊", "巡檢", "檢查止損"):
        return reconcile_state_with_binance(notify_if_repaired=False)

    m_cb = re.match(r"^(?:熔斷|回撤|DD)\s+([0-9.]+)$", upper_text)
    if m_cb:
        bot_state["max_daily_dd_pct"] = max(1.0, min(95.0, float(m_cb.group(1))))
        return f"🛡️ 回撤熔斷：-{bot_state['max_daily_dd_pct']:.1f}%"

    m_fl = re.match(r"^(?:底線|斷路|FLOOR)\s+([0-9.]+)$", upper_text)
    if m_fl:
        bot_state["emergency_floor_usdt"] = max(0.0, float(m_fl.group(1)))
        bot_state["emergency_brake_tripped"] = False
        return f"🛑 斷路底線：{bot_state['emergency_floor_usdt']:.1f} USDT"

    m_slp = re.match(r"^(?:滑點|SLIP)\s+([0-9.]+)$", upper_text)
    if m_slp:
        bot_state["max_entry_slippage_pct"] = max(0.1, min(10.0, float(m_slp.group(1))))
        return f"🛡️ 滑點上限：{bot_state['max_entry_slippage_pct']:.2f}%"

    m_mg = re.match(r"^(?:上限|保證金|MARGIN)\s+([0-9.]+)$", upper_text)
    if m_mg:
        bot_state["max_margin_pct"] = max(5.0, min(100.0, float(m_mg.group(1))))
        return f"🛡️ 保證上限：{bot_state['max_margin_pct']:.1f}%"

    m_nt = re.match(r"^(?:倍數|名義|MULT)\s+([0-9.]+)$", upper_text)
    if m_nt:
        bot_state["max_notional_mult"] = max(0.5, min(20.0, float(m_nt.group(1))))
        return f"🛡️ 名義上限：{bot_state['max_notional_mult']:.1f} 倍本金"

    if upper_text in ("ID", "USERID", "綁定"):
        return f"🔑 你的 LINE ID：\n{user_id}"

    return (
        "🤖 【幣安機器人 v4.4 選單】\n"
        "🔹 「1」查詢帳戶與持倉狀態\n"
        "🔹 「2」一鍵市價全平所有倉\n"
        "🔹 「3」手動將持倉減倉 50%\n"
        "🔹 「4」手動將止損移至保本\n"
        "🔹 「6」暫停 / 「7」恢復開倉\n"
        "🔹 「8」開關每小時整點推播\n"
        "🔹 「9」查看近 7 日統計週報\n"
        "🔹 「10」巡檢倉位並補掛止損\n"
        "🔹 「止損 價格」修改止損價\n"
        f"🔹 「上限 數字」保證金 {bot_state['max_margin_pct']:.0f}%\n"
        f"🔹 「倍數 數字」倉位限 {bot_state['max_notional_mult']:.1f}倍\n"
        f"🔹 「熔斷 數字」日回撤 -{bot_state['max_daily_dd_pct']:.0f}%\n"
        f"🔹 「底線 金額」強平線 {bot_state['emergency_floor_usdt']:.0f}U\n"
        f"🔹 「滑點 數字」滑點限 {bot_state['max_entry_slippage_pct']:.1f}%"
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
            pct = (sig.close_pct if (sig.close_pct and 0 < sig.close_pct < 100) else 50.0) / 100.0
            close_qty = client.format_qty(sym, abs(pos_amt) * pct)
            rem_qty = client.format_qty(sym, abs(pos_amt) - close_qty)
            if 0 < close_qty < abs(pos_amt) and (rem_qty * curr_p >= min_not):
                client.order(sym, side, close_qty, reduce_only=True)
                net_u = record_trade_log(sym, "TP1", pos_amt, close_qty, entry_price, curr_p)
                tp_lines = (
                    f"✂️ 減倉顆數：{_fmt_qty(close_qty)} 顆 ({int(round(pct*100))}%)\n"
                    f"📦 剩餘尾倉：{_fmt_qty(rem_qty)} 顆\n"
                    f"💵 落袋損益：🟢 {net_u:+.2f} USDT\n"
                )
            else:
                tp_lines = f"📦 保留全倉：{_fmt_qty(abs(pos_amt))} 顆直接移保本\n"

            be_buf = entry_price * (1.0 + BE_BUFFER_PCT / 100.0) if "LONG" in action else entry_price * (1.0 - BE_BUFFER_PCT / 100.0)
            target_sl = (max(be_buf, sig.stop_loss) if "LONG" in action else min(be_buf, sig.stop_loss)) if (sig.stop_loss and sig.stop_loss > 0) else be_buf
            client.cancel_all(sym)
            if not client.set_sl(sym, side, target_sl):
                rem_amt, _ = client.get_position_info(sym)
                if abs(rem_amt) > 0:
                    client.order(sym, side, abs(rem_amt), reduce_only=True)
                    record_trade_log(sym, "TP1_PROTECT", pos_amt, abs(rem_amt), entry_price, curr_p)
                send_line_push(f"🛡️ 【TP1 回落保護全平】\n🪙 交易幣種：{sym}\n⚠️ 觸發原因：現價穿過保本價")
                return {"success": True, "msg": "已全平保護"}
            send_line_push(
                f"🎯 【1.5R TP1 止盈與保本】\n"
                f"🪙 交易幣種：{sym}\n"
                f"{tp_lines}"
                f"🏷️ 開倉價格：{client.fmt_p(sym, entry_price)}\n"
                f"🛡️ 保本止損：{client.fmt_p(sym, target_sl)}"
            )
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
            if close_qty > 0 and (close_qty * curr_p >= min_not):
                client.order(sym, side, close_qty, reduce_only=True)
                net_u = record_trade_log(sym, "TP2", pos_amt, close_qty, entry_price, curr_p)
                tp2_lines = (
                    f"✂️ 再減顆數：{_fmt_qty(close_qty)} 顆\n"
                    f"📦 剩餘尾倉：{_fmt_qty(rem_qty)} 顆\n"
                    f"💵 落袋損益：🟢 {net_u:+.2f} USDT\n"
                )
            else:
                tp2_lines = f"📦 保留尾倉：{_fmt_qty(abs(pos_amt))} 顆衝刺\n"
            if sig.stop_loss and sig.stop_loss > 0:
                client.cancel_all(sym)
                if not client.set_sl(sym, side, sig.stop_loss):
                    rem_amt, _ = client.get_position_info(sym)
                    if abs(rem_amt) > 0:
                        client.order(sym, side, abs(rem_amt), reduce_only=True)
            cur_sl = client._current_sl.get(sym)
            send_line_push(
                f"🚀 【3.0R TP2 二階止盈】\n"
                f"🪙 交易幣種：{sym}\n"
                f"{tp2_lines}"
                f"🏷️ 開倉價格：{client.fmt_p(sym, entry_price)}\n"
                f"🛡️ 上推止損：{client.fmt_p(sym, cur_sl) if cur_sl else '保本止損'}"
            )
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
                send_line_push(f"🛡️ 【移動止損觸發出場】\n🪙 交易幣種：{sym}\n📍 出場價格：{client.fmt_p(sym, curr_p)}\n💵 結算損益：{net_u:+.2f} USDT")
                return {"success": True, "msg": "已市價出場"}
            fmt_sl = client.fmt_p(sym, sig.stop_loss)
            logger.info(f"🐢 {sym} 海龜移動止損已靜默更新至 {fmt_sl}")
            return {"success": True, "msg": f"止損已靜默更新至 {fmt_sl}"}

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
                send_line_push(
                    f"🏁 【止損 / 趨勢平倉完成】\n"
                    f"🪙 交易幣種：{sym} ({'多單' if pos_amt > 0 else '空單'})\n"
                    f"📦 平倉顆數：{_fmt_qty(abs(pos_amt))} 顆\n"
                    f"🏷️ 開倉價格：{client.fmt_p(sym, entry_price)}\n"
                    f"📍 出場價格：{client.fmt_p(sym, curr_p)}\n"
                    f"💵 本筆損益：{'🟢' if net_u >= 0 else '🔴'} {net_u:+.2f} USDT\n"
                    f"💰 帳戶權益：{total_eq:.2f} USDT"
                )
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
                reversed_note = f"🔄 反手平倉：{_fmt_qty(abs(pos_amt))}顆 ({rev_pnl:+.2f}U)\n"
                time.sleep(0.4)

            total_eq, avail, _ = client.get_equity()
            account_base = max(total_eq, avail)
            if check_daily_circuit_breaker(account_base) or not bot_state["trading_enabled"]:
                reason = "已觸發極限斷路" if bot_state["emergency_brake_tripped"] else ("今日達熔斷上限" if bot_state["circuit_breaker_tripped"] else "目前暫停開新倉")
                send_line_push(f"⏸️ 【收到訊號・略過新進場】\n{reversed_note}🪙 交易幣種：{sym} ({action})\n🛡️ 略過原因：{reason}\n💡 恢復指令：輸入 7 恢復")
                return {"success": True, "msg": reason}

            if not sig.stop_loss:
                raise HTTPException(status_code=400, detail="缺少止損價")
            lev = client.set_leverage(sym, sig.leverage or 40)
            if curr_p <= 0:
                curr_p = client.get_price(sym)

            if sig.signal_price and sig.signal_price > 0:
                adv_slip = ((curr_p - sig.signal_price) / sig.signal_price * 100.0) if action == "LONG" else ((sig.signal_price - curr_p) / sig.signal_price * 100.0)
                if adv_slip > bot_state["max_entry_slippage_pct"]:
                    send_line_push(
                        f"🛡️ 【滑點過大・放棄追價】\n"
                        f"{reversed_note}"
                        f"🪙 交易幣種：{sym} ({action})\n"
                        f"🏷️ 訊號價格：{client.fmt_p(sym, sig.signal_price)}\n"
                        f"📍 當前現價：{client.fmt_p(sym, curr_p)} (+{adv_slip:.1f}%)\n"
                        f"💡 攔截上限：{bot_state['max_entry_slippage_pct']:.1f}%"
                    )
                    return {"success": False, "msg": "滑點過大已攔截"}

            if (action == "LONG" and curr_p <= sig.stop_loss) or (action == "SHORT" and curr_p >= sig.stop_loss):
                return {"success": False, "msg": "現價已越過止損價"}

            risk_usd = account_base * ((sig.risk_pct or 1.5) / 100.0)
            safe_dist = max(abs(curr_p - sig.stop_loss), curr_p * 0.002)
            raw_qty = max(risk_usd / safe_dist, client.format_qty(sym, (min_not * 1.05) / curr_p, round_up=True) if (risk_usd / safe_dist * curr_p < min_not) else 0.0)
            # 智慧雙軌制：同時限制「單筆最大保證金佔比(20%)」與「單筆最大名義倉位倍數(3.2倍本金)」
            max_margin_qty = (account_base * (bot_state["max_margin_pct"] / 100.0) * lev) / curr_p
            max_notional_qty = (account_base * bot_state["max_notional_mult"]) / curr_p
            avail_cap_qty = ((avail if avail > 0 else account_base) * lev * 0.95) / curr_p
            target_qty = client.format_qty(sym, min(raw_qty, max_margin_qty, max_notional_qty, avail_cap_qty))
            if target_qty <= 0 or (target_qty * curr_p) < min_not:
                send_line_push(f"⚠️ 【保證金不足略過開倉】\n🪙 交易幣種：{sym} ({action})\n💵 可用保證：{avail:.2f} USDT")
                return {"success": False, "msg": "可用保證金不足"}

            side, sl_side = ("BUY" if action == "LONG" else "SELL"), ("SELL" if action == "LONG" else "BUY")
            client.cancel_all(sym)
            order_res = client.order(sym, side, target_qty)
            if not isinstance(order_res, dict) or "orderId" not in order_res:
                send_line_push(f"❌ 【{sym} 開倉失敗】\n⚠️ 錯誤訊息：{order_res}")
                raise HTTPException(status_code=400, detail=f"開倉失敗: {order_res}")

            actual_amt, actual_entry = client.verify_position_opened(sym, action, retries=3)
            if abs(actual_amt) <= 0:
                send_line_push(f"🚨 【{sym} 異常警報】\n⚠️ 狀態說明：市價單未偵測到持倉")
                return {"success": False, "msg": "未偵測到持倉"}

            base_entry = actual_entry if actual_entry > 0 else curr_p
            if not client.set_sl(sym, sl_side, sig.stop_loss):
                client.order(sym, sl_side, abs(actual_amt), reduce_only=True)
                send_line_push(f"🚨 【{sym} 止損掛單失敗】\n🛡️ 保護動作：已緊急平倉保護")
                raise HTTPException(status_code=400, detail="止損設定失敗已平倉")

            ref_p = sig.signal_price if (sig.signal_price and sig.signal_price > 0) else curr_p
            slip_pct = abs(base_entry - ref_p) / ref_p * 100.0 if ref_p > 0 else 0.0
            slip_note = f"⚠️ 成交滑點：{slip_pct:.2f}%\n" if slip_pct >= SLIPPAGE_WARN_PCT else ""
            fmt_entry_str = client.fmt_p(sym, base_entry)
            fmt_sl_str = client.fmt_p(sym, sig.stop_loss)
            tp_mode_str = "1.5R減40% / 3R減40%" if sig.use_tp2 else "1.5R減半保本+海龜"
            notional_u = abs(actual_amt) * base_entry
            used_margin = notional_u / max(lev, 1)
            send_line_push(
                f"{'🟢 【做多 LONG 開倉成功】' if action == 'LONG' else '🔴 【做空 SHORT 開倉成功】'}\n"
                f"{reversed_note}{slip_note}"
                f"🪙 交易幣種：{sym} ({lev}x)\n"
                f"📦 持倉數量：{notional_u:.2f}U ({_fmt_qty(abs(actual_amt))}顆)\n"
                f"🔒 倉位保證：{used_margin:.2f}U (佔{used_margin/account_base*100:.0f}%)\n"
                f"🏷️ 開倉價格：{fmt_entry_str}\n"
                f"🛡️ 止損價格：{fmt_sl_str}\n"
                f"🎯 止盈模式：{tp_mode_str}\n"
                f"💰 帳戶權益：{account_base:.2f} USDT"
            )
            return {"success": True, "qty": abs(actual_amt), "stop_loss": client.format_price(sym, sig.stop_loss)}
        return {"success": False, "msg": f"未知動作: {action}"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))

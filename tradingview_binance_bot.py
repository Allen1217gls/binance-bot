import os, re, time, math, hmac, base64, hashlib, logging, secrets, sqlite3, asyncio, threading
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP, ROUND_CEILING, InvalidOperation
from typing import Optional, Dict, Any, Tuple, List
from urllib.parse import urlencode
import requests
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("Hull_Turtle_Bot_v5")

# ==========================================
# 環境變數與安全設定
# ==========================================
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "").strip()
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "").strip()
WEBHOOK_PASSPHRASE = os.getenv("WEBHOOK_PASSPHRASE", "Apple").strip()
USE_TESTNET = os.getenv("USE_TESTNET", "false").lower() in ("true", "1", "yes")
FUTURES_URL = "https://testnet.binancefuture.com" if USE_TESTNET else "https://fapi.binance.com"

LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "").strip()
LINE_CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET", "").strip()
LINE_USER_ID = os.getenv("LINE_USER_ID", "").strip()
DEFAULT_SYMBOL = os.getenv("DEFAULT_SYMBOL", "SOLUSDT").strip().upper()

# 黃金風控與執行參數
DAILY_MAX_DD_PCT = Decimal(os.getenv("DAILY_MAX_DD_PCT", "40.0"))
EMERGENCY_FLOOR_USDT = Decimal(os.getenv("EMERGENCY_FLOOR_USDT", "30.0"))
MARGIN_ALERT_RATIO = Decimal(os.getenv("MARGIN_ALERT_RATIO", "60.0"))
MAX_ENTRY_SLIPPAGE_PCT = Decimal(os.getenv("MAX_ENTRY_SLIPPAGE_PCT", "1.5"))
MAX_MARGIN_PCT_PER_TRADE = Decimal(os.getenv("MAX_MARGIN_PCT_PER_TRADE", "20.0"))
MAX_NOTIONAL_MULT = Decimal(os.getenv("MAX_NOTIONAL_MULT", "3.2"))
MAX_MIN_NOTIONAL_RISK_PCT = Decimal(os.getenv("MAX_MIN_NOTIONAL_RISK_PCT", "3.2"))  # 湊滿 5U 最小門檻時允許之最大單筆風險%
SLIPPAGE_WARN_PCT = Decimal(os.getenv("SLIPPAGE_WARN_PCT", "0.5"))
BE_BUFFER_PCT = Decimal(os.getenv("BE_BUFFER_PCT", "0.15"))
FEE_RATE_PCT = Decimal(os.getenv("FEE_RATE_PCT", "0.05"))  # 標準加密貨幣合約單邊 Taker 費率 0.05% (雙邊 0.10%)
TRADFI_FEE_RATE_PCT = Decimal("0.025")                     # 傳統金融/股票永續合約 (如 SNDKUSDT) 單邊優惠費率 0.025%
SLIP_BUFFER_PCT = Decimal(os.getenv("SLIP_BUFFER_PCT", "0.05"))
MAX_SIGNAL_AGE_SEC = float(os.getenv("MAX_SIGNAL_AGE_SEC", "180.0"))
SIGNAL_DEDUP_SEC = float(os.getenv("SIGNAL_DEDUP_SEC", "300.0"))
EMERGENCY_SL_FALLBACK_PCT = Decimal(os.getenv("EMERGENCY_SL_FALLBACK_PCT", "2.5"))
RETAIN_OLD_ON_NEW_REJECT = os.getenv("RETAIN_OLD_ON_NEW_REJECT", "true").lower() in ("true", "1", "yes")
STRICT_ONE_WAY_ONLY = os.getenv("STRICT_ONE_WAY_ONLY", "false").lower() in ("true", "1", "yes")

# 選配進階功能開關 (預設全部關閉 False)
ALLOW_PYRAMIDING = os.getenv("ALLOW_PYRAMIDING", "false").lower() in ("true", "1", "yes")
TRAIL_PROFIT_ENABLED = os.getenv("TRAIL_PROFIT_ENABLED", "false").lower() in ("true", "1", "yes")
TRAIL_PROFIT_TRIGGER_U = Decimal(os.getenv("TRAIL_PROFIT_TRIGGER_U", "5.0"))
TRAIL_PROFIT_GIVEBACK_PCT = Decimal(os.getenv("TRAIL_PROFIT_GIVEBACK_PCT", "20.0"))

DB_PATH = os.getenv("DB_PATH", "/tmp/trades_history_v5.db")
TW_TZ = timezone(timedelta(hours=8))
webhook_lock = asyncio.Lock()

VALID_ACTIONS = {
    "LONG", "SHORT", "ADD_LONG", "ADD_SHORT",
    "TP1_LONG", "TP1_SHORT", "TP2_LONG", "TP2_SHORT",
    "UPDATE_SL", "CLOSE_LONG", "CLOSE_SHORT"
}

TRADFI_SYMBOLS = {"SNDKUSDT", "TSLAUSDT", "NVDAUSDT", "AAPLUSDT", "COINUSDT"}

bot_state: Dict[str, Any] = {
    "manual_paused": False,
    "circuit_breaker_tripped": False,
    "emergency_brake_tripped": False,
    "safe_mode_locked": False,
    "safe_mode_reason": "",
    "hedge_mode": False,
    "position_mode_verified": False,
    "max_daily_dd_pct": float(DAILY_MAX_DD_PCT),
    "emergency_floor_usdt": float(EMERGENCY_FLOOR_USDT),
    "max_entry_slippage_pct": float(MAX_ENTRY_SLIPPAGE_PCT),
    "max_margin_pct": float(MAX_MARGIN_PCT_PER_TRADE),
    "max_notional_mult": float(MAX_NOTIONAL_MULT),
    "allow_pyramiding": ALLOW_PYRAMIDING,
    "trail_profit_enabled": TRAIL_PROFIT_ENABLED,
    "trail_profit_trigger_u": float(TRAIL_PROFIT_TRIGGER_U),
    "trail_profit_giveback_pct": float(TRAIL_PROFIT_GIVEBACK_PCT),
    "hourly_push_enabled": False,
    "bound_user_id": LINE_USER_ID,
    "last_symbol": DEFAULT_SYMBOL,
    "last_margin_alert_ts": 0.0,
    "last_auto_reconcile_ts": 0.0,
    "consecutive_api_failures": 0,
    "api_disconnected_alerted": False,
}

recent_signals_cache: Dict[str, float] = {}
last_entry_ts: Dict[str, float] = {}
notified_close_ts: Dict[str, float] = {}
known_positions: Dict[str, Dict[str, Any]] = {}
peak_upnl_tracker: Dict[str, float] = {}
trade_logs_mem: List[Tuple[str, str, str, float]] = []

def _to_dec(val: Any, default: str = "0") -> Decimal:
    if val is None:
        return Decimal(default)
    if isinstance(val, Decimal):
        return val
    try:
        d = Decimal(str(val))
        if d.is_nan() or d.is_infinite():
            return Decimal(default)
        return d
    except (InvalidOperation, ValueError, TypeError):
        return Decimal(default)

def _sym_fee_rate_pct(sym: str) -> Decimal:
    """自動識別一般加密合約 (0.05%) 與 TradFi 優惠合約 (如 SNDKUSDT 0.025%) 之單邊費率"""
    return TRADFI_FEE_RATE_PCT if sym.strip().upper() in TRADFI_SYMBOLS else FEE_RATE_PCT

def _disp_sym(sym: str) -> str:
    s = sym.strip().upper()
    if s.endswith("USDT") and len(s) > 4:
        s = s[:-4]
    if s.startswith("1000") and len(s) > 4:
        s = s[4:]
    return s[:5]

def _full_sym(sym: str) -> str:
    s = sym.strip().upper()
    return s if len(s) <= 8 else _disp_sym(s)

def _sym_val(sym: str, val_str: str) -> str:
    ds = _disp_sym(sym)
    return f"{ds} ({val_str})" if (len(ds) + len(val_str) <= 11) else f"{ds}({val_str})"

def _norm_sym(s: str) -> str:
    s = s.strip().upper()
    return s if (not s or s.endswith("USDT")) else f"{s}USDT"

def _fmt_qty(val: Any) -> str:
    v = abs(float(_to_dec(val)))
    if v >= 10000:
        return f"{v/10000:.1f}萬".rstrip("0").rstrip(".")
    if abs(v - round(v)) < 1e-6:
        return str(int(round(v)))
    if v >= 100:
        return f"{v:.0f}"
    if v >= 1:
        return f"{v:.2f}".rstrip("0").rstrip(".")
    return f"{v:.4f}".rstrip("0").rstrip(".")

def _fmt_u(val: Any, signed: bool = False) -> str:
    v = float(_to_dec(val))
    av = abs(v)
    if av >= 100:
        return f"{v:+.0f}U" if signed else f"{av:.0f}U"
    return f"{v:+.2f}U" if signed else f"{av:.2f}U"

def _fmt_margin_share(used_margin: Any, account_base: Any) -> str:
    um = float(_to_dec(used_margin))
    ab = float(_to_dec(account_base))
    if ab <= 0:
        return "0%"
    pct = max(0.0, min(99.9, um / ab * 100.0))
    if 0 < pct < 1.0:
        return f"{pct:.1f}%"
    return f"{int(round(pct))}%"

def is_new_entry_allowed() -> Tuple[bool, str]:
    if bot_state["emergency_brake_tripped"]:
        return (False, "已觸發極限斷路停機")
    if bot_state["safe_mode_locked"]:
        return (False, f"安全模式鎖定({bot_state['safe_mode_reason'] or '狀態待復核'})")
    if bot_state["circuit_breaker_tripped"]:
        return (False, "今日已達回撤熔斷")
    if bot_state["manual_paused"]:
        return (False, "目前人工暫停開倉")
    return (True, "OK")

# ==========================================
# SQLite 持久化資料庫
# ==========================================
def init_db():
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("PRAGMA journal_mode=MEMORY;")
            conn.execute(
                "CREATE TABLE IF NOT EXISTS trade_logs ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, symbol TEXT, stage TEXT, "
                "side TEXT, qty REAL, entry_price REAL, exit_price REAL, pnl_usdt REAL, pnl_pct REAL)"
            )
            conn.execute("CREATE TABLE IF NOT EXISTS daily_meta (day TEXT PRIMARY KEY, start_eq REAL)")
            conn.execute(
                "CREATE TABLE IF NOT EXISTS processed_signals ("
                "signal_id TEXT PRIMARY KEY, symbol TEXT, action TEXT, event_stage TEXT, "
                "signal_time INTEGER, processed_ts REAL)"
            )
            conn.execute(
                "CREATE TABLE IF NOT EXISTS positions_state ("
                "symbol TEXT PRIMARY KEY, trade_id TEXT, side TEXT, qty TEXT, entry_price TEXT, "
                "init_sl TEXT, current_sl TEXT, sl_algo_id TEXT, stage TEXT, entry_order_id TEXT, updated_ts REAL)"
            )
            conn.execute("CREATE TABLE IF NOT EXISTS system_flags (k TEXT PRIMARY KEY, v TEXT)")
            conn.commit()
    except Exception as e:
        logger.warning(f"SQLite 初始化備援模式: {e}")

def save_system_flags():
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("PRAGMA journal_mode=MEMORY;")
            for k in ("manual_paused", "circuit_breaker_tripped", "emergency_brake_tripped", "safe_mode_locked", "safe_mode_reason"):
                conn.execute("INSERT OR REPLACE INTO system_flags (k, v) VALUES (?, ?)", (k, str(bot_state[k])))
            conn.commit()
    except Exception:
        pass

def load_system_flags():
    try:
        with sqlite3.connect(DB_PATH) as conn:
            rows = conn.execute("SELECT k, v FROM system_flags").fetchall()
            for k, v in rows:
                if k in ("manual_paused", "circuit_breaker_tripped", "emergency_brake_tripped", "safe_mode_locked"):
                    bot_state[k] = (str(v).lower() == "true")
                elif k == "safe_mode_reason":
                    bot_state[k] = str(v)
    except Exception:
        pass

def check_and_record_signal(
    sig_id: Optional[str],
    sig_time_ms: Optional[int],
    sym: str,
    action: str,
    stage: str,
    stop_loss: Optional[float],
    server_offset_ms: int = 0
) -> Tuple[bool, str]:
    now_s = time.time()
    now_ms = int(now_s * 1000) + server_offset_ms

    if sig_time_ms is not None and sig_time_ms > 0:
        age_sec = (now_ms - int(sig_time_ms)) / 1000.0
        if age_sec > MAX_SIGNAL_AGE_SEC:
            return (False, f"過期訊號已攔截(延遲{age_sec:.0f}秒)")
        if age_sec < -60.0:
            return (False, "未來異常時間戳已攔截")

    eff_id = (sig_id or "").strip()
    if not eff_id:
        sl_s = f"{float(stop_loss):.6f}" if stop_loss else "none"
        eff_id = f"LEGACY:{sym.upper()}:{action.upper()}:{stage}:{sl_s}:{int(now_s // max(1.0, SIGNAL_DEDUP_SEC))}"

    if now_s - recent_signals_cache.get(eff_id, 0.0) < SIGNAL_DEDUP_SEC:
        return (False, "重複訊號已攔截")

    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("PRAGMA journal_mode=MEMORY;")
            row = conn.execute("SELECT signal_id FROM processed_signals WHERE signal_id = ?", (eff_id,)).fetchone()
            if row:
                recent_signals_cache[eff_id] = now_s
                return (False, "持久化去重：重複訊號已攔截")
            conn.execute(
                "INSERT INTO processed_signals (signal_id, symbol, action, event_stage, signal_time, processed_ts) VALUES (?, ?, ?, ?, ?, ?)",
                (eff_id, sym.upper(), action.upper(), stage, int(sig_time_ms or now_ms), now_s)
            )
            if len(recent_signals_cache) > 300:
                conn.execute("DELETE FROM processed_signals WHERE processed_ts < ?", (now_s - 86400 * 3,))
            conn.commit()
    except Exception:
        pass

    recent_signals_cache[eff_id] = now_s
    if len(recent_signals_cache) > 500:
        for k in [k for k, t in recent_signals_cache.items() if now_s - t > 600.0]:
            recent_signals_cache.pop(k, None)
    return (True, eff_id)

def upsert_db_position_state(
    sym: str,
    side: str,
    qty: Decimal,
    entry_price: Decimal,
    init_sl: Decimal,
    current_sl: Decimal,
    sl_algo_id: str,
    stage: str = "INIT",
    entry_order_id: str = "",
    trade_id: str = ""
):
    sym = sym.upper()
    now_s = time.time()
    tid = trade_id or f"{sym}_{int(now_s)}"
    known_positions[sym] = {
        "trade_id": tid,
        "side": side.upper(),
        "amt": float(qty) if side.upper() == "LONG" else -float(qty),
        "qty_dec": qty,
        "entry_price": float(entry_price),
        "entry_price_dec": entry_price,
        "mark_price": float(entry_price),
        "init_sl_dec": init_sl,
        "current_sl_dec": current_sl,
        "sl_algo_id": str(sl_algo_id or ""),
        "stage": stage,
        "entry_order_id": str(entry_order_id or ""),
        "updated_ts": now_s,
    }
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("PRAGMA journal_mode=MEMORY;")
            conn.execute(
                "INSERT OR REPLACE INTO positions_state "
                "(symbol, trade_id, side, qty, entry_price, init_sl, current_sl, sl_algo_id, stage, entry_order_id, updated_ts) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (sym, tid, side.upper(), str(qty), str(entry_price), str(init_sl), str(current_sl), str(sl_algo_id or ""), stage, str(entry_order_id or ""), now_s)
            )
            conn.commit()
    except Exception as e:
        logger.warning(f"寫入 positions_state 異常: {e}")

def get_db_position_state(sym: str) -> Optional[Dict[str, Any]]:
    sym = sym.upper()
    try:
        with sqlite3.connect(DB_PATH) as conn:
            row = conn.execute(
                "SELECT symbol, trade_id, side, qty, entry_price, init_sl, current_sl, sl_algo_id, stage, entry_order_id, updated_ts "
                "FROM positions_state WHERE symbol = ?", (sym,)
            ).fetchone()
            if row:
                q_dec = _to_dec(row[3])
                ep_dec = _to_dec(row[4])
                return {
                    "symbol": row[0],
                    "trade_id": row[1],
                    "side": row[2],
                    "amt": float(q_dec) if row[2] == "LONG" else -float(q_dec),
                    "qty_dec": q_dec,
                    "entry_price": float(ep_dec),
                    "entry_price_dec": ep_dec,
                    "init_sl_dec": _to_dec(row[5]),
                    "current_sl_dec": _to_dec(row[6]),
                    "sl_algo_id": str(row[7] or ""),
                    "stage": str(row[8] or "INIT"),
                    "entry_order_id": str(row[9] or ""),
                    "updated_ts": float(row[10] or 0.0),
                }
    except Exception:
        pass
    return known_positions.get(sym)

def get_all_db_positions() -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    try:
        with sqlite3.connect(DB_PATH) as conn:
            rows = conn.execute(
                "SELECT symbol, trade_id, side, qty, entry_price, init_sl, current_sl, sl_algo_id, stage, entry_order_id, updated_ts FROM positions_state"
            ).fetchall()
            for row in rows:
                q_dec = _to_dec(row[3])
                ep_dec = _to_dec(row[4])
                out[row[0]] = {
                    "symbol": row[0],
                    "trade_id": row[1],
                    "side": row[2],
                    "amt": float(q_dec) if row[2] == "LONG" else -float(q_dec),
                    "qty_dec": q_dec,
                    "entry_price": float(ep_dec),
                    "entry_price_dec": ep_dec,
                    "init_sl_dec": _to_dec(row[5]),
                    "current_sl_dec": _to_dec(row[6]),
                    "sl_algo_id": str(row[7] or ""),
                    "stage": str(row[8] or "INIT"),
                    "entry_order_id": str(row[9] or ""),
                    "updated_ts": float(row[10] or 0.0),
                }
    except Exception:
        pass
    return out

def delete_db_position_state(sym: str):
    sym = sym.upper()
    known_positions.pop(sym, None)
    peak_upnl_tracker.pop(sym, None)
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("PRAGMA journal_mode=MEMORY;")
            conn.execute("DELETE FROM positions_state WHERE symbol = ?", (sym,))
            conn.commit()
    except Exception:
        pass

def get_or_save_daily_start_eq(current_eq: float, force_reset: bool = False) -> float:
    if current_eq <= 0:
        return 0.0
    now_tw = datetime.now(TW_TZ)
    anchor_day = (now_tw - timedelta(hours=8)).strftime("%Y-%m-%d")
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("PRAGMA journal_mode=MEMORY;")
            if not force_reset:
                row = conn.execute("SELECT start_eq FROM daily_meta WHERE day = ?", (anchor_day,)).fetchone()
                if row and float(row[0]) > 0:
                    return float(row[0])
            conn.execute("INSERT OR REPLACE INTO daily_meta (day, start_eq) VALUES (?, ?)", (anchor_day, float(current_eq)))
            conn.commit()
    except Exception:
        pass
    return float(current_eq)

def record_trade_log(symbol: str, stage: str, pos_amt: float, close_qty: float, entry_price: float, exit_price: float, exact_pnl: Optional[float] = None) -> float:
    if close_qty <= 0 or entry_price <= 0 or exit_price <= 0:
        return 0.0
    is_long = pos_amt > 0
    if exact_pnl is not None:
        net_pnl = float(exact_pnl)
    else:
        raw_pnl = (exit_price - entry_price) * close_qty if is_long else (entry_price - exit_price) * close_qty
        fee_r = float(_sym_fee_rate_pct(symbol)) / 100.0
        net_pnl = raw_pnl - ((entry_price + exit_price) * close_qty * fee_r)
    pnl_pct = ((exit_price - entry_price) / entry_price * 100.0) if is_long else ((entry_price - exit_price) / entry_price * 100.0)
    now_str = datetime.now(TW_TZ).strftime("%Y-%m-%d %H:%M:%S")
    trade_logs_mem.append((now_str, symbol.upper(), stage, float(net_pnl)))
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("PRAGMA journal_mode=MEMORY;")
            conn.execute(
                "INSERT INTO trade_logs (ts, symbol, stage, side, qty, entry_price, exit_price, pnl_usdt, pnl_pct) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (now_str, symbol.upper(), stage, "LONG" if is_long else "SHORT", float(close_qty), float(entry_price), float(exit_price), float(net_pnl), float(pnl_pct))
            )
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
        f"🔢 結算筆數：{len(rows)} 筆\n"
        f"🏆 週總勝率：{wr:.1f}% ({len(wins)}勝{len(losses)}敗)\n"
        f"💰 週總淨利：{_fmt_u(total_net, signed=True)}\n"
        f"⚖️ 均盈虧比：{rr:.2f}\n"
        f"📈 均賺均賠：+{avg_w:.1f}U / -{avg_l:.1f}U\n"
        f"🚀 最大單筆：+{max(wins) if wins else 0.0:.2f}U\n\n"
        f"📅 今日結算：{len(td_rows)} 筆 (勝{td_wr:.0f}%)\n"
        f"💵 今日實現：{_fmt_u(td_net, signed=True)}"
    )

def send_line_push(text: str, to_user_id: Optional[str] = None):
    token = LINE_CHANNEL_ACCESS_TOKEN
    uid = (to_user_id or bot_state.get("bound_user_id") or LINE_USER_ID).strip()
    if not token or not uid:
        return
    def _push():
        try:
            requests.post(
                "https://api.line.me/v2/bot/message/push",
                headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
                json={"to": uid, "messages": [{"type": "text", "text": text[:4900]}]},
                timeout=8
            )
        except Exception as e:
            logger.error(f"LINE 推播異常: {e}")
    threading.Thread(target=_push, daemon=True).start()

def send_line_reply(reply_token: str, text: str):
    if not LINE_CHANNEL_ACCESS_TOKEN or not reply_token:
        return
    try:
        requests.post(
            "https://api.line.me/v2/bot/message/reply",
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}"},
            json={"replyToken": reply_token, "messages": [{"type": "text", "text": text[:4900]}]},
            timeout=8
        )
    except Exception as e:
        logger.error(f"LINE 回覆異常: {e}")

# ==========================================
# 全 Decimal 精度對齊與防重複下單 BinanceClient
# ==========================================
class BinanceClient:
    def __init__(self, key: str, sec: str):
        self.key, self.sec, self.time_offset, self._last_info_load = key, sec, 0, 0.0
        self._specs: Dict[str, Dict[str, Any]] = {}
        self._sym_leverage: Dict[str, int] = {}
        self._current_sl: Dict[str, float] = {}
        self._tracked_algo_ids: Dict[str, str] = {}
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
        if time.time() - self._last_info_load < 60.0 and self._specs:
            return
        self._last_info_load = time.time()
        try:
            res = requests.get(f"{FUTURES_URL}/fapi/v1/exchangeInfo", timeout=10).json()
            for s in res.get("symbols", []):
                if s.get("status") not in (None, "TRADING"):
                    continue
                sym = str(s.get("symbol", "")).upper()
                if not sym:
                    continue
                price_prec = int(s.get("pricePrecision", 2))
                qty_prec = int(s.get("quantityPrecision", 3))
                step_d = Decimal("0.001")
                tick_d = Decimal("0.01")
                min_not_d = Decimal("5.0")
                min_qty_d = Decimal("0.001")
                for f in s.get("filters", []):
                    ft = f.get("filterType")
                    if ft == "LOT_SIZE":
                        raw_step = str(f.get("stepSize", "0.001")).rstrip("0").rstrip(".") or "1"
                        step_d = _to_dec(raw_step, "0.001")
                        min_qty_d = _to_dec(f.get("minQty", raw_step), "0.001")
                        qty_prec = len(raw_step.split(".")[1]) if "." in raw_step else 0
                    elif ft == "PRICE_FILTER":
                        raw_tick = str(f.get("tickSize", "0.01")).rstrip("0").rstrip(".") or "1"
                        tick_d = _to_dec(raw_tick, "0.01")
                        price_prec = len(raw_tick.split(".")[1]) if "." in raw_tick else 0
                    elif ft in ("MIN_NOTIONAL", "NOTIONAL"):
                        min_not_d = _to_dec(f.get("notional", f.get("minNotional", "5.0")), "5.0")
                self._specs[sym] = {
                    "step": step_d,
                    "tick": tick_d,
                    "min_notional": min_not_d,
                    "min_qty": min_qty_d,
                    "price_dec": price_prec,
                    "qty_dec": qty_prec,
                }
        except Exception as e:
            logger.error(f"載入 exchangeInfo 失敗: {e}")

    def is_valid_symbol(self, sym: str) -> bool:
        sym = sym.upper()
        if sym not in self._specs:
            self._load_exchange_info()
        if sym in self._specs:
            return True
        return bool(re.match(r"^[A-Z0-9]{2,20}USDT$", sym))

    def _get_spec(self, sym: str, ref_price: Decimal = Decimal("0")) -> Dict[str, Any]:
        sym = sym.upper()
        if sym not in self._specs:
            self._load_exchange_info()
        if sym in self._specs:
            return self._specs[sym]
        p = abs(ref_price)
        if p >= Decimal("100"):
            tick_d, p_dec, step_d, q_dec = Decimal("0.01"), 2, Decimal("0.001"), 3
        elif p >= Decimal("10"):
            tick_d, p_dec, step_d, q_dec = Decimal("0.001"), 3, Decimal("0.01"), 2
        elif p >= Decimal("1"):
            tick_d, p_dec, step_d, q_dec = Decimal("0.0001"), 4, Decimal("0.1"), 1
        else:
            tick_d, p_dec, step_d, q_dec = Decimal("0.000001"), 6, Decimal("1"), 0
        return {
            "step": step_d,
            "tick": tick_d,
            "min_notional": Decimal("5.0"),
            "min_qty": step_d,
            "price_dec": p_dec,
            "qty_dec": q_dec,
        }

    def quantize_qty(self, sym: str, raw_qty: Any, round_up: bool = False) -> Decimal:
        q = _to_dec(raw_qty, "0")
        if q <= Decimal("0"):
            return Decimal("0")
        spec = self._get_spec(sym)
        step = spec["step"]
        if step <= Decimal("0"):
            return q
        rounding_mode = ROUND_CEILING if round_up else ROUND_DOWN
        units = (q / step).to_integral_value(rounding=rounding_mode)
        return (units * step).quantize(step, rounding=ROUND_DOWN)

    def quantize_price(self, sym: str, raw_price: Any, rounding_mode=ROUND_HALF_UP) -> Decimal:
        p = _to_dec(raw_price, "0")
        if p <= Decimal("0"):
            return Decimal("0")
        spec = self._get_spec(sym, p)
        tick = spec["tick"]
        if tick <= Decimal("0"):
            return p
        units = (p / tick).to_integral_value(rounding=rounding_mode)
        return (units * tick).quantize(tick, rounding=rounding_mode)

    def format_qty(self, sym: str, raw_qty: Any, round_up: bool = False) -> float:
        return float(self.quantize_qty(sym, raw_qty, round_up=round_up))

    def format_price(self, sym: str, raw_price: Any) -> float:
        return float(self.quantize_price(sym, raw_price))

    def fmt_p(self, sym: str, raw_price: Any) -> str:
        if raw_price is None:
            return "--"
        d_val = _to_dec(raw_price, "0")
        if d_val <= Decimal("0"):
            return "--"
        qp = self.quantize_price(sym, d_val)
        spec = self._get_spec(sym, qp)
        dec = spec["price_dec"]
        norm_s = f"{d_val:f}".rstrip("0").rstrip(".")
        if "." in norm_s:
            actual_dec = len(norm_s.split(".")[1])
            if dec < actual_dec <= dec + 2:
                return f"{d_val:.{actual_dec}f}"
        return f"{qp:.{dec}f}"

    def fmt_q_api(self, sym: str, raw_qty: Any) -> str:
        dq = self.quantize_qty(sym, raw_qty, round_up=False)
        spec = self._get_spec(sym)
        dec = spec["qty_dec"]
        return f"{dq:.{dec}f}" if dec > 0 else str(int(dq))

    def get_min_notional_dec(self, sym: str) -> Decimal:
        return self._get_spec(sym)["min_notional"]

    def get_min_notional(self, sym: str) -> float:
        return float(self.get_min_notional_dec(sym))

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
                # 優化第4點：若為市價下單 POST 發生網路逾時，重試前先以 origClientOrderId 查詢是否已成交，100% 防重複送單！
                if method == "POST" and endpoint == "/fapi/v1/order" and data and "newClientOrderId" in data:
                    existing = self.get_order_by_client_id(data["symbol"], data["newClientOrderId"])
                    if existing and "orderId" in existing:
                        return existing
                if attempt == retries - 1:
                    return None
                time.sleep(1.0 * (attempt + 1))
        return None

    def get_order_by_client_id(self, sym: str, client_oid: str) -> Optional[Dict[str, Any]]:
        sym = sym.upper()
        p = {"symbol": sym, "origClientOrderId": client_oid, "timestamp": int(time.time() * 1000) + self.time_offset, "recvWindow": 60000}
        p["signature"] = self._sign(p)
        try:
            res = requests.get(f"{FUTURES_URL}/fapi/v1/order", headers={"X-MBX-APIKEY": self.key}, params=p, timeout=5).json()
            return res if isinstance(res, dict) and "orderId" in res else None
        except Exception:
            return None

    def verify_account_position_mode(self) -> Tuple[bool, bool, str]:
        self._sync()
        p = {"timestamp": int(time.time() * 1000) + self.time_offset, "recvWindow": 60000}
        p["signature"] = self._sign(p)
        res = self._safe_request("GET", "/fapi/v1/positionSide/dual", params=p, retries=2)
        if isinstance(res, dict) and "dualSidePosition" in res:
            is_hedge = bool(res.get("dualSidePosition"))
            bot_state["hedge_mode"] = is_hedge
            bot_state["position_mode_verified"] = True
            if is_hedge and STRICT_ONE_WAY_ONLY:
                bot_state["safe_mode_locked"] = True
                bot_state["safe_mode_reason"] = "帳戶為雙向持倉但設定強制單向"
                save_system_flags()
                return (False, is_hedge, "拒絕啟動：帳戶為雙向持倉模式(Hedge Mode)")
            return (True, is_hedge, "雙向Hedge模式(自動帶positionSide)" if is_hedge else "單向One-Way模式")
        return (False, bot_state.get("hedge_mode", False), "無法查詢持倉模式")

    def verify_and_set_leverage(self, sym: str, target_lev: int) -> Tuple[bool, int]:
        sym = sym.upper()
        candidates = [target_lev] + [x for x in (50, 40, 30, 25, 20, 15, 10, 5) if x < target_lev]
        for try_lev in candidates:
            p = {"symbol": sym, "leverage": try_lev, "timestamp": int(time.time() * 1000) + self.time_offset, "recvWindow": 60000}
            p["signature"] = self._sign(p)
            res = self._safe_request("POST", "/fapi/v1/leverage", data=p, retries=2)
            if isinstance(res, dict) and int(res.get("leverage", 0) or 0) == try_lev:
                self._sym_leverage[sym] = try_lev
                return (True, try_lev)
        return (False, 0)

    def get_equity_and_margin_ratio(self) -> Tuple[float, float, float, Any]:
        self._sync()
        p = {"timestamp": int(time.time() * 1000) + self.time_offset, "recvWindow": 60000}
        p["signature"] = self._sign(p)
        r2 = self._safe_request("GET", "/fapi/v2/account", params=p)
        if isinstance(r2, dict) and "code" not in r2 and ("totalMarginBalance" in r2 or "totalWalletBalance" in r2):
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

    def get_price_dec(self, sym: str) -> Decimal:
        try:
            raw_p = requests.get(f"{FUTURES_URL}/fapi/v1/ticker/price", params={"symbol": sym.upper()}, timeout=5).json().get("price")
            d = _to_dec(raw_p, "0")
            return self.quantize_price(sym, d) if d > Decimal("0") else Decimal("0")
        except Exception:
            return Decimal("0")

    def get_price(self, sym: str) -> float:
        return float(self.get_price_dec(sym))

    def order(self, sym: str, side: str, qty: Any, reduce_only: bool = False, position_side: Optional[str] = None) -> Any:
        sym = sym.upper()
        dq = self.quantize_qty(sym, qty, round_up=False)
        if dq <= Decimal("0"):
            return None
        q_str = self.fmt_q_api(sym, dq)
        client_oid = f"HTB_{sym[:6]}_{int(time.time()*1000)}_{secrets.token_hex(2)}"
        p: Dict[str, Any] = {
            "symbol": sym,
            "side": side.upper(),
            "type": "MARKET",
            "quantity": q_str,
            "newClientOrderId": client_oid,
            "timestamp": int(time.time() * 1000) + self.time_offset,
            "recvWindow": 60000,
        }
        if bot_state.get("hedge_mode"):
            if position_side:
                p["positionSide"] = position_side.upper()
            else:
                if reduce_only:
                    p["positionSide"] = "LONG" if side.upper() == "SELL" else "SHORT"
                else:
                    p["positionSide"] = "LONG" if side.upper() == "BUY" else "SHORT"
        else:
            if reduce_only:
                p["reduceOnly"] = "true"
        p["signature"] = self._sign(p)
        return self._safe_request("POST", "/fapi/v1/order", data=p, retries=3)

    def get_order_status(self, sym: str, order_id: Any) -> Optional[Dict[str, Any]]:
        sym = sym.upper()
        p = {"symbol": sym, "orderId": order_id, "timestamp": int(time.time() * 1000) + self.time_offset, "recvWindow": 60000}
        p["signature"] = self._sign(p)
        res = self._safe_request("GET", "/fapi/v1/order", params=p, retries=2)
        return res if isinstance(res, dict) and "orderId" in res else None

    def verify_order_fill(
        self,
        sym: str,
        order_res: Any,
        expected_qty: Decimal,
        is_entry: bool = False,
        expected_side_dir: str = "LONG"
    ) -> Tuple[bool, Decimal, Decimal, str]:
        sym = sym.upper()
        if not isinstance(order_res, dict) or "orderId" not in order_res:
            return (False, Decimal("0"), Decimal("0"), str(order_res))
        oid = order_res["orderId"]

        exec_qty = _to_dec(order_res.get("executedQty", "0"))
        avg_p = _to_dec(order_res.get("avgPrice", "0"))
        status = str(order_res.get("status", "")).upper()

        for attempt in range(3):
            if status == "FILLED" and exec_qty > Decimal("0") and avg_p > Decimal("0"):
                break
            info = self.get_order_status(sym, oid)
            if info:
                status = str(info.get("status", status)).upper()
                exec_qty = _to_dec(info.get("executedQty", exec_qty))
                avg_p = _to_dec(info.get("avgPrice", avg_p))
                if status == "FILLED" and exec_qty > Decimal("0"):
                    break
            time.sleep(0.25 * (attempt + 1))

        pos_amt, pos_ep = self.get_position_info_dec(sym, position_side=expected_side_dir if bot_state.get("hedge_mode") else None)
        if exec_qty <= Decimal("0"):
            if is_entry and abs(pos_amt) > Decimal("0"):
                exec_qty = abs(pos_amt)
                avg_p = pos_ep if pos_ep > Decimal("0") else self.get_price_dec(sym)
                status = "FILLED"
            elif not is_entry and status == "FILLED":
                exec_qty = expected_qty
                avg_p = avg_p if avg_p > Decimal("0") else self.get_price_dec(sym)

        if avg_p <= Decimal("0"):
            avg_p = pos_ep if pos_ep > Decimal("0") else self.get_price_dec(sym)

        is_ok = (status in ("FILLED", "PARTIALLY_FILLED") and exec_qty > Decimal("0"))
        return (is_ok, exec_qty, self.quantize_price(sym, avg_p), status or "UNKNOWN")

    def cancel_algo_order(self, sym: str, algo_id: Optional[str]) -> bool:
        if not algo_id:
            return True
        sym = sym.upper()
        p = {"symbol": sym, "algoId": str(algo_id), "timestamp": int(time.time() * 1000) + self.time_offset, "recvWindow": 60000}
        p["signature"] = self._sign(p)
        res = self._safe_request("DELETE", "/fapi/v1/algoOrder", params=p, retries=2)
        if isinstance(res, dict) and (res.get("code") in (None, 200, -2011) or "algoId" in res):
            if self._tracked_algo_ids.get(sym) == str(algo_id):
                self._tracked_algo_ids.pop(sym, None)
            return True
        return False

    def cancel_tracked_sl_only(self, sym: str):
        """精準撤銷機器人建立的 STOP_MARKET 條件止損單，絕不影響使用者手動掛的限價單或止盈單"""
        sym = sym.upper()
        tracked_id = self._tracked_algo_ids.get(sym)
        db_state = get_db_position_state(sym)
        if db_state and db_state.get("sl_algo_id"):
            tracked_id = tracked_id or db_state["sl_algo_id"]
        if tracked_id:
            self.cancel_algo_order(sym, tracked_id)
        for o in self.get_open_algo_orders(sym):
            aid = str(o.get("algoId", ""))
            cid = str(o.get("clientAlgoId", ""))
            otype = str(o.get("orderType", o.get("type", ""))).upper()
            if aid and (cid.startswith("HTBOT_") or aid == str(tracked_id or "") or otype in ("STOP_MARKET", "STOP")):
                self.cancel_algo_order(sym, aid)
        self._current_sl.pop(sym, None)
        self._tracked_algo_ids.pop(sym, None)

    def get_open_algo_orders(self, sym: str) -> List[Dict[str, Any]]:
        sym = sym.upper()
        p = {"symbol": sym, "timestamp": int(time.time() * 1000) + self.time_offset, "recvWindow": 60000}
        p["signature"] = self._sign(p)
        r1 = self._safe_request("GET", "/fapi/v1/openAlgoOrders", params=p, retries=2)
        orders = r1.get("orders", []) if isinstance(r1, dict) else (r1 if isinstance(r1, list) else [])
        return [o for o in orders if isinstance(o, dict) and o.get("symbol") == sym]

    def set_sl_and_verify(self, sym: str, side: str, price: Any, position_side: Optional[str] = None) -> Tuple[bool, str, Decimal]:
        sym = sym.upper()
        qp = self.quantize_price(sym, price)
        fp_str = self.fmt_p(sym, qp)
        for attempt in range(3):
            client_algo_id = f"HTBOT_{sym[:6]}_{int(time.time()*1000) % 100000000}_{attempt}"
            p: Dict[str, Any] = {
                "algoType": "CONDITIONAL",
                "symbol": sym,
                "side": side.upper(),
                "type": "STOP_MARKET",
                "triggerPrice": fp_str,
                "closePosition": "true",
                "workingType": "MARK_PRICE",
                "clientAlgoId": client_algo_id,
                "timestamp": int(time.time() * 1000) + self.time_offset,
                "recvWindow": 60000,
            }
            if bot_state.get("hedge_mode"):
                p["positionSide"] = (position_side or ("LONG" if side.upper() == "SELL" else "SHORT")).upper()
            p["signature"] = self._sign(p)
            res = self._safe_request("POST", "/fapi/v1/algoOrder", data=p, retries=2)
            if isinstance(res, dict) and "algoId" in res:
                new_aid = str(res["algoId"])
                open_orders = self.get_open_algo_orders(sym)
                if open_orders:
                    matched = any(str(o.get("algoId", "")) == new_aid for o in open_orders)
                    if not matched:
                        time.sleep(0.25)
                        open_orders = self.get_open_algo_orders(sym)
                self._current_sl[sym] = float(qp)
                self._tracked_algo_ids[sym] = new_aid
                return (True, new_aid, qp)
            # 若幣安回傳 -4130 (同方向已存在 closePosition=true 條件止損單)，立即精準清除舊止損單後重試！
            if isinstance(res, dict) and res.get("code") == -4130:
                self.cancel_tracked_sl_only(sym)
            time.sleep(0.25 * (attempt + 1))
        return (False, "", Decimal("0"))

    def replace_sl_safely(self, sym: str, sl_side: str, new_price: Any, position_side: Optional[str] = None) -> Tuple[bool, str, Decimal]:
        """
        更新止損單安全機制：
        1. 先記錄舊止損價與舊 algoId 作為備援。
        2. 嘗試掛新止損單 (若遇幣安 -4130 限制同向只能有一張 closePosition=true 止損單，自動精準撤除舊止損單並掛入新單)。
        3. 若新止損單因網路或非觸價原因失敗，自動掛回舊止損價保底！
        """
        sym = sym.upper()
        db_state = get_db_position_state(sym)
        old_aid = (db_state.get("sl_algo_id") if db_state else None) or self._tracked_algo_ids.get(sym)
        old_sl_dec = (db_state.get("current_sl_dec") if db_state else None) or _to_dec(self._current_sl.get(sym, 0))

        ok, new_aid, qp = self.set_sl_and_verify(sym, sl_side, new_price, position_side=position_side)
        if not ok or not new_aid:
            if old_aid:
                self.cancel_algo_order(sym, str(old_aid))
            ok, new_aid, qp = self.set_sl_and_verify(sym, sl_side, new_price, position_side=position_side)

        if not ok or not new_aid:
            if old_sl_dec and old_sl_dec > Decimal("0"):
                ok_restore, res_aid, res_qp = self.set_sl_and_verify(sym, sl_side, old_sl_dec, position_side=position_side)
                if ok_restore:
                    logger.warning(f"⚠️ {sym} 新止損 ({new_price}) 掛單未過，已自動恢復原止損 ({res_qp})")
            return (False, "", Decimal("0"))

        if old_aid and str(old_aid) != str(new_aid):
            self.cancel_algo_order(sym, str(old_aid))

        if db_state:
            upsert_db_position_state(
                sym=sym,
                side=db_state["side"],
                qty=db_state["qty_dec"],
                entry_price=db_state["entry_price_dec"],
                init_sl=db_state["init_sl_dec"],
                current_sl=qp,
                sl_algo_id=new_aid,
                stage=db_state["stage"],
                entry_order_id=db_state["entry_order_id"],
                trade_id=db_state["trade_id"],
            )
        return (True, new_aid, qp)

    def set_sl(self, sym: str, side: str, price: Any) -> bool:
        ok, _, _ = self.replace_sl_safely(sym, side, price)
        return ok

    def get_existing_sl_info(self, sym: str) -> Tuple[Optional[Decimal], Optional[str]]:
        sym = sym.upper()
        for o in self.get_open_algo_orders(sym):
            if o.get("orderType", o.get("type", "")) in ("STOP_MARKET", "STOP"):
                tp = _to_dec(o.get("triggerPrice", o.get("stopPrice", "0")))
                aid = str(o.get("algoId", ""))
                if tp > Decimal("0"):
                    qp = self.quantize_price(sym, tp)
                    self._current_sl[sym] = float(qp)
                    if aid:
                        self._tracked_algo_ids[sym] = aid
                    return (qp, aid)
        return (None, None)

    def get_existing_sl_price(self, sym: str) -> Optional[float]:
        qp, _ = self.get_existing_sl_info(sym)
        return float(qp) if qp is not None else None

    def get_order_net_pnl(self, sym: str, order_res: Any) -> Optional[float]:
        """向幣安 /fapi/v1/userTrades 輪詢查詢指定平倉單號之真實 realizedPnl 與手續費 (含等待撮合入庫重試)，確保與幣安『倉位歷史』100% 一致"""
        if not isinstance(order_res, dict) or "orderId" not in order_res:
            return None
        oid = order_res["orderId"]
        sym = sym.upper()
        for attempt in range(3):
            p = {"symbol": sym, "orderId": oid, "timestamp": int(time.time() * 1000) + self.time_offset, "recvWindow": 60000}
            p["signature"] = self._sign(p)
            res = self._safe_request("GET", "/fapi/v1/userTrades", params=p, retries=1)
            if isinstance(res, list) and len(res) > 0:
                realized_pnl = sum(float(t.get("realizedPnl", 0.0)) for t in res)
                close_fee = sum(float(t.get("commission", 0.0)) for t in res if t.get("commissionAsset") == "USDT")
                if abs(realized_pnl) > 0:
                    return realized_pnl - (close_fee * 2.0)
            time.sleep(0.35 * (attempt + 1))
        return None

    def get_recent_close_fill(self, sym: str, lookback_sec: int = 600) -> Optional[Dict[str, Any]]:
        sym = sym.upper()
        p = {"symbol": sym, "limit": 15, "timestamp": int(time.time() * 1000) + self.time_offset, "recvWindow": 60000}
        p["signature"] = self._sign(p)
        res = self._safe_request("GET", "/fapi/v1/userTrades", params=p, retries=2)
        if not isinstance(res, list) or not res:
            return None
        now_ms = int(time.time() * 1000) + self.time_offset
        cutoff_ms = now_ms - (lookback_sec * 1000)
        closing_fills = [t for t in res if int(t.get("time", 0)) >= cutoff_ms and abs(float(t.get("realizedPnl", 0.0))) > 0]
        if not closing_fills:
            return None
        latest_order_id = closing_fills[-1].get("orderId")
        order_fills = [t for t in closing_fills if t.get("orderId") == latest_order_id]
        total_qty = sum(float(t.get("qty", 0.0)) for t in order_fills)
        if total_qty <= 0:
            return None
        avg_exit = sum(float(t.get("price", 0.0)) * float(t.get("qty", 0.0)) for t in order_fills) / total_qty
        realized_pnl = sum(float(t.get("realizedPnl", 0.0)) for t in order_fills)
        fee = sum(float(t.get("commission", 0.0)) for t in order_fills if t.get("commissionAsset") == "USDT")
        net_pnl = realized_pnl - (fee * 2.0)
        close_side = str(order_fills[-1].get("side", "SELL")).upper()
        was_long = (close_side == "SELL")
        return {
            "qty": total_qty,
            "exit_price": self.format_price(sym, avg_exit),
            "net_pnl": net_pnl,
            "was_long": was_long,
            "fill_ts": int(order_fills[-1].get("time", now_ms)) / 1000.0,
        }

    def get_position_info_dec(self, sym: str, position_side: Optional[str] = None) -> Tuple[Decimal, Decimal]:
        sym = sym.upper()
        p = {"symbol": sym, "timestamp": int(time.time() * 1000) + self.time_offset, "recvWindow": 60000}
        p["signature"] = self._sign(p)
        res = self._safe_request("GET", "/fapi/v2/positionRisk", params=p)
        if isinstance(res, list):
            for pos in res:
                if pos.get("symbol") != sym:
                    continue
                ps = str(pos.get("positionSide", "BOTH")).upper()
                if position_side and ps not in ("BOTH", position_side.upper()):
                    continue
                amt = _to_dec(pos.get("positionAmt", "0"))
                ep = _to_dec(pos.get("entryPrice", "0")).normalize()
                lev_val = int(float(pos.get("leverage", self._sym_leverage.get(sym, 40)) or 40))
                self._sym_leverage[sym] = lev_val
                if abs(amt) > Decimal("0") or not bot_state.get("hedge_mode"):
                    return (amt, ep)
        return (Decimal("0"), Decimal("0"))

    def get_position_info(self, sym: str) -> Tuple[float, float]:
        amt, ep = self.get_position_info_dec(sym)
        return (float(amt), float(ep))

    def verify_position_zero(self, sym: str, retries: int = 4) -> bool:
        for i in range(retries):
            amt, _ = self.get_position_info_dec(sym)
            if abs(amt) == Decimal("0"):
                return True
            time.sleep(0.25 * (i + 1))
        return False

    def get_all_active_positions(self) -> List[Dict[str, Any]]:
        p = {"timestamp": int(time.time() * 1000) + self.time_offset, "recvWindow": 60000}
        p["signature"] = self._sign(p)
        res = self._safe_request("GET", "/fapi/v2/positionRisk", params=p)
        active = []
        if isinstance(res, list):
            for pos in res:
                amt_d = _to_dec(pos.get("positionAmt", "0"))
                if abs(amt_d) > Decimal("0"):
                    sym = str(pos.get("symbol", "")).upper()
                    raw_ep = _to_dec(pos.get("entryPrice", "0"))
                    raw_mp = _to_dec(pos.get("markPrice", "0"))
                    ep = raw_ep.normalize() if raw_ep > Decimal("0") else Decimal("0")
                    mp = self.quantize_price(sym, raw_mp if raw_mp > Decimal("0") else raw_ep)
                    upnl = float(_to_dec(pos.get("unRealizedProfit", "0")))
                    lev = int(float(pos.get("leverage", self._sym_leverage.get(sym, 40)) or 40))
                    self._sym_leverage[sym] = lev
                    notional = float(abs(amt_d) * (mp if mp > Decimal("0") else ep))
                    margin = notional / max(lev, 1)
                    roi_pct = (upnl / margin * 100.0) if margin > 0 else 0.0
                    if notional >= 0.5:
                        active.append({
                            "symbol": sym,
                            "amt": float(amt_d),
                            "qty_dec": abs(amt_d),
                            "entry_price": float(ep),
                            "entry_price_dec": ep,
                            "mark_price": float(mp),
                            "mark_price_dec": mp,
                            "upnl": upnl,
                            "notional": notional,
                            "leverage": lev,
                            "margin": margin,
                            "roi_pct": roi_pct,
                            "position_side": str(pos.get("positionSide", "BOTH")).upper(),
                        })
        return active

# ==========================================
# 智慧 Decimal 風控倉位計算 (含 5U 最小門檻彈性保底與最大風險保護，防漏掉 AGT 大波段)
# ==========================================
def calculate_order_qty_decimal(
    c: BinanceClient,
    sym: str,
    entry_price: Decimal,
    stop_loss: Decimal,
    equity_u: Decimal,
    avail_u: Decimal,
    risk_pct: Decimal,
    leverage: int,
    allow_min_notional_bump: bool = True
) -> Tuple[Decimal, str]:
    if entry_price <= Decimal("0") or stop_loss <= Decimal("0") or equity_u <= Decimal("0"):
        return (Decimal("0"), "價格或帳戶權益無效")

    stop_dist = abs(entry_price - stop_loss)
    min_safe_dist = entry_price * Decimal("0.002")
    eff_stop_dist = max(stop_dist, min_safe_dist)

    fee_r = _sym_fee_rate_pct(sym)
    cost_rate = (fee_r * Decimal("2") + SLIP_BUFFER_PCT) / Decimal("100")
    per_unit_loss = eff_stop_dist + (entry_price * cost_rate)
    if per_unit_loss <= Decimal("0"):
        return (Decimal("0"), "單位止損距離無效")

    risk_budget_u = equity_u * (risk_pct / Decimal("100"))
    raw_risk_qty = risk_budget_u / per_unit_loss

    min_notional = c.get_min_notional_dec(sym)
    spec = c._get_spec(sym, entry_price)
    min_qty = spec["min_qty"]

    # 若小資金 (如 90U) 在波動較大幣種 (如 AGTUSDT) 算出的倉位略低於 5U (例如 4.4U)，
    # 只要湊滿 5U 的實際止損風險不超過單筆容忍上限 (預設 3.2% 本金)，自動彈性保底湊滿 5U，絕不漏掉大行情！
    if allow_min_notional_bump and (raw_risk_qty * entry_price) < min_notional:
        bump_qty = c.quantize_qty(sym, (min_notional * Decimal("1.04")) / entry_price, round_up=True)
        bump_risk_u = bump_qty * per_unit_loss
        max_allowed_risk_u = equity_u * (MAX_MIN_NOTIONAL_RISK_PCT / Decimal("100"))
        if bump_risk_u <= max_allowed_risk_u:
            raw_risk_qty = bump_qty

    lev_d = Decimal(str(max(1, leverage)))
    max_margin_pct_d = _to_dec(bot_state["max_margin_pct"], "20.0") / Decimal("100")
    max_notional_mult_d = _to_dec(bot_state["max_notional_mult"], "3.2")

    max_margin_qty = (equity_u * max_margin_pct_d * lev_d) / entry_price
    max_notional_qty = (equity_u * max_notional_mult_d) / entry_price
    eff_avail = avail_u if avail_u > Decimal("0") else equity_u
    avail_cap_qty = (eff_avail * Decimal("0.92") * lev_d) / entry_price

    capped_qty = min(raw_risk_qty, max_margin_qty, max_notional_qty, avail_cap_qty)
    aligned_qty = c.quantize_qty(sym, capped_qty, round_up=False)

    if aligned_qty < min_qty or (aligned_qty * entry_price) < min_notional:
        return (
            Decimal("0"),
            f"風險預算倉位({_fmt_u(aligned_qty * entry_price)})低於最低門檻({_fmt_u(min_notional)})"
        )
    return (aligned_qty, "OK")

init_db()
load_system_flags()
client = BinanceClient(BINANCE_API_KEY, BINANCE_API_SECRET)
daily_start_balance = 0.0
last_report_date = None
last_hourly_key = None

def notify_and_log_closed_position(sym: str, prev_info: Optional[Dict[str, Any]], stage: str = "STOP_EXIT") -> bool:
    sym = sym.upper()
    now_t = time.time()
    if now_t - notified_close_ts.get(sym, 0.0) < 25.0:
        return False

    fill = client.get_recent_close_fill(sym, lookback_sec=600)
    if not fill and not prev_info:
        return False

    notified_close_ts[sym] = now_t
    client.cancel_tracked_sl_only(sym)
    delete_db_position_state(sym)

    was_long = (prev_info.get("amt", 0) > 0 or prev_info.get("side") == "LONG") if prev_info else (fill["was_long"] if fill else True)
    close_qty = fill["qty"] if fill else abs(float(prev_info.get("amt", 0.0)))
    exit_p = fill["exit_price"] if fill else (client.get_price(sym) or float(prev_info.get("entry_price", 0.0)))
    if prev_info and float(prev_info.get("entry_price", 0)) > 0:
        entry_p = float(prev_info["entry_price"])
    elif fill and close_qty > 0:
        price_diff = fill["net_pnl"] / close_qty
        entry_p = client.format_price(sym, (exit_p - price_diff) if was_long else (exit_p + price_diff))
    else:
        entry_p = exit_p

    exact_pnl = fill["net_pnl"] if fill else None
    pos_amt_signed = close_qty if was_long else -close_qty
    net_u = record_trade_log(sym, stage, pos_amt_signed, close_qty, entry_p, exit_p, exact_pnl=exact_pnl)
    total_eq, _, _ = client.get_equity()
    if total_eq > 0:
        check_daily_circuit_breaker(total_eq)

    fsym = _full_sym(sym)
    send_line_push(
        f"🏁 【止損 / 趨勢平倉完成】\n"
        f"🪙 交易幣種：{fsym} ({'多單' if was_long else '空單'})\n"
        f"📦 平倉顆數：{_fmt_qty(close_qty)} 顆\n"
        f"🏷️ 開倉價格：{client.fmt_p(sym, entry_p)}\n"
        f"📍 出場價格：{client.fmt_p(sym, exit_p)}\n"
        f"💵 本筆損益：{_fmt_u(net_u, signed=True)}\n"
        f"💰 帳戶權益：{total_eq:.2f} USDT"
    )
    return True

def reconcile_state_with_binance(notify_if_repaired: bool = True, strict_startup_check: bool = False) -> str:
    """
    優化第1點：主動雙向校對機制 (防重啟錯亂)
    若發現幣安有真實持倉但資料庫漏登或方向不同，自動以幣安真實持倉為準校正並補齊止損單！
    """
    try:
        client.verify_account_position_mode()
        positions = client.get_all_active_positions()
        db_map = get_all_db_positions()

        if not positions:
            for stale_sym in list(db_map.keys()):
                delete_db_position_state(stale_sym)
            return "📭 重啟檢查：目前無未平倉位"

        lines = [f"🔄 狀態對齊：接管 {len(positions)} 筆持倉"]
        repaired_lines = []
        anomaly_reasons = []

        active_syms = {p["symbol"] for p in positions}
        for stale_sym in list(db_map.keys()):
            if stale_sym not in active_syms:
                delete_db_position_state(stale_sym)

        for pos in positions:
            sym, amt = pos["symbol"], pos["amt"]
            side_str = "LONG" if amt > 0 else "SHORT"
            ep_dec = pos["entry_price_dec"]
            mp_dec = pos["mark_price_dec"] if pos["mark_price_dec"] > Decimal("0") else ep_dec
            qty_dec = pos["qty_dec"]
            bot_state["last_symbol"] = sym

            db_rec = db_map.get(sym)
            same_dir_rec = db_rec if (db_rec and db_rec.get("side") == side_str) else None

            ex_sl_dec, ex_aid = client.get_existing_sl_info(sym)
            if ex_sl_dec and ex_sl_dec > Decimal("0"):
                upsert_db_position_state(
                    sym=sym,
                    side=side_str,
                    qty=qty_dec,
                    entry_price=ep_dec,
                    init_sl=same_dir_rec["init_sl_dec"] if same_dir_rec else ex_sl_dec,
                    current_sl=ex_sl_dec,
                    sl_algo_id=ex_aid or (same_dir_rec["sl_algo_id"] if same_dir_rec else ""),
                    stage=same_dir_rec["stage"] if same_dir_rec else "RECONCILED",
                )
                lines.append(f"🛡️ 既有止損：{_sym_val(sym, client.fmt_p(sym, ex_sl_dec))}")
            else:
                sl_side = "SELL" if amt > 0 else "BUY"
                fb = EMERGENCY_SL_FALLBACK_PCT / Decimal("100")
                be_buf = BE_BUFFER_PCT / Decimal("100")
                if amt > 0:
                    safe_sl = ep_dec * (Decimal("1") + be_buf) if mp_dec >= ep_dec * Decimal("1.01") else min(ep_dec, mp_dec) * (Decimal("1") - fb)
                else:
                    safe_sl = ep_dec * (Decimal("1") - be_buf) if mp_dec <= ep_dec * Decimal("0.99") else max(ep_dec, mp_dec) * (Decimal("1") + fb)
                ok, new_aid, qp = client.replace_sl_safely(sym, sl_side, safe_sl, position_side=side_str)
                if ok:
                    upsert_db_position_state(
                        sym=sym,
                        side=side_str,
                        qty=qty_dec,
                        entry_price=ep_dec,
                        init_sl=qp,
                        current_sl=qp,
                        sl_algo_id=new_aid,
                        stage=same_dir_rec["stage"] if same_dir_rec else "REPAIRED",
                    )
                    rep_str = f"🔧 自動補掛：{_sym_val(sym, client.fmt_p(sym, qp))}"
                    lines.append(rep_str)
                    repaired_lines.append(rep_str)
                else:
                    rep_str = f"🚨 補掛失敗：{_disp_sym(sym)} (無止損)"
                    lines.append(rep_str)
                    repaired_lines.append(rep_str)
                    anomaly_reasons.append(f"{_disp_sym(sym)}無止損且補掛失敗")

        if anomaly_reasons:
            bot_state["safe_mode_locked"] = True
            bot_state["safe_mode_reason"] = ",".join(anomaly_reasons)[:16]
            save_system_flags()
            send_line_push(
                f"🚨 【狀態不一致・進入安全模式】\n"
                f"⚠️ 異常原因：{bot_state['safe_mode_reason']}\n"
                f"🔒 防護動作：已暫停新開倉\n"
                f"💡 解除方式：請查倉位後輸入 10"
            )
        elif repaired_lines and notify_if_repaired:
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
        save_system_flags()
        closed_lines = []
        for pos in client.get_all_active_positions():
            sym, amt = pos["symbol"], pos["amt"]
            notified_close_ts[sym] = time.time()
            client.cancel_tracked_sl_only(sym)
            order_res = client.order(sym, "SELL" if amt > 0 else "BUY", abs(amt), reduce_only=True, position_side=pos.get("position_side"))
            exact_p = client.get_order_net_pnl(sym, order_res)
            delete_db_position_state(sym)
            record_trade_log(sym, "EMERGENCY_BRAKE", amt, abs(amt), pos["entry_price"], pos["mark_price"], exact_pnl=exact_p)
            closed_lines.append(f"✂️ 強制清倉：{_sym_val(sym, _fmt_qty(abs(amt)) + '顆')}")
        closed_block = "\n".join(closed_lines) if closed_lines else "✂️ 強制清倉：目前無持倉"
        send_line_push(
            f"🛑 【總權益極限斷路觸發】\n"
            f"💰 當前權益：{current_eq:.2f} USDT\n"
            f"🚨 觸發底線：{floor_u:.2f} USDT\n"
            f"{closed_block}\n"
            f"🔒 系統狀態：需人工復核解除"
        )
        return True
    return bot_state["emergency_brake_tripped"]

def check_daily_circuit_breaker(current_eq: float) -> bool:
    global daily_start_balance
    if check_emergency_hard_brake(current_eq):
        return True
    if daily_start_balance <= 0 and current_eq > 0:
        daily_start_balance = get_or_save_daily_start_eq(current_eq)
    if daily_start_balance <= 0 or current_eq <= 0:
        return False
    dd_pct = (daily_start_balance - current_eq) / daily_start_balance * 100.0
    max_dd = bot_state["max_daily_dd_pct"]
    if dd_pct >= max_dd and not bot_state["circuit_breaker_tripped"]:
        bot_state["circuit_breaker_tripped"] = True
        save_system_flags()
        send_line_push(
            f"🚨 【每日最大回撤熔斷】\n"
            f"📊 今日基準：{daily_start_balance:.2f} USDT\n"
            f"💰 當前權益：{current_eq:.2f} USDT\n"
            f"📉 當日回撤：-{dd_pct:.1f}% (限-{max_dd:.0f}%)\n"
            f"🛡️ 防禦動作：鎖定至明早08:00"
        )
        return True
    return bot_state["circuit_breaker_tripped"]

def build_status_report(title: str = "📊 【即時帳戶與持倉查詢】") -> str:
    global daily_start_balance
    now_str = datetime.now(TW_TZ).strftime("%m-%d %H:%M:%S")
    total_eq, avail, margin_ratio, _ = client.get_equity_and_margin_ratio()
    if daily_start_balance <= 0 and total_eq > 0:
        daily_start_balance = get_or_save_daily_start_eq(total_eq)
    positions = client.get_all_active_positions()

    if bot_state["emergency_brake_tripped"]:
        mode_str = f"🛑 斷路停機 ({bot_state['emergency_floor_usdt']:.0f}U)"
    elif bot_state["safe_mode_locked"]:
        mode_str = "🔒 安全模式鎖定"
    elif bot_state["circuit_breaker_tripped"]:
        mode_str = f"🚨 熔斷防禦 (-{bot_state['max_daily_dd_pct']:.0f}%)"
    elif not bot_state["manual_paused"]:
        mode_str = "🟢 正常自動接單"
    else:
        mode_str = "⏸️ 人工暫停新倉"

    if daily_start_balance > 0 and total_eq > 0:
        diff = total_eq - daily_start_balance
        pct = max(-99.9, min(999.0, (diff / daily_start_balance) * 100.0))
        pct_s = f"{pct:+.0f}%" if abs(pct) >= 10.0 else f"{pct:+.1f}%"
        daily_pnl_str = f"{_fmt_u(diff, signed=True)} ({pct_s})"
    else:
        daily_pnl_str = "+0.00U (0.0%)"

    opt_tags = []
    if bot_state["trail_profit_enabled"]:
        opt_tags.append(f"鎖利{bot_state['trail_profit_trigger_u']:.0f}U")
    if bot_state["allow_pyramiding"]:
        opt_tags.append("順勢加碼")
    opt_str = "/".join(opt_tags) if opt_tags else "預設關閉"

    lines = [
        f"{title}",
        f"🕒 查詢時間：{now_str}",
        f"🤖 系統狀態：{mode_str}",
        f"💰 帳戶權益：{total_eq:.2f} USDT",
        f"💵 可用保證：{avail:.2f} USDT",
        f"📈 今日損益：{daily_pnl_str}",
        f"🛡️ 倉位上限：保證{bot_state['max_margin_pct']:.0f}%/{bot_state['max_notional_mult']:.1f}倍",
        f"🛑 防護底線：熔斷-{bot_state['max_daily_dd_pct']:.0f}%/底{bot_state['emergency_floor_usdt']:.0f}U",
        f"⚡ 進階選配：{opt_str}",
    ]
    if not positions:
        sym = bot_state.get("last_symbol", DEFAULT_SYMBOL)
        lines.append("📭 目前持倉：空手觀望中\n")
        lines.append(f"🔍 最近幣種：{_sym_val(sym, client.fmt_p(sym, client.get_price(sym)))}")
    else:
        lines.append(f"📦 目前持倉：{len(positions)}筆 (維持{margin_ratio:.1f}%)")
        for pos in positions:
            sym, amt, upnl = pos["symbol"], pos["amt"], pos["upnl"]
            fsym = _full_sym(sym)
            lev = pos.get("leverage", client._sym_leverage.get(sym, 40))
            margin_u = pos.get("margin", pos["notional"] / max(lev, 1))
            roi_pct = max(-99.9, min(999.0, pos.get("roi_pct", (upnl / margin_u * 100.0) if margin_u > 0 else 0.0)))
            roi_str = f"{roi_pct:+.0f}%" if abs(roi_pct) >= 10.0 else f"{roi_pct:+.1f}%"
            raw_sl = client._current_sl.get(sym) or client.get_existing_sl_price(sym)
            sl_str = client.fmt_p(sym, raw_sl) if raw_sl else "依條件單"
            ep_str = client.fmt_p(sym, pos["entry_price_dec"])
            mp_str = client.fmt_p(sym, pos["mark_price_dec"])
            qty_str = _fmt_qty(abs(amt))
            lines.append(
                f"\n🔸 【{fsym}】{'🟢做多' if amt > 0 else '🔴做空'} {lev}x\n"
                f"💵 未實損益：{_fmt_u(upnl, signed=True)} ({roi_str})\n"
                f"📦 持倉價值：{pos['notional']:.2f} USDT\n"
                f"🪙 持倉顆數：{qty_str} 顆\n"
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
            daily_start_balance = get_or_save_daily_start_eq(init_eq)
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
                daily_start_balance = get_or_save_daily_start_eq(total_eq)
            if total_eq > 0:
                check_daily_circuit_breaker(total_eq)

            prev_snapshot = get_all_db_positions()
            active_positions = client.get_all_active_positions()
            active_syms = {p["symbol"] for p in active_positions}

            for sym, prev_info in prev_snapshot.items():
                if sym not in active_syms:
                    notify_and_log_closed_position(sym, prev_info, stage="STOP_EXIT")

            # 優化第1點：每 60 秒主動雙向校對一次，若幣安有倉位但資料庫漏登或方向不同，自動補登並檢查止損！
            now_ts = time.time()
            if now_ts - bot_state.get("last_auto_reconcile_ts", 0.0) >= 60.0:
                bot_state["last_auto_reconcile_ts"] = now_ts
                for pos in active_positions:
                    sym = pos["symbol"]
                    side_str = "LONG" if pos["amt"] > 0 else "SHORT"
                    db_rec = prev_snapshot.get(sym)
                    if not db_rec or db_rec.get("side") != side_str:
                        reconcile_state_with_binance(notify_if_repaired=True)
                        break

            if bot_state["trail_profit_enabled"]:
                trig_u = bot_state["trail_profit_trigger_u"]
                gb_pct = bot_state["trail_profit_giveback_pct"]
                for pos in active_positions:
                    sym, amt, upnl = pos["symbol"], pos["amt"], pos["upnl"]
                    prev_peak = peak_upnl_tracker.get(sym, 0.0)
                    if upnl > prev_peak:
                        peak_upnl_tracker[sym] = upnl
                        prev_peak = upnl
                    if prev_peak >= trig_u and upnl <= prev_peak * (1.0 - gb_pct / 100.0):
                        notified_close_ts[sym] = time.time()
                        side = "SELL" if amt > 0 else "BUY"
                        order_res = client.order(sym, side, abs(amt), reduce_only=True, position_side=pos.get("position_side"))
                        ok_fill, exec_q, avg_p, _ = client.verify_order_fill(sym, order_res, pos["qty_dec"], is_entry=False, expected_side_dir="LONG" if amt > 0 else "SHORT")
                        exact_p = client.get_order_net_pnl(sym, order_res)
                        client.cancel_tracked_sl_only(sym)
                        delete_db_position_state(sym)
                        exit_p = float(avg_p) if ok_fill and avg_p > Decimal("0") else pos["mark_price"]
                        net_u = record_trade_log(sym, "TRAIL_PROFIT", amt, float(exec_q) if ok_fill else abs(amt), pos["entry_price"], exit_p, exact_pnl=exact_p)
                        send_line_push(
                            f"💰 【追蹤止盈回吐鎖利全平】\n"
                            f"🪙 交易幣種：{_full_sym(sym)}\n"
                            f"🚀 最高浮盈：{_fmt_u(prev_peak, signed=True)}\n"
                            f"💵 鎖定落袋：{_fmt_u(net_u, signed=True)}\n"
                            f"🛡️ 觸發條件：高點回吐 {gb_pct:.0f}%"
                        )

            if margin_ratio >= float(MARGIN_ALERT_RATIO) and (now_ts - bot_state["last_margin_alert_ts"] > 1800):
                bot_state["last_margin_alert_ts"] = now_ts
                send_line_push(f"⚠️ 【保證金率強平預警】\n🚨 保證金率：{margin_ratio:.1f}%\n💰 帳戶權益：{total_eq:.2f} USDT\n💡 建議操作：輸入 3 減倉或 2 全平")

            if now_tw.hour == 8 and now_tw.minute < 5 and last_report_date != today_date:
                was_tripped = bot_state["circuit_breaker_tripped"]
                if was_tripped:
                    bot_state["circuit_breaker_tripped"] = False
                    save_system_flags()
                if daily_start_balance > 0 and total_eq > 0:
                    diff = total_eq - daily_start_balance
                    pct = max(-99.9, min(999.0, (diff / daily_start_balance) * 100.0))
                    pct_s = f"{pct:+.0f}%" if abs(pct) >= 10.0 else f"{pct:+.1f}%"
                    reset_note = "\n🔄 熔斷重置：已恢復今日額度" if was_tripped else ""
                    daily_msg = f"🌅 【每日 08:00 損益日報】\n📅 結算日期：{today_date}\n💰 今日權益：{total_eq:.2f} USDT\n💵 可用保證：{avail:.2f} USDT\n📊 昨日基準：{daily_start_balance:.2f} USDT\n🏆 單日損益：{_fmt_u(diff, signed=True)} ({pct_s}){reset_note}"
                else:
                    daily_msg = f"🌅 【每日 08:00 損益日報】\n📅 結算日期：{today_date}\n💰 今日權益：{total_eq:.2f} USDT"
                send_line_push(daily_msg)
                if total_eq > 0:
                    daily_start_balance = get_or_save_daily_start_eq(total_eq, force_reset=True)
                last_report_date, last_hourly_key = today_date, hour_key
            elif bot_state["hourly_push_enabled"] and now_tw.minute < 5 and last_hourly_key != hour_key:
                send_line_push(build_status_report(title=f"⏰ 【整點巡檢報告 ({now_tw.hour:02d}:00)】"))
                last_hourly_key = hour_key
            time.sleep(25)
        except Exception:
            time.sleep(60)

@asynccontextmanager
async def lifespan(app: FastAPI):
    reconcile_summary = reconcile_state_with_binance(notify_if_repaired=False, strict_startup_check=True)
    threading.Thread(target=background_scheduler, daemon=True).start()
    if LINE_CHANNEL_ACCESS_TOKEN and (LINE_USER_ID or bot_state.get("bound_user_id")):
        send_line_push(
            f"🤖 【幣安機器人 v5.3】已上線\n"
            f"{reconcile_summary}\n"
            f"💡 快捷指令：輸入 0 查看選單"
        )
    yield

app = FastAPI(lifespan=lifespan)

class Signal(BaseModel):
    passphrase: str
    signal_id: Optional[str] = None
    signal_time: Optional[int] = None
    symbol: str
    action: str
    event_stage: Optional[str] = "SIGNAL"
    risk_pct: Optional[float] = 1.5
    stop_loss: Optional[float] = None
    leverage: Optional[int] = 40
    close_pct: Optional[float] = None
    signal_price: Optional[float] = None
    use_tp2: Optional[bool] = False

@app.get("/")
def index():
    allowed, reason = is_new_entry_allowed()
    return {
        "status": "online",
        "version": "v5.3 Anti-Wick-Washout & Sync",
        "new_entry_allowed": allowed,
        "gate_reason": reason,
        "manual_paused": bot_state["manual_paused"],
        "circuit_breaker_tripped": bot_state["circuit_breaker_tripped"],
        "emergency_brake_tripped": bot_state["emergency_brake_tripped"],
        "safe_mode_locked": bot_state["safe_mode_locked"],
    }

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

    if text in ("2", "平倉", "全平", "一鍵平倉", "緊急平倉") or upper_text.startswith(("平倉 ", "2 ")):
        parts = upper_text.split()
        target_sym = _norm_sym(parts[1]) if len(parts) > 1 else None
        positions = [p for p in client.get_all_active_positions() if (not target_sym or p["symbol"] == target_sym)]
        if not positions:
            sym_clean = target_sym or bot_state.get("last_symbol", DEFAULT_SYMBOL)
            client.cancel_tracked_sl_only(sym_clean)
            delete_db_position_state(sym_clean)
            return f"📭 目前持倉：無 ({_disp_sym(sym_clean)}已清)"
        closed = []
        for pos in positions:
            sym, amt = pos["symbol"], pos["amt"]
            notified_close_ts[sym] = time.time()
            side = "SELL" if amt > 0 else "BUY"
            order_res = client.order(sym, side, abs(amt), reduce_only=True, position_side=pos.get("position_side"))
            ok_fill, exec_q, avg_p, _ = client.verify_order_fill(sym, order_res, pos["qty_dec"], is_entry=False, expected_side_dir="LONG" if amt > 0 else "SHORT")
            exact_p = client.get_order_net_pnl(sym, order_res)
            client.cancel_tracked_sl_only(sym)
            delete_db_position_state(sym)
            exit_p = float(avg_p) if ok_fill and avg_p > Decimal("0") else pos["mark_price"]
            pnl_u = record_trade_log(sym, "MANUAL_CLOSE", amt, float(exec_q) if ok_fill else abs(amt), pos["entry_price"], exit_p, exact_pnl=exact_p)
            closed.append(f"✂️ 已平倉位：{_sym_val(sym, _fmt_u(pnl_u, signed=True))}")
        total_eq, _, _ = client.get_equity()
        return "🚨 【一鍵全平倉執行完畢】\n" + "\n".join(closed) + f"\n💰 最新權益：{total_eq:.2f} USDT"

    if text in ("3", "減半", "平半", "減倉") or upper_text.startswith(("減半 ", "平半 ", "減倉 ", "3 ")):
        parts = upper_text.split()
        target_sym = _norm_sym(parts[1]) if len(parts) > 1 else None
        positions = [p for p in client.get_all_active_positions() if (not target_sym or p["symbol"] == target_sym)]
        if not positions:
            return "📭 目前持倉：無持倉可減半"
        res = []
        for pos in positions:
            sym, amt = pos["symbol"], pos["amt"]
            hq_dec = client.quantize_qty(sym, pos["qty_dec"] * Decimal("0.5"))
            rq_dec = client.quantize_qty(sym, pos["qty_dec"] - hq_dec)
            if hq_dec <= Decimal("0") or rq_dec <= Decimal("0"):
                res.append(f"⚠️ 無法拆半：{_disp_sym(sym)} (已達最小1顆)")
            else:
                order_res = client.order(sym, "SELL" if amt > 0 else "BUY", hq_dec, reduce_only=True, position_side=pos.get("position_side"))
                ok_fill, exec_q, avg_p, _ = client.verify_order_fill(sym, order_res, hq_dec, is_entry=False, expected_side_dir="LONG" if amt > 0 else "SHORT")
                if ok_fill and exec_q > Decimal("0"):
                    rem_after, _ = client.get_position_info_dec(sym)
                    actual_rem = abs(rem_after) if abs(rem_after) > Decimal("0") else rq_dec
                    exact_p = client.get_order_net_pnl(sym, order_res)
                    pnl_est = record_trade_log(sym, "MANUAL_HALF", amt, float(exec_q), pos["entry_price"], float(avg_p), exact_pnl=exact_p)
                    db_s = get_db_position_state(sym)
                    if db_s:
                        upsert_db_position_state(
                            sym, db_s["side"], actual_rem, db_s["entry_price_dec"],
                            db_s["init_sl_dec"], db_s["current_sl_dec"], db_s["sl_algo_id"], stage="MANUAL_HALF"
                        )
                    res.append(f"✂️ 已減半倉：{_sym_val(sym, _fmt_u(pnl_est, signed=True))}")
                else:
                    res.append(f"⚠️ 減半未成：{_disp_sym(sym)}")
        return "✂️ 【手動減倉 50% 結果】\n" + "\n".join(res)

    if text in ("4", "保本", "移保本") or upper_text.startswith(("保本 ", "移保本 ", "4 ")):
        parts = upper_text.split()
        target_sym = _norm_sym(parts[1]) if len(parts) > 1 else None
        positions = [p for p in client.get_all_active_positions() if (not target_sym or p["symbol"] == target_sym)]
        if not positions:
            return "📭 目前持倉：無持倉可設保本"
        res = []
        for pos in positions:
            sym, amt = pos["symbol"], pos["amt"]
            ep_d, mp_d = pos["entry_price_dec"], pos["mark_price_dec"]
            be_buf = BE_BUFFER_PCT / Decimal("100")
            be_p = ep_d * (Decimal("1") + be_buf) if amt > 0 else ep_d * (Decimal("1") - be_buf)
            if (amt > 0 and mp_d <= be_p) or (amt < 0 and mp_d >= be_p):
                res.append(f"⚠️ 未達保本：{_disp_sym(sym)} (現價未過)")
            else:
                ok, _, qp = client.replace_sl_safely(sym, "SELL" if amt > 0 else "BUY", be_p, position_side="LONG" if amt > 0 else "SHORT")
                if ok:
                    res.append(f"🛡️ 已移保本：{_sym_val(sym, client.fmt_p(sym, qp))}")
                else:
                    res.append(f"❌ 掛單失敗：{_disp_sym(sym)} (保留原止損)")
        return "🛡️ 【手動移至保本結果】\n" + "\n".join(res)

    m_sl = re.match(r"^(?:5|止損|改止損|SL)\s+(?:([A-Z0-9]+)\s+)?([0-9.]+)$", upper_text)
    if m_sl:
        sym_arg, new_sl = m_sl.group(1), _to_dec(m_sl.group(2))
        positions = client.get_all_active_positions()
        if not positions:
            return "📭 目前持倉：無持倉可改止損"
        target_pos = None
        if sym_arg:
            norm_t = _norm_sym(sym_arg)
            for p in positions:
                if p["symbol"] == norm_t:
                    target_pos = p
                    break
            if not target_pos:
                return f"📭 找不到持倉：{_disp_sym(norm_t)}"
        elif len(positions) == 1:
            target_pos = positions[0]
        else:
            target_pos = min(positions, key=lambda p: abs(float(p["mark_price_dec"] or p["entry_price_dec"]) - float(new_sl)) / max(float(p["mark_price_dec"] or p["entry_price_dec"]), 1e-8))
        sym, amt = target_pos["symbol"], target_pos["amt"]
        ok, _, qp = client.replace_sl_safely(sym, "SELL" if amt > 0 else "BUY", new_sl, position_side="LONG" if amt > 0 else "SHORT")
        return f"🛡️ 止損更新：{_sym_val(sym, client.fmt_p(sym, qp))}" if ok else f"❌ 止損失敗：{_disp_sym(sym)} (保留舊單)"

    if text in ("6", "暫停", "停止", "暫停開倉"):
        bot_state["manual_paused"] = True
        save_system_flags()
        return "⏸️ 【已人工暫停開新倉】\n🛡️ 既有止損：照常運作\n💡 恢復指令：輸入 7 解除暫停"

    if text in ("7", "恢復", "啟動", "開始", "恢復開倉"):
        bot_state["manual_paused"] = False
        save_system_flags()
        if bot_state["emergency_brake_tripped"]:
            return (
                "⚠️ 【暫停已解・但斷路鎖定中】\n"
                "🛑 狀態說明：總權益曾跌破斷路底線\n"
                "💡 復核指令：復核斷路 確認"
            )
        if bot_state["circuit_breaker_tripped"]:
            return (
                "⚠️ 【暫停已解・但今日熔斷中】\n"
                "🚨 狀態說明：今日回撤已達熔斷上限\n"
                "🕗 自動解鎖：明早08:00重置"
            )
        if bot_state["safe_mode_locked"]:
            return (
                "⚠️ 【暫停已解・但處安全模式】\n"
                f"🔒 鎖定原因：{bot_state['safe_mode_reason']}\n"
                "💡 解除指令：輸入 10 對齊"
            )
        return "🟢 【已恢復自動開新倉】\n🤖 系統狀態：正常接收訊號中"

    if upper_text in ("復核斷路 確認", "解除斷路 確認", "RESET BRAKE"):
        bot_state["emergency_brake_tripped"] = False
        bot_state["manual_paused"] = False
        save_system_flags()
        return "🟢 【人工復核完成】\n🔓 斷路解除：已恢復正常自動接單"

    if upper_text in ("重置熔斷 確認", "解除熔斷 確認", "RESET DD"):
        bot_state["circuit_breaker_tripped"] = False
        eq, _, _ = client.get_equity()
        if eq > 0:
            daily_start_balance = get_or_save_daily_start_eq(eq, force_reset=True)
        save_system_flags()
        return "🟢 【人工重置熔斷完成】\n📊 今日基準：已重設為當前權益"

    if text in ("8", "整點推播", "每小時推播"):
        bot_state["hourly_push_enabled"] = not bot_state["hourly_push_enabled"]
        return f"⏰ 整點推播：{'🟢 已開啟' if bot_state['hourly_push_enabled'] else '⏸️ 已關閉'}"

    if text in ("9", "統計", "週報", "戰績", "報表", "歷史"):
        return build_trade_stats_report(days=7)

    if text in ("10", "對齊", "巡檢", "檢查止損", "解除安全"):
        bot_state["safe_mode_locked"] = False
        bot_state["safe_mode_reason"] = ""
        save_system_flags()
        return reconcile_state_with_binance(notify_if_repaired=False)

    if upper_text in ("鎖利", "鎖利 開", "追蹤止盈"):
        bot_state["trail_profit_enabled"] = not bot_state["trail_profit_enabled"]
        st = f"🟢 滿{bot_state['trail_profit_trigger_u']:.0f}U回吐{bot_state['trail_profit_giveback_pct']:.0f}%" if bot_state["trail_profit_enabled"] else "⏸️ 已關閉"
        return f"💰 追蹤鎖利：{st}"
    if upper_text in ("鎖利 關", "鎖利 OFF"):
        bot_state["trail_profit_enabled"] = False
        return "💰 追蹤鎖利：⏸️ 已關閉"
    m_tp_lock = re.match(r"^(?:鎖利|TRAIL)\s+([0-9.]+)(?:\s+([0-9.]+))?$", upper_text)
    if m_tp_lock:
        bot_state["trail_profit_enabled"] = True
        bot_state["trail_profit_trigger_u"] = max(0.5, float(m_tp_lock.group(1)))
        if m_tp_lock.group(2):
            bot_state["trail_profit_giveback_pct"] = max(5.0, min(60.0, float(m_tp_lock.group(2))))
        return f"💰 追蹤鎖利：滿{bot_state['trail_profit_trigger_u']:.1f}U回吐{bot_state['trail_profit_giveback_pct']:.0f}%"

    if upper_text in ("加碼", "加碼 開", "加碼 ON"):
        bot_state["allow_pyramiding"] = True
        return "🚀 順勢加碼：🟢 已開啟"
    if upper_text in ("加碼 關", "加碼 OFF"):
        bot_state["allow_pyramiding"] = False
        return "🚀 順勢加碼：⏸️ 已關閉"

    m_cb = re.match(r"^(?:熔斷|回撤|DD)\s+([0-9.]+)$", upper_text)
    if m_cb:
        bot_state["max_daily_dd_pct"] = max(1.0, min(95.0, float(m_cb.group(1))))
        return f"🛡️ 回撤熔斷：-{bot_state['max_daily_dd_pct']:.1f}%"

    m_fl = re.match(r"^(?:底線|斷路|FLOOR)\s+([0-9.]+)$", upper_text)
    if m_fl:
        bot_state["emergency_floor_usdt"] = max(0.0, float(m_fl.group(1)))
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
        "🤖 【幣安機器人 v5.3 選單】\n"
        "🔹 「1」查詢帳戶與持倉狀態\n"
        "🔹 「2」一鍵市價全平所有倉\n"
        "🔹 「3」手動將持倉減倉 50%\n"
        "🔹 「4」手動將止損移至保本\n"
        "🔹 「6」暫停 / 「7」解除暫停\n"
        "🔹 「8」開關每小時整點推播\n"
        "🔹 「9」查看近 7 日統計週報\n"
        "🔹 「10」對齊倉位解安全鎖\n"
        "🔹 「止損 價格」智慧更新止損\n"
        "🔹 「鎖利 數字」開啟回吐鎖利\n"
        "🔹 「加碼 開/關」順勢加碼開關\n"
        f"🔹 「上限 數字」保證金 {bot_state['max_margin_pct']:.0f}%\n"
        f"🔹 「倍數 數字」倉位限 {bot_state['max_notional_mult']:.1f}倍\n"
        f"🔹 「熔斷 數字」日回撤 -{bot_state['max_daily_dd_pct']:.0f}%\n"
        f"🔹 「底線 金額」強平線 {bot_state['emergency_floor_usdt']:.0f}U"
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

# ==========================================
# Webhook 訊號處理中心
# ==========================================
@app.post("/webhook")
async def handle_webhook(request: Request):
    if not WEBHOOK_PASSPHRASE:
        raise HTTPException(status_code=503, detail="伺服器尚未設定 WEBHOOK_PASSPHRASE 環境變數")

    try:
        sig = Signal(**(await request.json()))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid payload: {e}")

    if not secrets.compare_digest(sig.passphrase, WEBHOOK_PASSPHRASE):
        raise HTTPException(status_code=401, detail="密碼錯誤")

    sym = sig.symbol.strip().upper()
    action = sig.action.strip().upper()
    stage = (sig.event_stage or action).strip().upper()

    if action not in VALID_ACTIONS:
        raise HTTPException(status_code=400, detail=f"未授權之交易動作: {action}")
    if not client.is_valid_symbol(sym):
        raise HTTPException(status_code=400, detail=f"未知或無效之交易對: {sym}")
    if sig.stop_loss is not None:
        sl_check = _to_dec(sig.stop_loss, "-1")
        if sl_check <= Decimal("0"):
            raise HTTPException(status_code=400, detail="無效的止損價格 (必須 > 0)")

    fsym = _full_sym(sym)
    bot_state["last_symbol"] = sym

    async with webhook_lock:
        ok_sig, sig_msg = check_and_record_signal(
            sig.signal_id, sig.signal_time, sym, action, stage, sig.stop_loss, client.time_offset
        )
        if not ok_sig:
            return {"success": True, "msg": sig_msg}

        # ==========================================================
        # A. TP1 / TP2 減倉與防插針緩衝保本 (與 Pine Script v5.3 100% 同步)
        # ==========================================================
        if action in ("TP1_LONG", "TP1_SHORT", "TP2_LONG", "TP2_SHORT"):
            is_tp1 = action.startswith("TP1")
            is_long_act = "LONG" in action
            pos_amt_d, entry_price_d = client.get_position_info_dec(sym)
            if abs(pos_amt_d) <= Decimal("0") or (is_long_act and pos_amt_d < Decimal("0")) or (not is_long_act and pos_amt_d > Decimal("0")):
                return {"success": True, "msg": f"無對應持倉，略過{'TP1' if is_tp1 else 'TP2'}"}

            db_rec = get_db_position_state(sym)
            target_stage = "TP1_DONE" if is_tp1 else "TP2_DONE"
            if db_rec and db_rec.get("stage") == target_stage and db_rec.get("side") == ("LONG" if is_long_act else "SHORT"):
                return {"success": True, "msg": f"{target_stage} 已完成，略過重複減倉"}

            side = "SELL" if is_long_act else "BUY"
            pos_side = "LONG" if is_long_act else "SHORT"
            curr_p_d = client.get_price_dec(sym) or entry_price_d
            default_pct = Decimal("50.0") if is_tp1 else Decimal("66.67")
            raw_pct = _to_dec(sig.close_pct, str(default_pct))
            if raw_pct <= Decimal("0") or raw_pct >= Decimal("100"):
                raw_pct = default_pct
            pct_d = raw_pct / Decimal("100")

            close_qty_d = client.quantize_qty(sym, abs(pos_amt_d) * pct_d)
            rem_qty_d = client.quantize_qty(sym, abs(pos_amt_d) - close_qty_d)

            reduced_ok = False
            actual_rem_d = abs(pos_amt_d)
            if Decimal("0") < close_qty_d < abs(pos_amt_d) and rem_qty_d > Decimal("0"):
                order_res = client.order(sym, side, close_qty_d, reduce_only=True, position_side=pos_side)
                ok_fill, exec_q, avg_exit_p, fill_st = client.verify_order_fill(
                    sym, order_res, close_qty_d, is_entry=False, expected_side_dir=pos_side
                )
                if ok_fill and exec_q > Decimal("0"):
                    reduced_ok = True
                    post_amt_d, _ = client.get_position_info_dec(sym)
                    actual_rem_d = abs(post_amt_d) if abs(post_amt_d) > Decimal("0") else (abs(pos_amt_d) - exec_q)
                    exact_p = client.get_order_net_pnl(sym, order_res)
                    net_u = record_trade_log(sym, "TP1" if is_tp1 else "TP2", float(pos_amt_d), float(exec_q), float(entry_price_d), float(avg_exit_p), exact_pnl=exact_p)
                    tp_lines = (
                        f"✂️ 減倉顆數：{_fmt_qty(exec_q)} 顆 ({int(round(float(raw_pct)))}%)\n"
                        f"📦 剩餘尾倉：{_fmt_qty(actual_rem_d)} 顆\n"
                        f"💵 落袋損益：{_fmt_u(net_u, signed=True)}\n"
                    )
                else:
                    send_line_push(f"⚠️ 【{fsym} {'TP1' if is_tp1 else 'TP2'} 減倉未完全成交】\n🚨 狀態回報：{fill_st}")

            if not reduced_ok:
                tp_lines = f"📦 保留全倉：{_fmt_qty(abs(pos_amt_d))} 顆移保本\n"

            be_buf = BE_BUFFER_PCT / Decimal("100")
            be_sl_d = entry_price_d * (Decimal("1") + be_buf) if is_long_act else entry_price_d * (Decimal("1") - be_buf)
            # 直接採用 Pine Script v5.3 傳來之防插針緩衝保本價 (sig.stop_loss)，確保 TV 與幣安止損 100% 同步！
            target_sl_d = client.quantize_price(sym, sig.stop_loss) if (sig.stop_loss and sig.stop_loss > 0) else be_sl_d

            cur_sl_d, _ = client.get_existing_sl_info(sym)
            if cur_sl_d and cur_sl_d > Decimal("0"):
                target_sl_d = max(target_sl_d, cur_sl_d) if is_long_act else min(target_sl_d, cur_sl_d)

            sl_ok, new_aid, verified_sl_d = client.replace_sl_safely(sym, side, target_sl_d, position_side=pos_side)
            if not sl_ok:
                curr_p_d = client.get_price_dec(sym) or curr_p_d
                price_crossed_be = (is_long_act and curr_p_d <= target_sl_d) or (not is_long_act and curr_p_d >= target_sl_d)
                if price_crossed_be:
                    rem_amt_d, _ = client.get_position_info_dec(sym)
                    if abs(rem_amt_d) > Decimal("0"):
                        notified_close_ts[sym] = time.time()
                        close_res = client.order(sym, side, abs(rem_amt_d), reduce_only=True, position_side=pos_side)
                        ok_c, exec_c, avg_c, _ = client.verify_order_fill(sym, close_res, abs(rem_amt_d), is_entry=False, expected_side_dir=pos_side)
                        exact_p = client.get_order_net_pnl(sym, close_res)
                        client.cancel_tracked_sl_only(sym)
                        delete_db_position_state(sym)
                        record_trade_log(sym, "TP1_PROTECT", float(pos_amt_d), float(exec_c if ok_c else abs(rem_amt_d)), float(entry_price_d), float(avg_c if ok_c else curr_p_d), exact_pnl=exact_p)
                    send_line_push(f"🛡️ 【{'TP1' if is_tp1 else 'TP2'} 回落保護全平】\n🪙 交易幣種：{fsym}\n⚠️ 觸發原因：現價已穿保本價")
                    return {"success": True, "msg": "已全平保護"}
                verified_sl_d = cur_sl_d or target_sl_d

            upsert_db_position_state(
                sym=sym,
                side=pos_side,
                qty=actual_rem_d,
                entry_price=entry_price_d,
                init_sl=db_rec["init_sl_dec"] if db_rec else verified_sl_d,
                current_sl=verified_sl_d,
                sl_algo_id=new_aid,
                stage=target_stage if reduced_ok else (db_rec["stage"] if db_rec else "BE_MOVED"),
                entry_order_id=db_rec["entry_order_id"] if db_rec else "",
                trade_id=db_rec["trade_id"] if db_rec else "",
            )
            is_above_be = (is_long_act and verified_sl_d >= entry_price_d) or (not is_long_act and verified_sl_d <= entry_price_d)
            sl_label_s = "保本止損" if is_above_be else "緩衝止損"
            title_str = ("🎯 【1.5R TP1 止盈與保本】" if is_above_be else "🎯 【1.5R TP1 減半鎖利】") if is_tp1 else "🚀 【3.0R TP2 二階止盈】"
            send_line_push(
                f"{title_str}\n"
                f"🪙 交易幣種：{fsym}\n"
                f"{tp_lines}"
                f"🏷️ 開倉價格：{client.fmt_p(sym, entry_price_d)}\n"
                f"🛡️ {sl_label_s}：{client.fmt_p(sym, verified_sl_d)}"
            )
            return {"success": True, "msg": f"{'TP1' if is_tp1 else 'TP2'}完成"}

        # ==========================================================
        # B. 海龜移動止損更新 (UPDATE_SL)：徹底修復未觸價卻誤平倉 Bug！
        # ==========================================================
        if action == "UPDATE_SL":
            if not sig.stop_loss:
                return {"success": False, "msg": "缺少 stop_loss"}
            pos_amt_d, entry_price_d = client.get_position_info_dec(sym)
            if abs(pos_amt_d) <= Decimal("0"):
                return {"success": True, "msg": "無持倉"}
            is_long_pos = pos_amt_d > Decimal("0")
            pos_side = "LONG" if is_long_pos else "SHORT"
            sl_side = "SELL" if is_long_pos else "BUY"
            new_sl_d = client.quantize_price(sym, sig.stop_loss)

            cur_sl_d, _ = client.get_existing_sl_info(sym)
            if not cur_sl_d:
                db_rec = get_db_position_state(sym)
                if db_rec and db_rec.get("side") == pos_side:
                    cur_sl_d = db_rec["current_sl_dec"]
            if cur_sl_d and cur_sl_d > Decimal("0"):
                if (is_long_pos and new_sl_d <= cur_sl_d) or (not is_long_pos and new_sl_d >= cur_sl_d):
                    return {"success": True, "msg": f"既有止損 ({cur_sl_d}) 更有利，略過倒退更新"}

            sl_ok, new_aid, verified_sl_d = client.replace_sl_safely(sym, sl_side, new_sl_d, position_side=pos_side)
            if not sl_ok:
                # 關鍵防呆：只有當「當前市價真的已經穿過新止損價」時，才允許執行觸價市價平倉！
                # 若現價根本沒碰到止損價 (例如 ETH 空單現價 2383 < 止損 2410)，絕不誤平倉！
                curr_p_d = client.get_price_dec(sym)
                price_actually_crossed = (curr_p_d > Decimal("0")) and (
                    (is_long_pos and curr_p_d <= new_sl_d) or (not is_long_pos and curr_p_d >= new_sl_d)
                )
                if price_actually_crossed:
                    rem_amt_d, _ = client.get_position_info_dec(sym)
                    net_u = 0.0
                    if abs(rem_amt_d) > Decimal("0"):
                        notified_close_ts[sym] = time.time()
                        close_res = client.order(sym, sl_side, abs(rem_amt_d), reduce_only=True, position_side=pos_side)
                        ok_c, exec_c, avg_c, _ = client.verify_order_fill(sym, close_res, abs(rem_amt_d), is_entry=False, expected_side_dir=pos_side)
                        exact_p = client.get_order_net_pnl(sym, close_res)
                        client.cancel_tracked_sl_only(sym)
                        delete_db_position_state(sym)
                        curr_p_d = avg_c if (ok_c and avg_c > Decimal("0")) else curr_p_d
                        net_u = record_trade_log(sym, "TRAIL_SL", float(pos_amt_d), float(exec_c if ok_c else abs(rem_amt_d)), float(entry_price_d), float(curr_p_d), exact_pnl=exact_p)
                    send_line_push(f"🛡️ 【移動止損觸發出場】\n🪙 交易幣種：{fsym}\n📍 出場價格：{client.fmt_p(sym, curr_p_d)}\n💵 結算損益：{_fmt_u(net_u, signed=True)}")
                    return {"success": True, "msg": "現價已穿過止損，已市價出場"}
                logger.warning(f"⚠️ {sym} UPDATE_SL ({new_sl_d}) 暫未更新成功，但現價 ({curr_p_d}) 未觸及止損，保留原倉與原止損！")
                return {"success": False, "msg": "現價未觸及止損，已保留原倉與原止損"}

            fmt_sl = client.fmt_p(sym, verified_sl_d)
            if stage == "LOCK_BE":
                send_line_push(
                    f"🔒 【2.2R 尾倉推過成本保本】\n"
                    f"🪙 交易幣種：{fsym}\n"
                    f"🏷️ 開倉價格：{client.fmt_p(sym, entry_price_d)}\n"
                    f"🛡️ 保本止損：{fmt_sl}"
                )
            logger.info(f"🐢 {sym} 海龜移動止損已更新至 {fmt_sl} (algoId={new_aid})")
            return {"success": True, "msg": f"止損已更新至 {fmt_sl}"}

        # ==========================================================
        # C. 止損或趨勢反轉平倉 (CLOSE_LONG / CLOSE_SHORT)
        # ==========================================================
        if action in ("CLOSE_LONG", "CLOSE_SHORT"):
            if time.time() - last_entry_ts.get(sym, 0.0) < 6.0:
                return {"success": True, "msg": "剛完成反手開倉，略過延遲抵達之舊平倉指令"}
            prev_cached = get_db_position_state(sym)
            pos_amt_d, entry_price_d = client.get_position_info_dec(sym)
            if abs(pos_amt_d) > Decimal("0"):
                if ("LONG" in action and pos_amt_d < Decimal("0")) or ("SHORT" in action and pos_amt_d > Decimal("0")):
                    return {"success": True, "msg": "方向已變"}
                is_long_pos = pos_amt_d > Decimal("0")
                pos_side = "LONG" if is_long_pos else "SHORT"
                close_side = "SELL" if is_long_pos else "BUY"
                notified_close_ts[sym] = time.time()

                order_res = client.order(sym, close_side, abs(pos_amt_d), reduce_only=True, position_side=pos_side)
                ok_fill, exec_q, avg_exit_p, _ = client.verify_order_fill(
                    sym, order_res, abs(pos_amt_d), is_entry=False, expected_side_dir=pos_side
                )
                exact_p = client.get_order_net_pnl(sym, order_res)
                client.cancel_tracked_sl_only(sym)
                delete_db_position_state(sym)

                exit_p_f = float(avg_exit_p) if (ok_fill and avg_exit_p > Decimal("0")) else client.get_price(sym)
                closed_q_f = float(exec_q) if (ok_fill and exec_q > Decimal("0")) else float(abs(pos_amt_d))
                net_u = record_trade_log(sym, stage, float(pos_amt_d), closed_q_f, float(entry_price_d), exit_p_f, exact_pnl=exact_p)
                total_eq, _, _ = client.get_equity()
                check_daily_circuit_breaker(total_eq)

                # 優化第2點：若為「反手風控不足僅平舊倉 (REVERSE_SKIP_CLOSE)」，推播直覺清楚的說明！
                if stage == "REVERSE_SKIP_CLOSE":
                    old_dir_s = "多單" if is_long_pos else "空單"
                    new_dir_s = "空單" if is_long_pos else "多單"
                    send_line_push(
                        f"⚠️ 【反手風控不足・已平舊倉】\n"
                        f"🪙 交易幣種：{fsym}\n"
                        f"✂️ 已平舊倉：{old_dir_s} {_fmt_qty(closed_q_f)} 顆\n"
                        f"🚫 未開新倉：新{new_dir_s}低於最低門檻\n"
                        f"💵 舊倉結算：{_fmt_u(net_u, signed=True)}\n"
                        f"💰 帳戶權益：{total_eq:.2f} USDT"
                    )
                else:
                    send_line_push(
                        f"🏁 【止損 / 趨勢平倉完成】\n"
                        f"🪙 交易幣種：{fsym} ({'多單' if is_long_pos else '空單'})\n"
                        f"📦 平倉顆數：{_fmt_qty(closed_q_f)} 顆\n"
                        f"🏷️ 開倉價格：{client.fmt_p(sym, entry_price_d)}\n"
                        f"📍 出場價格：{client.fmt_p(sym, exit_p_f)}\n"
                        f"💵 本筆損益：{_fmt_u(net_u, signed=True)}\n"
                        f"💰 帳戶權益：{total_eq:.2f} USDT"
                    )
                return {"success": True, "msg": "已平倉"}

            if notify_and_log_closed_position(sym, prev_cached, stage="STOP_EXIT"):
                return {"success": True, "msg": "幣安條件止損已成交，已完成LINE通知與戰績登錄"}

            client.cancel_tracked_sl_only(sym)
            delete_db_position_state(sym)
            return {"success": True, "msg": "已清理機器人追蹤止損單"}

        # ==========================================================
        # D. 順勢金字塔加碼 (ADD_LONG / ADD_SHORT)
        # ==========================================================
        if action in ("ADD_LONG", "ADD_SHORT"):
            allowed, _ = is_new_entry_allowed()
            if not bot_state["allow_pyramiding"] or not allowed:
                return {"success": True, "msg": "順勢加碼未開啟或風控鎖定，略過"}
            pos_amt_d, old_entry_d = client.get_position_info_dec(sym)
            is_long = (action == "ADD_LONG")
            pos_side = "LONG" if is_long else "SHORT"
            if (is_long and pos_amt_d <= Decimal("0")) or (not is_long and pos_amt_d >= Decimal("0")):
                return {"success": True, "msg": "無同向底倉可加碼"}
            curr_p_d = client.get_price_dec(sym)
            if not sig.stop_loss or curr_p_d <= Decimal("0"):
                return {"success": False, "msg": "缺少止損或現價"}
            sl_d = client.quantize_price(sym, sig.stop_loss)
            total_eq, avail, _ = client.get_equity()
            eq_d = _to_dec(max(total_eq, avail))
            avail_d = _to_dec(avail if avail > 0 else total_eq)
            ok_lev, lev = client.verify_and_set_leverage(sym, sig.leverage or 40)
            if not ok_lev:
                return {"success": False, "msg": "槓桿驗證失敗，略過加碼"}

            add_qty_d, sz_reason = calculate_order_qty_decimal(
                client, sym, curr_p_d, sl_d, eq_d, avail_d, _to_dec(sig.risk_pct or 0.75), lev, allow_min_notional_bump=False
            )
            cur_notional_d = abs(pos_amt_d) * curr_p_d
            rem_cap_d = max(Decimal("0"), (eq_d * _to_dec(bot_state["max_notional_mult"])) - cur_notional_d)
            add_qty_d = client.quantize_qty(sym, min(add_qty_d, rem_cap_d / curr_p_d))
            if add_qty_d <= Decimal("0") or (add_qty_d * curr_p_d) < client.get_min_notional_dec(sym):
                return {"success": True, "msg": f"加碼倉位不足或達上限({sz_reason})"}

            side, sl_side = ("BUY" if is_long else "SELL"), ("SELL" if is_long else "BUY")
            order_res = client.order(sym, side, add_qty_d, position_side=pos_side)
            ok_fill, exec_q, _, _ = client.verify_order_fill(sym, order_res, add_qty_d, is_entry=True, expected_side_dir=pos_side)
            if not ok_fill or exec_q <= Decimal("0"):
                return {"success": False, "msg": "加碼下單未成交"}
            new_amt_d, new_entry_d = client.get_position_info_dec(sym)
            cur_sl_d, _ = client.get_existing_sl_info(sym)
            be_buf = BE_BUFFER_PCT / Decimal("100")
            be_new_d = new_entry_d * (Decimal("1") + be_buf) if is_long else new_entry_d * (Decimal("1") - be_buf)
            base_sl_d = cur_sl_d if (cur_sl_d and cur_sl_d > Decimal("0")) else sl_d
            safe_sl_d = max(base_sl_d, sl_d, be_new_d) if is_long else min(base_sl_d, sl_d, be_new_d)
            sl_ok, new_aid, verified_sl_d = client.replace_sl_safely(sym, sl_side, safe_sl_d, position_side=pos_side)
            send_line_push(
                f"🚀 【順勢金字塔加碼成功】\n"
                f"🪙 交易幣種：{fsym} ({lev}x)\n"
                f"➕ 加碼顆數：{_fmt_qty(exec_q)} 顆\n"
                f"🪙 總持倉量：{_fmt_qty(abs(new_amt_d))} 顆\n"
                f"🏷️ 最新均價：{client.fmt_p(sym, new_entry_d)}\n"
                f"🛡️ 保護止損：{client.fmt_p(sym, verified_sl_d if sl_ok else safe_sl_d)}"
            )
            return {"success": True, "added_qty": float(exec_q)}

        # ==========================================================
        # E. 10 步進場與反手交易流程
        # ==========================================================
        if action in ("LONG", "SHORT"):
            is_long_act = (action == "LONG")
            pos_side = "LONG" if is_long_act else "SHORT"
            pos_amt_d, old_entry_d = client.get_position_info_dec(sym)
            curr_p_d = client.get_price_dec(sym)
            if curr_p_d <= Decimal("0") and sig.signal_price and sig.signal_price > 0:
                curr_p_d = client.quantize_price(sym, sig.signal_price)
            min_not_d = client.get_min_notional_dec(sym)

            if ((is_long_act and pos_amt_d > Decimal("0")) or (not is_long_act and pos_amt_d < Decimal("0"))) and (abs(pos_amt_d) * curr_p_d >= min_not_d):
                return {"success": True, "msg": "已有同向持倉"}

            has_reverse_pos = (is_long_act and pos_amt_d < Decimal("0")) or (not is_long_act and pos_amt_d > Decimal("0"))

            total_eq, avail, _ = client.get_equity()
            account_base = max(total_eq, avail)
            check_daily_circuit_breaker(account_base)
            allowed, gate_reason = is_new_entry_allowed()
            if not allowed:
                retain_msg = "🛡️ 原倉處理：保留原倉與原止損" if (has_reverse_pos and RETAIN_OLD_ON_NEW_REJECT) else "📭 目前無反向倉位"
                send_line_push(
                    f"⏸️ 【風控攔截・略過新進場】\n"
                    f"🪙 交易幣種：{fsym} ({action})\n"
                    f"🛡️ 攔截原因：{gate_reason}\n"
                    f"{retain_msg}"
                )
                return {"success": False, "msg": gate_reason, "retained_old_position": has_reverse_pos}

            if not sig.stop_loss:
                raise HTTPException(status_code=400, detail="缺少止損價")
            sl_d = client.quantize_price(sym, sig.stop_loss)
            if (is_long_act and curr_p_d <= sl_d) or (not is_long_act and curr_p_d >= sl_d):
                return {"success": False, "msg": "現價已越過止損價，拒絕進場", "retained_old_position": has_reverse_pos}

            if sig.signal_price and sig.signal_price > 0:
                sig_p_d = _to_dec(sig.signal_price)
                adv_slip = float(((curr_p_d - sig_p_d) / sig_p_d * Decimal("100")) if is_long_act else ((sig_p_d - curr_p_d) / sig_p_d * Decimal("100")))
                if adv_slip > bot_state["max_entry_slippage_pct"]:
                    send_line_push(
                        f"🛡️ 【滑點過大・放棄追價】\n"
                        f"🪙 交易幣種：{fsym} ({action})\n"
                        f"🏷️ 訊號價格：{client.fmt_p(sym, sig_p_d)}\n"
                        f"📍 當前現價：{client.fmt_p(sym, curr_p_d)} (+{adv_slip:.1f}%)\n"
                        f"💡 原倉狀態：{'已保留原倉' if has_reverse_pos else '空手觀望'}"
                    )
                    return {"success": False, "msg": "滑點過大已攔截", "retained_old_position": has_reverse_pos}

            if not bot_state["position_mode_verified"]:
                client.verify_account_position_mode()
            ok_lev, lev = client.verify_and_set_leverage(sym, sig.leverage or 40)
            if not ok_lev or lev <= 0:
                send_line_push(f"🚨 【{fsym} 槓桿設定驗證失敗】\n🛡️ 防護動作：拒絕開新倉")
                return {"success": False, "msg": "槓桿驗證失敗", "retained_old_position": has_reverse_pos}

            old_margin_u = (abs(pos_amt_d) * curr_p_d / Decimal(str(lev))) if has_reverse_pos else Decimal("0")
            est_avail_d = _to_dec(avail if avail > 0 else account_base) + old_margin_u
            eq_d = _to_dec(account_base)
            risk_pct_d = _to_dec(sig.risk_pct or 1.5, "1.5")

            target_qty_d, sz_reason = calculate_order_qty_decimal(
                client, sym, curr_p_d, sl_d, eq_d, est_avail_d, risk_pct_d, lev
            )
            if target_qty_d <= Decimal("0"):
                # 優化第2點：反手或新倉風控不足時，明確告知原倉保留狀態與未開新倉原因
                new_dir_s = "多單" if is_long_act else "空單"
                old_dir_s = "空單" if is_long_act else "多單"
                send_line_push(
                    f"⚠️ 【反手風控不足・未開新倉】\n"
                    f"🪙 交易幣種：{fsym}\n"
                    f"🚫 未開新倉：新{new_dir_s}保證金或風險不足\n"
                    f"🛡️ 原倉狀態：{'已保留原' + old_dir_s + '與止損' if has_reverse_pos else '維持空手觀望'}\n"
                    f"💵 可用保證：{avail:.2f} USDT"
                )
                return {"success": False, "msg": sz_reason, "retained_old_position": has_reverse_pos}

            reversed_note = ""
            if has_reverse_pos:
                old_is_long = pos_amt_d > Decimal("0")
                old_pos_side = "LONG" if old_is_long else "SHORT"
                close_side = "SELL" if old_is_long else "BUY"
                notified_close_ts[sym] = time.time()

                close_res = client.order(sym, close_side, abs(pos_amt_d), reduce_only=True, position_side=old_pos_side)
                ok_close, exec_close_q, avg_close_p, close_st = client.verify_order_fill(
                    sym, close_res, abs(pos_amt_d), is_entry=False, expected_side_dir=old_pos_side
                )
                is_zero = client.verify_position_zero(sym, retries=4)
                if not ok_close or not is_zero:
                    bot_state["safe_mode_locked"] = True
                    bot_state["safe_mode_reason"] = f"{_disp_sym(sym)}反手舊倉未歸零"
                    save_system_flags()
                    send_line_push(
                        f"🚨 【{fsym} 反手平舊倉未歸零】\n"
                        f"⚠️ 訂單狀態：{close_st}\n"
                        f"🔒 防護動作：已中止開新倉並進入安全模式"
                    )
                    return {"success": False, "msg": "舊倉未完全歸零，中止開新倉"}

                exact_p = client.get_order_net_pnl(sym, close_res)
                client.cancel_tracked_sl_only(sym)
                delete_db_position_state(sym)
                rev_pnl = record_trade_log(
                    sym, "REVERSE_CLOSE", float(pos_amt_d), float(exec_close_q), float(old_entry_d), float(avg_close_p or curr_p_d), exact_pnl=exact_p
                )
                reversed_note = f"🔄 反手平倉：{_sym_val(sym, _fmt_u(rev_pnl, signed=True))}\n"

            side, sl_side = ("BUY" if is_long_act else "SELL"), ("SELL" if is_long_act else "BUY")
            order_res = client.order(sym, side, target_qty_d, position_side=pos_side)
            ok_entry, actual_qty_d, actual_entry_d, entry_st = client.verify_order_fill(
                sym, order_res, target_qty_d, is_entry=True, expected_side_dir=pos_side
            )
            if not ok_entry or actual_qty_d <= Decimal("0"):
                send_line_push(f"❌ 【{fsym} 開倉未成交】\n⚠️ 狀態說明：{entry_st}")
                raise HTTPException(status_code=400, detail=f"開倉失敗: {order_res}")

            base_entry_d = actual_entry_d if actual_entry_d > Decimal("0") else curr_p_d

            sl_ok, new_aid, verified_sl_d = client.replace_sl_safely(sym, sl_side, sl_d, position_side=pos_side)
            if not sl_ok or not new_aid:
                notified_close_ts[sym] = time.time()
                client.order(sym, sl_side, actual_qty_d, reduce_only=True, position_side=pos_side)
                delete_db_position_state(sym)
                send_line_push(f"🚨 【{fsym} 止損掛單驗證失敗】\n🛡️ 保護動作：已緊急平倉保護")
                raise HTTPException(status_code=400, detail="止損設定失敗已平倉")

            last_entry_ts[sym] = time.time()
            entry_oid = str(order_res.get("orderId", "")) if isinstance(order_res, dict) else ""
            upsert_db_position_state(
                sym=sym,
                side=pos_side,
                qty=actual_qty_d,
                entry_price=base_entry_d,
                init_sl=verified_sl_d,
                current_sl=verified_sl_d,
                sl_algo_id=new_aid,
                stage="INIT",
                entry_order_id=entry_oid,
            )
            peak_upnl_tracker[sym] = 0.0

            ref_p_d = _to_dec(sig.signal_price) if (sig.signal_price and sig.signal_price > 0) else curr_p_d
            slip_pct = float(abs(base_entry_d - ref_p_d) / ref_p_d * Decimal("100")) if ref_p_d > Decimal("0") else 0.0
            slip_note = f"⚠️ 成交滑點：{slip_pct:.2f}%\n" if slip_pct >= float(SLIPPAGE_WARN_PCT) else ""
            fmt_entry_str = client.fmt_p(sym, base_entry_d)
            fmt_sl_str = client.fmt_p(sym, verified_sl_d)
            tp_mode_str = "1.5R/3R二階止盈" if sig.use_tp2 else "1.5R減半+海龜線"
            notional_u = float(actual_qty_d * base_entry_d)
            used_margin = notional_u / max(lev, 1)
            margin_share_str = _fmt_margin_share(used_margin, account_base)

            send_line_push(
                f"{'🟢 【做多 LONG 開倉成功】' if is_long_act else '🔴 【做空 SHORT 開倉成功】'}\n"
                f"{reversed_note}{slip_note}"
                f"🪙 交易幣種：{fsym} ({lev}x)\n"
                f"📦 持倉價值：{notional_u:.2f} USDT\n"
                f"🪙 持倉顆數：{_fmt_qty(actual_qty_d)} 顆\n"
                f"🔒 倉位保證：{_fmt_u(used_margin)} (佔{margin_share_str})\n"
                f"🏷️ 開倉價格：{fmt_entry_str}\n"
                f"🛡️ 止損價格：{fmt_sl_str}\n"
                f"🎯 止盈模式：{tp_mode_str}\n"
                f"💰 帳戶權益：{account_base:.2f} USDT"
            )
            return {
                "success": True,
                "qty": float(actual_qty_d),
                "entry_price": float(base_entry_d),
                "stop_loss": float(verified_sl_d),
                "sl_algo_id": new_aid,
            }

        return {"success": False, "msg": f"未知動作: {action}"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))

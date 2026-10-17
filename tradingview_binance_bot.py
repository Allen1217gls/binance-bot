import os
import re
import time
import math
import hmac
import base64
import hashlib
import logging
import secrets
import asyncio
import threading
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

# LINE Messaging API 設定 (於 Zeabur 環境變數設定)
LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "").strip()
LINE_CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET", "").strip()
LINE_USER_ID = os.getenv("LINE_USER_ID", "").strip()
DEFAULT_SYMBOL = os.getenv("DEFAULT_SYMBOL", "SOLUSDT").strip().upper()

# 台灣時區 (UTC+8)
TW_TZ = timezone(timedelta(hours=8))

# 全域非同步鎖：防止毫秒級連續 Webhook 併發撞車
webhook_lock = asyncio.Lock()

# 機器人執行狀態控制 (可由 LINE 指令動態切換)
bot_state = {
    "trading_enabled": True,          # 是否允許 TradingView 新開倉訊號
    "hourly_push_enabled": False,     # 預設關閉每小時自動推播，節省 LINE 每月 200 則免費推播額度
    "bound_user_id": LINE_USER_ID,    # 自動綁定或環境變數設定的 LINE User ID
    "last_symbol": DEFAULT_SYMBOL,    # 最近一次操作的交易對
}


# ==========================================
# LINE 訊息推播與回覆工具函數
# ==========================================
def send_line_push(text: str, to_user_id: Optional[str] = None):
    """主動推播訊息到你的手機 LINE (不阻塞主交易執行緒)"""
    token = LINE_CHANNEL_ACCESS_TOKEN
    target_uid = (to_user_id or bot_state.get("bound_user_id") or LINE_USER_ID).strip()
    if not token or not target_uid:
        logger.info(f"ℹ️ 尚未設定 LINE_CHANNEL_ACCESS_TOKEN 或 LINE_USER_ID，略過 LINE 推播:\n{text}")
        return

    def _push():
        try:
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {token}"
            }
            payload = {
                "to": target_uid,
                "messages": [{"type": "text", "text": text[:4900]}]
            }
            res = requests.post("https://api.line.me/v2/bot/message/push", headers=headers, json=payload, timeout=8)
            if res.status_code != 200:
                logger.warning(f"LINE 推播失敗 ({res.status_code}): {res.text}")
        except Exception as e:
            logger.error(f"LINE 推播異常: {e}")

    threading.Thread(target=_push, daemon=True).start()


def send_line_reply(reply_token: str, text: str):
    """即時回覆你在 LINE 聊天室發送的指令 (100% 免費無上限、不扣每月 200 則推播額度)"""
    token = LINE_CHANNEL_ACCESS_TOKEN
    if not token or not reply_token:
        return
    try:
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}"
        }
        payload = {
            "replyToken": reply_token,
            "messages": [{"type": "text", "text": text[:4900]}]
        }
        res = requests.post("https://api.line.me/v2/bot/message/reply", headers=headers, json=payload, timeout=8)
        if res.status_code != 200:
            logger.warning(f"LINE 回覆失敗 ({res.status_code}): {res.text}")
    except Exception as e:
        logger.error(f"LINE 回覆異常: {e}")


class BinanceClient:
    def __init__(self, key: str, sec: str):
        self.key, self.sec = key, sec
        self.time_offset = 0
        self._precisions: Dict[str, Tuple[float, int, int, float]] = {}
        self._leverage_cache: Dict[str, int] = {}
        self._current_sl: Dict[str, float] = {}
        try:
            self._sync()
            self._load_exchange_info()
        except Exception as e:
            logger.warning(f"初始化連線提示: {e}")

    def _sync(self):
        try:
            res = requests.get(f"{FUTURES_URL}/fapi/v1/time", timeout=5).json()
            server_t = int(res.get("serverTime", time.time() * 1000))
            local_t = int(time.time() * 1000)
            self.time_offset = server_t - local_t
            logger.info(f"伺服器校時完成，偏移量: {self.time_offset}ms")
        except Exception:
            self.time_offset = 0

    def _load_exchange_info(self):
        try:
            res = requests.get(f"{FUTURES_URL}/fapi/v1/exchangeInfo", timeout=10).json()
            for s in res.get("symbols", []):
                sym = s.get("symbol")
                price_prec = int(s.get("pricePrecision", 2))
                step, dec, min_notional = 0.001, 3, 5.0
                for f in s.get("filters", []):
                    ftype = f.get("filterType")
                    if ftype == "LOT_SIZE":
                        step = float(f.get("stepSize", "0.001"))
                        dec = max(0, -int(round(math.log10(step)))) if step < 1 else 0
                    elif ftype == "PRICE_FILTER":
                        tick = float(f.get("tickSize", "0.01"))
                        if tick < 1:
                            price_prec = max(0, -int(round(math.log10(tick))))
                    elif ftype in ("MIN_NOTIONAL", "NOTIONAL"):
                        min_notional = float(f.get("notional", f.get("minNotional", "5.0")))
                self._precisions[sym] = (step, dec, price_prec, min_notional)
            logger.info("已成功載入幣安交易對數量、價格精度與最小名義價值資訊。")
        except Exception as e:
            logger.error(f"載入 exchangeInfo 失敗: {e}")

    def _sign(self, p: Dict[str, Any]) -> str:
        return hmac.new(self.sec.encode("utf-8"), urlencode(p).encode("utf-8"), hashlib.sha256).hexdigest()

    def _headers(self):
        return {"X-MBX-APIKEY": self.key}

    def _safe_request(self, method: str, endpoint: str, params: dict = None, data: dict = None, retries: int = 3) -> Any:
        url = f"{FUTURES_URL}{endpoint}"
        for attempt in range(retries):
            try:
                headers = self._headers()
                if method == "GET":
                    res = requests.get(url, headers=headers, params=params, timeout=10)
                elif method == "DELETE":
                    res = requests.delete(url, headers=headers, params=params, timeout=10)
                else:
                    headers["Content-Type"] = "application/x-www-form-urlencoded"
                    res = requests.post(url, headers=headers, data=data, timeout=10)

                data_json = res.json()
                if isinstance(data_json, dict) and data_json.get("code") == -1003:
                    time.sleep(2 * (attempt + 1))
                    continue
                if isinstance(data_json, dict) and "code" in data_json and data_json.get("code") != 200:
                    logger.error(f"幣安 API 回傳錯誤 ({endpoint}): {data_json}")
                return data_json
            except Exception as e:
                logger.error(f"API 請求異常: {e}")
                if attempt == retries - 1:
                    return None
                time.sleep(2)
        return None

    def get_equity(self) -> Tuple[float, float, Any]:
        self._sync()
        p = {"timestamp": int(time.time() * 1000) + self.time_offset, "recvWindow": 60000}
        p["signature"] = self._sign(p)

        res = self._safe_request("GET", "/fapi/v2/balance", params=p)
        if isinstance(res, list):
            for b in res:
                if b.get("asset") == "USDT":
                    eq = float(b.get("crossWalletBalance", b.get("balance", 0.0)))
                    avail = float(b.get("availableBalance", 0.0))
                    total_eq = max(eq, avail)
                    return (total_eq, avail, res)

        r2 = self._safe_request("GET", "/fapi/v2/account", params=p)
        if isinstance(r2, dict) and "code" not in r2:
            eq = float(r2.get("totalMarginBalance", r2.get("totalWalletBalance", 0.0)))
            avail = float(r2.get("availableBalance", 0.0))
            if eq <= 0 and "assets" in r2:
                for a in r2["assets"]:
                    if a.get("asset") == "USDT":
                        eq = float(a.get("marginBalance", a.get("walletBalance", 0.0)))
                        avail = float(a.get("availableBalance", 0.0))
                        break
            return (max(eq, avail), avail, r2)

        return (0.0, 0.0, res if res else r2)

    def get_price(self, sym: str) -> float:
        try:
            res = requests.get(f"{FUTURES_URL}/fapi/v1/ticker/price", params={"symbol": sym.upper()}, timeout=5).json()
            return float(res.get("price", 0.0))
        except Exception:
            return 0.0

    def get_min_notional(self, sym: str) -> float:
        sym = sym.upper()
        if sym not in self._precisions:
            self._load_exchange_info()
        _, _, _, min_notional = self._precisions.get(sym, (0.001, 3, 2, 5.0))
        return min_notional

    def format_qty(self, sym: str, raw_qty: float, round_up: bool = False) -> float:
        sym = sym.upper()
        if sym not in self._precisions:
            self._load_exchange_info()
        step, dec, _, _ = self._precisions.get(sym, (0.001, 3, 2, 5.0))
        steps_count = round(raw_qty / step, 8)
        if round_up:
            return round(math.ceil(steps_count) * step, dec)
        return round(math.floor(steps_count) * step, dec)

    def format_price(self, sym: str, raw_price: float) -> float:
        sym = sym.upper()
        if sym not in self._precisions:
            self._load_exchange_info()
        _, _, price_prec, _ = self._precisions.get(sym, (0.001, 3, 2, 5.0))
        return round(raw_price, price_prec)

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
        formatted_qty = self.format_qty(sym, qty)
        if formatted_qty <= 0:
            logger.warning(f"下單顆數為 0 ({sym})，略過市價單執行。")
            return None
        p = {
            "symbol": sym.upper(),
            "side": side.upper(),
            "type": "MARKET",
            "quantity": formatted_qty,
            "timestamp": int(time.time() * 1000) + self.time_offset,
            "recvWindow": 60000
        }
        if reduce_only:
            p["reduceOnly"] = "true"
        p["signature"] = self._sign(p)
        res = self._safe_request("POST", "/fapi/v1/order", data=p)
        logger.info(f"市價單回應: {res}")
        return res

    def set_sl(self, sym: str, side: str, price: float) -> bool:
        formatted_p = self.format_price(sym, price)
        p = {
            "algoType": "CONDITIONAL",
            "symbol": sym.upper(),
            "side": side.upper(),
            "type": "STOP_MARKET",
            "triggerPrice": formatted_p,
            "closePosition": "true",
            "workingType": "MARK_PRICE",
            "timestamp": int(time.time() * 1000) + self.time_offset,
            "recvWindow": 60000
        }
        p["signature"] = self._sign(p)
        res = self._safe_request("POST", "/fapi/v1/algoOrder", data=p)
        if isinstance(res, dict) and "algoId" in res:
            self._current_sl[sym.upper()] = formatted_p
            logger.info(f"✅ Algo 標記價格止損單掛單成功！觸發價: {formatted_p}, ID: {res.get('algoId')}")
            return True
        logger.error(f"❌ 幣安拒絕 Algo 止損單: {res}")
        return False

    def cancel_all(self, sym: str):
        """取消該交易對所有一般掛單與條件止損單"""
        sym = sym.upper()
        p1 = {"symbol": sym, "timestamp": int(time.time() * 1000) + self.time_offset, "recvWindow": 60000}
        p1["signature"] = self._sign(p1)
        r1 = self._safe_request("DELETE", "/fapi/v1/allOpenOrders", params=p1, retries=1)

        p2 = {"symbol": sym, "timestamp": int(time.time() * 1000) + self.time_offset, "recvWindow": 60000}
        p2["signature"] = self._sign(p2)
        r2 = self._safe_request("DELETE", "/fapi/v1/algoOpenOrders", params=p2, retries=1)
        self._current_sl.pop(sym, None)
        logger.info(f"撤單結果: 一般訂單={r1}, 條件單={r2}")

    def get_position_info(self, sym: str) -> Tuple[float, float]:
        p = {"symbol": sym.upper(), "timestamp": int(time.time() * 1000) + self.time_offset, "recvWindow": 60000}
        p["signature"] = self._sign(p)
        res = self._safe_request("GET", "/fapi/v2/positionRisk", params=p)
        if isinstance(res, list):
            for pos in res:
                if pos.get("symbol") == sym.upper():
                    amt = float(pos.get("positionAmt", 0.0))
                    entry_p = float(pos.get("entryPrice", 0.0))
                    return (amt, entry_p)
        return (0.0, 0.0)

    def get_all_active_positions(self) -> List[Dict[str, Any]]:
        """取得帳戶內所有非零持倉明細 (包含未實現損益、均價、標記價格)"""
        p = {"timestamp": int(time.time() * 1000) + self.time_offset, "recvWindow": 60000}
        p["signature"] = self._sign(p)
        res = self._safe_request("GET", "/fapi/v2/positionRisk", params=p)
        active = []
        if isinstance(res, list):
            for pos in res:
                amt = float(pos.get("positionAmt", 0.0))
                if abs(amt) > 0:
                    entry_p = float(pos.get("entryPrice", 0.0))
                    mark_p = float(pos.get("markPrice", 0.0))
                    upnl = float(pos.get("unRealizedProfit", 0.0))
                    notional = abs(amt) * (mark_p if mark_p > 0 else entry_p)
                    if notional >= 1.0:
                        active.append({
                            "symbol": pos.get("symbol", ""),
                            "amt": amt,
                            "entry_price": entry_p,
                            "mark_price": mark_p,
                            "upnl": upnl,
                            "notional": notional,
                        })
        return active


client = BinanceClient(BINANCE_API_KEY, BINANCE_API_SECRET)

yesterday_balance = 0.0
last_report_date = None
last_hourly_key = None


def build_status_report(title: str = "📊 【帳戶與持倉即時狀態】") -> str:
    """組裝完整帳戶餘額與持倉報告文字"""
    now_str = datetime.now(TW_TZ).strftime("%Y-%m-%d %H:%M:%S")
    total_eq, avail, _ = client.get_equity()
    positions = client.get_all_active_positions()
    mode_str = "🟢 正常自動接單中" if bot_state["trading_enabled"] else "⏸️ 已暫停新開倉 (僅保留平倉保護)"
    hourly_str = "開啟" if bot_state["hourly_push_enabled"] else "關閉 (輸入 1 可無限次免費查詢)"

    lines = [
        f"{title}",
        f"🕒 時間：{now_str}",
        f"🤖 狀態：{mode_str}",
        f"⏰ 整點推播：{hourly_str}",
        f"💰 總權益：{total_eq:.2f} USDT",
        f"💵 可用保證金：{avail:.2f} USDT",
    ]

    if not positions:
        sym = bot_state.get("last_symbol", DEFAULT_SYMBOL)
        curr_p = client.get_price(sym)
        lines.append(f"📭 目前持倉：空手觀望中 ({sym} 現價 {curr_p})")
    else:
        lines.append(f"📈 目前持倉 ({len(positions)} 筆)：")
        for pos in positions:
            sym = pos["symbol"]
            amt = pos["amt"]
            direction = "🟢 做多 LONG" if amt > 0 else "🔴 做空 SHORT"
            upnl = pos["upnl"]
            pnl_icon = "🟢" if upnl >= 0 else "🔴"
            sl_val = client._current_sl.get(sym, "依幣安條件單")
            lines.append(
                f"・{sym} {direction}\n"
                f"  顆數：{abs(amt)} | 價值：{pos['notional']:.1f}U\n"
                f"  成本：{pos['entry_price']} | 現價：{pos['mark_price']}\n"
                f"  浮動損益：{pnl_icon} {upnl:+.2f} USDT\n"
                f"  當前止損：{sl_val}"
            )
    return "\n".join(lines)


def background_scheduler():
    """背景排程：負責「每日 08:00 資產損益日報」與「選配之每小時整點持倉推播」"""
    global yesterday_balance, last_report_date, last_hourly_key
    try:
        init_eq, _, _ = client.get_equity()
        if init_eq > 0:
            yesterday_balance = init_eq
            logger.info(f"📊 初始資產基準已建立: {yesterday_balance:.2f} USDT")
    except Exception:
        pass

    while True:
        try:
            now_tw = datetime.now(TW_TZ)
            today_date = now_tw.date()
            hour_key = (today_date, now_tw.hour)

            # 1. 每天早上 08:00 每日資產餘額與損益日報
            if now_tw.hour == 8 and now_tw.minute < 5 and last_report_date != today_date:
                total_eq, avail, _ = client.get_equity()
                if yesterday_balance > 0:
                    diff = total_eq - yesterday_balance
                    pct = (diff / yesterday_balance) * 100
                    trend = "📈 獲利增加" if diff >= 0 else "📉 虧損減少"
                    daily_msg = (
                        f"🌅 【每日 08:00 帳戶餘額損益日報】\n"
                        f"🕒 日期：{today_date}\n"
                        f"💰 今日總權益：{total_eq:.2f} USDT\n"
                        f"💵 可用保證金：{avail:.2f} USDT\n"
                        f"📊 昨日基準：{yesterday_balance:.2f} USDT\n"
                        f"🏆 今日損益：{trend} {abs(diff):.2f} USDT ({pct:+.2f}%)"
                    )
                else:
                    daily_msg = (
                        f"🌅 【每日 08:00 帳戶餘額損益日報】\n"
                        f"🕒 日期：{today_date}\n"
                        f"💰 今日總權益：{total_eq:.2f} USDT (已建立今日基準)"
                    )

                logger.info(daily_msg.replace("\n", " | "))
                send_line_push(daily_msg)
                yesterday_balance = total_eq
                last_report_date = today_date
                last_hourly_key = hour_key

            # 2. 每隔一小時自動推播 (預設關閉以省每月 200 則免費額度，可由 LINE 輸入 8 開關)
            elif bot_state["hourly_push_enabled"] and now_tw.minute < 5 and last_hourly_key != hour_key:
                hourly_msg = build_status_report(title=f"⏰ 【每小時整點巡檢報告 ({now_tw.hour:02d}:00)】")
                logger.info(f"已執行 {now_tw.hour:02d}:00 每小時整點推播。")
                send_line_push(hourly_msg)
                last_hourly_key = hour_key

            time.sleep(25)
        except Exception as e:
            logger.error(f"背景排程任務異常: {e}")
            time.sleep(60)


@asynccontextmanager
async def lifespan(app: FastAPI):
    threading.Thread(target=background_scheduler, daemon=True).start()
    logger.info("已啟動背景排程服務 (每日 08:00 損益日報 + LINE 雙向遙控)。")
    if LINE_CHANNEL_ACCESS_TOKEN and (LINE_USER_ID or bot_state.get("bound_user_id")):
        send_line_push("🤖 幣安合約機器人 (v3.1 零衝突同步版) 已啟動上線！\n輸入「0」或「說明」可查看所有手機遙控指令。")
    yield


app = FastAPI(lifespan=lifespan)


class Signal(BaseModel):
    passphrase: str
    symbol: str
    action: str
    risk_pct: Optional[float] = 1.5
    stop_loss: Optional[float] = None
    leverage: Optional[int] = 10


@app.get("/")
def index():
    return {
        "status": "online",
        "feature": "Zeabur Tokyo Hull-Turtle Bot (v3.1 Synced TP1 + Two-Way LINE Control)",
        "trading_enabled": bot_state["trading_enabled"],
        "hourly_push_enabled": bot_state["hourly_push_enabled"],
        "line_configured": bool(LINE_CHANNEL_ACCESS_TOKEN),
        "line_user_bound": bool(bot_state.get("bound_user_id")),
    }


@app.get("/test-binance")
def test_binance():
    total_eq, avail, raw_debug = client.get_equity()
    price = client.get_price(DEFAULT_SYMBOL)
    return {
        "status": "success",
        "msg": "幣安連線診斷完成",
        "total_equity_usdt": total_eq,
        "available_balance_usdt": avail,
        "symbol_price": price,
        "api_key_check": f"{client.key[:6]}...{client.key[-4:]}" if client.key else "未設定API_KEY",
        "binance_raw_response": raw_debug
    }


# ==========================================
# LINE Bot 雙向指令處理中心 (/line-webhook)
# ==========================================
def execute_line_command(cmd_text: str, user_id: str) -> str:
    """解析你在 LINE 輸入的文字指令並執行對應幣安操作"""
    text = cmd_text.strip()
    upper_text = text.upper()

    # 若環境變數未綁定 LINE_USER_ID，第一次傳訊息時自動綁定你的帳號
    if not bot_state.get("bound_user_id") and user_id:
        bot_state["bound_user_id"] = user_id
        logger.info(f"✅ 已自動綁定 LINE User ID: {user_id}")

    # 安全白名單檢查：若已在 Zeabur 設定 LINE_USER_ID，則只允許你本人操控
    if LINE_USER_ID and user_id != LINE_USER_ID:
        return "⛔ 權限不足：此機器人僅限綁定之本人帳號操控。"

    # 1. 查詢狀態 (輸入 1 / 查詢 / 狀態 / 倉位 / 餘額) — 100% 免費回覆不扣額度！
    if text in ("1", "查詢", "狀態", "倉位", "餘額", "查帳"):
        return build_status_report(title="📊 【即時帳戶與持倉查詢】")

    # 2. 一鍵全平倉 (輸入 2 / 平倉 / 全平 / 一鍵平倉 或 平倉 SOLUSDT)
    if text in ("2", "平倉", "全平", "一鍵平倉", "緊急平倉") or upper_text.startswith("平倉 "):
        parts = upper_text.split()
        target_sym = parts[1] if len(parts) > 1 else None
        positions = client.get_all_active_positions()

        if target_sym:
            positions = [p for p in positions if p["symbol"] == target_sym]

        if not positions:
            sym_to_clean = target_sym or bot_state.get("last_symbol", DEFAULT_SYMBOL)
            client.cancel_all(sym_to_clean)
            return f"📭 目前沒有任何開倉部位（已同步清理 {sym_to_clean} 殘留掛單）。"

        closed_summary = []
        for pos in positions:
            sym = pos["symbol"]
            amt = pos["amt"]
            side = "SELL" if amt > 0 else "BUY"
            client.cancel_all(sym)
            client.order(sym, side, abs(amt), reduce_only=True)
            closed_summary.append(f"・{sym}：已市價全平 {abs(amt)} 顆 (原浮動損益 {pos['upnl']:+.2f}U)")

        total_eq, _, _ = client.get_equity()
        return (
            f"🚨 【LINE 一鍵全平倉執行完畢】\n"
            + "\n".join(closed_summary)
            + f"\n💰 平倉後總權益：{total_eq:.2f} USDT"
        )

    # 3. 手動減倉 50% (輸入 3 / 減半 / 平半)
    if text in ("3", "減半", "平半", "減倉"):
        positions = client.get_all_active_positions()
        if not positions:
            return "📭 目前沒有任何持倉可供減半。"
        results = []
        for pos in positions:
            sym = pos["symbol"]
            amt = pos["amt"]
            side = "SELL" if amt > 0 else "BUY"
            min_notional = client.get_min_notional(sym)
            half_qty = client.format_qty(sym, abs(amt) * 0.5)
            rem_qty = client.format_qty(sym, abs(amt) - half_qty)
            if half_qty <= 0 or (rem_qty * pos["mark_price"] < min_notional):
                results.append(f"・{sym}：持倉價值過小無法拆半 (剩餘將低於 {min_notional}U)")
            else:
                client.order(sym, side, half_qty, reduce_only=True)
                results.append(f"・{sym}：已市價減倉 50% ({half_qty} 顆)，剩餘 {rem_qty} 顆繼續持有")
        return "✂️ 【LINE 手動減倉 50% 結果】\n" + "\n".join(results)

    # 4. 手動一鍵移至成本價保本 (輸入 4 / 保本 / 移保本)
    if text in ("4", "保本", "移保本"):
        positions = client.get_all_active_positions()
        if not positions:
            return "📭 目前沒有任何持倉可設定保本。"
        results = []
        for pos in positions:
            sym = pos["symbol"]
            amt = pos["amt"]
            entry_p = pos["entry_price"]
            mark_p = pos["mark_price"]
            sl_side = "SELL" if amt > 0 else "BUY"
            if (amt > 0 and mark_p <= entry_p) or (amt < 0 and mark_p >= entry_p):
                results.append(f"・{sym}：目前現價 ({mark_p}) 尚未脫離成本價 ({entry_p})，無法移至保本（若要出場請輸入「2」平倉）")
            else:
                client.cancel_all(sym)
                ok = client.set_sl(sym, sl_side, entry_p)
                if ok:
                    results.append(f"・{sym}：✅ 止損已成功移至成本價 {client.format_price(sym, entry_p)} 保本！")
                else:
                    results.append(f"・{sym}：❌ 保本止損掛單失敗，請檢查現價是否過近")
        return "🛡️ 【LINE 手動移保本結果】\n" + "\n".join(results)

    # 5. 手動修改止損價格 (輸入：止損 145.5 或 SL 145.5)
    m_sl = re.match(r"^(?:5|止損|改止損|SL)\s+([0-9.]+)$", upper_text)
    if m_sl:
        new_sl = float(m_sl.group(1))
        positions = client.get_all_active_positions()
        if not positions:
            return "📭 目前無持倉，無法設定止損。"
        pos = positions[0]
        sym = pos["symbol"]
        amt = pos["amt"]
        sl_side = "SELL" if amt > 0 else "BUY"
        client.cancel_all(sym)
        ok = client.set_sl(sym, sl_side, new_sl)
        if ok:
            return f"✅ 已手動將 {sym} 標記價止損單更新為：{client.format_price(sym, new_sl)}"
        return f"❌ 更新 {sym} 止損至 {new_sl} 失敗（請確認價格方向是否合法）。"

    # 6. 暫停接收新開倉訊號 (輸入 6 / 暫停 / 停止)
    if text in ("6", "暫停", "停止", "暫停開倉"):
        bot_state["trading_enabled"] = False
        return "⏸️ 【已暫停自動開新倉】\n機器人現在會略過 TradingView 的新進場訊號（但帳上既有的止損、TP1 與平倉保護仍照常運作）。\n若要恢復自動接單，請輸入「7」或「恢復」。"

    # 7. 恢復接收新開倉訊號 (輸入 7 / 恢復 / 啟動)
    if text in ("7", "恢復", "啟動", "開始", "恢復開倉"):
        bot_state["trading_enabled"] = True
        return "🟢 【已恢復自動交易】\n機器人已重新開始接收 TradingView 所有進出場訊號！"

    # 8. 切換每小時整點自動推播開關 (輸入 8 / 整點推播)
    if text in ("8", "整點推播", "每小時推播"):
        bot_state["hourly_push_enabled"] = not bot_state["hourly_push_enabled"]
        status = "🟢 已開啟" if bot_state["hourly_push_enabled"] else "⏸️ 已關閉 (省免費推播額度)"
        return f"⏰ 【每小時整點自動推播】：{status}\n(提示：平時直接輸入「1」查詢完全免費且不扣每月 200 則推播額度！)"

    # 查詢我的 LINE User ID (輸入 ID / 綁定)
    if upper_text in ("ID", "USERID", "綁定"):
        return f"🔑 你的專屬 LINE_USER_ID 為：\n{user_id}\n\n(機器人已自動暫存綁定；你也可以將此字串貼到 Zeabur 環境變數 LINE_USER_ID 永久鎖定權限)"

    # 0. 顯示完整指令選單 (輸入 0 / 說明 / 指令 / 選單 / ? 或其他文字)
    return (
        "🤖 【幣安機器人 LINE 遙控指令選單】\n"
        "直接輸入數字或中文即可執行 (回覆完全免費)：\n\n"
        "👉 輸入「1」或「查詢」\n"
        "   查看即時總權益、可用餘額與持倉損益\n\n"
        "👉 輸入「2」或「平倉」\n"
        "   🚨 一鍵市價全平所有持倉並撤單\n\n"
        "👉 輸入「3」或「減半」\n"
        "   手動將目前持倉市價減倉 50% 落袋\n\n"
        "👉 輸入「4」或「保本」\n"
        "   手動將目前持倉止損移至開倉成本價\n\n"
        "👉 輸入「止損 價格」(如：止損 145.2)\n"
        "   手動修改當前持倉的止損觸發價\n\n"
        "👉 輸入「6」或「暫停」\n"
        "   暫停接收 TradingView 新開倉訊號\n\n"
        "👉 輸入「7」或「恢復」\n"
        "   恢復接收 TradingView 自動開倉\n\n"
        "👉 輸入「8」或「整點推播」\n"
        "   開關每小時整點自動推播 (預設關閉省額度)\n\n"
        f"🔑 你的 LINE ID：{user_id}"
    )


@app.post("/line-webhook")
async def line_webhook(request: Request):
    """接收來自 LINE 官方帳號的訊息 Webhook"""
    raw_body = await request.body()

    if LINE_CHANNEL_SECRET:
        signature = request.headers.get("X-Line-Signature", "")
        hash_bytes = hmac.new(LINE_CHANNEL_SECRET.encode("utf-8"), raw_body, hashlib.sha256).digest()
        expected_sig = base64.b64encode(hash_bytes).decode("utf-8")
        if not secrets.compare_digest(signature, expected_sig):
            raise HTTPException(status_code=400, detail="Invalid LINE signature")

    try:
        body_json = await request.json()
    except Exception:
        return {"status": "ok"}

    events = body_json.get("events", [])
    for ev in events:
        if ev.get("type") == "message" and ev.get("message", {}).get("type") == "text":
            reply_token = ev.get("replyToken", "")
            user_text = ev.get("message", {}).get("text", "")
            user_id = ev.get("source", {}).get("userId", "")
            async with webhook_lock:
                reply_msg = execute_line_command(user_text, user_id)
            send_line_reply(reply_token, reply_msg)

    return {"status": "ok"}


# ==========================================
# TradingView 訊號處理中心 (/webhook)
# ==========================================
@app.post("/webhook")
async def handle_webhook(request: Request):
    try:
        data = await request.json()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {e}")

    try:
        sig = Signal(**data)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Validation Error: {e}")

    if not secrets.compare_digest(sig.passphrase, WEBHOOK_PASSPHRASE):
        raise HTTPException(status_code=401, detail="密碼錯誤")

    sym, action = sig.symbol.upper(), sig.action.upper()
    bot_state["last_symbol"] = sym
    logger.info(f"收到指令: {action} {sym} (附帶止損: {sig.stop_loss})")

    async with webhook_lock:
        # ==========================================
        # A. TP1 保本與 50% 減倉處理 (與 TradingView 狀態機 100% 同步執行，零雙頭馬車衝突)
        # ==========================================
        if action in ("TP1_LONG", "TP1_SHORT"):
            pos_amt, entry_price = client.get_position_info(sym)
            if abs(pos_amt) <= 0:
                return {"success": True, "msg": "目前無持倉，略過TP1"}

            if ("LONG" in action and pos_amt < 0) or ("SHORT" in action and pos_amt > 0):
                return {"success": True, "msg": "持倉方向與TP1不符，略過"}

            side = "SELL" if "LONG" in action else "BUY"
            curr_p = client.get_price(sym)
            min_notional = client.get_min_notional(sym)
            close_qty = client.format_qty(sym, abs(pos_amt) * 0.5)
            rem_qty = client.format_qty(sym, abs(pos_amt) - close_qty)

            if 0 < close_qty < abs(pos_amt) and (rem_qty * curr_p >= min_notional):
                client.order(sym, side, close_qty, reduce_only=True)
                tp_note = f"已減倉 50% ({close_qty} 顆)，尾倉保留 {rem_qty} 顆"
            else:
                tp_note = f"尾倉拆半將低於 {min_notional}U，保留 100% 倉位 ({abs(pos_amt)} 顆) 直接移保本"

            target_sl = entry_price
            if entry_price > 0:
                if sig.stop_loss and sig.stop_loss > 0:
                    if "LONG" in action:
                        target_sl = max(entry_price, sig.stop_loss)
                    else:
                        target_sl = min(entry_price, sig.stop_loss)

                client.cancel_all(sym)
                sl_ok = client.set_sl(sym, side, target_sl)
                if not sl_ok:
                    rem_amt, _ = client.get_position_info(sym)
                    if abs(rem_amt) > 0:
                        client.order(sym, side, abs(rem_amt), reduce_only=True)
                    send_line_push(
                        f"🛡️ 【TP1 觸發回落保護全平】\n"
                        f"幣種：{sym}\n"
                        f"原因：設定保本止損 ({target_sl}) 時現價已穿過成本價，已自動市價全平尾倉保護利潤！"
                    )
                    return {"success": True, "msg": "TP1 觸發時價格已回落，已市價全平保護尾倉"}

            send_line_push(
                f"🎯 【1.5R TP1 減半止盈 & 保本成功】\n"
                f"幣種：{sym}\n"
                f"執行：{tp_note}\n"
                f"成本均價：{entry_price}\n"
                f"新保本止損價：{client.format_price(sym, target_sl)}\n"
                f"狀態：剩餘尾倉已零風險跟著海龜線奔跑！"
            )
            return {"success": True, "msg": "TP1已達成：減倉並移至保本止損價"}

        # ==========================================
        # B. 海龜移動止損即時同步更新 (UPDATE_SL)
        # ==========================================
        if action == "UPDATE_SL":
            if not sig.stop_loss:
                return {"success": False, "msg": "UPDATE_SL 缺少 stop_loss 參數"}
            pos_amt, entry_price = client.get_position_info(sym)
            if abs(pos_amt) <= 0:
                return {"success": True, "msg": "目前無持倉，略過移動止損更新"}

            sl_side = "SELL" if pos_amt > 0 else "BUY"
            client.cancel_all(sym)
            sl_ok = client.set_sl(sym, sl_side, sig.stop_loss)
            if not sl_ok:
                rem_amt, _ = client.get_position_info(sym)
                if abs(rem_amt) > 0:
                    client.order(sym, sl_side, abs(rem_amt), reduce_only=True)
                send_line_push(
                    f"🛡️ 【海龜移動止損觸發市價出場】\n"
                    f"幣種：{sym}\n"
                    f"新止損價：{sig.stop_loss} (現價已穿過止損線，已立即市價平倉出場！)"
                )
                return {"success": True, "msg": "現價已穿過新移動止損，已市價出場"}

            formatted_sl = client.format_price(sym, sig.stop_loss)
            send_line_push(
                f"🐢 【海龜移動止損上推更新】\n"
                f"幣種：{sym} ({'做多' if pos_amt > 0 else '做空'})\n"
                f"當前持倉：{abs(pos_amt)} 顆 (成本 {entry_price})\n"
                f"最新止損價：{formatted_sl}"
            )
            return {"success": True, "msg": f"移動止損已更新至 {formatted_sl}"}

        # ==========================================
        # C. 止損或盤整反轉平倉 (CLOSE_LONG / CLOSE_SHORT)
        # ==========================================
        if action in ("CLOSE_LONG", "CLOSE_SHORT"):
            pos_amt, entry_price = client.get_position_info(sym)
            if abs(pos_amt) > 0:
                if ("LONG" in action and pos_amt < 0) or ("SHORT" in action and pos_amt > 0):
                    return {"success": True, "msg": "持倉方向已改變，略過舊平倉指令"}
                client.cancel_all(sym)
                side = "SELL" if pos_amt > 0 else "BUY"
                raw_close_qty = abs(pos_amt)
                curr_p = client.get_price(sym)
                client.order(sym, side, raw_close_qty, reduce_only=True)
                total_eq, _, _ = client.get_equity()
                send_line_push(
                    f"🏁 【止損 / 趨勢出場平倉完成】\n"
                    f"幣種：{sym} ({'多單平倉' if pos_amt > 0 else '空單平倉'})\n"
                    f"平倉顆數：{raw_close_qty} 顆\n"
                    f"進場成本：{entry_price} | 出場參考價：{curr_p}\n"
                    f"💰 最新總權益：{total_eq:.2f} USDT"
                )
                return {"success": True, "msg": f"已強制清空所有剩餘部位 {raw_close_qty} {sym}"}

            client.cancel_all(sym)
            return {"success": True, "msg": "目前無持倉，已清理殘留掛單"}

        # ==========================================
        # D. 進場與趨勢反手開倉 (LONG / SHORT)
        # ==========================================
        if action in ("LONG", "SHORT"):
            pos_amt, old_entry = client.get_position_info(sym)
            curr_p = client.get_price(sym)
            min_notional = client.get_min_notional(sym)
            pos_notional = abs(pos_amt) * curr_p

            if ((action == "LONG" and pos_amt > 0) or (action == "SHORT" and pos_amt < 0)) and (pos_notional >= min_notional):
                logger.info(f"🛡️ 防重複開倉啟動：幣安帳戶已有正常同向 {sym} 持倉 ({pos_amt} ≈ {pos_notional:.2f}U)，略過重複進場。")
                return {"success": True, "msg": f"已有同向持倉 ({pos_amt})，不重複加倉"}

            reversed_note = ""
            if abs(pos_amt) > 0:
                logger.info(f"🔄 偵測到反向持倉或殘留灰塵單 ({pos_amt} ≈ {pos_notional:.2f}U)：正在徹底清空...")
                client.cancel_all(sym)
                close_side = "BUY" if pos_amt < 0 else "SELL"
                client.order(sym, close_side, abs(pos_amt), reduce_only=True)
                reversed_note = f"🔄 已先平掉原反向部位：{abs(pos_amt)} 顆 (均價 {old_entry})\n"
                time.sleep(0.4)

            if not bot_state["trading_enabled"]:
                send_line_push(
                    f"⏸️ 【收到 {sym} {action} 訊號，但目前已暫停開新倉】\n"
                    f"{reversed_note}若要恢復自動開倉，請在 LINE 輸入「7」或「恢復」。"
                )
                return {"success": True, "msg": "目前處於暫停開新倉模式，已略過新進場"}

            if not sig.stop_loss:
                raise HTTPException(status_code=400, detail="必須附帶止損價格")
            lev = sig.leverage or 10
            client.set_leverage(sym, lev)

            total_eq, avail, _ = client.get_equity()
            if curr_p <= 0:
                curr_p = client.get_price(sym)
            account_base = max(total_eq, avail)

            if account_base <= 0 or curr_p <= 0:
                raise HTTPException(status_code=400, detail="無法取得帳戶保證金餘額或現價")

            if action == "LONG" and curr_p <= sig.stop_loss:
                return {"success": False, "msg": f"現價 ({curr_p}) 已跌破止損價 ({sig.stop_loss})，放棄進場"}
            if action == "SHORT" and curr_p >= sig.stop_loss:
                return {"success": False, "msg": f"現價 ({curr_p}) 已突破止損價 ({sig.stop_loss})，放棄進場"}

            risk_usd = account_base * ((sig.risk_pct or 1.5) / 100.0)
            raw_dist = abs(curr_p - sig.stop_loss)
            safe_dist = max(raw_dist, curr_p * 0.002)

            raw_risk_qty = risk_usd / safe_dist
            min_notional_qty = client.format_qty(sym, (min_notional * 1.05) / curr_p, round_up=True)

            if raw_risk_qty * curr_p < min_notional:
                raw_risk_qty = min_notional_qty

            max_buying_power_qty = ((avail if avail > 0 else account_base) * lev * 0.95) / curr_p
            safe_raw_qty = min(raw_risk_qty, max_buying_power_qty)

            target_qty = client.format_qty(sym, safe_raw_qty)
            if target_qty <= 0 or (target_qty * curr_p) < min_notional:
                raise HTTPException(
                    status_code=400,
                    detail=f"計算倉位 ({target_qty} {sym}) 低於幣安最小下單要求 ({min_notional}U)"
                )

            side = "BUY" if action == "LONG" else "SELL"
            sl_side = "SELL" if side == "BUY" else "BUY"

            client.cancel_all(sym)
            order_res = client.order(sym, side, target_qty)
            if not isinstance(order_res, dict) or "orderId" not in order_res:
                send_line_push(f"❌ 【{sym} {action} 開倉失敗】\n幣安回應：{order_res}")
                raise HTTPException(status_code=400, detail=f"幣安拒絕開倉: {order_res}")

            sl_success = client.set_sl(sym, sl_side, sig.stop_loss)
            if not sl_success:
                client.order(sym, sl_side, target_qty, reduce_only=True)
                send_line_push(f"⚠️ 【{sym} {action} 止損掛單失敗，已緊急平倉保護】")
                raise HTTPException(status_code=400, detail="止損單設定失敗，已執行緊急平倉保護")

            _, actual_entry = client.get_position_info(sym)
            base_entry = actual_entry if actual_entry > 0 else curr_p
            formatted_sl = client.format_price(sym, sig.stop_loss)

            dir_title = "🟢 【做多 LONG 開倉成功】" if action == "LONG" else "🔴 【做空 SHORT 開倉成功】"
            send_line_push(
                f"{dir_title}\n"
                f"{reversed_note}"
                f"幣種：{sym} ({lev}x)\n"
                f"開倉顆數：{target_qty} 顆 (約 {target_qty * base_entry:.1f}U)\n"
                f"成交均價：{base_entry}\n"
                f"🛡️ 標記止損：{formatted_sl}\n"
                f"🎯 1.5R 減半保本：由 TradingView TP1 訊號同步觸發\n"
                f"💰 帳戶總權益：{account_base:.2f} USDT"
            )
            return {"success": True, "qty": target_qty, "stop_loss": formatted_sl}

        return {"success": False, "msg": f"未知動作: {action}"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))

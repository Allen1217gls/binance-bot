import os
import time
import math
import hmac
import hashlib
import logging
import secrets
import asyncio
import threading
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, Tuple
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

# 台灣時區 (UTC+8)
TW_TZ = timezone(timedelta(hours=8))

# 全域非同步鎖：防止毫秒級連續 Webhook (如 TP1 與 UPDATE_SL) 併發撞車
webhook_lock = asyncio.Lock()


class BinanceClient:
    def __init__(self, key: str, sec: str):
        self.key, self.sec = key, sec
        self.time_offset = 0
        # 儲存結構: sym -> (step_size, qty_decimals, price_precision, min_notional)
        self._precisions: Dict[str, Tuple[float, int, int, float]] = {}
        self._leverage_cache: Dict[str, int] = {}
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
        if round_up:
            return round(math.ceil(raw_qty / step) * step, dec)
        return round(math.floor(raw_qty / step) * step, dec)

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
            "workingType": "MARK_PRICE",  # ✅ 改用 MARK_PRICE 標記價格防惡意插針掃損
            "timestamp": int(time.time() * 1000) + self.time_offset,
            "recvWindow": 60000
        }
        p["signature"] = self._sign(p)
        res = self._safe_request("POST", "/fapi/v1/algoOrder", data=p)
        if isinstance(res, dict) and "algoId" in res:
            logger.info(f"✅ Algo 標記價格止損單掛單成功！觸發價: {formatted_p}, ID: {res.get('algoId')}")
            return True
        logger.error(f"❌ 幣安拒絕 Algo 止損單: {res}")
        return False

    def cancel_all(self, sym: str):
        sym = sym.upper()
        p1 = {"symbol": sym, "timestamp": int(time.time() * 1000) + self.time_offset, "recvWindow": 60000}
        p1["signature"] = self._sign(p1)
        r1 = self._safe_request("DELETE", "/fapi/v1/allOpenOrders", params=p1, retries=1)

        p2 = {"symbol": sym, "timestamp": int(time.time() * 1000) + self.time_offset, "recvWindow": 60000}
        p2["signature"] = self._sign(p2)
        r2 = self._safe_request("DELETE", "/fapi/v1/algoOpenOrders", params=p2, retries=1)
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


client = BinanceClient(BINANCE_API_KEY, BINANCE_API_SECRET)

# 📊 每日台灣時間 08:00 資產回報背景執行緒
yesterday_balance = 0.0
last_report_date = None


def daily_balance_reporter():
    global yesterday_balance, last_report_date
    while True:
        try:
            now_tw = datetime.now(TW_TZ)
            if now_tw.hour == 8 and now_tw.minute == 0:
                today_date = now_tw.date()
                if last_report_date != today_date:
                    total_eq, avail, _ = client.get_equity()
                    if yesterday_balance > 0:
                        diff = total_eq - yesterday_balance
                        pct = (diff / yesterday_balance) * 100
                        trend = "📈 增加" if diff >= 0 else "📉 減少"
                        logger.info(f"📊 【每日 08:00 資產報告】當前總權益: {total_eq:.2f} USDT | 相比昨天: {trend} {abs(diff):.2f} USDT ({pct:+.2f}%)")
                    else:
                        logger.info(f"📊 【每日 08:00 資產報告】當前總權益: {total_eq:.2f} USDT (基準已建立)")

                    yesterday_balance = total_eq
                    last_report_date = today_date
            time.sleep(45)
        except Exception as e:
            logger.error(f"每日資產報告背景任務異常: {e}")
            time.sleep(60)


# ✅ 採用新版 FastAPI lifespan 生命週期管理器，徹底消除 DeprecationWarning 警告
@asynccontextmanager
async def lifespan(app: FastAPI):
    threading.Thread(target=daily_balance_reporter, daemon=True).start()
    logger.info("已成功啟動每日台灣時間 08:00 資產報表背景監控服務。")
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
    return {"status": "online", "feature": "Zeabur Tokyo Hull-Turtle Atomic Reversal Bot (v2.1 Lifespan Clean)"}


@app.get("/test-binance")
def test_binance():
    total_eq, avail, raw_debug = client.get_equity()
    price = client.get_price("SOLUSDT")
    return {
        "status": "success",
        "msg": "幣安連線診斷完成",
        "total_equity_usdt": total_eq,
        "available_balance_usdt": avail,
        "sol_price": price,
        "api_key_check": f"{client.key[:6]}...{client.key[-4:]}" if client.key else "未設定API_KEY",
        "binance_raw_response": raw_debug
    }


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
    logger.info(f"收到指令: {action} {sym} (附帶止損: {sig.stop_loss})")

    async with webhook_lock:
        # ==========================================
        # A. TP1 保本與 50% 減倉處理 (含防裸奔與小資金保護)
        # ==========================================
        if action in ("TP1_LONG", "TP1_SHORT"):
            pos_amt, entry_price = client.get_position_info(sym)
            if abs(pos_amt) <= 0:
                return {"success": True, "msg": "目前無持倉，略過TP1"}

            if ("LONG" in action and pos_amt < 0) or ("SHORT" in action and pos_amt > 0):
                return {"success": True, "msg": "持倉方向與TP1不符，略過"}

            side = "SELL" if "LONG" in action else "BUY"
            close_qty = client.format_qty(sym, abs(pos_amt) * 0.5)

            # 1. 先執行 50% 減倉（此時舊止損單仍在場上保護）
            if 0 < close_qty < abs(pos_amt):
                client.order(sym, side, close_qty, reduce_only=True)
                logger.info(f"✅ TP1 減倉 50% ({close_qty} {sym}) 執行成功")
            else:
                logger.info(f"ℹ️ 持倉顆數 ({abs(pos_amt)}) 過小無法對半拆分，直接將全倉移至保本止損")

            # 2. 撤除舊止損並掛入成本價保本止損（若失敗則緊急全平防裸奔）
            if entry_price > 0:
                client.cancel_all(sym)
                sl_ok = client.set_sl(sym, side, entry_price)
                if not sl_ok:
                    rem_amt, _ = client.get_position_info(sym)
                    if abs(rem_amt) > 0:
                        client.order(sym, side, abs(rem_amt), reduce_only=True)
                    logger.warning("🛡️ TP1 時現價已穿過成本價導致保本損掛單失敗，已自動市價全平尾倉防止裸奔！")
                    return {"success": True, "msg": "TP1 觸發時價格已回落，已市價全平保護尾倉"}
                logger.info(f"✅ TP1 保本止損已成功移至成本價 {entry_price}")
            return {"success": True, "msg": "TP1已達成：減倉並移至成本價保本"}

        # ==========================================
        # B. 海龜移動止損即時同步更新 (UPDATE_SL)
        # ==========================================
        if action == "UPDATE_SL":
            if not sig.stop_loss:
                return {"success": False, "msg": "UPDATE_SL 缺少 stop_loss 參數"}
            pos_amt, _ = client.get_position_info(sym)
            if abs(pos_amt) <= 0:
                return {"success": True, "msg": "目前無持倉，略過移動止損更新"}

            sl_side = "SELL" if pos_amt > 0 else "BUY"
            client.cancel_all(sym)
            sl_ok = client.set_sl(sym, sl_side, sig.stop_loss)
            if not sl_ok:
                rem_amt, _ = client.get_position_info(sym)
                if abs(rem_amt) > 0:
                    client.order(sym, sl_side, abs(rem_amt), reduce_only=True)
                logger.warning(f"🛡️ 海龜移動止損 ({sig.stop_loss}) 掛單失敗（現價已穿過止損線），已立即市價平倉出場！")
                return {"success": True, "msg": "現價已穿過新移動止損，已市價出場"}

            logger.info(f"🐢 海龜移動止損已同步更新至幣安: {sig.stop_loss}")
            return {"success": True, "msg": f"移動止損已更新至 {sig.stop_loss}"}

        # ==========================================
        # C. 止損或盤整反轉平倉 (CLOSE_LONG / CLOSE_SHORT)
        # ==========================================
        if action in ("CLOSE_LONG", "CLOSE_SHORT"):
            pos_amt, _ = client.get_position_info(sym)
            if abs(pos_amt) > 0:
                if ("LONG" in action and pos_amt < 0) or ("SHORT" in action and pos_amt > 0):
                    return {"success": True, "msg": "持倉方向已改變，略過舊平倉指令"}
                client.cancel_all(sym)
                side = "SELL" if pos_amt > 0 else "BUY"
                close_qty = client.format_qty(sym, abs(pos_amt))
                res = client.order(sym, side, close_qty, reduce_only=True)
                logger.info(f"✅ 平倉執行完畢: 方向={side}, 顆數={close_qty}, 回應={res}")
                return {"success": True, "msg": f"已全平倉 {close_qty} {sym}"}
            return {"success": True, "msg": "目前無持倉，略過平倉"}

        # ==========================================
        # D. 進場與趨勢反手開倉 (LONG / SHORT)
        # ==========================================
        if action in ("LONG", "SHORT"):
            pos_amt, _ = client.get_position_info(sym)

            # 1. 同方向持倉：防重複加倉保護
            if (action == "LONG" and pos_amt > 0) or (action == "SHORT" and pos_amt < 0):
                logger.info(f"🛡️ 防重複開倉啟動：幣安帳戶已有同向 {sym} 持倉 ({pos_amt})，略過重複進場。")
                return {"success": True, "msg": f"已有同向持倉 ({pos_amt})，不重複加倉"}

            # 2. 反向持倉：趨勢反轉時自動先平掉舊反向單，再無縫反手開新單
            if (action == "LONG" and pos_amt < 0) or (action == "SHORT" and pos_amt > 0):
                logger.info(f"🔄 偵測到趨勢反手 ({action})：正在自動平倉舊反向持倉 ({pos_amt})...")
                client.cancel_all(sym)
                close_side = "BUY" if pos_amt < 0 else "SELL"
                close_qty = client.format_qty(sym, abs(pos_amt))
                client.order(sym, close_side, close_qty, reduce_only=True)
                time.sleep(0.3)

            if not sig.stop_loss:
                raise HTTPException(status_code=400, detail="必須附帶止損價格")
            lev = sig.leverage or 10
            client.set_leverage(sym, lev)

            total_eq, avail, _ = client.get_equity()
            curr_p = client.get_price(sym)
            account_base = max(total_eq, avail)

            if account_base <= 0 or curr_p <= 0:
                raise HTTPException(status_code=400, detail="無法取得帳戶保證金餘額或現價")

            risk_usd = account_base * ((sig.risk_pct or 1.5) / 100.0)
            dist = abs(curr_p - sig.stop_loss)
            if dist <= 0:
                raise HTTPException(status_code=400, detail="止損距離為0")

            # 🛡️ 計算風險顆數與 93U 小資金最小下單名義價值 (MIN_NOTIONAL) 自動保護
            raw_risk_qty = risk_usd / dist
            min_notional = client.get_min_notional(sym)
            min_notional_qty = client.format_qty(sym, (min_notional * 1.05) / curr_p, round_up=True)

            # 若風險算出的倉位略低於幣安最小下單金額 (例如 5U)，自動補足至最小可開倉顆數
            if raw_risk_qty * curr_p < min_notional:
                logger.info(f"ℹ️ 風險計算倉位價值 ({raw_risk_qty * curr_p:.2f}U) 低於幣安最小門檻 ({min_notional}U)，自動微調補足至最小可下單顆數 {min_notional_qty}")
                raw_risk_qty = min_notional_qty

            # 🛡️ 購買力上限保護：避免超過「可用餘額 × 槓桿」導致 -2019 報錯
            max_buying_power_qty = ((avail if avail > 0 else account_base) * lev * 0.95) / curr_p
            safe_raw_qty = min(raw_risk_qty, max_buying_power_qty)

            target_qty = client.format_qty(sym, safe_raw_qty)
            if target_qty <= 0 or (target_qty * curr_p) < min_notional:
                raise HTTPException(
                    status_code=400,
                    detail=f"計算倉位 ({target_qty} {sym} ≈ {target_qty * curr_p:.2f}U) 低於幣安最小下單要求 ({min_notional}U)"
                )

            side = "BUY" if action == "LONG" else "SELL"
            sl_side = "SELL" if side == "BUY" else "BUY"

            client.cancel_all(sym)
            order_res = client.order(sym, side, target_qty)
            if not isinstance(order_res, dict) or "orderId" not in order_res:
                raise HTTPException(status_code=400, detail=f"幣安拒絕開倉: {order_res}")

            sl_success = client.set_sl(sym, sl_side, sig.stop_loss)
            if not sl_success:
                client.order(sym, sl_side, target_qty, reduce_only=True)
                raise HTTPException(status_code=400, detail="止損單設定失敗，已執行緊急平倉保護")

            logger.info(f"✅ 開單與強制標記價止損全套成功: 總本金={account_base:.2f}U, 風險預算={risk_usd:.2f}U, 顆數={target_qty} {sym}, 止損價={sig.stop_loss}")
            return {"success": True, "qty": target_qty, "stop_loss": sig.stop_loss}

        return {"success": False, "msg": f"未知動作: {action}"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))

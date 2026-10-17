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

# 台灣時區 (UTC+8)
TW_TZ = timezone(timedelta(hours=8))

# 全域非同步鎖：防止毫秒級連續 Webhook 併發撞車
webhook_lock = asyncio.Lock()


class BinanceClient:
    def __init__(self, key: str, sec: str):
        self.key, self.sec = key, sec
        self.time_offset = 0
        self._precisions: Dict[str, Tuple[float, int, int, float]] = {}
        self._leverage_cache: Dict[str, int] = {}
        self._tp1_placed: Dict[str, bool] = {}
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

    def set_tp_limit(self, sym: str, side: str, qty: float, price: float) -> bool:
        """開倉時直接預掛 1.5R 的 50% 限價止盈單 (Maker 0.02% 低手續費 + 零延遲抓插針)"""
        formatted_qty = self.format_qty(sym, qty)
        formatted_p = self.format_price(sym, price)
        if formatted_qty <= 0 or formatted_p <= 0:
            return False
        p = {
            "symbol": sym.upper(),
            "side": side.upper(),
            "type": "LIMIT",
            "timeInForce": "GTC",
            "quantity": formatted_qty,
            "price": formatted_p,
            "reduceOnly": "true",
            "timestamp": int(time.time() * 1000) + self.time_offset,
            "recvWindow": 60000
        }
        p["signature"] = self._sign(p)
        res = self._safe_request("POST", "/fapi/v1/order", data=p)
        if isinstance(res, dict) and "orderId" in res:
            self._tp1_placed[sym.upper()] = True
            logger.info(f"✅ 已預掛 1.5R 減半限價止盈單 (Maker): {side} {formatted_qty} {sym} @ {formatted_p}")
            return True
        logger.warning(f"⚠️ 預掛 1.5R 限價止盈單未成功 (將由 TP1 訊號市價執行): {res}")
        self._tp1_placed[sym.upper()] = False
        return False

    def get_open_tp_orders(self, sym: str) -> List[Dict[str, Any]]:
        """查詢目前尚未成交的預掛限價止盈單"""
        p = {"symbol": sym.upper(), "timestamp": int(time.time() * 1000) + self.time_offset, "recvWindow": 60000}
        p["signature"] = self._sign(p)
        res = self._safe_request("GET", "/fapi/v1/openOrders", params=p)
        if isinstance(res, list):
            return [o for o in res if o.get("type") == "LIMIT" and o.get("reduceOnly") is True]
        return []

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
            logger.info(f"✅ Algo 標記價格止損單掛單成功！觸發價: {formatted_p}, ID: {res.get('algoId')}")
            return True
        logger.error(f"❌ 幣安拒絕 Algo 止損單: {res}")
        return False

    def cancel_algo_only(self, sym: str):
        """僅取消條件止損單 (保留帳上預掛的 50% 限價止盈單不被誤刪)"""
        sym = sym.upper()
        p2 = {"symbol": sym, "timestamp": int(time.time() * 1000) + self.time_offset, "recvWindow": 60000}
        p2["signature"] = self._sign(p2)
        r2 = self._safe_request("DELETE", "/fapi/v1/algoOpenOrders", params=p2, retries=1)
        logger.info(f"更新止損撤銷舊條件單結果: {r2}")

    def cancel_all(self, sym: str):
        """取消該交易對所有一般掛單與條件止損單"""
        sym = sym.upper()
        p1 = {"symbol": sym, "timestamp": int(time.time() * 1000) + self.time_offset, "recvWindow": 60000}
        p1["signature"] = self._sign(p1)
        r1 = self._safe_request("DELETE", "/fapi/v1/allOpenOrders", params=p1, retries=1)

        p2 = {"symbol": sym, "timestamp": int(time.time() * 1000) + self.time_offset, "recvWindow": 60000}
        p2["signature"] = self._sign(p2)
        r2 = self._safe_request("DELETE", "/fapi/v1/algoOpenOrders", params=p2, retries=1)
        self._tp1_placed[sym] = False
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

yesterday_balance = 0.0
last_report_date = None


def daily_balance_reporter():
    global yesterday_balance, last_report_date
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
            if now_tw.hour == 8 and now_tw.minute < 5 and last_report_date != today_date:
                total_eq, avail, _ = client.get_equity()
                if yesterday_balance > 0:
                    diff = total_eq - yesterday_balance
                    pct = (diff / yesterday_balance) * 100
                    trend = "📈 增加" if diff >= 0 else "📉 減少"
                    logger.info(f"📊 【每日 08:00 資產報告】當前總權益: {total_eq:.2f} USDT | 相比昨日基準: {trend} {abs(diff):.2f} USDT ({pct:+.2f}%)")
                else:
                    logger.info(f"📊 【每日 08:00 資產報告】當前總權益: {total_eq:.2f} USDT (基準已建立)")

                yesterday_balance = total_eq
                last_report_date = today_date
            time.sleep(30)
        except Exception as e:
            logger.error(f"每日資產報告背景任務異常: {e}")
            time.sleep(60)


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
    tp1_rr: Optional[float] = 1.5


@app.get("/")
def index():
    return {"status": "online", "feature": "Zeabur Tokyo Hull-Turtle Bot (v2.6 Pre-Placed 1.5R Limit TP1 + Trail Safe)"}


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
        # A. TP1 保本觸發：確認預掛 50% 止盈單狀態 + 將尾倉止損移至成本價
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

            open_tp_orders = client.get_open_tp_orders(sym)
            if open_tp_orders:
                unfilled_qty = sum(max(0.0, float(o.get("origQty", 0.0)) - float(o.get("executedQty", 0.0))) for o in open_tp_orders)
                client.cancel_all(sym)
                rem_after = client.format_qty(sym, abs(pos_amt) - unfilled_qty)
                if 0 < unfilled_qty < abs(pos_amt) and (rem_after * curr_p >= min_notional):
                    client.order(sym, side, unfilled_qty, reduce_only=True)
                    logger.info(f"✅ TP1 補平未成交限價單差額 ({unfilled_qty} {sym})，尾倉保留 {rem_after} {sym}")
            elif not client._tp1_placed.get(sym, False):
                close_qty = client.format_qty(sym, abs(pos_amt) * 0.5)
                rem_qty = client.format_qty(sym, abs(pos_amt) - close_qty)
                if 0 < close_qty < abs(pos_amt) and (rem_qty * curr_p >= min_notional):
                    client.order(sym, side, close_qty, reduce_only=True)
                    logger.info(f"✅ TP1 市價減倉 50% ({close_qty} {sym}) 執行成功，尾倉保留 {rem_qty} {sym}")
            else:
                logger.info(f"🎯 幣安預掛的 1.5R 減半限價止盈單已自動成交！目前剩餘尾倉 {abs(pos_amt)} {sym} 繼續奔跑。")

            if entry_price > 0:
                target_sl = entry_price
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
                    logger.warning("🛡️ TP1 時現價已穿過保本止損價導致掛單失敗，已自動市價全平尾倉防止裸奔！")
                    return {"success": True, "msg": "TP1 觸發時價格已回落，已市價全平保護尾倉"}
                logger.info(f"✅ TP1 保本止損已成功設定至 {target_sl}")
            return {"success": True, "msg": "TP1已達成：50%止盈完成並移至保本止損價"}

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
            client.cancel_algo_only(sym)
            sl_ok = client.set_sl(sym, sl_side, sig.stop_loss)
            if not sl_ok:
                client.cancel_all(sym)
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
                raw_close_qty = abs(pos_amt)
                res = client.order(sym, side, raw_close_qty, reduce_only=True)
                logger.info(f"✅ 強制全平倉執行完畢: 方向={side}, 顆數={raw_close_qty}, 回應={res}")
                return {"success": True, "msg": f"已強制清空所有剩餘部位 {raw_close_qty} {sym}"}

            client.cancel_all(sym)
            return {"success": True, "msg": "目前無持倉，已清理殘留掛單"}

        # ==========================================
        # D. 進場與趨勢反手開倉 (LONG / SHORT) + 同時預掛 1.5R 減半限價止盈單與止損單
        # ==========================================
        if action in ("LONG", "SHORT"):
            pos_amt, _ = client.get_position_info(sym)
            curr_p = client.get_price(sym)
            min_notional = client.get_min_notional(sym)
            pos_notional = abs(pos_amt) * curr_p

            if ((action == "LONG" and pos_amt > 0) or (action == "SHORT" and pos_amt < 0)) and (pos_notional >= min_notional):
                logger.info(f"🛡️ 防重複開倉啟動：幣安帳戶已有正常同向 {sym} 持倉 ({pos_amt} ≈ {pos_notional:.2f}U)，略過重複進場。")
                return {"success": True, "msg": f"已有同向持倉 ({pos_amt})，不重複加倉"}

            if abs(pos_amt) > 0:
                logger.info(f"🔄 偵測到反向持倉或殘留灰塵單 ({pos_amt} ≈ {pos_notional:.2f}U)：正在徹底清空以準備新開倉...")
                client.cancel_all(sym)
                close_side = "BUY" if pos_amt < 0 else "SELL"
                client.order(sym, close_side, abs(pos_amt), reduce_only=True)
                time.sleep(0.4)

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
                logger.warning(f"🛡️ 放棄開多：幣安現價 ({curr_p}) 已低於或等於止損價 ({sig.stop_loss})，避免開倉後立即止損。")
                return {"success": False, "msg": f"現價 ({curr_p}) 已跌破止損價 ({sig.stop_loss})，放棄進場"}
            if action == "SHORT" and curr_p >= sig.stop_loss:
                logger.warning(f"🛡️ 放棄開空：幣安現價 ({curr_p}) 已高於或等於止損價 ({sig.stop_loss})，避免開倉後立即止損。")
                return {"success": False, "msg": f"現價 ({curr_p}) 已突破止損價 ({sig.stop_loss})，放棄進場"}

            risk_usd = account_base * ((sig.risk_pct or 1.5) / 100.0)
            raw_dist = abs(curr_p - sig.stop_loss)
            safe_dist = max(raw_dist, curr_p * 0.002)

            raw_risk_qty = risk_usd / safe_dist
            min_notional_qty = client.format_qty(sym, (min_notional * 1.05) / curr_p, round_up=True)

            if raw_risk_qty * curr_p < min_notional:
                logger.info(f"ℹ️ 風險計算倉位價值低於幣安最小門檻，自動微調至最小可下單顆數 {min_notional_qty}")
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
                raise HTTPException(status_code=400, detail=f"幣安拒絕開倉: {order_res}")

            sl_success = client.set_sl(sym, sl_side, sig.stop_loss)
            if not sl_success:
                client.order(sym, sl_side, target_qty, reduce_only=True)
                raise HTTPException(status_code=400, detail="止損單設定失敗，已執行緊急平倉保護")

            _, actual_entry = client.get_position_info(sym)
            base_entry = actual_entry if actual_entry > 0 else curr_p
            actual_risk_dist = abs(base_entry - sig.stop_loss)
            rr_ratio = sig.tp1_rr or 1.5
            tp1_price = (base_entry + actual_risk_dist * rr_ratio) if action == "LONG" else (base_entry - actual_risk_dist * rr_ratio)

            tp1_qty = client.format_qty(sym, target_qty * 0.5)
            rem_qty = client.format_qty(sym, target_qty - tp1_qty)
            if tp1_qty > 0 and (tp1_qty * tp1_price >= min_notional) and (rem_qty * base_entry >= min_notional):
                client.set_tp_limit(sym, sl_side, tp1_qty, tp1_price)
            else:
                client._tp1_placed[sym] = False
                logger.info(f"ℹ️ 總倉位拆分 50% 後低於 {min_notional}U 門檻，不預掛拆單止盈，待 TP1 時直接將 100% 倉位止損移至保本價。")

            logger.info(f"✅ 開單全套完成: 本金={account_base:.2f}U, 總顆數={target_qty} {sym}, 止損={sig.stop_loss}, 預掛50%止盈價={client.format_price(sym, tp1_price)}")
            return {"success": True, "qty": target_qty, "stop_loss": sig.stop_loss, "tp1_limit_price": client.format_price(sym, tp1_price)}

        return {"success": False, "msg": f"未知動作: {action}"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))

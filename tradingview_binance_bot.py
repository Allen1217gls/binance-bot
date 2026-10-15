import os
import time
import math
import hmac
import hashlib
import logging
import secrets
import threading
from datetime import datetime
from typing import Optional, Dict, Any, Tuple
from urllib.parse import urlencode

import requests
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("ADX_SuperTrend_Bot")

BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "").strip()
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "").strip()
WEBHOOK_PASSPHRASE = os.getenv("WEBHOOK_PASSPHRASE", "Apple").strip()
USE_TESTNET = os.getenv("USE_TESTNET", "false").lower() in ("true", "1", "yes")
FUTURES_URL = "https://testnet.binancefuture.com" if USE_TESTNET else "https://fapi.binance.com"

class BinanceClient:
    def __init__(self, key: str, sec: str):
        self.key, self.sec = key, sec
        self.time_offset = 0
        self._precisions: Dict[str, Tuple[float, int, int]] = {}
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
                step, dec = 0.001, 3
                for f in s.get("filters", []):
                    if f.get("filterType") == "LOT_SIZE":
                        step = float(f.get("stepSize", "0.001"))
                        dec = max(0, -int(math.log10(step))) if step < 1 else 0
                    elif f.get("filterType") == "PRICE_FILTER":
                        tick = float(f.get("tickSize", "0.01"))
                        if tick < 1:
                            price_prec = max(0, -int(math.log10(tick)))
                self._precisions[sym] = (step, dec, price_prec)
            logger.info("已成功載入幣安交易對數量與價格精度資訊。")
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

    def format_qty(self, sym: str, raw_qty: float) -> float:
        sym = sym.upper()
        if sym not in self._precisions:
            self._load_exchange_info()
        step, dec, _ = self._precisions.get(sym, (0.001, 3, 2))
        return round(math.floor(raw_qty / step) * step, dec)

    def format_price(self, sym: str, raw_price: float) -> float:
        sym = sym.upper()
        if sym not in self._precisions:
            self._load_exchange_info()
        _, _, price_prec = self._precisions.get(sym, (0.001, 3, 2))
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

    def set_sl(self, sym: str, side: str, price: float):
        formatted_p = self.format_price(sym, price)
        p = {
            "algoType": "CONDITIONAL",
            "symbol": sym.upper(), 
            "side": side.upper(), 
            "type": "STOP_MARKET", 
            "triggerPrice": formatted_p,
            "closePosition": "true",
            "workingType": "CONTRACT_PRICE",
            "timestamp": int(time.time() * 1000) + self.time_offset,
            "recvWindow": 60000
        }
        p["signature"] = self._sign(p)
        res = self._safe_request("POST", "/fapi/v1/algoOrder", data=p)
        if isinstance(res, dict) and "algoId" in res:
            logger.info(f"✅ Algo 強制止損單掛單成功！觸發價: {formatted_p}, ID: {res.get('algoId')}")
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

app = FastAPI()
client = BinanceClient(BINANCE_API_KEY, BINANCE_API_SECRET)
trade_memory: Dict[str, Dict[str, Any]] = {}

# 📊 每日 08:00 資產回報背景執行緒
yesterday_balance = 0.0
last_report_date = None

def daily_balance_reporter():
    global yesterday_balance, last_report_date
    while True:
        try:
            now = datetime.now() # 伺服器本地時間 (通常雲端主機是 UTC，可依需求調整)
            # 檢查是否為早上 8 點 0 分
            if now.hour == 8 and now.minute == 0:
                today_date = now.date()
                if last_report_date != today_date:
                    total_eq, avail, _ = client.get_equity()
                    if yesterday_balance > 0:
                        diff = total_eq - yesterday_balance
                        pct = (diff / yesterday_balance) * 100 if yesterday_balance > 0 else 0
                        trend = "📈 增加" if diff >= 0 else "📉 減少"
                        logger.info(f"📊 【每日 08:00 資產報告】當前總權益: {total_eq:.2f} USDT | 相比昨天: {trend} {abs(diff):.2f} USDT ({pct:+.2f}%)")
                    else:
                        logger.info(f"📊 【每日 08:00 資產報告】當前總權益: {total_eq:.2f} USDT (初始化基準已建立)")
                    
                    yesterday_balance = total_eq
                    last_report_date = today_date
            
            # 每 45 秒檢查一次，避免錯過整點
            time.sleep(45)
        except Exception as e:
            logger.error(f"每日資產報告背景任務異常: {e}")
            time.sleep(60)

@app.on_event("startup")
def startup_event():
    # 啟動背景執行緒
    threading.Thread(target=daily_balance_reporter, daemon=True).start()
    logger.info("已成功啟動每日 08:00 資產報表背景監控服務。")

class Signal(BaseModel):
    passphrase: str
    symbol: str
    action: str
    risk_pct: Optional[float] = 2.0
    stop_loss: Optional[float] = None
    leverage: Optional[int] = 10

@app.get("/")
def index():
    return {"status": "online", "feature": "Zeabur Tokyo Ultra-Precision Bot + Daily Report"}

@app.get("/test-binance")
def test_binance():
    total_eq, avail, raw_debug = client.get_equity()
    price = client.get_price("ETHUSDT")
    return {
        "status": "success",
        "msg": "幣安連線診斷完成",
        "total_equity_usdt": total_eq,
        "available_balance_usdt": avail,
        "eth_price": price,
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
    logger.info(f"收到指令: {action} {sym}")

    if action in ("TP1_LONG", "TP1_SHORT"):
        pos_amt, entry_price = client.get_position_info(sym)
        if abs(pos_amt) > 0:
            side = "SELL" if "LONG" in action else "BUY"
            close_qty = client.format_qty(sym, abs(pos_amt) * 0.5)
            client.cancel_all(sym)
            client.order(sym, side, close_qty, reduce_only=True)
            if entry_price > 0:
                client.set_sl(sym, side, entry_price)
                logger.info(f"✅ TP1 減倉 50% 成功，止損移至成本價 {entry_price}")
            return {"success": True, "msg": f"TP1已達成：平倉50%並移至成本價保本"}
        return {"success": True, "msg": "目前無持倉，略過TP1"}

    if action in ("CLOSE_LONG", "CLOSE_SHORT"):
        pos_amt, _ = client.get_position_info(sym)
        if abs(pos_amt) > 0:
            client.cancel_all(sym)
            side = "SELL" if "LONG" in action else "BUY"
            close_qty = client.format_qty(sym, abs(pos_amt))
            res = client.order(sym, side, close_qty, reduce_only=True)
            logger.info(f"✅ 趨勢反轉全平執行完畢: 方向={side}, 顆數={close_qty}, 回應={res}")
            return {"success": True, "msg": f"趨勢反轉：已全平倉 {close_qty} {sym}"}
        return {"success": True, "msg": "目前無持倉，略過平倉"}

    if action in ("LONG", "SHORT"):
        pos_amt, _ = client.get_position_info(sym)
        if abs(pos_amt) > 0:
            logger.info(f"🛡️ 防重複開倉啟動：幣安帳戶已有 {sym} 持倉 ({pos_amt})，略過重複進場。")
            return {"success": True, "msg": f"已有持倉 ({pos_amt})，不重複加倉"}

        if not sig.stop_loss:
            raise HTTPException(status_code=400, detail="必須附帶止損價格")
        if sig.leverage:
            client.set_leverage(sym, sig.leverage)

        total_eq, avail, _ = client.get_equity()
        curr_p = client.get_price(sym)
        account_base = max(total_eq, avail)

        if account_base <= 0 or curr_p <= 0:
            raise HTTPException(status_code=400, detail="無法取得帳戶保證金餘額或現價")

        risk_usd = account_base * ((sig.risk_pct or 2.0) / 100.0)
        dist = abs(curr_p - sig.stop_loss)
        if dist <= 0:
            raise HTTPException(status_code=400, detail="止損距離為0")
        
        target_qty = client.format_qty(sym, risk_usd / dist)
        if target_qty <= 0:
            raise HTTPException(status_code=400, detail=f"計算倉位低於最小下單量 (本金={account_base}U, 風險={risk_usd:.2f}U)")

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

        logger.info(f"✅ 開單與強制止損全套成功: 總本金={account_base}U, 2%風險={risk_usd:.2f}U, 顆數={target_qty}{sym}, 止損價={sig.stop_loss}")
        return {"success": True, "qty": target_qty, "stop_loss": sig.stop_loss}
    
    return {"success": False, "msg": f"未知動作: {action}"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))

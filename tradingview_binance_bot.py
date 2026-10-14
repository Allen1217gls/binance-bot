import os
import time
import math
import hmac
import hashlib
import logging
import secrets
from typing import Optional, Dict, Any, Tuple
from urllib.parse import urlencode

import requests
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("ADX_SuperTrend_Bot")

BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "")
WEBHOOK_PASSPHRASE = os.getenv("WEBHOOK_PASSPHRASE", "Apple")
USE_TESTNET = os.getenv("USE_TESTNET", "false").lower() in ("true", "1", "yes")
FUTURES_URL = "https://testnet.binancefuture.com" if USE_TESTNET else "https://fapi.binance.com"

class BinanceClient:
    def __init__(self, key: str, sec: str):
        self.key, self.sec = key, sec
        self.time_offset = 0
        self._precisions: Dict[str, Tuple[float, int]] = {}
        
        try:
            self._sync()
            self._load_exchange_info()
        except Exception as e:
            logger.warning(f"啟動時初始化連線失敗: {e}")

    def _sync(self):
        try:
            res = requests.get(f"{FUTURES_URL}/fapi/v1/time", timeout=5).json()
            self.time_offset = res.get("serverTime", int(time.time() * 1000)) - int(time.time() * 1000)
        except Exception:
            self.time_offset = 0

    def _load_exchange_info(self):
        try:
            res = requests.get(f"{FUTURES_URL}/fapi/v1/exchangeInfo", timeout=10).json()
            for s in res.get("symbols", []):
                sym = s.get("symbol")
                for f in s.get("filters", []):
                    if f.get("filterType") == "LOT_SIZE":
                        step = float(f.get("stepSize", "0.001"))
                        dec = max(0, -int(math.log10(step))) if step < 1 else 0
                        self._precisions[sym] = (step, dec)
            logger.info("已成功預先載入所有交易對的精度資訊。")
        except Exception as e:
            logger.error(f"載入 exchangeInfo 失敗: {e}")

    def _sign(self, p: Dict[str, Any]) -> str:
        return hmac.new(self.sec.encode(), urlencode(p).encode(), hashlib.sha256).hexdigest()

    def _headers(self):
        return {"X-MBX-APIKEY": self.key, "Content-Type": "application/x-www-form-urlencoded"}

    def _safe_request(self, method: str, endpoint: str, params: dict = None, data: dict = None, retries: int = 3) -> Any:
        url = f"{FUTURES_URL}{endpoint}"
        for attempt in range(retries):
            try:
                if method == "GET":
                    res = requests.get(url, headers=self._headers(), params=params, timeout=10)
                else:
                    res = requests.post(url, headers=self._headers(), data=data, timeout=10)
                
                data_json = res.json()
                
                if isinstance(data_json, dict) and data_json.get("code") == -1003:
                    logger.warning(f"觸發幣安頻率限制 (-1003)，正在進行第 {attempt+1} 次重試...")
                    time.sleep(3 * (attempt + 1))
                    continue
                return data_json
            except Exception as e:
                logger.error(f"API 請求異常 (嘗試 {attempt+1}/{retries}): {e}")
                if attempt == retries - 1:
                    return None
                time.sleep(2)
        return None

    def get_equity(self) -> float:
        p = {"timestamp": int(time.time() * 1000) + self.time_offset, "recvWindow": 5000}
        p["signature"] = self._sign(p)
        res = self._safe_request("GET", "/fapi/v2/account", params=p)
        if isinstance(res, dict):
            return float(res.get("totalMarginBalance", res.get("availableBalance", 0.0)))
        return 0.0

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
        step, dec = self._precisions.get(sym, (0.001, 3))
        return round(math.floor(raw_qty / step) * step, dec)

    def set_leverage(self, sym: str, lev: int):
        p = {"symbol": sym.upper(), "leverage": lev, "timestamp": int(time.time() * 1000) + self.time_offset}
        p["signature"] = self._sign(p)
        self._safe_request("POST", "/fapi/v1/leverage", data=p)

    def order(self, sym: str, side: str, qty: float, reduce_only: bool = False):
        p = {"symbol": sym.upper(), "side": side.upper(), "type": "MARKET", "quantity": qty, "timestamp": int(time.time() * 1000) + self.time_offset}
        if reduce_only:
            p["reduceOnly"] = "true"
        p["signature"] = self._sign(p)
        return self._safe_request("POST", "/fapi/v1/order", data=p)

    def set_sl(self, sym: str, side: str, price: float, qty: float):
        # 🛠️ 修正止損參數：明確指定數量與價格，避免 closePosition 在某些帳戶模式下報錯
        p = {
            "symbol": sym.upper(), 
            "side": side.upper(), 
            "type": "STOP_MARKET", 
            "stopPrice": round(price, 2),
            "quantity": qty,
            "reduceOnly": "true",
            "workingType": "CONTRACT_PRICE",
            "timestamp": int(time.time() * 1000) + self.time_offset
        }
        p["signature"] = self._sign(p)
        res = self._safe_request("POST", "/fapi/v1/order", data=p)
        
        if isinstance(res, dict) and "orderId" in res:
            logger.info(f"✅ 止損單掛單成功！價格: {price}, 數量: {qty}, 訂單ID: {res.get('orderId')}")
            return True
        else:
            logger.error(f"❌ 幣安拒絕止損單！完整回應: {res}")
            return False

    def cancel_all(self, sym: str):
        p = {"symbol": sym.upper(), "timestamp": int(time.time() * 1000) + self.time_offset}
        p["signature"] = self._sign(p)
        url = f"{FUTURES_URL}/fapi/v1/allOpenOrders"
        try:
            return requests.delete(url, headers=self._headers(), data=p, timeout=5).json()
        except Exception:
            return {}

    def get_pos(self, sym: str) -> float:
        p = {"symbol": sym.upper(), "timestamp": int(time.time() * 1000) + self.time_offset}
        p["signature"] = self._sign(p)
        res = self._safe_request("GET", "/fapi/v2/positionRisk", params=p)
        if isinstance(res, list):
            for pos in res:
                if pos.get("symbol") == sym.upper():
                    return abs(float(pos.get("positionAmt", 0.0)))
        return 0.0

app = FastAPI()
client = BinanceClient(BINANCE_API_KEY, BINANCE_API_SECRET)
trade_memory: Dict[str, Dict[str, Any]] = {}

class Signal(BaseModel):
    passphrase: str
    symbol: str
    action: str
    risk_pct: Optional[float] = 2.0
    stop_loss: Optional[float] = None
    leverage: Optional[int] = 10

@app.get("/")
def index():
    return {"status": "online", "feature": "Strict Stop-Loss Protected Bot"}

@app.post("/webhook")
async def handle_webhook(request: Request):
    try:
        data = await request.json()
    except Exception as e:
        logger.error(f"解析 JSON 失敗: {e}")
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {e}")

    try:
        sig = Signal(**data)
    except Exception as e:
        logger.error(f"Pydantic 模型驗證失敗: {e}")
        raise HTTPException(status_code=400, detail=f"Validation Error: {e}")

    if not secrets.compare_digest(sig.passphrase, WEBHOOK_PASSPHRASE):
        raise HTTPException(status_code=401, detail="密碼錯誤")

    sym, action = sig.symbol.upper(), sig.action.upper()
    if sig.leverage:
        client.set_leverage(sym, sig.leverage)

    if action in ("TP1_LONG", "TP1_SHORT"):
        pos = client.get_pos(sym)
        if pos > 0:
            side = "SELL" if "LONG" in action else "BUY"
            client.order(sym, side, client.format_qty(sym, pos * 0.5), reduce_only=True)
            client.cancel_all(sym)
            entry_p = trade_memory.get(sym, {}).get("entry_p")
            if entry_p:
                client.set_sl(sym, side, entry_p, pos * 0.5)
            return {"success": True, "msg": "TP1已達成：平倉50%並保本"}

    if action in ("CLOSE_LONG", "CLOSE_SHORT"):
        client.cancel_all(sym)
        pos = client.get_pos(sym)
        if pos > 0:
            side = "SELL" if "LONG" in action else "BUY"
            client.order(sym, side, pos, reduce_only=True)
            trade_memory.pop(sym, None)
            return {"success": True, "msg": "趨勢反轉：全平倉"}

    if action in ("LONG", "SHORT"):
        if not sig.stop_loss:
            raise HTTPException(status_code=400, detail="必須附帶止損價格")
        equity = client.get_equity()
        curr_p = client.get_price(sym)
        risk_usd = equity * ((sig.risk_pct or 2.0) / 100.0)
        dist = abs(curr_p - sig.stop_loss)
        if dist <= 0:
            raise HTTPException(status_code=400, detail="止損距離為0")
        
        target_qty = client.format_qty(sym, risk_usd / dist)
        if target_qty <= 0:
            raise HTTPException(status_code=400, detail="計算倉位低於最小下單量")

        side = "BUY" if action == "LONG" else "SELL"
        sl_side = "SELL" if side == "BUY" else "BUY"

        # 1. 執行市價開單
        order_res = client.order(sym, side, target_qty)
        if not isinstance(order_res, dict) or "orderId" not in order_res:
            logger.error(f"❌ 開倉失敗: {order_res}")
            raise HTTPException(status_code=400, detail="幣安拒絕開倉")

        # 2. 🛡️ 掛上止損單（帶有數量與 reduceOnly）
        sl_success = client.set_sl(sym, sl_side, sig.stop_loss, target_qty)
        
        # 3. 如果止損單掛單失敗，立即執行緊急平倉保護，防止裸奔
        if not sl_success:
            logger.critical(f"🚨 嚴重警告：止損單掛單失敗！為了防止爆倉，正在執行緊急市價平倉...")
            client.order(sym, sl_side, target_qty, reduce_only=True)
            raise HTTPException(status_code=400, detail="止損單設定失敗，已執行緊急平倉保護")

        trade_memory[sym] = {"entry_p": curr_p, "qty": target_qty}
        logger.info(f"✅ 開單與強制止損全套成功: 本金={equity}U, 2%風險={risk_usd:.2f}U, 顆數={target_qty}{sym}, 止損價={sig.stop_loss}")
        return {"success": True, "qty": target_qty, "stop_loss": sig.stop_loss}
    
    return {"success": False, "msg": f"未知動作: {action}"}

@app.on_event("startup")
async def startup_event():
    try:
        logger.info("正在測試連接幣安期貨 API...")
        equity = client.get_equity()
        logger.info(f"✅ 幣安 API 連線成功！當前帳戶保證金餘額 (Equity): {equity} USDT")
    except Exception as e:
        logger.error(f"❌ 幣安 API 連線失敗！錯誤: {e}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))

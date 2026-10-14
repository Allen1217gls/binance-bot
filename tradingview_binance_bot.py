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
            logger.warning(f"啟動時初始化連線失敗（可能 IP 仍被封鎖中），將繼續運行: {e}")

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
            logger.info("已成功預先載入所有交易對的精度資訊，節省 API 權重。")
        except Exception as e:
            logger.error(f"載入 exchangeInfo 失敗: {e}")

    def _sign(self, p: Dict[str, Any]) -> str:
        return hmac.new(self.sec.encode(), urlencode(p).encode(), hashlib.sha256).hexdigest()

    def _headers(self):
        return {"X-MBX-APIKEY": self.key, "Content-Type": "application/x-www-form-urlencoded"}

    def get_equity(self) -> float:
        p = {"timestamp": int(time.time() * 1000) + self.time_offset, "recvWindow": 5000}
        p["signature"] = self._sign(p)
        res = requests.get(f"{FUTURES_URL}/fapi/v2/account", headers=self._headers(), params=p, timeout=10).json()
        return float(res.get("totalMarginBalance", res.get("availableBalance", 0.0)))

    def get_price(self, sym: str) -> float:
        return float(requests.get(f"{FUTURES_URL}/fapi/v1/ticker/price", params={"symbol": sym.upper()}).json().get("price", 0.0))

    def format_qty(self, sym: str, raw_qty: float) -> float:
        sym = sym.upper()
        if sym not in self._precisions:
            try:
                res = requests.get(f"{FUTURES_URL}/fapi/v1/exchangeInfo").json()
                for s in res.get("symbols", []):
                    if s.get("symbol") == sym:
                        for f in s.get("filters", []):
                            if f.get("filterType") == "LOT_SIZE":
                                step = float(f.get("stepSize", "0.001"))
                                dec = max(0, -int(math.log10(step))) if step < 1 else 0
                                self._precisions[sym] = (step, dec)
            except Exception:
                self._precisions[sym] = (0.001, 3)
        step, dec = self._precisions.get(sym, (0.001, 3))
        return round(math.floor(raw_qty / step) * step, dec)

    def set_leverage(self, sym: str, lev: int):
        p = {"symbol": sym.upper(), "leverage": lev, "timestamp": int(time.time() * 1000) + self.time_offset}
        p["signature"] = self._sign(p)
        requests.post(f"{FUTURES_URL}/fapi/v1/leverage", headers=self._headers(), data=p)

    def order(self, sym: str, side: str, qty: float, reduce_only: bool = False):
        p = {"symbol": sym.upper(), "side": side.upper(), "type": "MARKET", "quantity": qty, "timestamp": int(time.time() * 1000) + self.time_offset}
        if reduce_only:
            p["reduceOnly"] = "true"
        p["signature"] = self._sign(p)
        return requests.post(f"{FUTURES_URL}/fapi/v1/order", headers=self._headers(), data=p).json()

    def set_sl(self, sym: str, side: str, price: float):
        p = {"symbol": sym.upper(), "side": side.upper(), "type": "STOP_MARKET", "stopPrice": round(price, 2), "closePosition": "true", "timestamp": int(time.time() * 1000) + self.time_offset}
        p["signature"] = self._sign(p)
        return requests.post(f"{FUTURES_URL}/fapi/v1/order", headers=self._headers(), data=p).json()

    def cancel_all(self, sym: str):
        p = {"symbol": sym.upper(), "timestamp": int(time.time() * 1000) + self.time_offset}
        p["signature"] = self._sign(p)
        return requests.delete(f"{FUTURES_URL}/fapi/v1/allOpenOrders", headers=self._headers(), data=p).json()

    def get_pos(self, sym: str) -> float:
        p = {"symbol": sym.upper(), "timestamp": int(time.time() * 1000) + self.time_offset}
        p["signature"] = self._sign(p)
        try:
            res = requests.get(f"{FUTURES_URL}/fapi/v2/positionRisk", headers=self._headers(), params=p).json()
            if isinstance(res, list):
                for pos in res:
                    if pos.get("symbol") == sym.upper():
                        return abs(float(pos.get("positionAmt", 0.0)))
            else:
                logger.warning(f"取得持倉 API 回傳非預期格式: {res}")
        except Exception as e:
            logger.error(f"取得持倉發生錯誤: {e}")
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
    return {"status": "online", "feature": "2% Risk Auto-Sizing & SuperTrend Bot"}

@app.post("/webhook")
async def handle_webhook(request: Request):
    try:
        body_bytes = await request.body()
        body_str = body_bytes.decode("utf-8")
        logger.info(f"收到來自 TradingView 的原始訊息: {body_str}")
        
        data = await request.json()
    except Exception as e:
        logger.error(f"解析 JSON 失敗: {e}")
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {e}")

    try:
        sig = Signal(**data)
    except Exception as e:
        logger.error(f"Pydantic 模型驗證失敗: {e}, 收到數據為: {data}")
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
                client.set_sl(sym, side, entry_p)
            return {"success": True, "msg": "TP1已達成：平倉50%並推移至成本價保本"}

    if action in ("CLOSE_LONG", "CLOSE_SHORT"):
        client.cancel_all(sym)
        pos = client.get_pos(sym)
        if pos > 0:
            side = "SELL" if "LONG" in action else "BUY"
            client.order(sym, side, pos, reduce_only=True)
            trade_memory.pop(sym, None)
            return {"success": True, "msg": "趨勢反轉：全倉平倉出場"}

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
        client.order(sym, side, target_qty)
        client.set_sl(sym, "SELL" if side == "BUY" else "BUY", sig.stop_loss)
        trade_memory[sym] = {"entry_p": curr_p, "qty": target_qty}
        logger.info(f"開單成功: 本金={equity}U, 2%風險={risk_usd:.2f}U, 止損距離={dist:.2f}, 下單顆數={target_qty}{sym}")
        return {"success": True, "qty": target_qty, "equity": equity, "risk_usd": risk_usd}
    
    return {"success": False, "msg": f"未知動作: {action}"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))

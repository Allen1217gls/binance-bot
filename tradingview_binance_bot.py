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

BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "").strip()
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "").strip()
WEBHOOK_PASSPHRASE = os.getenv("WEBHOOK_PASSPHRASE", "Apple").strip()
USE_TESTNET = os.getenv("USE_TESTNET", "false").lower() in ("true", "1", "yes")
FUTURES_URL = "https://testnet.binancefuture.com" if USE_TESTNET else "https://fapi.binance.com"

class BinanceClient:
    def __init__(self, key: str, sec: str):
        self.key, self.sec = key, sec
        self.time_offset = 0
        self._precisions: Dict[str, Tuple[float, int]] = {}
        self._sync()

    def _sync(self):
        try:
            r = requests.get(f"{FUTURES_URL}/fapi/v1/time", timeout=5)
            if r.status_code == 200:
                self.time_offset = r.json().get("serverTime", int(time.time() * 1000)) - int(time.time() * 1000)
        except Exception:
            self.time_offset = 0

    def _sign(self, p: Dict[str, Any]) -> str:
        return hmac.new(self.sec.encode("utf-8"), urlencode(p).encode("utf-8"), hashlib.sha256).hexdigest()

    def _headers(self):
        return {"X-MBX-APIKEY": self.key, "Content-Type": "application/x-www-form-urlencoded"}

    def get_equity(self) -> float:
        p = {"timestamp": int(time.time() * 1000) + self.time_offset, "recvWindow": 5000}
        p["signature"] = self._sign(p)
        r = requests.get(f"{FUTURES_URL}/fapi/v2/account", headers=self._headers(), params=p, timeout=10)
        data = r.json()
        if r.status_code != 200:
            raise HTTPException(status_code=400, detail={"error": "幣安拒絕連線", "原因": data.get("msg", data)})
        return float(data.get("totalMarginBalance", data.get("availableBalance", 0.0)))

    def get_price(self, sym: str) -> float:
        r = requests.get(f"{FUTURES_URL}/fapi/v1/ticker/price", params={"symbol": sym.upper()}, timeout=5)
        if r.status_code == 200:
            return float(r.json().get("price", 0.0))
        raise HTTPException(status_code=400, detail=f"無法取得 {sym} 即時價格")

    def format_qty(self, sym: str, raw_qty: float) -> float:
        sym = sym.upper()
        if sym not in self._precisions:
            try:
                r = requests.get(f"{FUTURES_URL}/fapi/v1/exchangeInfo", timeout=5)
                if r.status_code == 200:
                    for s in r.json().get("symbols", []):
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
        requests.post(f"{FUTURES_URL}/fapi/v1/leverage", headers=self._headers(), data=p, timeout=5)

    def order(self, sym: str, side: str, qty: float, reduce_only: bool = False):
        p = {"symbol": sym.upper(), "side": side.upper(), "type": "MARKET", "quantity": qty, "timestamp": int(time.time() * 1000) + self.time_offset}
        if reduce_only:
            p["reduceOnly"] = "true"
        p["signature"] = self._sign(p)
        r = requests.post(f"{FUTURES_URL}/fapi/v1/order", headers=self._headers(), data=p, timeout=10)
        data = r.json()
        if r.status_code != 200:
            raise HTTPException(status_code=400, detail={"error": "幣安下單失敗", "原因": data.get("msg", data)})
        return data

    def set_sl(self, sym: str, side: str, price: float):
        p = {"symbol": sym.upper(), "side": side.upper(), "type": "STOP_MARKET", "stopPrice": round(price, 2), "closePosition": "true", "timestamp": int(time.time() * 1000) + self.time_offset}
        p["signature"] = self._sign(p)
        return requests.post(f"{FUTURES_URL}/fapi/v1/order", headers=self._headers(), data=p, timeout=10).json()

    def cancel_all(self, sym: str):
        p = {"symbol": sym.upper(), "timestamp": int(time.time() * 1000) + self.time_offset}
        p["signature"] = self._sign(p)
        r = requests.delete(f"{FUTURES_URL}/fapi/v1/allOpenOrders", headers=self._headers(), params=p, timeout=10)
        return r.json()

    def get_pos(self, sym: str) -> float:
        p = {"symbol": sym.upper(), "timestamp": int(time.time() * 1000) + self.time_offset}
        p["signature"] = self._sign(p)
        r = requests.get(f"{FUTURES_URL}/fapi/v2/positionRisk", headers=self._headers(), params=p, timeout=10)
        data = r.json()
        if r.status_code != 200:
            raise HTTPException(status_code=400, detail={"error": "幣安查詢持倉失敗", "原因": data.get("msg", data)})
        if isinstance(data, list):
            for pos in data:
                if isinstance(pos, dict) and pos.get("symbol") == sym.upper():
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
    return {"status": "online", "feature": "2% Risk Auto-Sizing & SuperTrend Bot"}

@app.get("/test-binance")
def test_binance():
    """1秒診斷幣安 API 連線狀態"""
    equity = client.get_equity()
    price = client.get_price("ETHUSDT")
    return {"status": "success", "msg": "幣安連線完全正常！", "futures_equity_usdt": equity, "eth_price": price}

@app.post("/webhook")
async def handle_webhook(sig: Signal):
    if not secrets.compare_digest(sig.passphrase, WEBHOOK_PASSPHRASE):
        raise HTTPException(status_code=401, detail="密碼錯誤 (Invalid Passphrase)")

    sym, action = sig.symbol.upper(), sig.action.upper()
    if sig.leverage:
        client.set_leverage(sym, sig.leverage)

    # 1. TP1 減倉 50%
    if action in ("TP1_LONG", "TP1_SHORT"):
        pos = client.get_pos(sym)
        if pos > 0:
            side = "SELL" if "LONG" in action else "BUY"
            client.order(sym, side, client.format_qty(sym, pos * 0.5), reduce_only=True)
            client.cancel_all(sym)
            entry_p = trade_memory.get(sym, {}).get("entry_p")
            if entry_p: client.set_sl(sym, side, entry_p)
            return {"success": True, "msg": "TP1已達成：平倉50%並保本"}
        return {"success": True, "msg": "目前無持倉"}

    # 2. 趨勢反轉平倉
    if action in ("CLOSE_LONG", "CLOSE_SHORT"):
        client.cancel_all(sym)
        pos = client.get_pos(sym)
        if pos > 0:
            side = "SELL" if "LONG" in action else "BUY"
            client.order(sym, side, pos, reduce_only=True)
            trade_memory.pop(sym, None)
            return {"success": True, "msg": "趨勢反轉：全倉平倉出場"}
        return {"success": True, "msg": "目前無持倉，不需平倉"}

    # 3. 2% 風險開倉
    if action in ("LONG", "SHORT"):
        if not sig.stop_loss: raise HTTPException(status_code=400, detail="開倉必須附帶止損價格")
        equity = client.get_equity()
        curr_p = client.get_price(sym)
        risk_usd = equity * ((sig.risk_pct or 2.0) / 100.0)
        dist = abs(curr_p - sig.stop_loss)
        if dist <= 0: raise HTTPException(status_code=400, detail="止損距離不可為 0")
        
        target_qty = client.format_qty(sym, risk_usd / dist)
        if target_qty <= 0: raise HTTPException(status_code=400, detail=f"計算倉位低於最小下單量 (本金={equity}U, 風險={risk_usd:.2f}U)")

        side = "BUY" if action == "LONG" else "SELL"
        client.order(sym, side, target_qty)
        client.set_sl(sym, "SELL" if side == "BUY" else "BUY", sig.stop_loss)
        trade_memory[sym] = {"entry_p": curr_p, "qty": target_qty}
        return {"success": True, "qty": target_qty, "equity": equity, "risk_usd": risk_usd}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))

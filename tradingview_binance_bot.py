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
            self.time_offset = res.get("serverTime", int(time.time() * 1000)) - int(time.time() * 1000)
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
        return hmac.new(self.sec.encode(), urlencode(p).encode(), hashlib.sha256).hexdigest()

    def _headers(self):
        return {"X-MBX-APIKEY": self.key, "Content-Type": "application/x-www-form-urlencoded"}

    def _safe_request(self, method: str, endpoint: str, params: dict = None, data: dict = None, retries: int = 3) -> Any:
        url = f"{FUTURES_URL}{endpoint}"
        for attempt in range(retries):
            try:
                if method == "GET":
                    res = requests.get(url, headers=self._headers(), params=params, timeout=10)
                elif method == "DELETE":
                    res = requests.delete(url, headers=self._headers(), data=data, timeout=10)
                else:
                    res = requests.post(url, headers=self._headers(), data=data, timeout=10)
                
                data_json = res.json()
                if isinstance(data_json, dict) and data_json.get("code") == -1003:
                    logger.warning(f"觸發幣安頻率限制，重試中 ({attempt+1}/{retries})...")
                    time.sleep(2 * (attempt + 1))
                    continue
                return data_json
            except Exception as e:
                logger.error(f"API 請求異常: {e}")
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
        p = {"symbol": sym, "leverage": lev, "timestamp": int(time.time() * 1000) + self.time_offset}
        p["signature"] = self._sign(p)
        res = self._safe_request("POST", "/fapi/v1/leverage", data=p)
        if isinstance(res, dict) and res.get("leverage") == lev:
            self._leverage_cache[sym] = lev

    def order(self, sym: str, side: str, qty: float, reduce_only: bool = False):
        p = {"symbol": sym.upper(), "side": side.upper(), "type": "MARKET", "quantity": qty, "timestamp": int(time.time() * 1000) + self.time_offset}
        if reduce_only:
            p["reduceOnly"] = "true"
        p["signature"] = self._sign(p)
        return self._safe_request("POST", "/fapi/v1/order", data=p)

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
            "timestamp": int(time.time() * 1000) + self.time_offset
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
        p1 = {"symbol": sym, "timestamp": int(time.time() * 1000) + self.time_offset}
        p1["signature"] = self._sign(p1)
        self._safe_request("DELETE", "/fapi/v1/allOpenOrders", data=p1, retries=1)
        
        p2 = {"symbol": sym, "timestamp": int(time.time() * 1000) + self.time_offset}
        p2["signature"] = self._sign(p2)
        return self._safe_request("DELETE", "/fapi/v1/algoOpenOrders", data=p2, retries=1)

    def get_position_info(self, sym: str) -> Tuple[float, float]:
        """
        🚀 直接向幣安調取真實持倉顆數與開倉成本均價 (entryPrice)
        徹底杜絕重啟主機遺失成本價的風險！
        """
        p = {"symbol": sym.upper(), "timestamp": int(time.time() * 1000) + self.time_offset}
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

class Signal(BaseModel):
    passphrase: str
    symbol: str
    action: str
    risk_pct: Optional[float] = 2.0
    stop_loss: Optional[float] = None
    leverage: Optional[int] = 10

@app.get("/")
def index():
    return {"status": "online", "feature": "Tokyo Zero-Memory-Loss & Double Protection Bot"}

@app.get("/test-binance")
def test_binance():
    equity = client.get_equity()
    price = client.get_price("ETHUSDT")
    return {"status": "success", "msg": "幣安連線完全正常！", "futures_equity_usdt": equity, "eth_price": price}

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

    # 1. TP1 達到 1.5R：平倉 50% ＋ 直接向幣安抓真實成本價移至保本
    if action in ("TP1_LONG", "TP1_SHORT"):
        pos_amt, entry_price = client.get_position_info(sym)
        if abs(pos_amt) > 0:
            side = "SELL" if "LONG" in action else "BUY"
            client.order(sym, side, client.format_qty(sym, abs(pos_amt) * 0.5), reduce_only=True)
            client.cancel_all(sym)
            if entry_price > 0:
                client.set_sl(sym, side, entry_price)
                logger.info(f"✅ TP1 減倉 50% 成功，已向幣安調取真實進場價 {entry_price} 掛保本止損！")
            return {"success": True, "msg": f"TP1已達成：平倉50%並移至真實成本價 {entry_price} 保本"}
        return {"success": True, "msg": "目前無持倉，略過TP1"}

    # 2. 趨勢反轉（翻紅或翻綠）：市價全平
    if action in ("CLOSE_LONG", "CLOSE_SHORT"):
        pos_amt, _ = client.get_position_info(sym)
        if abs(pos_amt) > 0:
            client.cancel_all(sym)
            side = "SELL" if "LONG" in action else "BUY"
            client.order(sym, side, abs(pos_amt), reduce_only=True)
            logger.info(f"✅ 趨勢反轉：已全平倉 {abs(pos_amt)} {sym}")
            return {"success": True, "msg": "趨勢反轉：全平倉"}
        return {"success": True, "msg": "目前無持倉，略過平倉"}

    # 3. 進場開倉：雙重防呆 ＋ 2% 風險反算 ＋ Algo 掛止損
    if action in ("LONG", "SHORT"):
        # 🛡️【第三道防線：防重複開倉金鐘罩】
        pos_amt, _ = client.get_position_info(sym)
        if abs(pos_amt) > 0:
            logger.info(f"🛡️ 防重複開倉啟動：幣安帳戶已有 {sym} 持倉 ({pos_amt})，直接略過重複進場訊號。")
            return {"success": True, "msg": f"已有持倉 ({pos_amt})，不重複加倉"}

        if not sig.stop_loss:
            raise HTTPException(status_code=400, detail="必須附帶止損價格")
        if sig.leverage:
            client.set_leverage(sym, sig.leverage)

        equity = client.get_equity()
        curr_p = client.get_price(sym)
        if equity <= 0 or curr_p <= 0:
            raise HTTPException(status_code=400, detail="無法取得帳戶餘額或現價")

        risk_usd = equity * ((sig.risk_pct or 2.0) / 100.0)
        dist = abs(curr_p - sig.stop_loss)
        if dist <= 0:
            raise HTTPException(status_code=400, detail="止損距離為0")
        
        target_qty = client.format_qty(sym, risk_usd / dist)
        if target_qty <= 0:
            raise HTTPException(status_code=400, detail="計算倉位低於最小下單量")

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

        logger.info(f"✅ 開單與強制止損全套成功: 本金={equity}U, 2%風險={risk_usd:.2f}U, 顆數={target_qty}{sym}, 止損價={sig.stop_loss}")
        return {"success": True, "qty": target_qty, "stop_loss": sig.stop_loss}
    
    return {"success": False, "msg": f"未知動作: {action}"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))

import os
import time
import hmac
import hashlib
import logging
import secrets
from typing import Optional, Dict, Any
from urllib.parse import urlencode

import requests
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request, status
from pydantic import BaseModel, Field

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("TradingViewBinanceBot")

BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "")
WEBHOOK_PASSPHRASE = os.getenv("WEBHOOK_PASSPHRASE", "CHANGE_ME_IN_ENV")
USE_TESTNET = os.getenv("USE_TESTNET", "false").lower() in ("true", "1", "yes")

SPOT_BASE_URL = "https://testnet.binance.vision" if USE_TESTNET else "https://api.binance.com"
FUTURES_BASE_URL = "https://testnet.binancefuture.com" if USE_TESTNET else "https://fapi.binance.com"

class BinanceClient:
    def __init__(self, api_key: str, api_secret: str, use_testnet: bool = False):
        self.api_key = api_key
        self.api_secret = api_secret
        self.use_testnet = use_testnet
        self.spot_url = SPOT_BASE_URL
        self.futures_url = FUTURES_BASE_URL
        self.time_offset = 0
        self._sync_time()

    def _sync_time(self):
        try:
            res = requests.get(f"{self.spot_url}/api/v3/time", timeout=5)
            if res.status_code == 200:
                server_time = res.json().get("serverTime", int(time.time() * 1000))
                local_time = int(time.time() * 1000)
                self.time_offset = server_time - local_time
                logger.info(f"Server time synced. Offset: {self.time_offset}ms")
        except Exception as e:
            logger.warning(f"Failed to sync time: {e}")
            self.time_offset = 0

    def _get_timestamp(self) -> int:
        return int(time.time() * 1000) + self.time_offset

    def _sign(self, params: Dict[str, Any]) -> str:
        query_string = urlencode(params)
        return hmac.new(
            self.api_secret.encode("utf-8"),
            query_string.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

    def _headers(self) -> Dict[str, str]:
        return {
            "X-MBX-APIKEY": self.api_key,
            "Content-Type": "application/x-www-form-urlencoded",
        }

    def place_spot_order(self, symbol: str, side: str, order_type: str = "MARKET",
                         quantity: Optional[float] = None, quote_quantity: Optional[float] = None,
                         price: Optional[float] = None) -> Dict[str, Any]:
        endpoint = f"{self.spot_url}/api/v3/order"
        params: Dict[str, Any] = {
            "symbol": symbol.upper(),
            "side": side.upper(),
            "type": order_type.upper(),
            "timestamp": self._get_timestamp(),
            "recvWindow": 5000,
        }
        if order_type.upper() == "MARKET":
            if quote_quantity is not None and side.upper() == "BUY":
                params["quoteOrderQty"] = quote_quantity
            elif quantity is not None:
                params["quantity"] = quantity
            else:
                raise ValueError("Spot MARKET requires quantity or quote_quantity.")
        elif order_type.upper() == "LIMIT":
            params["quantity"] = quantity
            params["price"] = price
            params["timeInForce"] = "GTC"

        params["signature"] = self._sign(params)
        resp = requests.post(endpoint, headers=self._headers(), data=params, timeout=10)
        data = resp.json()
        if resp.status_code != 200:
            raise HTTPException(status_code=400, detail=data)
        return data

    def set_futures_leverage(self, symbol: str, leverage: int) -> Dict[str, Any]:
        endpoint = f"{self.futures_url}/fapi/v1/leverage"
        params = {
            "symbol": symbol.upper(),
            "leverage": leverage,
            "timestamp": self._get_timestamp(),
            "recvWindow": 5000,
        }
        params["signature"] = self._sign(params)
        return requests.post(endpoint, headers=self._headers(), data=params, timeout=10).json()

    def place_futures_order(self, symbol: str, side: str, order_type: str = "MARKET",
                            quantity: Optional[float] = None, price: Optional[float] = None,
                            reduce_only: bool = False) -> Dict[str, Any]:
        endpoint = f"{self.futures_url}/fapi/v1/order"
        params: Dict[str, Any] = {
            "symbol": symbol.upper(),
            "side": side.upper(),
            "type": order_type.upper(),
            "timestamp": self._get_timestamp(),
            "recvWindow": 5000,
        }
        if reduce_only:
            params["reduceOnly"] = "true"
        if order_type.upper() == "MARKET":
            params["quantity"] = quantity
        elif order_type.upper() == "LIMIT":
            params["quantity"] = quantity
            params["price"] = price
            params["timeInForce"] = "GTC"

        params["signature"] = self._sign(params)
        resp = requests.post(endpoint, headers=self._headers(), data=params, timeout=10)
        data = resp.json()
        if resp.status_code != 200:
            raise HTTPException(status_code=400, detail=data)
        return data

class TradingViewSignal(BaseModel):
    passphrase: str
    symbol: str
    action: str
    market_type: str = "spot"
    order_type: str = "MARKET"
    quantity: Optional[float] = None
    quote_quantity: Optional[float] = None
    price: Optional[float] = None
    leverage: Optional[int] = None
    reduce_only: Optional[bool] = False

app = FastAPI(title="TradingView Binance Bot")
binance_client = BinanceClient(BINANCE_API_KEY, BINANCE_API_SECRET, USE_TESTNET)

@app.get("/")
def index():
    return {"status": "online", "service": "TradingView Binance Webhook Bot"}

@app.get("/health")
def health():
    return {"status": "healthy", "timestamp": int(time.time())}

@app.post("/webhook")
async def handle_webhook(signal: TradingViewSignal, request: Request):
    if not secrets.compare_digest(signal.passphrase, WEBHOOK_PASSPHRASE):
        raise HTTPException(status_code=401, detail="Invalid passphrase")

    action = signal.action.upper()
    market = signal.market_type.lower()
    symbol = signal.symbol.upper()

    if market == "spot":
        if action in ("BUY", "LONG"):
            res = binance_client.place_spot_order(symbol, "BUY", signal.order_type, signal.quantity, signal.quote_quantity, signal.price)
        elif action in ("SELL", "SHORT", "CLOSE"):
            res = binance_client.place_spot_order(symbol, "SELL", signal.order_type, signal.quantity, None, signal.price)
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported action: {action}")
    elif market in ("futures", "future", "usdm"):
        if signal.leverage:
            try:
                binance_client.set_futures_leverage(symbol, signal.leverage)
            except Exception:
                pass
        if action in ("BUY", "LONG"):
            res = binance_client.place_futures_order(symbol, "BUY", signal.order_type, signal.quantity, signal.price, signal.reduce_only or False)
        elif action in ("SELL", "SHORT"):
            res = binance_client.place_futures_order(symbol, "SELL", signal.order_type, signal.quantity, signal.price, signal.reduce_only or False)
        elif action in ("CLOSE_LONG", "CLOSE_BUY"):
            res = binance_client.place_futures_order(symbol, "SELL", signal.order_type, signal.quantity, signal.price, reduce_only=True)
        elif action in ("CLOSE_SHORT", "CLOSE_SELL"):
            res = binance_client.place_futures_order(symbol, "BUY", signal.order_type, signal.quantity, signal.price, reduce_only=True)
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported action: {action}")
    else:
        raise HTTPException(status_code=400, detail="Invalid market_type")

    return {"success": True, "order_id": res.get("orderId"), "status": res.get("status")}

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port)

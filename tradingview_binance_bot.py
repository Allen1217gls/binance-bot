class BinanceClient:
    def __init__(self, key: str, sec: str):
        self.key, self.sec = key, sec
        self.time_offset = 0
        self._precisions: Dict[str, Tuple[float, int]] = {}
        self._sync()
        self._load_exchange_info()  # 👈 改成在初始化時一次載入所有交易規則，避免頻繁打 API

    def _sync(self):
        try:
            res = requests.get(f"{FUTURES_URL}/fapi/v1/time", timeout=5).json()
            self.time_offset = res.get("serverTime", int(time.time() * 1000)) - int(time.time() * 1000)
        except Exception:
            self.time_offset = 0

    def _load_exchange_info(self):
        """ ✅ 啟動時一次抓取所有幣別的精度，保護 API 權重 """
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
        # 👈 直接從記憶體字典拿資料，不再發送 requests 請求
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
        res = requests.get(f"{FUTURES_URL}/fapi/v2/positionRisk", headers=self._headers(), params=p).json()
        for pos in res:
            if pos.get("symbol") == sym.upper():
                return abs(float(pos.get("positionAmt", 0.0)))
        return 0.0

#!.venv/bin/python
import asyncio,base64,json,time,websockets,os,logging,redis,requests
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding

from kalshi_ref import KalshiHttpClient
from sortedcontainers import SortedDict

KEY_ID = os.getenv("PROD_KEYID")
PRIVATE_KEY_PATH = os.getenv("PROD_KEYFILE")
WS_URL = "wss://api.elections.kalshi.com/trade-api/ws/v2"
KALSHI_URL = "https://api.elections.kalshi.com"

class KalshiOrderBook:

    def __init__(self,tickers: list[str], r: redis.Redis):
        self.tickers = tickers
        self.r = r
        self.url = "wss://api.elections.kalshi.com/trade-api/ws/v2"
        self.ws: websockets.WebSocketClientProtocol | None = None

        self.reconnect_delay = 1
        self.max_reconnect_delay = 60

        # Map: market_ticker -> {market_id, yes: SortedDict, no: SortedDict}
        self.books = {}

        self.orderbook_delta_id: int | None = None

        self.unsubscribed_event = asyncio.Event()

    def _process_snapshot(self, msg) -> None:
        try:
            ticker = msg["market_ticker"]
            market_id = msg.get("market_id")

            if ticker not in self.books:
                self.books[ticker] = {
                    "market_id": market_id,
                    "yes": SortedDict(lambda x: -x),
                    "no": SortedDict(lambda x: -x),
                }

            self.books[ticker]["yes"].clear()
            self.books[ticker]["no"].clear()

            for side in ("yes", "no"):
                if side in msg:
                    for price, volume in msg[side]:
                        self.books[ticker][side][price] = volume
        except Exception:
            logging.exception("Error processing snapshot: %s", msg)

    def _process_delta(self, msg) -> None:
        try:
            ticker = msg["market_ticker"]
            side = msg["side"]  # "yes" or "no"
            price = msg["price"]
            delta = msg["delta"]

            if ticker not in self.books:
                logging.debug("Delta for unknown ticker %s, ignoring", ticker)
                return

            current = self.books[ticker][side].get(price, 0)
            new = current + delta
            if new > 0:
                self.books[ticker][side][price] = new
            else:
                self.books[ticker][side].pop(price, None)
        except Exception:
            logging.exception("Error processing delta: %s", msg)

    def _emit_top(self, ticker: str) -> None:
        if ticker not in self.books:
            return

        try:
            yes_top = next(iter(self.books[ticker]["yes"]), None)
            no_top = next(iter(self.books[ticker]["no"]), None)

            # Safely fetch volumes and build strings
            if yes_top is not None:
                yes_vol = self.books[ticker]["yes"].get(yes_top, 0)
                no_str = f"{100 - yes_top}@{yes_vol}"
            else:
                no_str = "N/A"

            if no_top is not None:
                no_vol = self.books[ticker]["no"].get(no_top, 0)
                yes_str = f"{100 - no_top}@{no_vol}"
            else:
                yes_str = "N/A"

            try:
                r.hset('tickers', ticker, json.dumps({'no': no_str, 'yes': yes_str}))
            except Exception:
                # put_nowait can raise if the queue is bounded and full
                logging.exception("Failed to enqueue orderbook payload for %s", ticker)
        except Exception:
            logging.exception("Error emitting top-of-book for %s", ticker)

    @staticmethod
    def _sign(priv_key, text: str) -> str:
        sig = priv_key.sign(
            text.encode(),
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH,
            ),
            hashes.SHA256(),
        )
        return base64.b64encode(sig).decode()

    def _auth_headers(self, priv_key):
        ts = str(int(time.time() * 1000))
        sig = self._sign(priv_key, ts + "GET" + "/trade-api/ws/v2")
        try:
            access_key = os.environ["PROD_KEYID"]
        except KeyError:
            logging.error("Environment variable PROD_KEYID not set")
            raise
        return {
            "KALSHI-ACCESS-KEY": access_key,
            "KALSHI-ACCESS-SIGNATURE": sig,
            "KALSHI-ACCESS-TIMESTAMP": ts,
        }

    async def run(self) -> None:
        priv_path = os.environ.get("PROD_KEYFILE")
        if not priv_path:
            logging.error("PROD_KEYFILE env var not set; orderbook task exiting")
            return

        try:
            with open(priv_path, "rb") as f:
                priv_key = serialization.load_pem_private_key(f.read(), password=None)
        except Exception:
            logging.exception("Failed to load private key from %s; orderbook task exiting", priv_path)
            return

        headers = self._auth_headers(priv_key)
        while True:
            try:
                async with websockets.connect(self.url, additional_headers=headers) as ws:
                    self.ws = ws
                    logging.info("WebSocket connected")
                    await self.ws.send(
                        json.dumps(
                            {
                                "id": 1,
                                "cmd": "subscribe",
                                "params": {
                                    "channels": ["orderbook_delta"],
                                    "market_tickers": self.tickers,
                                },
                            }
                        )
                    )
                    async for raw in ws:
                        try:
                            data = json.loads(raw)
                        except json.JSONDecodeError:
                            logging.exception("Failed to decode JSON from upstream: %s", raw)
                            continue

                        msg = data.get("msg", {})
                        typ = data.get("type")

                        if typ == "subscribed" and msg.get("channel") == "orderbook_delta":
                            self.orderbook_delta_id = msg.get("sid")
                        elif typ == "orderbook_snapshot":
                            self._process_snapshot(msg)
                            self._emit_top(msg.get("market_ticker", "unknown"))
                        elif typ == "orderbook_delta":
                            self._process_delta(msg)
                            self._emit_top(msg.get("market_ticker", "unknown"))
                        else:
                            logging.debug("Upstream message of unknown type: %s", data)
            except (ConnectionResetError, websockets.ConnectionClosedError):
                logging.error("connection Reset")
                pass

if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)
    with open(PRIVATE_KEY_PATH, "rb") as f:
        private_key = serialization.load_pem_private_key(f.read(), password=None)
    tickers = []
    response = requests.get('https://api.elections.kalshi.com/trade-api/v2/markets', {'series_ticker': 'KXATPMATCH', 'status': 'open'})
    tickers.extend([m['ticker'] for m in response.json()['markets']])
    response = requests.get('https://api.elections.kalshi.com/trade-api/v2/markets', {'series_ticker': 'KXEPLGAME', 'status': 'open'})
    tickers.extend([m['ticker'] for m in response.json()['markets']])
    print(tickers)
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    client = KalshiHttpClient(os.getenv("PROD_KEYID"),private_key)
    response = client.get('/trade-api/v2/portfolio/positions')
    orderbook = KalshiOrderBook(tickers,r)

    asyncio.run(orderbook.run())
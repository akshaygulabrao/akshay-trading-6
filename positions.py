#!.venv/bin/python
import asyncio,base64,json,time,websockets,os,logging,redis
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding

from kalshi_ref import KalshiHttpClient


KEY_ID = os.getenv("PROD_KEYID")
PRIVATE_KEY_PATH = os.getenv("PROD_KEYFILE")
WS_URL = "wss://api.elections.kalshi.com/trade-api/ws/v2"
KALSHI_URL = "https://api.elections.kalshi.com"

def create_headers(private_key, method: str, path: str) -> dict:
    """Create authentication headers"""
    timestamp = str(int(time.time() * 1000))
    msg_string = timestamp + method + path.split("?")[0]
    signature = sign_pss_text(private_key, msg_string)

    return {
        "Content-Type": "application/json",
        "KALSHI-ACCESS-KEY": KEY_ID,
        "KALSHI-ACCESS-SIGNATURE": signature,
        "KALSHI-ACCESS-TIMESTAMP": timestamp,
    }
def sign_pss_text(private_key, text: str) -> str:
    """Sign message using RSA-PSS"""
    message = text.encode("utf-8")
    signature = private_key.sign(
        message,
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
        hashes.SHA256(),
    )
    return base64.b64encode(signature).decode("utf-8")


async def track_positions(r: redis.Redis):
    with open(PRIVATE_KEY_PATH, "rb") as f:
        private_key = serialization.load_pem_private_key(f.read(), password=None)
    ws_headers = create_headers(private_key, "GET", "/trade-api/ws/v2")
    while True:
        try:
            async with websockets.connect(WS_URL, additional_headers=ws_headers) as websocket:
                    mkt_pos_subscribe_msg = {
                        "id": 1,
                        "cmd": "subscribe",
                        "params": {
                            "channels": ["market_positions"],
                        },
                    }
                    await websocket.send(json.dumps(mkt_pos_subscribe_msg))
                    mkt_pos_subscribe_msg = {
                        "id": 2,
                        "cmd": "subscribe",
                        "params": {
                            "channels": ["fill"],
                        },
                    }
                    await websocket.send(json.dumps(mkt_pos_subscribe_msg))
                    async for message in websocket:
                        message = json.loads(message)
                        logging.info(message)
                        if message["type"] == "market_position":
                            update = message["msg"]
                            r.hset('positions', mapping = {update['market_ticker']:update['position']})
                        elif message["type"] == "fill":
                            pass
        except ConnectionResetError:
            pass
        except asyncio.CancelledError:
            logging.error("cancelled")
            raise

if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)
    with open(PRIVATE_KEY_PATH, "rb") as f:
        private_key = serialization.load_pem_private_key(f.read(), password=None)
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    client = KalshiHttpClient(os.getenv("PROD_KEYID"),private_key)
    response = client.get('/trade-api/v2/portfolio/positions')
    for mkt in response['market_positions']:
        logging.info((mkt['ticker'],mkt['position']))
        r.hset('positions',mapping = {mkt['ticker']: mkt['position']})
        
    asyncio.run(track_positions(r))
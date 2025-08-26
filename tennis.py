import json
import logging
import os
import re
import uuid
from cryptography.hazmat.primitives import serialization
from kalshi_ref import KalshiHttpClient
import redis

team2mkt = {"Gabriel Diallo": {"long": "KXATPMATCH-25AUG25DIADZU-DIA", "short": "KXATPMATCH-25AUG25DIADZU-DZU"}, "Damir Dzumhur": {"long": "KXATPMATCH-25AUG25DIADZU-DZU", "short": "KXATPMATCH-25AUG25DIADZU-DIA"}}
team2odds = {}
team2opp_list = [("Reilly Opelka", "Carlos Alcaraz")]
team1longbid = '',''
team2opp =  {}
for a, b in team2opp_list:
    team2opp[a] = b; team2opp[b] = a

def maybe_place_order(team1, odds1, team2, odds2, vig):
    """
    Place or amend a *single* limit buy on the YES side of the
    market that backs `team1`.  Order tracking is now done via
    Redis keys  orders:<ticker>
    """
    try:
        long_ticker  = team2mkt[team1]["long"]
        short_ticker = team2mkt[team1]["short"]

        positions_long  = int(r.hget("positions", long_ticker)  or 0)
        positions_short = int(r.hget("positions", short_ticker) or 0)
        logging.info(f"{positions_long},{positions_short}")

        # Active order id / client id stored in Redis
        order_key = f"orders:{long_ticker}"
        active = r.hmget(order_key, "order_id", "client_id")
        active_order_id, active_client_id = active[0], active[1]

        # Price we want to bid
        target_price = odds1 - 1 - round(vig * 50)

        # CASE 1 – no position and no active order  →  create new
        if positions_long == 0 and active_order_id is None:
            c_uid = str(uuid.uuid4())
            order_bid = {
                "ticker": long_ticker,
                "side": "yes",
                "action": "buy",
                "count": 1,
                "type": "limit",
                "yes_price": target_price,
                "client_order_id": c_uid,
                "post_only": True,
            }
            resp = client.post("/trade-api/v2/portfolio/orders", order_bid)
            new_oid = resp["order"]["order_id"]
            r.hset(order_key, mapping={"order_id": new_oid, "client_id": c_uid})
            return

        # CASE 2 – no position, but we have an active order  →  amend
        elif positions_long == 0 and active_order_id is not None:
            new_c_uid = str(uuid.uuid4())
            amend = {
                "ticker": long_ticker,
                "side": "yes",
                "client_order_id": active_client_id,
                "updated_client_order_id": new_c_uid,
                "action": "buy",
                "count": 1,
                "type": "limit",
                "yes_price": target_price,
                "post_only": True,
            }
            resp = client.post(
                f"/trade-api/v2/portfolio/orders/{active_order_id}/amend", amend
            )
            new_oid = resp["order"]["order_id"]
            r.hset(order_key, mapping={"order_id": new_oid, "client_id": new_c_uid})
            return

        # CASE 3 – we already have a position, do nothing
        # (optional cleanup if you want to cancel stale orders)
    except Exception:
        logging.exception("maybe_place_order failed")
        raise





def convert_odds(odds1, odds2):
    """Convert moneyline odds to probabilities and return vig."""
    prob = lambda o: 100/(float(o)+100) if float(o) > 0 else -float(o)/(-float(o)+100)
    p1, p2 = prob(odds1), prob(odds2)
    total = p1 + p2
    return p1/total, p2/total, total - 1

def process_message(msg):
    """Process incoming Redis messages."""
    try:
        if not (msg[0] in [17,24]):
            return
        if msg[0] == 24: # opponents come in seperately
            team1 = msg[2]
            team1_odds = re.sub(r'[−–—]', '-', msg[3])
            team2odds[team1] = team1_odds
            team2 = team2opp.get(team1,None)
            if not team2:
                print("Opposing team not found")
                return
            team2_odds = team2odds.get(team2,None)
            if not team2_odds:
                print("Opposing odds not found")
                return
            
        elif msg[0] == 17 and len(msg) > 31:
            team1_odds = re.sub(r'[−–—]', '-', msg[12])
            team2_odds = re.sub(r'[−–—]', '-', msg[22])
            team1 = msg[10]
            team2 = msg[20]
            team2odds[team1] = team1_odds
            team2odds[team2] = team2_odds
        elif msg[0] == 17 and len(msg) == 31:
            team1_odds = re.sub(r'[−–—]', '-', msg[12])
            team1 = msg[10]
            team2odds[team1] = team1_odds
            team2 = team2opp.get(team1,None)
            if not team2:
                print("Opposing team not found")
                return
            team2_odds = team2odds.get(team2,None)
            if not team2_odds:
                print("Opposing odds not found")
                return

        print(team1,team1_odds, team2,team2_odds)
        probs1,probs2,vig= convert_odds(team1_odds,team2_odds)
        odds1 = round(probs1 * 100)
        odds2 = round(probs2 * 100)
        print(team1,odds1,team2,odds2,vig)
        if team1 in team2mkt or team2 in team2mkt:
            maybe_place_order(team1,odds1,team2,odds2,vig)

    except:
        print(locals())
        raise

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)
r = redis.Redis(host='localhost', port=6379, decode_responses=True)
pubsub = r.pubsub(ignore_subscribe_messages=True)
pubsub.subscribe('us-open-men')
with open(os.getenv("PROD_KEYFILE"), "rb") as f:
    private_key = serialization.load_pem_private_key(f.read(), password=None)
client = KalshiHttpClient(os.getenv("PROD_KEYID"), private_key)
logging.info(client.get_balance())


for message in pubsub.listen():
    process_message(json.loads(message['data']))
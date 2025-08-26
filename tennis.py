import json
import logging
import os
import re
import uuid
from cryptography.hazmat.primitives import serialization
from kalshi_ref import KalshiHttpClient
import redis

team2mkt = {"Jan-Lennard Struff": {"long": "KXATPMATCH-25AUG25STRMCD-STR", "short": "KXATPMATCH-25AUG25STRMCD-MCD"}}
team2odds = {}
team2opp = [("Jan-Lennard Struff","Mackenzie McDonald"),("Hugo Dellien","Kamil Majchrzak")]

for a, b in team2opp:
    team2opp[a] = b; team2opp[b] = a

def maybe_place_order(team1, odds1, team2, odds2):
    positions_long = r.hget("positions", team2mkt[team1]["long"])
    positions_short = r.hget("positions", team2mkt[team1]["short"])
    positions_long = int(positions_long) if positions_long is not None else 0
    positions_short = int(positions_short) if positions_short is not None else 0
    logging.info(f"{positions_long},{positions_short}")

    if positions_long == 0 and len(team1longbid) == 0:
        order_bid = {
            "ticker": team2mkt[team1]["long"],
            "side": "yes",
            "action": "buy",
            "count": 1,
            "type" : "limit",
            "yes_price": odds1 - 1,
            "client_order_id": str(uuid.uuid4()),
            "post_only": True,
        }
        print(order_bid)
        # order_id = client.post('/trade-api/v2/portfolio/orders',order_bid)
        # team1longbid.add(order_id)
        print(team1longbid)
    else:
        print(positions_long,team1longbid)




def convert_odds(odds1, odds2):
    """Convert moneyline odds to probabilities."""
    prob = lambda o: 100/(float(o)+100) if float(o) > 0 else -float(o)/(-float(o)+100)
    p1, p2 = prob(odds1), prob(odds2)
    total = p1 + p2
    return p1/total, p2/total

def process_message(msg):
    """Process incoming Redis messages."""
    try:
        print(msg)
        if not (msg[0] in [17,24]):
            return
        if msg[0] == 24: # opponents come in seperately
            team1 = msg[2]
            team1_odds = re.sub(r'[−–—]', '-', msg[3])
            if team1 not in team2odds:
                team2odds[team1] = team1_odds
            
            team2 = team2opp[team1]
            if team2 in team2odds:
                team2_odds = team2odds[team2]
            else:
                print("Opposing odds not found")
                return
            
        elif msg[0] == 17 and len(msg) > 31:
            team1_odds = re.sub(r'[−–—]', '-', msg[12])
            team2_odds = re.sub(r'[−–—]', '-', msg[22])
            team1 = msg[10]
            team2 = msg[20]

        print(team1,team1_odds, team2,team2_odds)
        probs1,probs2= convert_odds(team1_odds,team2_odds)
        odds1 = round(probs1 * 100)
        odds2 = round(probs2 * 100)
        print(team1,odds1,team2,odds2)
        if team1 in team2mkt:
            maybe_place_order(team1,odds1,team2,odds2)

    except:
        print(msg)
        raise

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)
r = redis.Redis(host='localhost', port=6379, decode_responses=True)
pubsub = r.pubsub(ignore_subscribe_messages=True)
pubsub.subscribe('us-open-men')
with open(os.getenv("PROD_KEYFILE"), "rb") as f:
    private_key = serialization.load_pem_private_key(f.read(), password=None)
client = KalshiHttpClient(os.getenv("PROD_KEYID"), private_key)
logging.info(client.get_balance())
team1longbid = set()

for message in pubsub.listen():
    process_message(json.loads(message['data']))
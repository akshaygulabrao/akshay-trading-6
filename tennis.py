import json
import logging
import os
import re
import uuid
from cryptography.hazmat.primitives import serialization
from kalshi_ref import KalshiHttpClient
import redis

team2mkt = {"Daniil Medvedev": {"long": "KXATPMATCH-25AUG24MEDBON-MED", "short": "KXATPMATCH-25AUG24MEDBON-BON"}}

def maybe_place_order(team1, odds1, team2, odds2):
    positions_long = r.hget("positions", team2mkt[team1]["long"])
    positions_short = r.hget("positions", team2mkt[team1]["short"])
    positions_long = positions_long if positions_long is not None else 0
    positions_short = positions_short if positions_short is not None else 0
    logging.info(f"{positions_long},{positions_short}")


def convert_odds(odds1, odds2):
    """Convert moneyline odds to probabilities."""
    prob = lambda o: 100/(float(o)+100) if float(o) > 0 else -float(o)/(-float(o)+100)
    p1, p2 = prob(odds1), prob(odds2)
    total = p1 + p2
    return p1/total, p2/total

def process_message(msg):
    """Process incoming Redis messages."""
    try:
        if not ((msg[2] == "Moneyline" and msg[0] == 17)) or msg[22] in ["FeaturedSubcategory", "PrimaryMarket"]:
            return

        # Extract and clean team data
        team1_odds = re.sub(r'[−–—]', '-', msg[12])
        team2_odds = re.sub(r'[−–—]', '-', msg[22])
        
        try:
            prob1, prob2 = convert_odds(team1_odds, team2_odds)
        except Exception as e:
            logging.error(f"Failed to convert odds: {msg}")
            raise

        # Print market information
        team1, odds1 = msg[10], round(prob1 * 100)
        team2, odds2 = msg[20], round(prob2 * 100)
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

for message in pubsub.listen():
    process_message(json.loads(message['data']))
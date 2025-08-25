#!/usr/bin/env python3
import json
import logging
import os
import re
import uuid
from cryptography.hazmat.primitives import serialization
from kalshi_ref import KalshiHttpClient
import redis

# Initialize Redis connection
r = redis.Redis(host='localhost', port=6379, decode_responses=True)
pubsub = r.pubsub(ignore_subscribe_messages=True)
pubsub.subscribe('baseball')

team2kalshi_mkts = {"CIN Reds": {"long": "KXMLBGAME-25AUG24CINAZ-CIN", "short": "KXMLBGAME-25AUG24CINAZ-AZ"},
                    "ARI Diamondbacks": {"long": "KXMLBGAME-25AUG24CINAZ-AZ", "short": "KXMLBGAME-25AUG24CINAZ-CIN"},
                    "LA Dodgers": {"long": "KXMLBGAME-25AUG24LADSD-LAD", "short": "KXMLBGAME-25AUG24LADSD-SD"}, 
                    "SD Padres": {"long": "KXMLBGAME-25AUG24LADSD-SD", "short": "KXMLBGAME-25AUG24LADSD-LAD"}
                    }

# Initialize Kalshi client
with open(os.getenv("PROD_KEYFILE"), "rb") as f:
    private_key = serialization.load_pem_private_key(f.read(), password=None)
client = KalshiHttpClient(os.getenv("PROD_KEYID"), private_key)
logging.info(client.get_balance())

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
        


    except:
        print(msg)
        raise

def place_order(ticker, price):
    """Place a buy order if no position exists."""
    pos = r.hget('positions', ticker)
    pos = pos if pos is not None else 0

    order = {
        'client_order_id': str(uuid.uuid4()),
        'action': 'buy',
        'side': 'yes',
        'ticker': ticker,
        'type': 'limit',
        'yes_price': price,
        'count': 1,
        'post_only': True
    }
    logging.info(f"Placing order: {order}")


print("Listening...")
for message in pubsub.listen():
    process_message(json.loads(message['data']))
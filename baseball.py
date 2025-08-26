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

team2kalshi_id = {"CIN Reds": "CIN", "ARI Diamondbacks": "AZ", "LA Dodgers": "LAD", "SD Padres": "SD",
                  "SEA Mariners": "SEA", "Athletics": "ATH", "DET Tigers": "DET"}


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
        if msg[0] == 17 and len(msg) > 31:
            team1_odds = re.sub(r'[−–—]', '-', msg[12])
            team2_odds = re.sub(r'[−–—]', '-', msg[22])
            team1 = msg[10]
            team2 = msg[20]
            team1_kalshi = team2kalshi_id.get(team1,None)
            team2_kalshi = team2kalshi_id.get(team2,None)
            r.hset(f"baseball:odds", mapping = {team1: team1_odds, team2: team2_odds})
            print(f"{team1_kalshi}: {team1_odds}, {team2_kalshi}: {team2_odds}")

        elif msg[0] == 17 and len(msg) == 31:
            team1_odds = re.sub(r'[−–—]', '-', msg[12])
            team1 = msg[10]
            print(team1,team1_odds)
        elif msg[0] == 24 and "ML" in msg[1]:
            team1 = msg[2]
            team1_odds = re.sub(r'[−–—]', '-', msg[3])
            team1_kalshi = team2kalshi_id.get(team1,None)
            r.hset(f"baseball:odds", mapping = {team1: team1_odds})
            print(f"{team1_kalshi}: {team1_odds}")
        

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
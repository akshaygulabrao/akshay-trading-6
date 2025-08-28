#!.venv/bin/python
from dataclasses import dataclass
import json, requests
from datetime import datetime
import logging
import os
import re
import uuid
from cryptography.hazmat.primitives import serialization
from kalshi_ref import KalshiHttpClient
import redis
import utils

global player2tickers,player2opp,allowed_to_trade,client,r

r = redis.Redis(host="localhost", port=6379, decode_responses=True)
with open(os.getenv("PROD_KEYFILE"), "rb") as f:
    private_key = serialization.load_pem_private_key(f.read(), password=None)
client = KalshiHttpClient(os.getenv("PROD_KEYID"), private_key)
player2tickers, player2opp = utils.get_tennis_mappings()
allowed_to_trade = set(["Jacob Fearnley"])




def convert_odds(odds1, odds2):
    """Convert moneyline odds to probabilities and return vig."""
    prob = lambda o: (
        100 / (float(o) + 100) if float(o) > 0 else -float(o) / (-float(o) + 100)
    )
    p1 = prob(odds1)

    if odds2 is None:
        return p1, None, None

    p2 = prob(odds2)
    total = p1 + p2
    return p1, p2, total - 1


def maybe_place_order(team1, odds1, team2, odds2):
    """Modified to adjust orders based on existing exposure and edge requirements."""
    try:
        if odds2 > odds1:
            team1,odds1,team2,odds2 = team2,odds2,team1,odds1
        
        sell_side1, buy_side1 = player2tickers[team1]
        
        positions = lambda t: int(r.hget("positions", t) or 0)
        net1 = positions(sell_side1) - positions(buy_side1)
        net2 = -net1
        
        odds1,odds2 = round(odds1*100),round(odds2*100)
        logging.info(f"{team1,odds1,team2,odds2}")

        def order_packet(ticker,action,price):
            order = {
                "ticker": ticker,
                "action": action,
                "side": "yes",
                "type": "limit",
                "yes_price": price,
                "count": 1,
                "client_order_id": str(uuid.uuid4()),
                "post_only": True
            }
            return order
        
        sell_side1_buy = r.hget("orders", f"{sell_side1}:buy")
        sell_side1_sell = r.hget("orders", f"{sell_side1}:sell")
        buy_side1_buy = r.hget("orders", f"{buy_side1}:buy")
        buy_side1_sell = r.hget("orders", f"{buy_side1}:sell")


        def create_order(ticker, action, price):
            """Create and place a new order"""
            order = order_packet(ticker, action, price)
            client_id = order['client_order_id']
            try:
                response = client.post("/trade-api/v2/portfolio/orders", order)
                public_id = response['order']['order_id']
                r.hset("orders", f"{ticker}:{action}", f"{public_id}:{client_id}")
                return True
            except requests.exceptions.HTTPError:
                logging.error(f"Order Creation failed for {ticker} {action}, becasuse it would execute as a market order")
                print(order)
                return False

        def update_order(ticker, action, price, redis_value):
            """Update an existing order"""
            public_id, private_id = redis_value.split(":")
            order = order_packet(ticker, action, price)
            order["client_order_id"] = private_id
            new_private_id = str(uuid.uuid4())
            order['updated_client_order_id'] = new_private_id
            try:
                public_id = client.post(f'/trade-api/v2/portfolio/orders/{public_id}/amend', order)
                r.hset('orders', f"{ticker}:{action}", f"{public_id['order']['order_id']}:{new_private_id}")
                return True
            except requests.exceptions.HTTPError:
                logging.error(f"Order update failed for {ticker} {action}, because it would become marketable")
                print(order)
                return False

        def manage_order(ticker, action, price, redis_value):
            """Manage order creation or update based on whether it exists in Redis"""
            if redis_value is None:
                return create_order(ticker, action, price)
            else:
                return update_order(ticker, action, price, redis_value)

        # Now the main logic becomes much simpler:
        logging.info("net1 %s", net1)
        manage_order(sell_side1, "buy", 100 - odds2, sell_side1_buy)
        manage_order(buy_side1, "sell", odds2, buy_side1_sell)
        manage_order(sell_side1, "sell", odds1, sell_side1_sell)
        manage_order(buy_side1, "buy", 100 - odds1, buy_side1_buy)

    except Exception:
        logging.exception("maybe_place_order failed")
        raise

def process_message(msg):
    """Process incoming Redis messages."""
    try:
        team1 = ""
        if msg[0] not in [17, 24]:
            return
        if msg[0] == 24 and "ML" not in msg[1]:
            return
        if msg[0] == 17:
            # Process message type 17
            teams = []
            for i in range(len(msg)):
                if isinstance(msg[i], str) and "ML" in msg[i]:
                    teams.append([msg[i + 1], msg[i + 3]])
            
            if len(teams) >= 2:
                # Both teams have odds
                team1, team1_odds = teams[0]
                team2, team2_odds = teams[1]
            elif len(teams) == 1:
                # Only one team has odds, set other to 100%
                team1, team1_odds = teams[0]
                team2 = player2opp[team1]
                team2_odds = "-10000"
            
            # Clean and store odds
            if team1_odds:
                team1_odds = re.sub(r"[−–—]", "-", team1_odds)
                r.hset("us-open-men:odds", team1, team1_odds)
            if team2_odds:
                team2_odds = re.sub(r"[−–—]", "-", team2_odds)
                r.hset("us-open-men:odds", team2, team2_odds)

        elif msg[0] == 24 and "ML" in msg[1]:
            team1 = msg[2]
            team1_odds = re.sub(r"[−–—]", "-", msg[3])
            r.hset(f"us-open-men:odds", mapping={team1: team1_odds})
            if team1 not in player2opp:
                return
            team2 = player2opp[team1]
            team2_odds = r.hget("us-open-men:odds", team2)

        logging.info("%s", f"{team1,team1_odds,team2,team2_odds}")
        team1_odds, team2_odds, vig = convert_odds(team1_odds, team2_odds)
        if team1 in player2tickers and team2 in player2tickers and \
            (team1 in allowed_to_trade or team2 in allowed_to_trade):
            maybe_place_order( team1, team1_odds, team2, team2_odds)

    except:
        print(locals())
        raise


if __name__ == "__main__":
    logging.basicConfig(format="%(asctime)s %(message)s", level=logging.INFO)
    pubsub = r.pubsub(ignore_subscribe_messages=True)
    pubsub.subscribe("us-open-men")
    logging.info(client.get_balance())

    for message in pubsub.listen():
        process_message( json.loads(message["data"]))

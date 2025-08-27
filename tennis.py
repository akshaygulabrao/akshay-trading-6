#!.venv/bin/python
import json,requests
from datetime import datetime
import logging
import os
import re
import uuid
from cryptography.hazmat.primitives import serialization
from kalshi_ref import KalshiHttpClient
import redis

def get_tennis_mappings():
    markets = requests.get(
        "https://api.elections.kalshi.com/trade-api/v2/markets",
        params={"series_ticker": "KXATPMATCH", "status": "open"}
    ).json()['markets']
    
    # Group by match using the vs pattern in titles
    matches = {}
    for m in markets:
        if match := re.search(r'the\s+(.+?)\s+match', m['title']):
            match_name = match.group(1)
            player_name = re.search(r'Will\s+(.+?)\s+win', m['title']).group(1)
            matches.setdefault(match_name, {})[player_name] = m['ticker']
    
    # Create both mappings
    player2tickers = {}
    player2opp = {}
    
    for players in matches.values():
        if len(players) == 2:
            player_list = list(players.items())
            p1, t1 = player_list[0]
            p2, t2 = player_list[1]
            
            # Create ticker mapping
            player2tickers[p1] = [t1, t2]
            player2tickers[p2] = [t2, t1]
            
            # Create opponent mapping (two-way)
            player2opp[p1] = p2
            player2opp[p2] = p1
    print("Player -> [own_ticker, opp_ticker]:")
    for player, tickers in player2tickers.items():
        print(f"{player:25} -> [{tickers[0]}, {tickers[1]}]")

    print("\nPlayer -> opponent:")
    for player, opponent in player2opp.items():
        print(f"{player:25} -> {opponent}")
    return player2tickers, player2opp


def convert_odds(odds1, odds2):
    """Convert moneyline odds to probabilities and return vig."""
    prob = lambda o: 100/(float(o)+100) if float(o) > 0 else -float(o)/(-float(o)+100)
    p1 = prob(odds1)
    
    if odds2 is None:
        return p1, None, None
    
    p2 = prob(odds2)
    total = p1 + p2
    return p1, p2, total - 1

def maybe_place_order(team1, odds1, team2, odds2, vig,client:KalshiHttpClient,r:redis.Redis):
    try:
        # logging.info(f"%s",locals())
        assert player2opp[team1] == team2 and player2opp[team2] == team1
        sell_side1, buy_side1  = player2tickers[team1]
        sell_side2,buy_side2 = player2tickers[team2]

        positions_long1  = int(r.hget("positions", sell_side1)  or 0)
        positions_short1 = int(r.hget("positions", buy_side1) or 0)
        logging.info(f"{positions_long1},{positions_short1}")

        positions_long2 = int(r.hget("positions", sell_side2) or 0)
        positions_short2 = int(r.hget("positions", buy_side2) or 0)
        logging.info(f"{positions_long2},{positions_short2}")

        logging.info(f"limit sell {sell_side1} @ {odds1:0.2f}")
        logging.info(f"limit buy {buy_side1} @ { 1- odds1:0.2f}")
        
        client_order = r.hget('orders', f'{sell_side1}:sell')
        public_order, client_order = (None, None) if client_order is None else client_order.split(':')
        if positions_long1 == 0 and positions_short1 == 0 and public_order is None and client_order is None:
            client_market_order_id_sell = str(uuid.uuid4())
            market_order_sell = {
                'ticker': sell_side1,
                'side' : 'yes',
                'action': 'sell',
                'count': 1,
                'type': 'market',
                'buy_max_cost': round(odds1*100),
                'client_order_id': client_market_order_id_sell
            }
            client_market_order_id_buy = str(uuid.uuid4())
            market_order_buy = {
                'ticker': buy_side1,
                'side' : 'yes',
                'action': 'buy',
                'count': 1,
                'type': 'market',
                'buy_max_cost': round((1-odds1)*100),
                'client_order_id': client_market_order_id_buy
            }
            try:
                client.post('/trade-api/v2/portfolio/orders',market_order_buy)
                client.post('/trade-api/v2/portfolio/orders',market_order_sell)
                logging.info("filled???")
            except requests.exceptions.HTTPError:
                logging.info("Coudln't fill (obviously)")
                pass
        else:
            logging.info("existing position or limit order")
            


        
        # logging.info(f"limit sell {sell_side2} @ {odds2:0.2f}")
        # logging.info(f"limit buy {buy_side2} @ { 1- odds2:0.2f}")




        
        # logging.info(f"limit sell {sell_side2} @ {odds2:0.2f}")



    #     # Active order id / client id stored in Redis
    #     order_key = f"orders:{long_ticker}"
    #     active = r.hmget(order_key, "order_id", "client_id")
    #     active_order_id, active_client_id = active[0], active[1]

    #     # Price we want to bid
    #     target_price = odds1 - 1 - round(vig * 50)

    #     # CASE 1 – no position and no active order  →  create new
    #     if positions_long == 0 and active_order_id is None:
    #         c_uid = str(uuid.uuid4())
    #         order_bid = {
    #             "ticker": long_ticker,
    #             "side": "yes",
    #             "action": "buy",
    #             "count": 1,
    #             "type": "limit",
    #             "yes_price": target_price,
    #             "client_order_id": c_uid,
    #             "post_only": True,
    #         }
    #         resp = client.post("/trade-api/v2/portfolio/orders", order_bid)
    #         new_oid = resp["order"]["order_id"]
    #         r.hset(order_key, mapping={"order_id": new_oid, "client_id": c_uid})
    #         return

    #     # CASE 2 – no position, but we have an active order  →  amend
    #     elif positions_long == 0 and active_order_id is not None:
    #         new_c_uid = str(uuid.uuid4())
    #         amend = {
    #             "ticker": long_ticker,
    #             "side": "yes",
    #             "client_order_id": active_client_id,
    #             "updated_client_order_id": new_c_uid,
    #             "action": "buy",
    #             "count": 1,
    #             "type": "limit",
    #             "yes_price": target_price,
    #             "post_only": True,
    #         }
    #         resp = client.post(
    #             f"/trade-api/v2/portfolio/orders/{active_order_id}/amend", amend
    #         )
    #         new_oid = resp["order"]["order_id"]
    #         r.hset(order_key, mapping={"order_id": new_oid, "client_id": new_c_uid})
    #         return

    #     # CASE 3 – we already have a position, do nothing
    #     # (optional cleanup if you want to cancel stale orders)
    except Exception:
        logging.exception("maybe_place_order failed")
        raise







def process_message(msg, player2tickers, players2opp,allowed_to_trade,client,r):
    """Process incoming Redis messages."""
    try:
        if msg[0] not in [17,24]: return
        if msg[0] == 17 and len(msg) > 31:
            teams = []
            for i in range(len(msg)):
                if isinstance(msg[i],str) and "ML" in msg[i]:
                    teams.append([msg[i+1],msg[i+3]])
            team1,team1_odds = teams[0]
            team2,team2_odds = teams[1]
            team1_odds = re.sub(r'[−–—]', '-', team1_odds)
            team2_odds = re.sub(r'[−–—]', '-', team2_odds)

            r.hset(f"us-open-men:odds", mapping = {team1: team1_odds, team2: team2_odds})

        elif msg[0] == 17 and len(msg) == 31:
            team1_odds = re.sub(r'[−–—]', '-', msg[12])
            team1 = msg[10]
            r.hset("us-open-men:odds", team1, team1_odds)
            team2 = player2opp[team1]
            team2_odds = None
            r.hdel("us-open-men:odds", team2)

        elif msg[0] == 24 and "ML" in msg[1]:
            team1 = msg[2]
            team1_odds = re.sub(r'[−–—]', '-', msg[3])
            r.hset(f"us-open-men:odds", mapping = {team1: team1_odds})
            team2 = player2opp[team1]
            team2_odds = r.hget("us-open-men:odds", team2)
            
        logging.info(f"{team1,team1_odds,team2,team2_odds}")
        team1_odds,team2_odds,vig = convert_odds(team1_odds,team2_odds)
        if team1 in allowed_to_trade or team2 in allowed_to_trade:
            maybe_place_order(team1,team1_odds,team2, team2_odds,vig,client,r)


    except:
        print(locals())
        raise


if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    pubsub = r.pubsub(ignore_subscribe_messages=True)
    pubsub.subscribe('us-open-men')
    with open(os.getenv("PROD_KEYFILE"), "rb") as f:
        private_key = serialization.load_pem_private_key(f.read(), password=None)
    client = KalshiHttpClient(os.getenv("PROD_KEYID"), private_key)
    logging.info(client.get_balance())

    player2tickers, player2opp = get_tennis_mappings()
    allowed_to_trade = set(["Roberto Bautista Agut"])

    for message in pubsub.listen():
        process_message(json.loads(message['data']),player2tickers,player2opp,allowed_to_trade,client,r)
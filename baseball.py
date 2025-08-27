#!.venv/bin/python
import json,requests
import logging
import os
import re
import uuid
from cryptography.hazmat.primitives import serialization
from kalshi_ref import KalshiHttpClient
import redis
from datetime import datetime


def get_baseball_mappings():
    team2kalshi_id = {
    "CIN Reds": "CIN",
    "ARI Diamondbacks": "AZ",
    "LA Dodgers": "LAD",
    "SD Padres": "SD",
    "SEA Mariners": "SEA",
    "Athletics": "ATH",
    "DET Tigers": "DET",
    "WAS Nationals": "WSH",
    "NY Yankees": "NYY",
    "TB Rays": "", 
    "CLE Guardians": "CLE",  
    "ATL Braves": "ATL",     
    "MIA Marlins": "",      
    "PHI Phillies": "",     
    "MIL Brewers": "MIL",
    "KC Royals": "KC",
    "CHI White Sox": "",    
    "COL Rockies": "",      
    "HOU Astros": "",       
    "CHI Cubs": "CHC",       
    "SF Giants": "SF",       
    "BAL Orioles": "",      
    "BOS Red Sox": "",      
    "TOR Blue Jays": "",    
    "MIN Twins": "MIN",      
    "LA Angels": "",         
    "Texas Rangers": "",     
    "New York Mets": "",     
    "Pittsburgh Pirates": "", 
    "St. Louis Cardinals": "" 
    }
    markets = requests.get("https://api.elections.kalshi.com/trade-api/v2/markets", params={"series_ticker": "KXMLBGAME", "status": "open"}).json()['markets']
    k = {v: k for k, v in team2kalshi_id.items() if v}
    p2o, p2t = {}, {}
    s = set()
    for m in markets:
        t = m['ticker']
        parts = t.split('-')
        if len(parts) < 3: continue
        g = f"{parts[0]}-{parts[1]}"
        if g in s: continue
        s.add(g)
        ts = parts[1][7:]
        t1, t2 = None, None
        for l in range(2,4):
            c = ts[:l]
            if c in k:
                t1, t2 = c, ts[l:]
                break
        if not t1 or not t2:
            for l in range(2,4):
                c = ts[-l:]
                if c in k:
                    t2, t1 = c, ts[:-l]
                    break
        if not t1 or not t2 or t1 not in k or t2 not in k: continue
        n1, n2 = k[t1], k[t2]
        p2o[n1], p2o[n2] = n2, n1
        p2t[n1] = [f"{g}-{t1}", f"{g}-{t2}"]
        p2t[n2] = [f"{g}-{t2}", f"{g}-{t1}"]
    return p2o, p2t



def convert_odds(odds1, odds2):
    """Convert moneyline odds to probabilities and return vig."""
    prob = lambda o: 100/(float(o)+100) if float(o) > 0 else -float(o)/(-float(o)+100)
    p1 = prob(odds1)
    
    if odds2 is None:
        return p1, None, None
    
    p2 = prob(odds2)
    total = p1 + p2
    return p1, p2, total - 1

def process_message(msg, player2tickers, player2opp,allowed_to_trade,client,r):
    """Process incoming Redis messages."""
    try:
        if msg[0] not in [17,24]: return
        if msg[0] == 17 and msg[2] != "Moneyline": return
        if msg[0] == 17 and len(msg) > 31 and msg[2] == "Moneyline":
            teams = []
            for i in range(len(msg)):
                if isinstance(msg[i],str) and "ML" in msg[i]:
                    teams.append([msg[i+1],msg[i+3]])
            team1,team1_odds = teams[0]
            team2,team2_odds = teams[1]
            team1_odds = re.sub(r'[−–—]', '-', team1_odds)
            team2_odds = re.sub(r'[−–—]', '-', team2_odds)

            r.hset(f"baseball:odds", mapping = {team1: team1_odds, team2: team2_odds})

        elif msg[0] == 17 and len(msg) == 31:
            team1_odds = re.sub(r'[−–—]', '-', msg[12])
            team1 = msg[10]
            r.hset("baseball:odds", team1, team1_odds)
            team2 = player2opp[team1]
            team2_odds = None
            r.hdel("baseball:odds", team2)

        elif msg[0] == 24 and "ML" in msg[1]:
            team1 = msg[2]
            team1_odds = re.sub(r'[−–—]', '-', msg[3])
            r.hset(f"baseball:odds", mapping = {team1: team1_odds})
            team2 = player2opp[team1]
            team2_odds = r.hget("baseball:odds", team2)
            
        logging.info(f"{team1,team1_odds,team2,team2_odds}")
        team1_odds,team2_odds,vig = convert_odds(team1_odds,team2_odds)
        if team1 in allowed_to_trade or team2 in allowed_to_trade:
            maybe_place_order(team1,team1_odds,team2, team2_odds,vig,client,r)


    except:
        print(player2opp)
        raise


def maybe_place_order(ticker, price):
    """Place a buy order if no position exists."""
    pass
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

if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    pubsub = r.pubsub(ignore_subscribe_messages=True)
    pubsub.subscribe('baseball')
    with open(os.getenv("PROD_KEYFILE"), "rb") as f:
        private_key = serialization.load_pem_private_key(f.read(), password=None)
    client = KalshiHttpClient(os.getenv("PROD_KEYID"), private_key)
    logging.info(client.get_balance())

    player2opp,player2tickers = get_baseball_mappings()
    allowed_to_trade = set([""])

    for message in pubsub.listen():
        process_message(json.loads(message['data']),player2tickers,player2opp,allowed_to_trade,client,r)
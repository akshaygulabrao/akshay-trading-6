#!.venv/bin/python
import json, requests
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
        params={"series_ticker": "KXATPMATCH", "status": "open"},
    ).json()["markets"]

    m = requests.get(
        "https://api.elections.kalshi.com/trade-api/v2/markets",
        params={"series_ticker": "KXWTAMATCH", "status": "open"},
    ).json()["markets"]
    markets.extend(m)
    # Group by match using the vs pattern in titles
    matches = {}
    for m in markets:
        if match := re.search(r"the\s+(.+?)\s+match", m["title"]):
            match_name = match.group(1)
            player_name = re.search(r"Will\s+(.+?)\s+win", m["title"]).group(1)
            matches.setdefault(match_name, {})[player_name] = m["ticker"]

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
    prob = lambda o: (
        100 / (float(o) + 100) if float(o) > 0 else -float(o) / (-float(o) + 100)
    )
    p1 = prob(odds1)

    if odds2 is None:
        return p1, None, None

    p2 = prob(odds2)
    total = p1 + p2
    return p1, p2, total - 1


def maybe_place_order(
    team1,
    odds1,
    team2,
    odds2,
    vig,
    player2opp,
    player2tickers,
    client: KalshiHttpClient,
    r: redis.Redis,
):
    """
    Modified to adjust orders based on existing exposure.
    """
    try:
        # Validate that teams are opponents
        assert player2opp[team1] == team2 and player2opp[team2] == team1
        
        # Get tickers for both teams
        sell_side1, buy_side1 = player2tickers[team1]
        sell_side2, buy_side2 = player2tickers[team2]

        # Get current positions for both teams
        positions_long1 = int(r.hget("positions", buy_side1) or 0)
        positions_short1 = int(r.hget("positions", sell_side1) or 0)
        positions_long2 = int(r.hget("positions", buy_side2) or 0)
        positions_short2 = int(r.hget("positions", sell_side2) or 0)

        # Calculate net exposure for each team
        net_exposure_team1 = positions_short1 - positions_long1
        net_exposure_team2 = -net_exposure_team1

        logging.info(f"Net exposure {team1}: {net_exposure_team1}")
        logging.info(f"Net exposure {team2}: {net_exposure_team2}")

        # Define potential orders with their impact on exposure
        orders_to_consider = [
            # Sell YES on team1 (increases short exposure on team1)
            {
                "ticker": sell_side1,
                "side": "yes", 
                "action": "sell",
                "count": 1,
                "type": "limit",
                "yes_price": round(odds1 * 100),
                "client_order_id": str(uuid.uuid4()),
                "impact_team1": -1,  
                "impact_team2": +1
            },
            # Buy YES on team2 (increases short exposure on team 1)
            {
                "ticker": buy_side1,
                "side": "yes",
                "action": "buy", 
                "count": 1,
                "type": "limit",
                "yes_price": round((1 - odds1) * 100),
                "client_order_id": str(uuid.uuid4()),
                "impact_team1": -1,
                "impact_team2": +1
            },
            # Sell YES on team2 (increases short exposure on team2)
            {
                "ticker": sell_side2,
                "side": "yes",
                "action": "sell",
                "count": 1,
                "type": "limit",
                "yes_price": round(odds2 * 100),
                "client_order_id": str(uuid.uuid4()),
                "impact_team1": +1,
                "impact_team2": -1
            },
            # Buy YES on team1 (increases short exposure on team 2)
            {
                "ticker": buy_side2,
                "side": "yes",
                "action": "buy",
                "count": 1,
                "type": "limit",
                "yes_price": round((1 - odds2) * 100),
                "client_order_id": str(uuid.uuid4()),
                "impact_team1": +1,
                "impact_team2": -1
            }
        ]

        # Filter orders based on current exposure
        filtered_orders = []
        for order in orders_to_consider:
            # Check if order would increase unfavorable exposure
            impact_team1 = order['impact_team1']
            impact_team2 = order['impact_team2']
            
            # Skip orders that increase net long exposure if already long
            if (impact_team1 > 0 and net_exposure_team1 > 0) or \
               (impact_team2 > 0 and net_exposure_team2 > 0):
                logging.info(f"skipping {order['ticker']}:{order['action']}, too much exposure")
                continue
                
            # Skip orders that increase net short exposure if already short
            if (impact_team1 < 0 and net_exposure_team1 < 0) or \
               (impact_team2 < 0 and net_exposure_team2 < 0):
                logging.info(f"skipping {order['ticker']}:{order['action']}, too much exposure")
                continue
                
            filtered_orders.append(order)

        # Check for existing orders for the filtered set
        orders_to_place = []
        for order in filtered_orders:
            order_key = f"{order['ticker']}:{order['action']}"
            logging.info(f"{order_key}")
            if not r.hget("orders", order_key):
                orders_to_place.append(order)
            else:
                public_id,private_id = r.hget("orders",order_key).split(":")
                logging.info(f"Existing order found for {order_key}, skipping")
                updated_client_id = str(uuid.uuid4())
                order = {
                    "ticker": order['ticker'],
                    "side": order['side'],
                    "action": order['action'],
                    "count": 1,
                    "type": "limit",
                    "yes_price": order['yes_price'],
                    "client_order_id": private_id,
                    "updated_client_order_id": updated_client_id,
                    "impact_team1": +1,
                    "impact_team2": -1
                }
                try:
                    logging.info(f"amending order {order}")
                    public_order_id = client.post(f'/trade-api/v2/portfolio/orders/{public_id}/amend', order)
                    r.hset("orders", order_key, f"{public_order_id['order']['order_id']}:{order['updated_client_order_id']}")
                except requests.exceptions.HTTPError as e:
                    logging.error("Couldnt place order")
                    r.hdel("orders", order_key)


        # Place the filtered orders
        for order in orders_to_place:
            try:
                logging.info(f"Placing {order['ticker']}:{order['action']}")
                response = client.post("/trade-api/v2/portfolio/orders", order)
                public_order_id = response['order']['order_id']
                client_order_id = order['client_order_id']
                
                # Store order mapping in Redis
                order_key = f"{order['ticker']}:{order['action']}"
                r.hset("orders", order_key, f"{public_order_id}:{client_order_id}")
                
                logging.info(f"Placed {order['action']} order for {order['ticker']} "
                           f"at price {order['yes_price']/100:.2f}")
                
            except requests.exceptions.HTTPError as e:
                logging.error(f"Failed to place order for {order['ticker']}: {e}")
                raise

    except Exception:
        logging.exception("maybe_place_order failed")
        raise

def process_message(msg, player2tickers, player2opp, allowed_to_trade, client, r):
    """Process incoming Redis messages."""
    try:
        team1= ""
        if msg[0] not in [17, 24]:
            return
        if msg[0] == 17 and len(msg) > 31:
            teams = []
            for i in range(len(msg)):
                if isinstance(msg[i], str) and "ML" in msg[i]:
                    teams.append([msg[i + 1], msg[i + 3]])
            team1, team1_odds = teams[0]
            team2, team2_odds = teams[1]
            team1_odds = re.sub(r"[−–—]", "-", team1_odds)
            team2_odds = re.sub(r"[−–—]", "-", team2_odds)

            r.hset(f"us-open-men:odds", mapping={team1: team1_odds, team2: team2_odds})

        elif msg[0] == 17 and len(msg) == 31:
            team1_odds = re.sub(r"[−–—]", "-", msg[12])
            team1 = msg[10]
            r.hset("us-open-men:odds", team1, team1_odds)
            team2 = player2opp[team1]
            team2_odds = None
            r.hdel("us-open-men:odds", team2)

        elif msg[0] == 24 and "ML" in msg[1]:
            team1 = msg[2]
            team1_odds = re.sub(r"[−–—]", "-", msg[3])
            r.hset(f"us-open-men:odds", mapping={team1: team1_odds})
            if team1 not in player2opp:
                return
            team2 = player2opp[team1]
            team2_odds = r.hget("us-open-men:odds", team2)

        logging.info(f"{team1,team1_odds,team2,team2_odds}")

        team1_odds, team2_odds, vig = convert_odds(team1_odds, team2_odds)
        if team1 in allowed_to_trade or team2 in allowed_to_trade:
            maybe_place_order(
                team1,
                team1_odds,
                team2,
                team2_odds,
                vig,
                player2opp,
                player2tickers,
                client,
                r,
            )

    except:
        print(locals())
        raise


if __name__ == "__main__":
    logging.basicConfig(format="%(asctime)s %(message)s", level=logging.INFO)
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    pubsub = r.pubsub(ignore_subscribe_messages=True)
    pubsub.subscribe("us-open-men")
    with open(os.getenv("PROD_KEYFILE"), "rb") as f:
        private_key = serialization.load_pem_private_key(f.read(), password=None)
    client = KalshiHttpClient(os.getenv("PROD_KEYID"), private_key)
    logging.info(client.get_balance())

    player2tickers, player2opp = get_tennis_mappings()
    allowed_to_trade = set(["Holger Rune"])
    result = r.delete('orders')
    print(f"Keys deleted: {result}") 

    for message in pubsub.listen():
        process_message(
            json.loads(message["data"]),
            player2tickers,
            player2opp,
            allowed_to_trade,
            client,
            r,
        )

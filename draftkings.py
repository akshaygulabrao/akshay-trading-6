#!.venv/bin/python
import asyncio, redis, logging, msgpack, os, argparse,json,re
from functools import partial
from playwright.async_api import async_playwright

def flatten(a,ans):
    for item in a:
        if isinstance(item,list):
            flatten(item,ans)
        if isinstance(item,dict):
            for k,v in item.items():
                ans.extend([k,v])
        elif isinstance(item,(str,int,float)):
            ans.append(item)

def on_message(msg, r: redis.Redis, channel_name='football'):
    try:
        if isinstance(msg, str):
            return
        decoded = msgpack.unpackb(msg, raw=False)
        if decoded[1] == "update":
            ans = []
            flatten(decoded[2],ans)
            print(ans)
            r.publish(channel_name,json.dumps(ans))

    except Exception as e:
        print("on_message error:", e)

async def handle_response(response, r: redis.Redis, channel_name):
    if response.headers.get('content-type', '').lower() == 'application/json':
        try:
            pass
        except json.JSONDecodeError:
            logging.warning(f"Expected JSON but got invalid JSON")
        except Exception as e:
            logging.error(f"Error processing JSON response from")

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--url', default='https://sportsbook.draftkings.com/live',
                        help='URL to open (default: %(default)s)')
    parser.add_argument('--channel', default='football',
                        help='Redis channel name (default: %(default)s)')
    args = parser.parse_args()

    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()
        page.on("response", lambda response: asyncio.create_task(
            handle_response(response, r, args.channel)
        ))
        await page.goto(args.url,wait_until='domcontentloaded',timeout=60000)

        def handle_ws(ws):
            ws.on("framereceived", lambda msg: on_message(msg, r, args.channel))

        page.on("websocket", handle_ws)
        await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
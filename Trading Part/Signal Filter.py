import logging
from binance.um_futures import UMFutures
import pandas as pd
import asyncio
import re
import time
import nest_asyncio
import weakref
import threading
from telethon import TelegramClient, events
from binance.lib.utils import config_logging
from binance.error import ClientError
from datetime import datetime
from binance.websocket.um_futures.websocket_client import UMFuturesWebsocketClient

api_id = ''
api_hash = ''
phone = '+'
group_chat_name = ''

key = ""
secret = ""

config_logging(logging, logging.INFO)
logging.getLogger('telethon').setLevel(logging.WARNING)
um_futures_client = UMFutures(key=key, secret=secret)

holding_position = {}
symbol_timers = {}
opening_orders = {}
symbol_info_dict = {}
tasks = weakref.WeakSet()
loop = asyncio.get_event_loop()
weak_loop = weakref.ref(loop)
fixed_listen_key = ""

async def telegram_start():
    global tasks
    client = TelegramClient('anon', api_id, api_hash)
    await client.start(phone)
    print("Telegram connected.")
    group_chat_entity = None
    async for dialog in client.iter_dialogs():
        if dialog.name == group_chat_name:
            group_chat_entity = dialog.entity
            break
    if group_chat_entity is None:
        print(f"Error: Cannot find any entity corresponding to '{group_chat_name}'")
    
    @client.on(events.NewMessage(chats=[group_chat_entity]))
    async def new_message_handler(event):
        tele_message = event.message.message
        symbol, direction, bodonglv_value, close_value = extract_info_from_message(tele_message)
        if symbol and direction and bodonglv_value and close_value:
            signal_decision = signal_handler(symbol, direction)
            time_now = datetime.now().strftime("%H:%M:%S")
            print(time_now, signal_decision)
            while signal_decision != "PENDING":
                if signal_decision == "OPPOSITE":
                    print("Opposite signal, stop loss immediatly")
                    await stop_market_opposite(um_futures_client, symbol, direction, close_value)
                    break
                if signal_decision == "FAIL":
                    print("Signal Ignore")
                    break
                elif signal_decision == "BEST":
                    base_amount = 50
                    await place_new_order(um_futures_client, symbol, direction, close_value, base_amount)
                    tasks.add(asyncio.create_task(opening_order_check(symbol)))
                    print('OI good, open 50USDT')
                    print('-----------------------------------------------------------------------')
                    break
                elif signal_decision == "BETTER":
                    base_amount = 30
                    await place_new_order(um_futures_client, symbol, direction, close_value, base_amount)
                    tasks.add(asyncio.create_task(opening_order_check(symbol)))
                    print('OI better, open 30USDT')
                    print('-----------------------------------------------------------------------')
                    break
                elif signal_decision == "EXIST":
                    print("Position Exist")
                    print('-----------------------------------------------------------------------')
                    break
            if signal_decision == "PENDING":
                um_futures_client.cancel_open_orders(symbol=symbol, recvWindow=2000)
                print("Pending order update new entry price")
                print('-----------------------------------------------------------------------')
                signal_decision == "PASS"
                
    await client.run_until_disconnected()
    
def signal_handler(symbol, direction):
    if symbol in holding_position:
        print('holding_position',holding_position)
        message_direction = "Long" if direction == "å‘ä¸Š" else "Short"
        print(f"Symbol {symbol} is already in holding position.")
        if message_direction != holding_position[symbol]['position_direction']:
            signal_decision = "OPPOSITE"
        else:
            signal_decision = "EXIST"
    elif symbol in symbol_timers:
        signal_decision = "EXIST"
    elif symbol in opening_orders:
        signal_decision = "PENDING"
    else:
        try:
            oidf = grab_history_OI(um_futures_client, symbol)
            last_two_pct_changes = calculate_OI_strength(um_futures_client, symbol, oidf)
            third_last_strength = categorize_strength(last_two_pct_changes[-3])
            second_last_strength = categorize_strength(last_two_pct_changes[-2])
            last_strength = categorize_strength(last_two_pct_changes[-1])
            final_score = calculate_score(direction, third_last_strength, second_last_strength, last_strength)
            print(final_score)
        except ClientError:
            final_score = 0
            signal_decision = "FAIL"
        if final_score >= 1.25 :
            signal_decision = "BEST"
        elif final_score >= 1:
            signal_decision = "BETTER"
        else:
            signal_decision = "FAIL"            
    return signal_decision

def grab_history_OI(client, symbol):
    openInterest30m = client.open_interest_hist(symbol, "15m")
    oidf = pd.DataFrame(openInterest30m)
    oidf['sumOpenInterest'] = oidf['sumOpenInterest'].astype(float)
    oidf['open_interest_change'] = oidf['sumOpenInterest'].diff()
    oidf['open_interest_pct_change'] = oidf['open_interest_change'] / oidf['sumOpenInterest'].shift(1) * 100
    return oidf

def calculate_OI_strength(client, symbol, oidf):
    present_open_interest = float(client.open_interest(symbol)['openInterest'])
    last_value = oidf.iloc[-1]['sumOpenInterest']
    last_pct_change = (present_open_interest - last_value) / last_value * 100
    last_two_pct_changes = oidf['open_interest_pct_change'].tail(2).values.tolist()
    last_two_pct_changes.append(last_pct_change)
    return last_two_pct_changes

def categorize_strength(pct_change):
    if pct_change <= -0.75:
        return 'Strong Decrease'
    elif pct_change <= -0.5:
        return 'Weak Decrease'
    elif pct_change >= 0.75:
        return 'Strong Increase'
    elif pct_change >= 0.5:
        return 'Weak Increase'
    else:
        return 'No Significant Change'

def calculate_score(direction, third_last_strength, second_last_strength, last_strength):
    if direction == "å‘ä¸Š":
        score_mapping = {
            'Strong Decrease': [-0.5, -0.75, -1],
            'Weak Decrease': [-0.25, -0.5, -0.75],
            'Strong Increase': [0.5, 0.75, 1],
            'Weak Increase': [0.25, 0.5, 0.75],
            'No Significant Change': [0, 0, 0]}
    elif direction == "å‘ä¸‹":
        score_mapping = {
            'Strong Decrease': [0.5, 0.75, 1],
            'Weak Decrease': [0.25, 0.5, 0.75],
            'Strong Increase': [-0.5, -0.75, -1],
            'Weak Increase': [-0.25, -0.5, -0.75],
            'No Significant Change': [0, 0, 0]}
    score = (score_mapping[third_last_strength][0] +
             score_mapping[second_last_strength][1] +
             score_mapping[last_strength][2])
    return score

def extract_info_from_message(message):
    pinzhong = re.search(r'å“ç§\s*:\s*(\w+)', message)
    fangxiang = re.search(r'æ–¹å‘\s*:\s*(\S+?),', message)
    bodonglv = re.search(r'æ³¢åŠ¨çŽ‡\s*:\s*([\d.]+)%', message)
    closeprice = re.search(r'close\s*:\s*([\d.]+)', message)
    if pinzhong and fangxiang and bodonglv and closeprice:
        symbol = pinzhong.group(1)
        direction = fangxiang.group(1)
        bodonglv_value = float(bodonglv.group(1)) / 100
        close_value = float(closeprice.group(1))
        print(f"Symbol: {symbol}, Direction: {direction}, Close: {close_value}")
        return symbol, direction, bodonglv_value, close_value
    return None, None, None, None

async def get_symbol_info_dict(client):
    global symbol_info_dict
    print('Get symbol dict')
    while True:
        exchange_info = client.exchange_info()
        symbol_list = exchange_info['symbols']
        for symbol_info in symbol_list:
            s = symbol_info['symbol']
            price_precision = symbol_info['pricePrecision']
            quantity_precision = symbol_info['quantityPrecision']
            minQty = float(next(filter_info['minQty'] for filter_info in symbol_info['filters'] if filter_info['filterType'] == 'LOT_SIZE'))
            symbol_info_dict[s] = {'pricePrecision': price_precision, 'quantityPrecision': quantity_precision, 'minQty': minQty}
        return symbol_info_dict
        await asyncio.sleep(24*60*60)

async def place_new_order(client, symbol, position_side, close_value, base_amount):
    global symbol_info_dict, opening_orders
    price = round(close_value, symbol_info_dict[symbol]['pricePrecision'])
    quantity = round(base_amount / price, symbol_info_dict[symbol]['quantityPrecision'])
    side = 'BUY' if position_side == 'å‘ä¸Š' else 'SELL'
    try:
        response = client.new_order(
            symbol=symbol,
            side=side,
            type="LIMIT",
            quantity=quantity,
            timeInForce="GTC",
            price=price,
            newClientOrderId = symbol + '_OP'
        )
        time_now = datetime.now().strftime("%H:%M:%S")
        print(time_now)
    except ClientError as error:
        logging.error(
            "Found error. status: {}, error code: {}, error message: {}".format(
                error.status_code, error.error_code, error.error_message))

async def stop_market_opposite(client, symbol, position_side, close_value):
    position_detail = holding_position[symbol]
    reduce_quantity = float(position_detail["position_amount"])
    side = 'SELL' if reduce_quantity > 0 else 'BUY'
    reduce_quantity = abs(round(reduce_quantity, symbol_info_dict[symbol]['quantityPrecision']))
    try:
        response = client.new_order(
            symbol=symbol,
            side=side,
            type="MARKET",
            quantity=reduce_quantity,
            closePosition=True,
            timeInForce="GTC",
            newClientOrderId = symbol + '_SM'
        )
        time_now = datetime.now().strftime("%H:%M:%S")
        print(time_now)
    except ClientError as error:
        logging.error(
            "Found error. status: {}, error code: {}, error message: {}".format(
                error.status_code, error.error_code, error.error_message))
        
async def symbol_timer(symbol):
    global symbol_timers
    await asyncio.sleep(60 * 15)
    if symbol in symbol_timers:
        del symbol_timers[symbol]
        print('Cooldown finished.')

async def opening_order_check(symbol):
    global opening_orders
    await asyncio.sleep(495)
    try:
        response = um_futures_client.cancel_order(symbol=symbol, origClientOrderId=symbol +'_OP', recvWindow=2000)
        print(symbol, 'Pending order overtimed, cancel pending position')
        um_futures_client.cancel_open_orders(symbol=symbol, recvWindow=2000)
    except ClientError as error:
        print(symbol,'Order filled')
        
def binance_message_handler(message):
    global holding_position
    global opening_orders
    global symbol_timers
    global error_occurred
    if 'result' in message:
        print("Start")
    else:
        print(message)
        event_type = message.get('e')
        if event_type == "listenKeyExpired":
            error_occurred = True
            print("listenKeyExpired from message_handler")
        elif event_type == 'ACCOUNT_UPDATE':
            account_info = message.get('a')
            if account_info != None:
                event_reason = account_info.get('m')
                if event_reason == 'ORDER':
                    account_information = account_info.get('B')
                    position_update_information = account_info.get('P')                  
                    for position in position_update_information:
                        symbol = position.get('s')
                        position_amount = position.get('pa')
                        entry_price = position.get('ep')
                        position_direction = "Long" if float(position_amount) > 0 else "Short"
                        position_update = {'position_amount': position_amount, 'entry_price': entry_price, 'position_direction': position_direction}
                        holding_position[symbol] = position_update
                    if symbol in holding_position:
                        position_update = {'position_amount': position_amount, 'entry_price': entry_price, 'position_direction': position_direction}
                        holding_position[symbol] = position_update
        elif event_type == "ORDER_TRADE_UPDATE":
            order_info = message.get('o')
            csorderID = order_info['c']
            symbol = order_info['s']
            custom_id = order_info['c']
            if order_info.get('x') == 'NEW' and order_info.get('X') == 'NEW' and custom_id == symbol + '_OP':
                opening_orders[symbol] = "Order placed"
            elif order_info.get('x') == 'TRADE' and order_info.get('X') == 'FILLED' and custom_id == symbol + '_OP':
                del opening_orders[symbol]
            elif order_info.get('x') == 'CANCELED' and order_info.get('X') == 'CANCELED' and custom_id == symbol + '_OP':
                del opening_orders[symbol]
            elif order_info.get('x') == 'TRADE' and order_info.get('X') == 'FILLED' and custom_id == symbol + '_TK':
                symbol_timers[symbol] = "Order filled"
                del holding_position[symbol]
                tasks.add(loop.create_task(symbol_timer(symbol))) 
            elif order_info.get('x') == 'TRADE' and order_info.get('X') == 'FILLED' and custom_id == symbol + '_ST':
                symbol_timers[symbol] = "Order filled"
                del holding_position[symbol]
                tasks.add(loop.create_task(symbol_timer(symbol)))
            elif order_info.get('x') == 'TRADE' and order_info.get('X') == 'FILLED' and custom_id == symbol + '_SM':
                symbol_timers[symbol] = "Order filled"
                del holding_position[symbol]
                tasks.add(loop.create_task(symbol_timer(symbol)))
        
def keep_alive_listen_key(client, listen_key):
    global error_occurred
    print('Keep listenkey alive start')
    while True:
        try:
            time.sleep(29*50)
            client.renew_listen_key(listen_key)
        except Exception as error:
            print(f"Renew listenkey: {error}")
            error_occurred = True

def get_new_listen_key(client):
    global error_occurred
    global fixed_listen_key
    try:
        ws_response = client.new_listen_key()
        fixed_listen_key = ws_response["listenKey"]
    except Exception as error:
        print(f"Get new listenkey: {error}")
        error_occurred = True

async def websocket_start():
    global error_occurred
    error_occurred = False
    while True:
        try:
            if error_occurred:
                print("Restarting due to expired listen key...")
                error_occurred = False  # Reset the error flag
            get_new_listen_key(um_futures_client)
            time.sleep(0.3)
            ws_client = UMFuturesWebsocketClient()
            ws_client.user_data(
                            listen_key = fixed_listen_key,
                            id=1,
                            callback = binance_message_handler)
            ws_client.start()
            await asyncio.sleep(23*60*60)
            ws_client.close()
            print("Websocket connection expired,23hour")
            time.sleep(2)
        except Exception as error:
            print("websocket re-connect failed")

async def main():
    global tasks
    global loop
    try:
        get_new_listen_key(um_futures_client)
        tasks.add(asyncio.create_task(get_symbol_info_dict(um_futures_client)))
        keep_alive_thread = threading.Thread(target=keep_alive_listen_key, args=(um_futures_client, fixed_listen_key), daemon=True)
        keep_alive_thread.start()
        await asyncio.gather(telegram_start(),websocket_start())
    except Exception as error:
        print("Main Problem")
        print(f"Error in main: {error}")

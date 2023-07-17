import logging
from binance.um_futures import UMFutures
import pandas as pd
import asyncio
import re
import time
from telethon import TelegramClient, events
from binance.lib.utils import config_logging
from binance.error import ClientError
import nest_asyncio
import weakref
from datetime import datetime
import threading
from binance.websocket.um_futures.websocket_client import UMFuturesWebsocketClient


# Telegram bot settings
api_id = ''
api_hash = ''
phone = ''
group_chat_name = ''

# Binance API settings
key = ""
secret = ""

config_logging(logging, logging.INFO)
logging.getLogger('telethon').setLevel(logging.WARNING)
um_futures_client = UMFutures(key=key, secret=secret)
#ws_response = um_futures_client.new_listen_key()

holding_position = {}
symbol_timers = {}
opening_orders = {}
symbol_info_dict = {}
tasks = weakref.WeakSet()
loop = asyncio.get_event_loop()

async def main():
    global tasks
    global loop
    tasks.add(asyncio.create_task(get_symbol_info_dict(um_futures_client)))
    await asyncio.gather(telegram_start(),websocket_start())
    for task in tasks:
        task.cancel()

async def telegram_start():
    global tasks
    client = TelegramClient('anon', api_id, api_hash)
    await client.start(phone)
    print("Client connected.")
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
            print(signal_decision)
            while signal_decision != "PENDING":
                if signal_decision == "FAIL":
                    print("Signal Ignore")
                    break
                elif signal_decision == "BEST":
                    base_amount = 50
                    await place_new_order(um_futures_client, symbol, direction, close_value, base_amount)
                    tasks.add(asyncio.create_task(opening_order_check(symbol)))
                    print('OI Fine, trade 50 usdt')
                    print('-----------------------------------------------------------------------')
                    break
                elif signal_decision == "BETTER":
                    base_amount = 30
                    await place_new_order(um_futures_client, symbol, direction, close_value, base_amount)
                    tasks.add(asyncio.create_task(opening_order_check(symbol)))
                    print('OI Fine, trade 30 usdt')
                    print('-----------------------------------------------------------------------')
                    break
                elif signal_decision == "EXIST":
                    print("Position Exist")
                    print('-----------------------------------------------------------------------')
                    break
            if signal_decision == "PENDING":
                um_futures_client.cancel_open_orders(symbol=symbol, recvWindow=2000)
                print("Pending Order Update")
                print('-----------------------------------------------------------------------')
                signal_decision == "PASS"
                
    await client.run_until_disconnected()
        
def signal_handler(symbol, direction):
    # signal_decision == None
    oidf = grab_history_OI(um_futures_client, symbol)
    try:
        last_two_pct_changes = calculate_OI_strength(um_futures_client, symbol, oidf)
        third_last_strength = categorize_strength(last_two_pct_changes[-3])
        second_last_strength = categorize_strength(last_two_pct_changes[-2])
        last_strength = categorize_strength(last_two_pct_changes[-1])
        final_score = calculate_score(direction, third_last_strength, second_last_strength, last_strength)
    except ClientError:
        final_score = 0
        print(final_score)
        signal_decision = "FAIL"
    if symbol in holding_position or symbol in symbol_timers:
        signal_decision = "EXIST"
    else:
        if symbol in opening_orders:
            signal_decision = "PENDING"
        else:
            if final_score >= 1.25 :
                signal_decision = "BEST"
            elif final_score >= 1:
                signal_decision = "BETTER"
            else:
                signal_decision = "FAIL"
                
    return signal_decision

def grab_history_OI(client, symbol):
    # um_futures_client = UMFutures()
    openInterest30m = client.open_interest_hist(symbol, "15m")
    oidf = pd.DataFrame(openInterest30m)
    oidf['sumOpenInterest'] = oidf['sumOpenInterest'].astype(float)
    oidf['open_interest_change'] = oidf['sumOpenInterest'].diff()
    oidf['open_interest_pct_change'] = oidf['open_interest_change'] / oidf['sumOpenInterest'].shift(1) * 100
    return oidf

def calculate_OI_strength(client, symbol, oidf):
    present_open_interest = float(client.open_interest(symbol)['openInterest'])
    print("The present OI is: ", present_open_interest)
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
    elif pct_change >= 0.35:
        return 'Weak Increase'
    else:
        return 'No Significant Change'

def calculate_score(direction, third_last_strength, second_last_strength, last_strength):
    if direction == "向上":
        score_mapping = {
            'Strong Decrease': [-0.5, -0.75, -1],
            'Weak Decrease': [-0.25, -0.5, -0.75],
            'Strong Increase': [0.5, 0.75, 1],
            'Weak Increase': [0.25, 0.5, 0.75],
            'No Significant Change': [0, 0, 0]}
    elif direction == "向下":
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
    pinzhong = re.search(r'品种\s*:\s*(\w+)', message)
    fangxiang = re.search(r'方向\s*:\s*(\S+?),', message)
    bodonglv = re.search(r'波动率\s*:\s*([\d.]+)%', message)
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
    print('Get Symbol Info')
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
    side = 'BUY' if position_side == '向上' else 'SELL'
    try:
        response = client.new_order(
            symbol=symbol,
            side=side,
            type="LIMIT",
            quantity=quantity,
            timeInForce="GTC",
            price=price,
            newClientOrderId = symbol + '_OPEN'
        )
        time_now = datetime.now().strftime("%H:%M:%S")
        print(time_now)
    except ClientError as error:
        logging.error(
            "Found error. status: {}, error code: {}, error message: {}".format(
                error.status_code, error.error_code, error.error_message
            )
        )
        
async def symbol_timer(symbol):
    global symbol_timers
    await asyncio.sleep(60 * 15)
    if symbol in symbol_timers:
        del symbol_timers[symbol]
        print('Countdown Finished')

async def opening_order_check(symbol):
    global opening_orders
    await asyncio.sleep(495)
    try:
        response = um_futures_client.cancel_order(symbol=symbol, origClientOrderId=symbol +'_OPEN', recvWindow=2000)
        print(symbol, 'Pending Order Expired')
        um_futures_client.cancel_open_orders(symbol=symbol, recvWindow=2000)
        # del opening_orders[symbol]
    except ClientError as error:
        print(symbol,'Order Filled')
        
def binance_message_handler(message):
    global holding_position
    global opening_orders
    global symbol_timers
    if 'result' in message:
        print("Start")
        return
    else:
        event_type = message.get('e')
        if event_type == 'ACCOUNT_UPDATE':
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
                        position_update = {'position_amount': position_amount, 'entry_price': entry_price}
                        holding_position[symbol] = position_update
                    if symbol in holding_position:
                        position_update = {'position_amount': position_amount, 'entry_price': entry_price}
                        holding_position[symbol] = position_update
                        print('来自account update 持仓信息：', holding_position)
                        
        elif event_type == "ORDER_TRADE_UPDATE":
            order_info = message.get('o')
            csorderID = order_info['c']
            symbol = order_info['s']
            custom_id = order_info['c']
            if order_info.get('x') == 'NEW' and order_info.get('X') == 'NEW' and custom_id == symbol + '_OPEN':
                opening_orders[symbol] = "已挂单"
                print('opening_orders',opening_orders)
            elif order_info.get('x') == 'TRADE' and order_info.get('X') == 'FILLED' and custom_id == symbol + '_OPEN':
                del opening_orders[symbol]
                print('opening_orders',opening_orders)
            elif order_info.get('x') == 'CANCELED' and order_info.get('X') == 'CANCELED' and custom_id == symbol + '_OPEN':
                del opening_orders[symbol]
                print('opening_orders',opening_orders)
            elif order_info.get('x') == 'TRADE' and order_info.get('X') == 'FILLED' and custom_id == symbol + '_TAKE':
                symbol_timers[symbol] = "已成交"
                del holding_position[symbol]
                print('holding_position',holding_position)
                tasks.add(loop.create_task(symbol_timer(symbol))) 
            elif order_info.get('x') == 'TRADE' and order_info.get('X') == 'FILLED' and custom_id == symbol + '_STOP':
                symbol_timers[symbol] = "已成交"
                del holding_position[symbol]
                print('holding_position', holding_position)
                tasks.add(loop.create_task(symbol_timer(symbol)))

async def delayed_close(client):
    global ws_response
    await asyncio.sleep(24 * 60 * 53)  # Wait for 24 hours
    ws_client.close()
    print("Re-generate Websocket connection")
    um_futures_client.close_listen_key(ws_response['listenKey'])
    ws_response = um_futures_client.new_listen_key()
    
async def websocket_start():
    global ws_response
    global error_occurred
    loop = asyncio.get_event_loop()
    ws_response = um_futures_client.new_listen_key()
    while True:
        # Reset the error_occurred flag
        error_occurred = False
        ws_client = UMFuturesWebsocketClient()
        ws_client.user_data(
                    listen_key=ws_response["listenKey"],
                    id=1,
                    callback=binance_message_handler,
                )
        ws_client.start()
        loop.create_task(delayed_close(ws_client))
        while not error_occurred:
            await asyncio.sleep(1)

        print("An error occurred in one of the threads. Restarting the script in 60 seconds...")
        time.sleep(15)

nest_asyncio.apply()
asyncio.get_event_loop().run_until_complete(main())

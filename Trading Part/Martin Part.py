import logging
import time
import math
import asyncio
import nest_asyncio
import weakref
from binance.um_futures import UMFutures
from binance.lib.utils import config_logging
from binance.error import ClientError
from binance.websocket.um_futures.websocket_client import UMFuturesWebsocketClient

config_logging(logging, logging.DEBUG)

# Binance API settings
key = ""
secret = ""

um_futures_client = UMFutures(key=key, secret=secret)
#ws_response = um_futures_client.new_listen_key()

# TP and SL config
stop_loss_percent = 0.02
take_profit_percent = 0.012

# Necessery data dict
tp_order_ids = {}
add_order_ids = {}
sl_order_ids = {}
holding_position = {}
symbol_info_dict = {}
symbol_counters = {}

tasks = weakref.WeakSet()
loop = asyncio.get_event_loop()

# TP, SL and ADD's configration
add_position_multiplier = 1.5
price_change_rate = 0.015
max_counter = 5

def delete_closed_position(symbol):
    global holding_position, tp_order_ids
    if float(holding_position[symbol]["position_amount"]) == 0:
        if float(holding_position[symbol]["entry_price"]) == 0:
            del holding_position[symbol]
            del tp_order_ids[symbol]
            print("止盈单记录已删除")
    else:
        pass
    
def message_handler(message):
    global exchange_symbol_basic_information
    global tp_order_ids
    global sl_order_ids
    global add_order_ids
    global holding_position
    global symbol_counters
    if 'result' in message:
        print("Start")
        return
    else:
        event_type = message.get('e')
        if event_type == 'ACCOUNT_UPDATE':   # update holding position information
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
                        delete_closed_position(symbol)
                        print('来自account update 持仓信息：', holding_position)

        elif event_type == 'ORDER_TRADE_UPDATE':
            order_info = message.get('o')
            csorderID = order_info['c']
            symbol = order_info['s']
            custom_id = order_info['c']
            if order_info.get('x') == 'TRADE' and order_info.get('X') == 'FILLED' and custom_id == symbol + '_OPEN':
                print('有新建仓：', symbol)
                symbol_counters[symbol] = 0
                #print('计数器：', symbol_counters)
                place_take_profit_order(symbol, take_profit_percent, symbol_info_dict, holding_position)
                time.sleep(0.5)
                place_additional_order(symbol, add_position_multiplier, price_change_rate, symbol_info_dict, add_order_ids, holding_position)
                
            elif order_info.get('x') == 'TRADE' and order_info.get('X') == 'FILLED' and custom_id == symbol + '_TAKE':
                um_futures_client.cancel_open_orders(symbol=symbol, recvWindow=2000)
                print(symbol, '止盈单成交')
                #del tp_order_ids[symbol]
                #del sl_order_ids[symbol]
                del symbol_counters[symbol]
                
            elif order_info.get('x') == 'TRADE' and order_info.get('X') == 'FILLED' and custom_id == symbol + '_STOP':
                um_futures_client.cancel_open_orders(symbol=symbol, recvWindow=2000)
                print(symbol, "止损单成交")
                #del tp_order_ids[symbol]
                #del sl_order_ids[symbol]
                del symbol_counters[symbol]
                
            elif order_info.get('x') == 'TRADE' and order_info.get('X') == 'FILLED' and custom_id == symbol + '_ADD':
                if symbol in holding_position:
                    if symbol in symbol_counters:
                        if symbol_counters[symbol] == max_counter:
                            place_stop_loss_order(symbol, stop_loss_percent, symbol_info_dict, holding_position)
                            time.sleep(0.5)
                            place_take_profit_order(symbol, take_profit_percent, symbol_info_dict, holding_position)
                        elif symbol_counters[symbol] < max_counter:
                            symbol_counters[symbol] += 1
                            um_futures_client.cancel_open_orders(symbol=symbol, recvWindow=2000)
                            del tp_order_ids[symbol]
                            print('止盈单已取消！')
                            time.sleep(0.5)
                            place_additional_order(symbol, add_position_multiplier, price_change_rate, symbol_info_dict, add_order_ids, holding_position)
                            time.sleep(0.5)
                            place_take_profit_order(symbol, take_profit_percent, symbol_info_dict, holding_position)
                            print(symbol, "加仓成交, 新止盈单")

            elif order_info.get('x') == 'NEW' and order_info.get('X') == 'NEW':
                if custom_id == symbol + '_TAKE':
                    tp_amount = order_info["q"]
                    tp_price = order_info["p"]
                    tp_update = {'tp_amount': tp_amount, 'tp_price': tp_price}
                    tp_order_ids[symbol] = tp_update
                    print('止盈单已挂单', tp_order_ids)
                    
                elif custom_id == symbol + '_ADD':
                    add_amount = order_info["q"]
                    add_price = order_info["p"]
                    add_update = {'add_amount': add_amount, 'add_price': add_price}
                    add_order_ids[symbol] = add_update
                    print('加仓已挂单', add_order_ids)

                elif custom_id == symbol + '_STOP':
                    sl_amount = order_info["q"]
                    sl_price = order_info["p"]
                    sl_update = {'sl_amount': sl_amount, 'sl_price': sl_price}
                    sl_order_ids[symbol] = sl_update
                    print('止损已挂单', sl_order_ids)

            elif order_info.get('x') == 'CANCELED' and order_info.get('X') == 'CANCELED':
                if custom_id == symbol + '_TAKE':
                    #del tp_order_ids[symbol]
                    #print('止盈单已取消！')
                    pass
                elif custom_id == symbol + '_ADD':
                    del add_order_ids[symbol]
                    print('加仓单已取消!')
                elif custom_id == symbol + '_STOP':
                    del sl_order_ids[symbol]
                    print('止损单已取消!')
        
def round_to_tick_size(value, tick_size):
    tick_size_decimal_places = int(-math.log10(tick_size))
    return round(round(value / tick_size) * tick_size, tick_size_decimal_places)
            
async def get_symbol_info_dict(client):
    global symbol_info_dict
    print('获取交易所信息')
    while True:
        exchange_info = client.exchange_info()
        symbol_list = exchange_info['symbols']
        for symbol_info in symbol_list:
            symbol = symbol_info['symbol']
            price_precision = symbol_info['pricePrecision']
            quantity_precision = symbol_info['quantityPrecision']
            for filter_info in symbol_info['filters']:
                if filter_info['filterType'] == 'PRICE_FILTER':
                    tick_size = float(filter_info['tickSize'])
                elif filter_info['filterType'] == 'LOT_SIZE':
                    minQty = float(filter_info['minQty'])
            symbol_info_dict[symbol] = {
                'pricePrecision': price_precision,
                'quantityPrecision': quantity_precision,
                'minQty': minQty,
                'tickSize': tick_size}
        await asyncio.sleep(24*60*60)

def place_stop_loss_order(symbol, stop_loss_percent, symbol_info_dict, holding_position):
    position_detail = holding_position[symbol]
    reduce_quantity = float(position_detail["position_amount"])
    side = 'SELL' if reduce_quantity > 0 else 'BUY'
    sl = float(position_detail["entry_price"]) * (1 - stop_loss_percent) if side == 'SELL' else float(position_detail["entry_price"]) * (1 + stop_loss_percent)
    sl = round_to_tick_size(sl, symbol_info_dict[symbol]['tickSize'])
    reduce_quantity = abs(round(reduce_quantity, symbol_info_dict[symbol]['quantityPrecision']))
    try:
        stop_loss_market = um_futures_client.new_order(
            symbol=symbol,
            side=side,
            type='LIMIT',
            price=sl,
            newClientOrderId = symbol +'_STOP',
            reduceOnly='true',
            quantity=abs(reduce_quantity),
            timeInForce="GTC"
        )

    except ClientError as error:
        print("Found error. status: {}, error code: {}, error message: {}".format(
            error.status_code, error.error_code, error.error_message
        )
             )
def place_additional_order(symbol, add_position_multiplier, price_change_rate, symbol_info_dict, add_order_ids, holding_position):
    position_detail = holding_position[symbol]
    position_amt = float(position_detail["position_amount"])
    side = 'SELL' if position_amt < 0 else 'BUY'
    add_counter = symbol_counters[symbol]
    if add_counter == 0:
        last_add_amt = float(position_detail["position_amount"])
        last_add_price = float(position_detail["entry_price"])
    else:
        add_detail = add_order_ids[symbol]
        last_add_amt = float(add_detail["add_amount"])
        last_add_price = float(add_detail["add_price"])
    additional_order_price = last_add_price * (1 + price_change_rate * (add_position_multiplier**add_counter)) if side == "SELL" else last_add_price * (1 - price_change_rate * (add_position_multiplier**add_counter))
    additional_order_price = round_to_tick_size(additional_order_price, symbol_info_dict[symbol]['tickSize'])
    additional_order_quantity = abs(round(last_add_amt * add_position_multiplier, symbol_info_dict[symbol]['quantityPrecision']))
    try:
        additional_order = um_futures_client.new_order(
            symbol=symbol,
            side=side,
            type='LIMIT',
            price=additional_order_price,
            newClientOrderId = symbol+'_ADD',
            quantity=abs(additional_order_quantity),
            timeInForce="GTC",
        )
    except ClientError as error:
        print(
            "Found error. status: {}, error code: {}, error message: {}".format(
                error.status_code, error.error_code, error.error_message
            )
        )
def place_take_profit_order(symbol, take_profit_percent, symbol_info_dict, holding_position):
    # Check if there is previous order exist
    tp_detail = holding_position[symbol]
    reduce_quantity = float(tp_detail["position_amount"])
    side = 'SELL' if reduce_quantity > 0 else 'BUY'
    tp = float(tp_detail["entry_price"]) * (1 + take_profit_percent) if side == 'SELL' else float(tp_detail["entry_price"]) * (1 - take_profit_percent)
    tp = round_to_tick_size(tp, symbol_info_dict[symbol]['tickSize'])
    try:
        take_profit_limit = um_futures_client.new_order(
            symbol=symbol,
            side=side,
            type='LIMIT',
            price=tp,
            newClientOrderId = symbol+'_TAKE',
            quantity=abs(reduce_quantity),
            timeInForce="GTC"
        )
    except ClientError as error:
        print(
            "Found error. status: {}, error code: {}, error message: {}".format(
                error.status_code, error.error_code, error.error_message
            )
        )

async def delayed_close(client):
    global ws_response
    await asyncio.sleep(24 * 60 * 45)  # Wait for 24 hours
    ws_client.close()
    um_futures_client.close_listen_key(ws_response["listenKey"])
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
                    callback=message_handler,
                )
        ws_client.start()
        loop.create_task(delayed_close(ws_client))
        while not error_occurred:
            await asyncio.sleep(1)
        print("An error occurred in one of the threads. Restarting the script in 10 seconds...")
        time.sleep(10)
        
async def main():
    global tasks
    global loop
    tasks.add(asyncio.create_task(get_symbol_info_dict(um_futures_client)))
    await asyncio.gather(websocket_start())
    for task in tasks:
        task.cancel()

nest_asyncio.apply()
asyncio.get_event_loop().run_until_complete(main())

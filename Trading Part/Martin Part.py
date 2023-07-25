import logging
import time
import threading
import math
#import asyncio
#import nest_asyncio
#import weakref
from datetime import datetime
from binance.um_futures import UMFutures
from binance.lib.utils import config_logging
from binance.error import ClientError
from binance.websocket.um_futures.websocket_client import UMFuturesWebsocketClient
config_logging(logging, logging.DEBUG)

# Binance API settings
key = ""
secret = ""

um_futures_client = UMFutures(key=key, secret=secret)
#tasks = weakref.WeakSet()
#loop = asyncio.get_event_loop()

# TP and SL config
stop_loss_percent = 0.02
take_profit_percent = 0.012

# Necessery information configration
tp_order_ids = {}
add_order_ids = {}
sl_order_ids = {}
holding_position = {}
exchange_symbol_basic_information = {}
symbol_counters = {}
fixed_listen_key = ""

# TP,SL,ADD configration
add_position_multiplier = 1.5
price_change_rate = 0.015
max_counter = 5

def get_new_listen_key(client):
    global fixed_listen_key
    try:
        ws_response = client.new_listen_key()
        fixed_listen_key = ws_response["listenKey"]
    except Exception as error:
        print(f"Get new listenkey. Error in update_symbol_info_dict thread: {error}")
        error_occurred = True
    
def delete_closed_position(symbol):
    global holding_position, tp_order_ids
    if float(holding_position[symbol]["position_amount"]) == 0:
        if float(holding_position[symbol]["entry_price"]) == 0:
            try:
                del holding_position[symbol]
                del tp_order_ids[symbol]
                print("止盈单记录已删除")
            except Exception as error:
                print(f"Error in update_symbol_info_dict thread: {error}")
                error_occurred = True
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
        print("Message handler start")
        return
    else:
        try:
            time_now = datetime.now().strftime("%H:%M:%S")
            print(time_now)
            event_type = message.get('e')
            if event_type == "listenKeyExpired":
                global error_occurred
                error_occurred = True
                print("listenKeyExpired来自message_handler")
                
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
                            position_update = {'position_amount': position_amount, 'entry_price': entry_price}
                            holding_position[symbol] = position_update
                        if symbol in holding_position:
                            position_update = {'position_amount': position_amount, 'entry_price': entry_price}
                            holding_position[symbol] = position_update
                            delete_closed_position(symbol)
                            print('来自account update 持仓信息：', position_update)

            elif event_type == 'ORDER_TRADE_UPDATE':
                order_info = message.get('o')
                csorderID = order_info['c']
                symbol = order_info['s']
                custom_id = order_info['c']
                if order_info.get('x') == 'TRADE' and order_info.get('X') == 'FILLED' and custom_id == symbol + '_OP':
                    print('有新建仓：', symbol)
                    symbol_counters[symbol] = 0
                    #print('计数器：', symbol_counters)
                    place_take_profit_order(symbol, take_profit_percent, symbol_info_dict, holding_position)
                    time.sleep(0.5)
                    place_additional_order(symbol, add_position_multiplier, price_change_rate, symbol_info_dict, add_order_ids, holding_position)

                elif order_info.get('x') == 'TRADE' and order_info.get('X') == 'FILLED' and custom_id == symbol+'_TK':
                    um_futures_client.cancel_open_orders(symbol=symbol, recvWindow=2000)
                    print(symbol, '止盈单成交')
                    #del tp_order_ids[symbol]
                    #del sl_order_ids[symbol]
                    del symbol_counters[symbol]

                elif order_info.get('x') == 'TRADE' and order_info.get('X') == 'FILLED' and custom_id == symbol + '_ST':
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
                                time.sleep(0.2)
                                place_take_profit_order(symbol, take_profit_percent, symbol_info_dict, holding_position)
                            elif symbol_counters[symbol] < max_counter:
                                symbol_counters[symbol] += 1
                                um_futures_client.cancel_open_orders(symbol=symbol, recvWindow=2000)
                                del tp_order_ids[symbol]
                                print('止盈单已取消！')
                                place_additional_order(symbol, add_position_multiplier, price_change_rate, symbol_info_dict, add_order_ids, holding_position)
                                time.sleep(0.2)
                                place_take_profit_order(symbol, take_profit_percent, symbol_info_dict, holding_position)
                                print(symbol, "加仓成交, 新止盈单")

                elif order_info.get('x') == 'NEW' and order_info.get('X') == 'NEW':
                    if custom_id == symbol+'_TK':
                        tp_amount = order_info["q"]
                        tp_price = order_info["p"]
                        tp_update = {'tp_amount': tp_amount, 'tp_price': tp_price}
                        tp_order_ids[symbol] = tp_update
                        print('止盈单已挂单', symbol)

                    elif custom_id == symbol + '_ADD':
                        add_amount = order_info["q"]
                        add_price = order_info["p"]
                        add_update = {'add_amount': add_amount, 'add_price': add_price}
                        add_order_ids[symbol] = add_update
                        print('加仓已挂单', symbol)

                    elif custom_id == symbol + '_ST':
                        sl_amount = order_info["q"]
                        sl_price = order_info["p"]
                        sl_update = {'sl_amount': sl_amount, 'sl_price': sl_price}
                        sl_order_ids[symbol] = sl_update
                        print('止损已挂单', symbol)

                elif order_info.get('x') == 'CANCELED' and order_info.get('X') == 'CANCELED':
                    if custom_id == symbol+'_TK':
                        #del tp_order_ids[symbol]
                        #print('止盈单已取消！')
                        pass
                    elif custom_id == symbol + '_ADD':
                        del add_order_ids[symbol]
                        print('加仓单已取消!')
                    elif custom_id == symbol + '_ST':
                        del sl_order_ids[symbol]
                        print('止损单已取消!')
        except Exception as error:
            print("Message handler Problem")
            print(f"Error in keep_alive_listen_key thread: {error}")
            error_occurred = True
        
def round_to_tick_size(value, tick_size):
    tick_size_decimal_places = int(-math.log10(tick_size))
    return round(round(value / tick_size) * tick_size, tick_size_decimal_places)
            
def get_symbol_info_dict(client):
    global symbol_info_dict
    print("Get Exchange Info")
    symbol_info_dict = {}
    while True:
        exchange_info = client.exchange_info()
        symbol_list = exchange_info['symbols']
        for symbol_info in symbol_list:
            symbol = symbol_info['symbol']
            price_precision = symbol_info['pricePrecision']
            quantity_precision = symbol_info['quantityPrecision']
            minQty = float(next(filter_info['minQty'] for filter_info in symbol_info['filters'] if filter_info['filterType'] == 'LOT_SIZE'))
            tick_size = float(next(filter_info['tickSize'] for filter_info in symbol_info['filters'] if filter_info['filterType'] == 'PRICE_FILTER'))
            symbol_info_dict[symbol] = {
            'pricePrecision': price_precision,
            'quantityPrecision': quantity_precision,
            'minQty': minQty,
            'tickSize': tick_size}
        return symbol_info_dict
        time.sleep(48*60*60)

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
            newClientOrderId = symbol +'_ST',
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
            newClientOrderId = symbol+'_TK',
            quantity=abs(reduce_quantity),
            timeInForce="GTC"
        )
    except ClientError as error:
        print(
            "Found error. status: {}, error code: {}, error message: {}".format(
                error.status_code, error.error_code, error.error_message
            )
        )
    
def keep_alive_listen_key(client, listen_key):
    global error_occurred
    print('Keep listenkey alive start')
    while True:
        try:
            time.sleep(30*60)
            client.renew_listen_key(listen_key)
        except Exception as error:
            print(f"Renew listenkey: {error}")
            error_occurred = True

def websocket_start():
    global error_occurred
    error_occurred = False
    get_new_listen_key(um_futures_client)
    keep_alive_thread = threading.Thread(target=keep_alive_listen_key, args=(um_futures_client, fixed_listen_key), daemon=True)
    keep_alive_thread.start()
    while True:
        get_new_listen_key(um_futures_client)
        time.sleep(0.3)
        ws_client = UMFuturesWebsocketClient()
        ws_client.user_data(
                        listen_key = fixed_listen_key,
                        id=1,
                        callback = message_handler)
        ws_client.start()
        while not error_occurred:
            time.sleep(23*60*60)
            ws_client.close()
            print("Websocket连接已更新，23小时")
            break
        print("Websocket_start检测到error，关闭当前连接重启websocket")
        try:
            ws_client.close()
            time.sleep(5)
        except Exception as error:
            print("websocket重启失败")

def main():
    global tasks
    #global loop
    try:
        get_symbol_thread = threading.Thread(target=get_symbol_info_dict, args=(um_futures_client,), daemon=True)
        get_symbol_thread.start()
        websocket_start()
    except Exception as error:
        print("Main Problem")
        print(f"Error in keep_alive_listen_key thread: {error}")
    for task in tasks:
        task.cancel()
if __name__ == "__main__":
    error_occurred = False
    main()

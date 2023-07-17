import asyncio
import re
from telethon import TelegramClient, events
import nest_asyncio

api_id = ''
api_hash = ''
phone = ''
group_chat_name = ''

async def main():
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
        return

    @client.on(events.NewMessage(chats=[group_chat_entity]))
    async def new_message_handler(event):
        message = event.message.message

        # Extract information using regular expressions
        pinzhong = re.search(r'品种\s*:\s*(\w+)', message)
        fangxiang = re.search(r'方向\s*:\s*(\S+?),', message)
        bodonglv = re.search(r'波动率\s*:\s*([\d.]+)%', message)
        closeprice = re.search(r'close\s*:\s*([\d.]+)', message)  # Extract "close" value

        if pinzhong and fangxiang and bodonglv and closeprice:
            symbol = pinzhong.group(1)
            direction = fangxiang.group(1)
            bodonglv_value = float(bodonglv.group(1)) / 100
            close_value = float(closeprice.group(1))

            print(f"Symbol: {symbol}, Direction: {direction}, Volatility: {bodonglv_value * 100}%, Close: {close_value}")

    await client.run_until_disconnected()

nest_asyncio.apply()
await main()

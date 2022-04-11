import logging
import pandas as pd
from network_crawler import add_messages
from tqdm import tqdm
from telegram import SyncTelegramClient
from telethon.errors.rpcerrorlist import ChannelPrivateError

# Initialize telegram client
telethon_api = SyncTelegramClient()
# Configure logging
logging.basicConfig(filename='log.log', level=logging.DEBUG)


def store_chat_messages(chat_id, offset_id=0):
    try:
        # Fetch the last 100 messages
        messages = telethon_api.fetch_messages(
            chat=chat_id,
            offset_id=offset_id
        )
    except ValueError:
        print('ValueError in chat', chat_id)
    add_messages(chat_id, messages)

def store_can_view_participants():
    df_chats = pd.read_csv('data/chats.csv')
    stored_chat_ids = list(df_chats.iloc[:,0])
    can_view_participants_column = [can_view_participants(chat_id) for chat_id in tqdm(stored_chat_ids)]
    df_chats['can_view_participants'] = can_view_participants_column
    df_chats.to_csv('chats_can_view.csv', index=False)

def can_view_participants(chat_id):
    try:
        info = telethon_api.get_chat_info(chat_id)
    except ChannelPrivateError:
        print('Could not access chat', chat_id, 'because it is private')
        return 0
    if 'full_chat' in info.keys() and 'hidden_prehistory' in info['full_chat'].keys() and info['full_chat']['hidden_prehistory']:
        print('Prehistory hidden in chat', info["chats"][0]["title"])
    if 'full_chat' in info.keys() and 'can_view_participants' in info['full_chat'].keys():
        return 1 if info['full_chat']['can_view_participants'] else 0
    else:
        print('err')
        return 0


if __name__ == '__main__':
    pass
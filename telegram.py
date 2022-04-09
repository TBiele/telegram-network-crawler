# Code adapted from Miguel Angel Garcia-Gutierrez Espina

import json
import logging
from telethon.sync import TelegramClient
from telethon.tl import functions
from telethon.errors.rpcerrorlist import ChannelPrivateError
from telethon.tl.functions.messages import GetHistoryRequest

# Configure logging
logging.basicConfig(filename='log.log', level=logging.DEBUG)

class SyncTelegramClient:
    def __init__(self):
        """Initialize Telegram client using the credentials given in config.json."""
        with open('config.json', 'r') as file:
            data = file.read()
        config = json.loads(data)
        api_id = config['credentials']['api_id']
        api_hash = config['credentials']['api_hash']
        if api_id != '' and api_hash != '':
            self._client = TelegramClient("session", api_id, api_hash)
        else:
            raise Exception("Please set your api_id and api_hash in config.json. More information can be found at https://core.telegram.org/api/obtaining_api_id.")

    # Call the API once to fetch 100 messages
    def fetch_messages(self, chat, size=100, offset_id=0, max_id=0, min_id=0, offset_date=None):
        with self._client as client:
            try:
                history = client(GetHistoryRequest(
                    peer=chat,
                    limit=size, # 100 is the max number of messages that can be retrieved per request
                    offset_date=offset_date,
                    offset_id=offset_id,
                    max_id=max_id,
                    min_id=min_id,
                    add_offset=0,
                    hash=0
                ))  
            except ChannelPrivateError:
                print('Chat', chat, 'is private')
                return None
        return history.messages

    def get_chat_info(self, chat):
        with self._client as client:
            data = client(functions.channels.GetFullChannelRequest(channel=chat)).to_json()
        return json.loads(data)

    def is_private(self, chat):
        with self._client as client:
            result = client.get_entity(chat).restricted
        return result # Boolean

    def get_chat_name(self, chat_id):
        return self.get_chat_info(chat_id)["chats"][0]["title"]

    def get_chat_metadata(self, chat):
        """
        Get meta information about the given chat.
        
        chat - id or username of the chat
        """
        chat_info = self.get_chat_info(chat)
        type = 'broadcast'
        if chat_info["chats"][0]['megagroup'] == True:
            type = 'megagroup'
        if chat_info["chats"][0]['gigagroup'] == True:
            type = 'gigagroup'
        can_comment = 1
        if type == 'broadcast':
            can_comment = 0 if len(chat_info["chats"]) == 1 else 1
        metadata = {
            'id': chat_info["chats"][0]["id"],
            'title': chat_info["chats"][0]["title"],
            'username': chat_info['chats'][0]['username'],
            'type': type,
            'can_comment': can_comment
        }
        return metadata
    
    # Try to join the chat
    def join_chat(self, chat):
        print("Joining", self.get_chat_info(chat)["chats"][0]["username"])
        try:
            with self._client as client:
                client(functions.channels.JoinChannelRequest(channel=chat))
        except Exception as e:
            print("Failed to join chat:", e)

    # Leave the chat
    def leave_chat(self, chat):
        print("Leaving", self.get_chat_info(chat)["chats"][0]["username"])
        try:
            with self._client as client:
                client(functions.channels.LeaveChannelRequest(channel=chat))
        except Exception as e:
            print("Failed to join chat:", e)

    def print_user_dialogs(self):
        """Print the name and id of all chats of the user whose credentials are used. Being able to access these personal chats might be useful for testing."""
        with self._client as client:
            for dialog in client.iter_dialogs():
                print(dialog.name, dialog.entity.id)


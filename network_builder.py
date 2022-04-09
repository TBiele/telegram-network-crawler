import datetime
import logging
import os
import pandas as pd
import shutil
from data_model import CHATS_COLUMNS, SCANNED_COLUMNS, NODES_COLUMNS, EDGES_COLUMNS, MESSAGES_COLUMNS
from telegram import SyncTelegramClient
from telethon.errors.rpcerrorlist import ChannelPrivateError
import traceback
from tqdm import tqdm

from chat_lists import misinformation_channel_usernames, misinformation_channel_ids

# Initialize telegram client
telethon_api = SyncTelegramClient()
# Configure logging
logging.basicConfig(filename='log.log', level=logging.DEBUG)


def initialize_data():
    """Initialize the csv files. Data which was previously collected is lost."""
    # Show 'Are you Sure?' message
    if input("Are you sure you want to initialize the data? All previously collected data will be lost. Please enter (y/n)") != "y":
        exit()
    initialize_network()
    # Create messages directory if it does not exist
    if not os.path.exists('data/messages'):
        os.makedirs('data/messages')
    # Initialize csv files for storing Pandas dataframes
    df_chats = pd.DataFrame(columns=CHATS_COLUMNS)
    df_chats.to_csv('data/chats.csv', index=False)
    print('Initialized data')

def initialize_network():
    """Initialize the network csv files. Data which was previously collected is lost."""
    if input("Are you sure you want to initialize the network? All previously collected network data will be lost. Please enter (y/n)") != "y":
        exit()
    # If the edges directory is not empty, i. e. contains old data, delete it
    if os.path.exists('data/network/edges') and not len(os.listdir('data/network/edges')) == 0:
        shutil.rmtree('data/network/edges')
    # If the graphs directory is not empty, i. e. contains old data, delete it
    if os.path.exists('data/network/graphs') and not len(os.listdir('data/network/graphs')) == 0:
        shutil.rmtree('data/network/graphs')
    # Create edges directory if it does not exist
    if not os.path.exists('data/network/edges'):
        os.makedirs('data/network/edges')
    # Create graphs directory if it does not exist
    if not os.path.exists('data/network/graphs'):
        os.makedirs('data/network/graphs')
    # Initialize csv files for storing Pandas dataframes
    df_scanned = pd.DataFrame(columns=SCANNED_COLUMNS)
    df_scanned.to_csv('data/network/scanned_log.csv', index=False)
    df_nodes = pd.DataFrame(columns=NODES_COLUMNS)
    df_nodes.to_csv('data/network/nodes.csv', index=False)
    print('Initialized network')

def add_chats_by_id(chats):
    """
    Adds chats from the given list of ids to chats.csv.

    chats - A list of chat ids
    """
    df_chats = pd.read_csv('data/chats.csv')
    stored_chat_ids = list(df_chats.iloc[:,0])
    chat_ids = [chat_id for chat_id in chats if chat_id not in stored_chat_ids]
    for chat_id in chat_ids:
        if type(chat_id) != int:
            raise TypeError('The list should contain ids as integers')
        chat_metadata = None
        try:
            chat_metadata = telethon_api.get_chat_metadata(chat_id)
        except ValueError:
            print('ValueError in chat ' + str(chat_id) + '. This probably means that the chat is not known by its id yet. You need to first retrieve it in some other way. If you know the username, use add_chat_by_username instead. See https://docs.telethon.dev/en/latest/concepts/entities.html#summary for more information.')
            df_chats.to_csv('data/chats.csv', index=False)
            return
        except ChannelPrivateError:
            print('The chat', chat_id, 'could not be added to chats.csv because it is private.')
            continue
        df_chats.loc[len(df_chats.index)] = [chat_id, chat_metadata['title'], chat_metadata['username'], chat_metadata['type'], chat_metadata['can_comment']]
        stored_chat_ids.append(chat_id)
    df_chats.to_csv('data/chats.csv', index=False)

def add_chats_by_username(chats):
    """
    Adds chats from the given list of usernames to chats.csv.

    chats - A list of chat usernames
    """
    if os.path.isfile('data/chats.csv'):
        df_chats = pd.read_csv('data/chats.csv')
    else:
        df_chats = pd.DataFrame(columns=CHATS_COLUMNS)
    already_stored_ids = list(df_chats.iloc[:,0])
    already_stored_usernames = [username.lower() for username in list(df_chats.dropna().iloc[:,2])]
    chat_usernames = [username for username in chats if username.lower() not in already_stored_usernames]
    for chat_username in tqdm(chat_usernames):
        if type(chat_username) != str:
            raise TypeError('The list should contain usernames as strings')
        try:
            # TODO: Check if chat is already stored before using api
            chat_metadata = telethon_api.get_chat_metadata(chat_username)
        except ValueError as error:
            print('The chat', chat_username, 'was not added because:', error)
            continue
        except ChannelPrivateError:
            print('The chat', chat_username, 'could not be added to chats.csv because it is private.')
            continue
        except Exception as error:
            print('The chat', chat_username, 'could not be added to chats.csv due to an error:')
            print(error)
            continue
        chat_id = chat_metadata['id']
        if chat_id not in already_stored_ids:
            df_chats.loc[len(df_chats.index)] = [chat_id, chat_metadata['title'], chat_metadata['username'], chat_metadata['type'], chat_metadata['can_comment']]
    df_chats.to_csv('data/chats.csv', index=False)

def usernames_to_ids(usernames):
    """
    Transforms a list of usernames into a list of ids. The chats with the corresponding names must exist in chats.csv.

    usernames - The list of usernames that is transformed. Usernames are not case-sensitive.
    """
    if not os.path.isfile('data/chats.csv'):
        print('chats.csv does not exist yet. You need to call initialize_data first.')
        return None
    # Read chat.csv, drop rows with missing values
    df_chats = pd.read_csv('data/chats.csv').dropna()
    # Transform username column to lower case, so that differences in upper/lower case between usernames list and stored usernames do not matter.
    df_chats['username'] = df_chats['username'].str.lower()
    df_chats = df_chats.set_index('username')
    ids = None
    # For each username in the list, try to find a corresponding entry in the stored chats, return the ids
    try:
        ids = [int(df_chats.loc[username.lower()]['id']) for username in usernames]
    except TypeError:
        print('There might be a duplicate in chats.csv.')
        print(traceback.format_exc())
    except KeyError as missing_username:
        print('The chat of username', missing_username, 'does not exist in chats.csv. You need to add it first using add_chats_by_username.')
    return ids

def set_network_seed(seed):
    """
    Initializes the network by adding the seed chats from which to start crawling.

    seed - List of ids of the chats that are set as the initial nodes of the network
    """
    df_chats = pd.read_csv('data/chats.csv').set_index('id')
    df_nodes = pd.read_csv('data/network/nodes.csv')
    if not df_nodes.empty:
        print('There is still old network data. You must call initialize_network() before setting a new network seed.')
        exit()
    for chat_id in seed:
        chat = None
        if chat_id in df_chats.index:
            chat = df_chats.loc[chat_id]
        else:
            print('The chat with id', chat_id, 'does not exist in chats.csv. You need to add it first.')
            exit()
        df_nodes.loc[len(df_nodes.index)] = [chat_id, chat['name'], 1, 0]
    df_nodes.to_csv('data/network/nodes.csv', index=False)
    print('Network seed set')

def set_network_seed_by_usernames(seed):
    """
    Initializes the network by adding the seed chats from which to start crawling.

    seed - List of usernames of the chats that are set as the initial nodes of the network
    """
    ids = usernames_to_ids(seed)
    if ids is not None:
        set_network_seed(ids)

def add_nodes(nodes_id_list):
    """
    Adds the given nodes to the nodes.csv file. Only chats that are stored in chats.csv can be added as nodes.

    nodes_id_list - List of ids of the nodes to be added to the network
    """
    df_chats = pd.read_csv('data/chats.csv').set_index('id')
    df_nodes = pd.read_csv('data/network/nodes.csv')
    for node_id in nodes_id_list:
        try:
            node_name = df_chats.loc[node_id]['name']
            df_nodes.loc[len(df_nodes.index)] = [node_id, node_name, 0, 0]
        except KeyError:
            print('Cannot add chat', node_id, 'as a node because it was not added to chats.csv.')
    df_nodes.to_csv('data/network/nodes.csv', index=False)

def add_edges(chat_id, edges):
    """
    Adds the given edges to the edges csv file of the corresponding chat.

    chat_id - Id of the chat the forward edges were found in
    edges - The list of edges to be added
    """
    df_nodes = pd.read_csv('data/network/nodes.csv').set_index('chat_id')
    edges_file_path = 'data/network/edges/'+str(chat_id)+'.csv'
    # If the chat already has an edge file, read it. Otherwise initialize an empty DataFrame.
    if os.path.isfile(edges_file_path):
        df_edges = pd.read_csv(edges_file_path)
    else:
        df_edges = pd.DataFrame(columns=EDGES_COLUMNS)
    df_edges = df_edges.set_index('message_id')
    for edge in edges:
        message_id, forwarded_from = edge
        df_edges.loc[message_id] = [forwarded_from]
        if not forwarded_from in df_nodes.index:
            df_nodes.loc[forwarded_from] = ['', 0, 0]
        df_nodes.at[forwarded_from, 'in_degree'] += 1
    df_edges.to_csv(edges_file_path)
    df_nodes.to_csv('data/network/nodes.csv')

def add_messages(chat_id, messages):
    """
    Adds the given messages to the messages csv file of the corresponding chat.

    messages - List of messages to be added
    """
    messages_file_path = 'data/messages/'+str(chat_id)+'.csv'
    # Create messages directory if it does not exist
    if not os.path.exists('data/messages'):
        os.makedirs('data/messages')
    # If the chat already has a message file, read it. Otherwise initialize an empty DataFrame.
    if os.path.isfile(messages_file_path):
        df_messages = pd.read_csv(messages_file_path)
    else:
        df_messages = pd.DataFrame(columns=MESSAGES_COLUMNS)
    df_messages = df_messages.set_index('id')
    for message in messages:
        message_id = message.id
        message_content = message.message
        message_forwarded = 1 if message.fwd_from else 0
        message_date = message.date
        message_views = message.views
        message_forwards = message.forwards
        df_messages.loc[message_id] = [message_content, message_forwarded, message_date, message_views, message_forwards]
    df_messages.to_csv(messages_file_path)

""" This function does not work in Ipython """
def scan_chat(nodes_in_network_id_list, chat_id, batch_size=100, offset_id=0, offset_date=None):
    """Scans the given chat for forwarded messages from other chats in order to construct a network of chats. Stores all messages in messages.csv.

    Args:
        nodes_in_network_id_list: List of ids of all chats that are already part of the network.
        chat: Id of the chat that is going to be searched for forwards.
    Returns:
        new_nodes: Nodes in the network that were newly identified.
        forward_edges: a list of tuples (ch_destination ,ch_origin). This means that a message was forwarded from
            ch_origin to ch_destination.
        newest_message: the newest messages fetched from the chat in this run.
        oldest_message: the oldest message fetched from the chat in this run.
    """
    new_nodes = []
    forward_edges = []
    total_messages = 0
    newest_message = None
    oldest_message = None
    while total_messages < batch_size:
        try:
            # Fetch the last 100 messages
            messages = telethon_api.fetch_messages(
                chat=chat_id,
                offset_id=offset_id,
                offset_date=offset_date
            )
        except ValueError:
            print('ValueError in chat', chat_id)
            break
        except Exception as e:
            print('Exception in chat', chat_id, ':', e)
            break
        if not messages:
            break

        add_messages(chat_id, messages)
        newest_message = messages[0]
        for m in messages:
            oldest_message = m
            # If a msg was forwarded from another chat, append it to the list
            if m.fwd_from and hasattr(m.fwd_from ,'from_id') and hasattr(m.fwd_from.from_id, 'channel_id') and m.fwd_from.from_id.channel_id != chat_id:
                forwarded_from_id = m.fwd_from.from_id.channel_id
                try:
                    forward_edges.append((m.id, forwarded_from_id))
                    if not telethon_api.is_private(forwarded_from_id): # Just calling is_private on a private chat causes ChannelPrivateError
                        if forwarded_from_id not in new_nodes and forwarded_from_id not in nodes_in_network_id_list:
                            new_nodes.append(forwarded_from_id)
                except ChannelPrivateError:
                    logging.info(str(forwarded_from_id) + ' is private')
            total_messages += 1
            if total_messages >= batch_size:
                break
        offset_id = messages[len(messages) - 1].id

    return new_nodes, forward_edges, newest_message, oldest_message

def extend_network(iterations=1, scan_size=100, only_scan_chats=None, max_date=None, min_degree=0):
    """
    Take nodes from the network corresponding to chats that have not been scanned yet, search for forwarded messages in these chats, use them to extend the network.
    
    The chat ids along with the ranges of messages scanned are stored in data/network/scanned_log.csv.
    The network edges are stored in data/network/edges/<chat id>.csv, where the file is named after the chat the messages were forwarded to.
    Nodes are stored in data/network/nodes.csv and if they were discovered for the first time, the chat is stored in data/chats.csv

    iterations - The number of iterations in which the chats not scanned so far are taken from chats.csv and scanned to obtain new nodes/edges for the network
    scan_size - The number of messages that are scanned for forwards in each chat
    only_scan_chats - List of chat ids. If given, the network is only extended from these chats.
    min_degree - The minimum degree of a node from which the network is extended.
    """
    if not os.path.isfile('data/chats.csv'):
        print('chats.csv does not exist yet. You need to call initialize_data first.')
        return
    
    if only_scan_chats != None and type(only_scan_chats[0]) != int:
        print('only_scan_chats must be a list of ids')
        return

    # create edges directory if it does not exist
    if not os.path.exists('data/network/edges'):
        os.makedirs('data/network/edges')

    offset_date = datetime.datetime(*max_date).replace(hour=23, minute=59, second=59, microsecond=999999) if max_date != None else None
    
    for i in range(iterations):
        print('Extending network: Iteration', i+1, 'of', iterations)
        # Determine which chats to scan
        df_nodes = pd.read_csv('data/network/nodes.csv')
        already_stored_nodes_id_list = list(df_nodes.iloc[:,0])
        df_scanned_log = pd.read_csv('data/network/scanned_log.csv')
        scanned_nodes_id_list = list(df_scanned_log.iloc[:,0])
        if only_scan_chats != None:
            chats_to_scan = [chat_id for chat_id in already_stored_nodes_id_list if chat_id in only_scan_chats and chat_id not in scanned_nodes_id_list]
        else:
            chats_to_scan = [chat_id for chat_id in already_stored_nodes_id_list if chat_id not in scanned_nodes_id_list]
        df_nodes = df_nodes.set_index('chat_id')

        for chat_id in tqdm(chats_to_scan):
            # Prevent scanning a chat that has already been scanned. Prevent scanning a chat that has a degree of less than min_degree.
            if chat_id not in scanned_nodes_id_list and df_nodes.at[chat_id, 'in_degree'] >= min_degree:
                new_nodes_found, forward_edges, newest_message, oldest_message = scan_chat(already_stored_nodes_id_list, chat_id, batch_size=scan_size, offset_date=offset_date)
                if newest_message != None and oldest_message != None:
                    # log the range of messages scanned
                    scanned_nodes_id_list.append(chat_id)
                    df_scanned_log.loc[len(df_scanned_log.index)] = [chat_id, newest_message.id, newest_message.date, oldest_message.id, oldest_message.date]
                    df_scanned_log.to_csv('data/network/scanned_log.csv', index=False)
                    # store newly discovered chats as well as nodes and edges
                    add_chats_by_id(new_nodes_found)
                    add_nodes(new_nodes_found)
                    already_stored_nodes_id_list.extend(new_nodes_found)
                    add_edges(chat_id, forward_edges)
                else:
                    print('Chat', chat_id, 'contains no messages')
                    scanned_nodes_id_list.append(chat_id)
                    now = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).replace(microsecond=0).isoformat().replace('T', ' ')
                    df_scanned_log.loc[len(df_scanned_log.index)] = [chat_id, 0, now, 0, now]
                    df_scanned_log.to_csv('data/network/scanned_log.csv', index=False)


def extend_with_older_forwards(chat_id, scan_size=100):
    """
    Scan the given number of messages in the chat prior to the currently oldest scanned message. Identify forwards and use them to extend the network.

    chat_id - Id of the chat to be scanned
    scan_size - The number of messages that are scanned for forwards in the chat
    """
    df_scanned_log = pd.read_csv('data/network/scanned_log.csv')
    chat_row = df_scanned_log.loc[df_scanned_log['chat_id'] == chat_id]
    if chat_row.empty:
        print('The chat with id', chat_id, 'has not been scanned yet. Therefore it cannot be extended.')
        return
    oldest_message_id = int(chat_row['oldest_message_id'])
    df_nodes = pd.read_csv('data/network/nodes.csv')
    nodes_id_list = list(df_nodes.iloc[:,0])
    new_nodes_found, forward_edges, _, oldest_message = scan_chat(
        nodes_id_list, 
        chat_id, 
        batch_size=scan_size, 
        offset_id=oldest_message_id
    )
    if oldest_message != None:
        # If there were no older messages, oldest_message is None
        # Log the new oldest message scanned
        df_scanned_log = pd.read_csv('data/network/scanned_log.csv').set_index('chat_id')
        df_scanned_log.at[chat_id, 'oldest_message_id'] = oldest_message.id
        df_scanned_log.to_csv('data/network/scanned_log.csv')
        # store newly discovered chats as well as nodes and edges
        add_chats_by_id(new_nodes_found)
        add_nodes(new_nodes_found)
        add_edges(chat_id, forward_edges)

def extend_all_with_older_forwards(scan_size=100):
    """
    Scan the given number of messages in all chats in the network prior to the currently oldest scanned message in each chat. Identify forwards and use them
    to extend the network.

    scan_size - The number of messages that are scanned for forwards in each chat
    """
    df_scanned_log = pd.read_csv('data/network/scanned_log.csv')
    scanned_nodes_id_list = list(df_scanned_log.iloc[:,0])
    for chat_id in tqdm(scanned_nodes_id_list):
        extend_with_older_forwards(chat_id, scan_size)

def extend_chats_with_older_forwards(chat_ids=[], scan_size=100):
    """
    Scan the given number of messages in all chats in the list, prior to the currently oldest scanned message in each chat. Identify forwards and use them
    to extend the network.

    chat_ids - Ids of the chats to be scanned
    scan_size - The number of messages that are scanned for forwards in each chat
    """
    if len(chat_ids) > 0 and type(chat_ids[0]) != int:
        print('chat_ids must be a list of ids')
        return

    df_scanned_log = pd.read_csv('data/network/scanned_log.csv')
    scanned_nodes_id_list = list(df_scanned_log.iloc[:,0])
    for chat_id in tqdm(chat_ids):
        if chat_id not in scanned_nodes_id_list:
            print('The chat with id', chat_id, 'has not been scanned yet. Therefore it cannot be extended.')
        else:
            extend_with_older_forwards(chat_id, scan_size)


if __name__ == "__main__":
    pass
    # add_chats_by_username(misinformation_channel_usernames)
    # initialize_network()
    # set_network_seed_by_usernames(misinformation_channel_usernames)
    # extend_network(iterations=1, scan_size=100, max_date=(2022, 2, 28), min_degree=5)
    # extend_chats_with_older_forwards(misinformation_channel_ids, scan_size=200)
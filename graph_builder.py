import logging
import networkx as nx
import os
import pandas as pd
import pickle
from networkx import in_degree_centrality
from telegram import SyncTelegramClient
from telethon.errors.rpcerrorlist import ChannelPrivateError

# Initialize telegram client
telethon_api = SyncTelegramClient()
# Configure logging
logging.basicConfig(filename='log.log', level=logging.DEBUG)


def build_graph(min_edge_weight_threshold=0, min_in_degree_threshold=0):
    """
    Build and store a networkx graph instance of the network of Telegram chats using the crawled data.

    Args:
        min_edge_weight_threshold (int, optional): Threshold for the minimum weight (forwards from one chat to the other)
            of edges that are added to the graph. Defaults to 0.
        min_in_degree_threshold (int, optional): Threshold for the minimum in-degree (number of chats that forwarded from this chat)
            of nodes that are added to the graph. Chats that were scanned are added regardless of in-degree. Defaults to 0.
    """
    
    # Instantiate the network graph
    G = nx.DiGraph()
    # Get the ids and names of all chats that have been scanned
    df_scanned_log = pd.read_csv('data/network/scanned_log.csv').set_index('chat_id')
    df_chats = pd.read_csv('data/chats.csv').set_index('id')
    df_nodes = pd.read_csv('data/network/nodes.csv').set_index('chat_id')
    df_scanned = df_scanned_log.join(df_chats)

    # Construct the network graph
    for scanned_chat in df_scanned.itertuples(index=True):
        chat_id = scanned_chat[0]
        if not G.has_node(chat_id):
            G.add_node(chat_id, label=scanned_chat.name)
        try:
            df_edges = pd.read_csv('data/network/edges/'+str(chat_id)+'.csv')
        except FileNotFoundError:
            # Skip chats with no edges file
            continue
        weighted_edges = df_edges["forwarded_from"].value_counts()
        for forwarded_from, edge_weight in weighted_edges.items():
            # Only add nodes and edges that exceed the defined thresholds
            if df_nodes.at[forwarded_from, 'in_degree'] >= min_in_degree_threshold and edge_weight >= min_edge_weight_threshold:
                # If the node corresponding to the chat forwarded_from does not exist yet, create it
                if not G.has_node(forwarded_from):
                    forwarded_name = ''
                    try:
                        forwarded_name = df_chats.at[forwarded_from, 'name']
                    except:
                        # If the chat name is not in the database yet, fetch it
                        try:
                            forwarded_name = telethon_api.get_chat_name(forwarded_from)
                        except ChannelPrivateError:
                            logging.info(str(forwarded_from) + ' is private')
                            continue
                    G.add_node(forwarded_from, label=forwarded_name)
                G.add_edge(chat_id, forwarded_from, value=edge_weight)
    
    # Create graphs directory if it does not exist
    if not os.path.exists('data/network/graphs'):
        os.makedirs('data/network/graphs')
    # Store network graph in pickle file
    nodes_string = f"nodes_restricted_with_{min_in_degree_threshold}" if min_in_degree_threshold > 0 else "nodes_complete"
    edges_string = f"edges_restricted_with_{min_edge_weight_threshold}" if min_edge_weight_threshold > 0 else "edges_complete"
    pickle.dump(G, open(f"data/network/graphs/{nodes_string}_{edges_string}.p", "wb"))


def get_degree_ranking(graph_name):
    graph = pickle.load(open('data/network/graphs/'+graph_name+'.p', 'rb'))
    return sorted(in_degree_centrality(graph).items(), key=lambda item: item[1], reverse=True)


def get_top_k_degree_chats(graph_name, k):
    degree_ranking = get_degree_ranking(graph_name)
    top_k = degree_ranking[:k]
    df_top_k = pd.DataFrame(top_k, columns=['id', 'centrality'])
    df_chats = pd.read_csv('data/chats.csv')
    df_top_k = df_top_k.set_index('id').join(df_chats.set_index('id'))
    return df_top_k


if __name__ == "__main__":
    build_graph(min_edge_weight_threshold=2, min_in_degree_threshold=2)
    # df_top_k = get_top_k_degree_chats("full_graph", 20)

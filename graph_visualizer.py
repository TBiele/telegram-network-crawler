import os
import pickle
from pyvis.network import Network


def show_graph(graph_name):
    file_path = 'data/network/graphs/'+graph_name+'.p'
    if not os.path.isfile(file_path):
        print("Graph not found. You may need to run build_graph() first.")
        return
    graph = pickle.load(open(file_path, 'rb'))
    # Set network graph parameters
    nt = Network('900', '1300', directed=True)
    nt.barnes_hut()
    nt.show_buttons(filter_=['physics'])
    nt.from_nx(graph)
    # Show the network graph
    nt.show(graph_name+'.html')


if __name__ == "__main__":
    show_graph(graph_name='nodes_restricted_with_3_edges_restricted_with_5')

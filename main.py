from Node import Node
import time
import threading
import random
import networkx as nx
import json
import matplotlib.pyplot as plt
import os


def deActiveNodeRandomly(nodes,baseTime,totalTime=300):
    node = None
    while node is None:
        node = random.choice(nodes)
        if node.state == 'deActive':
            node = None
    node.deActive()
    if time.time() - baseTime < totalTime:
        timer = threading.Timer(10,deActiveNodeRandomly,(nodes,baseTime))
        timer.start()

def drawGraphs():
    options = {
        'node_color': 'pink',
        'node_size': 10000,
        'width': 2,
        'arrowstyle': '-|>',
        'arrowsize': 10,
        'font_size': 10,
        'edge_color':'green',
        'with_labels':True,
    }
    for i in range(6):
        graph = nx.DiGraph()
        json_inp = json.load(open('./json_output/report_node_'+str(i)+'.json'))
        vertices = [tuple(vertex) for vertex in json_inp['topology']['vertex']]
        edges = []
        for x in json_inp['topology']['edges']:
            temp = []
            for y in x:
                temp.append(tuple(y))
            edges.append(tuple(temp))
        plt.rcParams["figure.figsize"] = (20,10)
        graph.add_nodes_from(vertices)
        graph.add_edges_from(edges,weight=1)
        current_node_color = 'yellow' if json_inp['state'] == 'Active' else 'red'
        colors = ['pink' if x!=tuple(json_inp['address']) else current_node_color for x in graph.nodes()]
        options['node_color'] = colors
        pos = nx.spring_layout(graph,scale=5,k=5)
        nx.draw(graph,pos,**options)
        # plt.show()
        plt.savefig('./network_graphs/graph_node_'+str(i)+'.png',format="PNG")
        plt.clf()

def prepare():
    if not os.path.exists('./json_output'):
        os.makedirs('./json_output')
    if not os.path.exists('./network_graphs'):
        os.makedirs('./network_graphs')

def runNetwork():
    baseTime = time.time()
    nodes = [Node(i,baseTime,300) for i in range(6)]
    addresses = [node.socket.getsockname() for node in nodes]
    for node in nodes:
        node.start(addresses)
    timer = threading.Timer(10,deActiveNodeRandomly,(nodes,baseTime))
    timer.start()
    for node in nodes:
        node.selectThread.join()
        node.recvThread.join()
    for node in nodes:
        node.report()

if __name__ == "__main__":
    prepare()
    runNetwork()
    drawGraphs()
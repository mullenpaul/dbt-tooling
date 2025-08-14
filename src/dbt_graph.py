import plotly.graph_objects as go

import networkx as nx
import random
from typing import List
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import SeedNode, ModelNode, GenericTestNode, SingularTestNode, UnitTestNode



def build_dbt_graph(manifest: Manifest):
    di = nx.DiGraph()
    #print(manifest)
    sources = manifest.sources
    print(sources)
    for source in sources:
        print(source.name)
        di.add_node(source.name)
        nx.set_node_attributes(di, {source.name: (100, 100)}, "pos")
    models = dbt_helpers.get_node_type(manifest.nodes, ModelNode)
    seeds = dbt_helpers.get_node_type(manifest.nodes, SeedNode)
    for node in models:
        name = node.node_info["unique_id"]
        print(node.node_info.keys())
        print(name)
        di.add_node(name)
        nx.set_node_attributes(di, {name: name}, "name")
    
    for seed in seeds:
        name = seed.node_info["unique_id"]
        print(name)
        di.add_node(name)
    details_list = {

    }
    for node in di.nodes():
        print(f"chunking {node}")
        name_parts = node.split(".")
        group = name_parts[0]
        stage = name_parts[1]
        model_name = name_parts[2]
        details = {
            "name": node,
            "group": group,
            "stage": stage,
            "model_name": model_name
        }
        details_list[node] = details
        print(details)
    print(details_list)
    nx.set_node_attributes(di, details_list)
    print(di.nodes['model.jaffle_shop.customers'])

    for node in models:
        data = [(node.node_info["unique_id"], dep) for dep in node.depends_on_nodes]
        print(data)
        for i in data:
            di.add_edge(i[0], i[1])
    
    
    return determine_positions(di)

def determine_positions(di):
    y_offset = 0
    pos = {

    }
    for node in di.nodes():
        name = node
        print(di.nodes[name])
        print(di.nodes[name]['group'])
        pos[name] = (random.random() * 300, random.random() * 300)
        y_offset += 1
    nx.set_node_attributes(di, pos, "pos")
    return di

def build_graph_figure(di):
    edge_x = []
    edge_y = []
    for edge in di.edges():
        nodes = di.nodes()
        print(f"{nodes[edge[0]]} --- {nodes[edge[1]]}")
        x0, y0 = di.nodes[edge[0]]['pos']
        x1, y1 = di.nodes[edge[1]]['pos']
        edge_x.append(x0)
        edge_x.append(x1)
        edge_x.append(None)
        edge_y.append(y0)
        edge_y.append(y1)
        edge_y.append(None)

    edge_trace = go.Scatter(
        x=edge_x, y=edge_y,
        line=dict(width=0.5, color='#888'),
        hoverinfo='none',
        mode='lines')

    node_x = []
    node_y = []
    for node in di.nodes():
        x, y = di.nodes[node]['pos']
        node_x.append(x)
        node_y.append(y)

    node_trace = go.Scatter(
        x=node_x, y=node_y,
        mode='markers',
        hoverinfo='text',
        marker=dict(
            showscale=True,
            # colorscale options
            #'Greys' | 'YlGnBu' | 'Greens' | 'YlOrRd' | 'Bluered' | 'RdBu' |
            #'Reds' | 'Blues' | 'Picnic' | 'Rainbow' | 'Portland' | 'Jet' |
            #'Hot' | 'Blackbody' | 'Earth' | 'Electric' | 'Viridis' |
            colorscale='YlGnBu',
            reversescale=True,
            color=[],
            size=10,
            colorbar=dict(
                thickness=15,
                title=dict(
                text='Node Connections',
                side='right'
                ),
                xanchor='left',
            ),
            line_width=2))

    node_adjacencies = []
    node_text = []
    for n in di.nodes:
        node_adjacencies.append(1)
        print(n)
        node_text.append(n)

    node_trace.marker.color = node_adjacencies
    node_trace.text = node_text

    text_trace = go.Scatter(
        x=node_x, y=[i + 1 for i in node_y],
        mode='text',
        hoverinfo='text',
        marker=dict(
            showscale=True,
            # colorscale options
            #'Greys' | 'YlGnBu' | 'Greens' | 'YlOrRd' | 'Bluered' | 'RdBu' |
            #'Reds' | 'Blues' | 'Picnic' | 'Rainbow' | 'Portland' | 'Jet' |
            #'Hot' | 'Blackbody' | 'Earth' | 'Electric' | 'Viridis' |
            colorscale='YlGnBu',
            reversescale=True,
            color=[],
            size=10,
            colorbar=dict(
                thickness=15,
                title=dict(
                text='Node Connections',
                side='right'
                ),
                xanchor='left',
            ),
            line_width=2))
    text_trace.text = node_text

    fig = go.Figure(data=[edge_trace, node_trace, text_trace],
                layout=go.Layout(
                    title=dict(
                        text="<br>Network graph made with Python",
                        font=dict(
                            size=16
                        )
                    ),
                    showlegend=False,
                    hovermode='closest',
                    margin=dict(b=20,l=5,r=5,t=40),
                    xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                    yaxis=dict(showgrid=False, zeroline=False, showticklabels=False))
                    )
    return fig

if __name__ == '__main__':
    di = nx.DiGraph()
    di.add_nodes_from([1,2,3])
    pos = {
        1: (100, 100),
        2: (150, 100),
        3: (100, 150)
    }
    di.add_edge(1, 2)
    di.add_edge(2, 3)
    nx.set_node_attributes(di, pos, "pos")
    import dbt_helpers
    target_folder = "../jaffle_shop_duckdb"
    man = dbt_helpers.get_manifest(target_folder)
    di = build_dbt_graph(man) #nx.random_geometric_graph(200, 0.125)
    fig = build_graph_figure(di)
    fig.show()
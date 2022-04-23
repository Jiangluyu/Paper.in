from pyspark.sql.functions import explode, lit
from graphframes import *

def undirect_df_edge(df_edge):
    """ Convert the edges of a directed graph to an undirected graph"""
    # Record the other columns except for src and dst
    other_cols = [col for col in df_edge.columns if col not in ("src", "dst")]

    # Reverse the direction
    df_edge_reverse = df_edge \
            .select("dst", "src", *other_cols) \
            .toDF("src", "dst", *other_cols)
    
    # Combine the original edges and reversed edges
    # to form an undirected graph
    return df_edge.union(df_edge_reverse)

def create_graph_ref(df_papers):
    """ Create a graph based on reference of papers """
    # Node is the papers
    df_node = df_papers \
        .select("paper_doi") \
        .toDF("id")
    
    # Edge is the reference link directed to cited paper
    df_edge = df_papers \
        .filter(df_papers.refs_doi.isNotNull()) \
        .select(df_papers.paper_doi, explode(df_papers.refs_doi)) \
        .toDF("src", "dst")

    return GraphFrame(df_node, df_edge)

def create_graph_author(df_papers):
    """ Create a k-partite graph based on authorship """

    # Node is both the paper and author
    df_node_paper = df_papers \
        .select(df_papers.paper_doi, lit("paper")) \
        .toDF("id", "node_type")
    df_node_author = df_papers \
        .select(explode(df_papers.author_id), lit("author")) \
        .distinct() \
        .toDF("id", "node_type")

    # Union the papers and authors as nodes
    df_node = df_node_paper.union(df_node_author)

    # The edge is the authorship relation
    # The edge is undirected, so it needs to create both directions
    # from paper to author and from author to paper
    df_edge = df_papers \
        .select(df_papers.paper_doi, explode(df_papers.author_id)) \
        .toDF("src", "dst")

    # Convert the directed graph to undirected graph
    df_edge = undirect_df_edge(df_edge)
    
    return GraphFrame(df_node, df_edge)

def create_graph_ref_author(graph_ref, graph_author, ref_undirected=False):
    """ 
    Combine the reference graph and author graph together
    to form a k-partite graph including 
    both reference relations and authorship relation.

    `ref_undirected`: Whether to convert the reference relation to undirected graph
    """
    # The node comes directly from author graph
    df_node = graph_author.vertices

    # The relation of reference
    df_edge_ref = graph_ref.edges \
        .select("src", "dst", lit("ref")) \
        .toDF("src", "dst", "edge_type")

    # Convert the reference link to undirected link
    if ref_undirected:
        df_edge_ref = undirect_df_edge(df_edge_ref)

    # Relation of authorship
    df_edge_author = graph_author.edges \
        .select("src", "dst", lit("author")) \
        .toDF("src", "dst", "edge_type")

    # Combine the reference and authorship relations
    df_edge = df_edge_ref.union(df_edge_author)

    return GraphFrame(df_node, df_edge)

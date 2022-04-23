from graphframes import *
from .graph_create import undirect_df_edge

def ref_pagerank(graph_ref, resetProbability=0.15, 
        maxIter=30, tol=None):
    """
    Compute the pagerank of value on reference graph
    to evaluate the importance of the paper
    """
    return graph_ref \
        .pageRank(resetProbability=resetProbability, 
                maxIter=maxIter, tol=tol) \
        .vertices \
        .toDF("paper_doi", "pagerank") \
        .orderBy("pagerank", ascending=False)

def ref_topic_pagerank(graph_ref, start_paper_doi, 
        resetProbability=0.15, maxIter=10, 
        undirected=False):
    """
    Compute the topic-specific pagerank from a start paper
    based on reference graph
    to evaluate the similarity between start paper with other papers

    `start_paper_doi`: Can be one paper or a list of papers.\\
    `undirected`: Whether to convert the graph 
    to an undirected graph for more connections.
    """
    # Convert the graph to undirected graph if specified
    if undirected:
        df_edge = undirect_df_edge(graph_ref.edges)
        graph_ref = GraphFrame(graph_ref.vertices, df_edge)
    
    # Compute the topic-specific pagerank
    if isinstance(start_paper_doi, str):
        pagerank = graph_ref. \
            pageRank(resetProbability=resetProbability, 
                sourceId=start_paper_doi, maxIter=maxIter) \
            .vertices
        pagerank = pagerank \
            .filter(pagerank.id != start_paper_doi) \
            .filter(pagerank.pagerank > 1e-5) \
            .orderBy("pagerank", ascending=False) \
            .toDF("related_paper", "pagerank")
        
        return pagerank
        
    elif isinstance(start_paper_doi, (list, tuple)):
        # Compute the topic-specific pagerank in parallel
        pagerank = graph_ref. \
            parallelPersonalizedPageRank(resetProbability=resetProbability, 
                sourceIds=start_paper_doi, maxIter=maxIter) \
            .vertices
        
        # Convert the result to a dataframe of paper pairs
        # and sort the result 
        pagerank = pagerank \
            .rdd \
            .flatMap(lambda row:
                    [(p, row['id'], float(rank))
                    for p, rank in zip(start_paper_doi, row['pageranks'])
                    if row['id'] != p and rank > 1e-5]
                ) \
            .toDF(["start_paper", "related_paper", "pagerank"])
        return pagerank
    else:
        return None

def ref_all_topic_pagerank(graph_ref, 
        resetProbability=0.15, maxIter=10, undirected=False):
    """ 
    Compute topic-specific pagerank of all the papers 
    (compute the similarity for every papar pair)
    """
    # Remove the vertices that do not have reference links
    graph_ref = graph_ref.dropIsolatedVertices()

    all_papers = graph_ref.vertices.rdd.flatMap(lambda x: x).collect()
    pageranks = ref_topic_pagerank(graph_ref, all_papers, 
        resetProbability=resetProbability, 
        maxIter=maxIter, undirected=undirected)
    
    return pageranks

    # # The implementation below is too slow
    # # Convert the graph to undirected graph if specified
    # if undirected:
    #     df_edge = undirect_df_edge(graph_ref.edges)
    #     graph_ref = GraphFrame(graph_ref.vertices, df_edge)
    
    # result = None
    
    # for row in graph_ref.vertices.toLocalIterator():
    #     start_paper = row["id"]
        # pagerank = ref_topic_pagerank(graph_ref, start_paper, 
        #     resetProbability=resetProbability, 
        #     maxIter=maxIter, undirected=False)
        
    #     # Clean the pagerank with zero value
    #     # sort the value
    #     pagerank = pagerank \
    #         .filter("pagerank > 1e-5") \
    #         .filter(pagerank.id != start_paper) \
    #         .orderBy("pagerank", ascending=False)
        
    #     # Keep only top N papers
    #     if keep_top:
    #         pagerank = pagerank.limit(keep_top)
        
    #     pagerank = pagerank \
    #         .select(lit(start_paper), "id", "pagerank") \
    #         .toDF("start_paper", "related_paper", "pagerank")
        
    #     # Store the result
    #     if not result:
    #         result = pagerank
    #     else:
    #         result = result.union(pagerank)

    # return result

def author_simrank(graph_author, start_author, 
        resetProbability=0.15, maxIter=10):
    """
    Compute the SimRank of authors
    to evaluate the similarity between authors

    `graph_author`: Can input either `graph_author` or `graph_ref_author`
    (the latter will consider both authorship and reference)
    """
    simrank = graph_author. \
        pageRank(resetProbability=resetProbability, 
            sourceId=start_author, maxIter=maxIter) \
        .vertices

    simrank = simrank \
        .filter(simrank.node_type == 'author') \
        .filter(simrank.pagerank > 1e-5) \
        .filter(simrank.id != start_author) \
        .select("id", "pagerank") \
        .toDF("related_author", "simrank") \
        .orderBy("simrank", ascending=False)
    
    return simrank

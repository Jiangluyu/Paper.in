# Combine all methods to retrieve related papers/authors
# and convert it to Python list or Pandas DataFrame

from utils.pagerank import ref_pagerank, ref_topic_pagerank, \
    author_simrank
from utils.lpa import related_paper_lpa

def get_paper_pagerank(graph_ref, resetProbability=0.15, 
        maxIter=30, tol=None):
    """
    Get the PageRank of papers based on reference graph

    Output: A Pandas DataFrame consisting of two columns:
    "paper_doi" and "pagerank".\\
    The DataFrame is sorted by "pagerank" descendingly.
    """

    return ref_pagerank(graph_ref, 
            resetProbability=resetProbability, 
            maxIter=maxIter, tol=tol) \
        .toPandas()

def get_related_paper_topic_pagerank(graph_ref, start_paper_doi, 
        resetProbability=0.15, maxIter=10, undirected=False):
    """
    Compute the topic-specific pagerank from a start paper
    based on reference graph
    to evaluate the similarity between start paper with other papers

    `start_paper_doi`: Can be one paper or a list of papers.\\
    `undirected`: Whether to convert the graph 
    to an undirected graph for more connections

    Output: \\
    For one start paper, 
    output a Pandas DataFrame consists of two columns:
    "related_paper", "pagerank" \\
    For a list of start papers, 
    output a Pandas DataFrame consists of three columns:
    "start_paper", "related_paper", "pagerank"
    """
    return ref_topic_pagerank(graph_ref, start_paper_doi, 
            resetProbability=resetProbability, 
            maxIter=maxIter, undirected=undirected) \
        .toPandas()

def get_related_author_simrank(graph_author, start_author, 
        resetProbability=0.15, maxIter=10):
    """
    Compute the SimRank of authors
    to evaluate the similarity between authors

    `graph_author`: Can input either `graph_author` or `graph_ref_author`
    (the latter will consider both authorship and reference)

    Output: A Pandas Dataframe consisting of two columns: 
    "related_author" and "simrank". \\
    The DataFrame is sorted by "simrank" descendingly.
    """

    simrank = author_simrank(graph_author, start_author, 
        resetProbability=resetProbability, maxIter=maxIter)
    
    return simrank.toPandas()

def get_related_paper_lpa(graph_ref, 
        source_paper, maxIter=30):
    """ 
    Get the related papers as a list using LPA method

    Output: a list of related papers' doi
    """
    df_related_papers = related_paper_lpa(graph_ref, 
            source_paper, maxIter=maxIter)

    # Convert it to a Python list
    return df_related_papers.rdd.flatMap(lambda x: x).collect()

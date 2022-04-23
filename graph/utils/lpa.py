from graphframes import *

def graph_ref_lpa(graph_ref, maxIter=30):
    """ 
    Use label propagation to generate clusters of related papers
    based on reference network
    """
    return graph_ref \
        .labelPropagation(maxIter=maxIter)

def related_paper_lpa(graph_ref, source_paper, maxIter=30):
    """ 
    Retrieve the related papers as a list
    from the same LPA cluster of source paper
    """
    # Get the result of LPA method
    df_paper_lpa = graph_ref_lpa(graph_ref, maxIter=maxIter)

    # Collect all papers in the same cluster as the source paper
    df_related_papers = df_paper_lpa \
        .filter(df_paper_lpa.id == source_paper) \
        .alias("a") \
        .join(df_paper_lpa.alias("b"), "label") \
        .select("b.id") \
        .toDF("related_paper")
    
    # Remove the source paper
    df_related_papers = df_related_papers \
        .filter(df_related_papers.related_paper != source_paper)
    
    return df_related_papers

import os
import pandas as pd
import psycopg2
from pyspark.sql import SparkSession

from utils.graph_create import *
from retrieve import *

# The SQL query to read the data from PostgreSQL
# query_authors = 'select author_id from authors'
query_papers = '''
    select paper_doi, 
    author_id, refs_doi 
    from public."PaperInfo_paper"
'''

# Read the PostgreSQL configuration from environment variables
# Set the variables before executing
postgres_db_name = os.environ['POSTGRES_DB_NAME']
postgres_user = os.environ['POSTGRES_USER']
postgres_passwd = os.environ['POSTGRES_PASSWD']

if __name__ == '__main__':
    # Read the data from PostgreSQL
    conn = psycopg2.connect(dbname=postgres_db_name, 
            user=postgres_user, password=postgres_passwd)
    # df_authors = pd.read_sql_query(query_authors, conn)
    df_papers = pd.read_sql_query(query_papers, conn)

    spark = SparkSession \
        .builder \
        .appName('Paper-graph') \
        .getOrCreate()

    # Convert the pandas dataframes to spark dataframe
    # df_authors = spark.createDataFrame(df_authors).repartition(10)
    df_papers = spark.createDataFrame(df_papers).repartition(10)

    df_papers.cache()

    # Create the graph based on reference and authorship
    graph_ref = create_graph_ref(df_papers)
    graph_author = create_graph_author(df_papers)

    graph_ref.cache()
    graph_author.cache()

    # Create the graph combining both reference and authorship relations
    graph_ref_author = create_graph_ref_author(graph_ref, 
            graph_author, ref_undirected=True)
    graph_ref_author.cache()

    # Remove the input dataframes from cache to free memory space
    df_papers.unpersist()

    # Get pagerank of every paper
    print(get_paper_pagerank(graph_ref, maxIter=20))

    # Get the topic specific pagerank from a given start paper
    print(get_related_paper_topic_pagerank(graph_ref, 
        '10.1109/ICCV.2011.6126229', undirected=True))

    # Get the related authors from a given author using simrank
    print(get_related_author_simrank(graph_ref_author, 
        '88855'))

    # Get the related papers using LPA method
    print(get_related_paper_lpa(graph_ref, 
        '10.1109/ICCV.2011.6126229', maxIter=15))

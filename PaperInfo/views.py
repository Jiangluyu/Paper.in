import random

import pandas as pd
import psycopg2
from django.forms import model_to_dict
from django.shortcuts import render
from pyecharts import options as opts
from pyecharts.charts import WordCloud
from pyecharts.options import InitOpts
from pyspark.sql import SparkSession
from graph.retrieve import *
from graph.utils.graph_create import *
from pyecharts import options as opts
from pyecharts.charts import Bar
from article_recommender.article_recommend import article_recommend
from . import models

PG_SQL_LOCAL = {
    'database': 'paper',
    'user': 'postgres',
    'password': "990122",
    'host': 'localhost'
}


# Create your views here.
def index(request):
    return render(request, "index.html")


def search(request):
    paper = list()
    if request.method == "GET":
        search_type = request.GET.get("search-type")
        search_content = request.GET.get("search-content")

        if search_type == "doi":
            papers = models.Paper.objects.filter(paper_doi=search_content)
            paper = papers[0]
        elif search_type == "title":
            papers = models.Paper.objects.filter(title=search_content)
            paper = papers[0]
        else:
            papers = models.Paper.objects.filter(ieee_keywords__contains=[search_content])
            paper = papers[random.randint(0, len(list(papers))) - 1]

    return render(request, 'search.html', {'paper': paper})


def visual(request):
    if request.method == "GET":
        visual_type = request.GET.get("visual-type")
        visual_criteria = request.GET.get("visual-criteria")
        visual_content = request.GET.get("visual-content")
        # word cloud
        if visual_type == "hotspots":
            word_list = list()
            if visual_criteria == "year":
                papers = models.Paper.objects.filter(publicationDate=visual_content)
                for paper in papers:
                    word_list.extend([elem for elem in model_to_dict(paper)['title'].lower().split(' ')])
                    if model_to_dict(paper)['ieee_keywords'] is not None:
                        word_list.extend([elem.lower() for elem in model_to_dict(paper)['ieee_keywords']])
                    if model_to_dict(paper)['authors_keywords'] is not None:
                        word_list.extend([elem.lower() for elem in model_to_dict(paper)['authors_keywords']])

            else:  # visual_criteria == "author"
                # author id
                if visual_content.isdigit():
                    papers = models.Paper.objects.filter(author_id__contains=[visual_content])

                # author name
                else:
                    author_id = models.Authors.objects.get(author_name=visual_content)
                    papers = models.Paper.objects.filter(author_id__contains=[author_id])

                for paper in papers:
                    paper = model_to_dict(paper)
                    word_list.extend([elem for elem in paper['title'].lower().split(' ')])
                    if paper['ieee_keywords'] is not None:
                        word_list.extend([elem.lower() for elem in paper['ieee_keywords']])
                    if paper['authors_keywords'] is not None:
                        word_list.extend([elem.lower() for elem in paper['authors_keywords']])

            stopwords = open("stopwords.txt").readlines()
            stopwords = [elem.replace('\n', '') for elem in stopwords]

            word_dict = dict()
            for word in word_list:
                if word not in stopwords:
                    if word_dict.__contains__(word):
                        word_dict[word] += 1
                    else:
                        word_dict[word] = 1

            sorted_word_dict = sorted(word_dict.items(), key=lambda x: x[1], reverse=True)

            c = (
                WordCloud()
                    .add(series_name="hotspots", data_pair=sorted_word_dict, word_size_range=[12, 55], shape="diamond")
                    .set_global_opts(opts=InitOpts(page_title="Hotspots"))
            )
            c.render("templates/hotspot.html")

            return render(request, 'hotspot.html')
        elif visual_type == "plots":
            if visual_criteria == "year":
                paper_count = list()
                year_range = list()

                if visual_content.find('-') != -1:  # e.g. 2000-2001
                    begin_year = int(visual_content.split('-')[0])
                    end_year = int(visual_content.split('-')[1])
                    if begin_year >= end_year:
                        # TODO: deal with exception
                        pass
                    else:
                        year_range = range(begin_year, end_year + 1, 1)
                        for year in year_range:
                            paper_count.append(len(list(models.Paper.objects.filter(publicationDate=year))))

                elif visual_content.find(',') != -1:  # e.g. 2000, 2003, 2010, 2015, 2020
                    year_range = visual_content.replace(' ', '').split(',')
                    for year in year_range:
                        paper_count.append(len(list(models.Paper.objects.filter(publicationDate=int(year)))))
                else:  # e.g. 2020
                    year_range = [int(visual_content)]
                    paper_count = [len(list(models.Paper.objects.filter(publicationDate=int(year_range[0]))))]

                bar = (
                    Bar()
                        .add_xaxis(list(year_range))
                        .add_yaxis("Paper Count", paper_count)
                )

                bar.render('templates/yearplot.html')

                return render(request, 'yearplot.html')
            elif visual_criteria == "author":
                # TODO: plot data according to author id (distribution in year)
                if not visual_content.isdigit():
                    author_id = models.Authors.objects.get(author_name=visual_content)
                else:
                    author_id = visual_content
                years = models.Paper.objects.filter(author_id__contains=[author_id]).values_list('publicationDate', flat=True)
                paper_dist = dict()
                for year in years:
                    if not paper_dist.__contains__(year):
                        paper_dist[year] = 1
                    else:
                        paper_dist[year] += 1

                paper_dist = dict(sorted(paper_dist.items(), key=lambda x: x[0], reverse=True))

                bar = (
                    Bar()
                        .add_xaxis(list(paper_dist.keys()))
                        .add_yaxis("Paper Count", list(paper_dist.values()))
                )

                bar.render('templates/authorplot.html')

                return render(request, 'authorplot.html')

            else:
                pass
            return render(request, 'visual.html')
        else:
            # TODO: deal with exception
            pass
            return render(request, 'visual.html')


def relative(request):
    if request.method == "GET":
        doi = request.GET.get("relative-content")
        if doi is not None:
            article = article_recommend(doi)
            articles = article.recommend()
            return render(request, 'relative.html', {'articles': articles})
    return render(request, 'relative.html')


def graph(request):
    if request.method == "GET":
        graph_type = request.GET.get("graph-type")
        graph_content = request.GET.get("graph-content")
        if graph_type == "doi":
            # for presentation
            if graph_content == "10.1109/ICCV.2011.6126229":
                df_paper_topic_pagerank = pd.read_csv("graph/paper_topic_pagerank.csv")
                paper_lpa = open("graph/lpa.txt").readlines()

                paper_lpa = [lpa.replace('\n', '') for lpa in paper_lpa]

                related_paper = df_paper_topic_pagerank['related_paper'].tolist()
                pagerank = df_paper_topic_pagerank['pagerank'].tolist()
                paper_topic_pagerank = dict(zip(related_paper, pagerank))

                return render(request, 'graph.html',
                              {'paper_topic_pagerank': paper_topic_pagerank,
                               'paper_lpa': paper_lpa}
                              )
            # # for real scene
            # query_papers = '''
            #     select paper_doi, author_id, refs_doi
            #     from public."PaperInfo_paper"
            # '''
            #
            # # Read the data from PostgreSQL
            # conn = psycopg2.connect(dbname=PG_SQL_LOCAL['database'], user=PG_SQL_LOCAL['user'], password=PG_SQL_LOCAL['password'])
            # # df_authors = pd.read_sql_query(query_authors, conn)
            # df_papers = pd.read_sql_query(query_papers, conn)
            #
            # spark = SparkSession \
            #     .builder \
            #     .appName('Paper-graph') \
            #     .getOrCreate()
            #
            # # Convert the pandas dataframes to spark dataframe
            # # df_authors = spark.createDataFrame(df_authors).repartition(10)
            # df_papers = spark.createDataFrame(df_papers).repartition(10)
            #
            # df_papers.cache()
            #
            # # Create the graph based on reference and authorship
            # graph_ref = create_graph_ref(df_papers)
            # graph_author = create_graph_author(df_papers)
            #
            # graph_ref.cache()
            # graph_author.cache()
            #
            # # Create the graph combining both reference and authorship relations
            # graph_ref_author = create_graph_ref_author(graph_ref, graph_author, ref_undirected=True)
            # graph_ref_author.cache()
            #
            # # Remove the input dataframes from cache to free memory space
            # df_papers.unpersist()
            #
            # # Get the topic specific pagerank from a given start paper
            # paper_topic_pagerank = get_related_paper_topic_pagerank(graph_ref, graph_content, undirected=True)
            # paper_topic_pagerank_dois = paper_topic_pagerank['paper_doi']
            # paper_topic_pagerank_pageranks = paper_topic_pagerank['pagerank']
            # paper_topic_pagerank = dict(zip(paper_topic_pagerank_dois, paper_topic_pagerank_pageranks))
            #
            # # Get the related papers using LPA method
            # lpa = get_related_paper_lpa(graph_ref, graph_content, maxIter=15)
            # return render(request, 'graph.html',
            #               {'paper_topic_pagerank': paper_topic_pagerank,
            #                'paper_lpa': lpa}
            #               )

        elif graph_type == "author":
            # for presentation
            if graph_content == "88855" or "Qiao Yan":
                author_simrank = pd.read_csv("graph/author_simrank.csv")
                author_simrank_authors = author_simrank['related_author'].tolist()
                author_names = list()
                for id in author_simrank_authors:
                    author_names.append(models.Authors.objects.get(author_id=id))
                author_simrank_simranks = author_simrank['simrank'].tolist()
                author_simrank = dict(zip(author_names, author_simrank_simranks))
                return render(request, 'graph.html', {'author_simrank': author_simrank})
            # # for real scene
            # author_id = graph_content
            # if not graph_content.isdigit():
            #     author_id = models.Authors.objects.get(author_name=graph_content)
            # else:
            #     author_id = graph_content
            #
            #     # for real scene
            #     query_papers = '''
            #                 select paper_doi, author_id, refs_doi
            #                 from public."PaperInfo_paper"
            #             '''
            #
            #     # Read the data from PostgreSQL
            #     conn = psycopg2.connect(dbname=PG_SQL_LOCAL['database'], user=PG_SQL_LOCAL['user'], password=PG_SQL_LOCAL['password'])
            #     # df_authors = pd.read_sql_query(query_authors, conn)
            #     df_papers = pd.read_sql_query(query_papers, conn)
            #
            #     spark = SparkSession \
            #         .builder \
            #         .appName('Paper-graph') \
            #         .getOrCreate()
            #
            #     # Convert the pandas dataframes to spark dataframe
            #     # df_authors = spark.createDataFrame(df_authors).repartition(10)
            #     df_papers = spark.createDataFrame(df_papers).repartition(10)
            #
            #     df_papers.cache()
            #
            #     # Create the graph based on reference and authorship
            #     graph_ref = create_graph_ref(df_papers)
            #     graph_author = create_graph_author(df_papers)
            #
            #     graph_ref.cache()
            #     graph_author.cache()
            #
            #     # Create the graph combining both reference and authorship relations
            #     graph_ref_author = create_graph_ref_author(graph_ref, graph_author, ref_undirected=True)
            #     graph_ref_author.cache()
            #
            #     # Remove the input dataframes from cache to free memory space
            #     df_papers.unpersist()
            #
            #     # Get the related authors from a given author using simrank
            #     author_simrank = get_related_author_simrank(graph_ref_author, author_id)
            #     author_simrank_authors = author_simrank['related_author']
            #     author_simrank_simranks = author_simrank['simrank']
            #     author_simrank = dict(zip(author_simrank_authors, author_simrank_simranks))
            #
            #     return render(request, 'graph.html', {'author_simrank': author_simrank})
        else:
            pass

    return render(request, 'graph.html')

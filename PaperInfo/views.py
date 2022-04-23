from django.forms import model_to_dict
from django.shortcuts import render
from pyecharts import options as opts
from pyecharts.charts import WordCloud
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
        elif search_type == "title":
            papers = models.Paper.objects.filter(title=search_content)
        else:
            papers = models.Paper.objects.filter(ieee_keywords__contains=[search_content])

        if papers:
            paper = papers[0]

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
                    papers = models.Paper.objects.get(author_id__contains=[visual_content])

                # author name
                else:
                    author_id = models.Authors.objects.get(author_name=visual_content)
                    papers = models.Paper.objects.get(author_id__contains=[author_id])

                papers = model_to_dict(papers)

                word_list.extend([elem for elem in papers['title'].lower().split(' ')])
                if papers['ieee_keywords'] is not None:
                    word_list.extend([elem.lower() for elem in papers['ieee_keywords']])
                if papers['authors_keywords'] is not None:
                    word_list.extend([elem.lower() for elem in papers['authors_keywords']])

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
                .set_global_opts(title_opts=opts.TitleOpts(title="hotspots"))
                .render("templates/hotspot.html")
            )

            return render(request, 'hotspot.html')
        elif visual_type == "plots":
            if visual_criteria == "year":
                # TODO: plot data according to year (groupby) (year v.s. number of paper)
                pass

            elif visual_criteria == "author":
                # TODO: plot data according to author id (distribution in year)
                pass

            else:
                pass
            return render(request, 'visual.html')
        else:
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
        pass
    # TODO: save the results and hardcode here
    return render(request, 'graph.html')


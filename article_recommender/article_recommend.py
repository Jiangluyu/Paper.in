from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, ArrayType
from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.feature import Word2Vec
from pyspark.ml.feature import Word2VecModel
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.feature import CountVectorizerModel
from pyspark.ml.feature import IDF
from pyspark.ml.feature import IDFModel
from pyspark.ml.feature import BucketedRandomProjectionLSH, MinHashLSH
import jieba.posseg as pseg


def spark_session():
    spark = SparkSession \
        .builder \
        .appName("test1") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    return spark


def read_data(spark, filename):
    df = spark.read.load(filename, format='json')
    df_data = df.select("doi", "title", "abstract")
    # print(df.count())
    df_data = df_data.dropna()
    df_data = df_data.dropDuplicates()
    return df_data


def read_stopwords(spark):
    spark.sparkContext.addFile('stopwords.txt')
    stop_words = open('./stopwords.txt', 'r', encoding='utf_8').readlines()
    stop_words = [line.strip() for line in stop_words]
    # print(stop_words)
    return stop_words


# 1. segmentation
def seg(x):
    seg_list = pseg.lcut(x)
    filtered_words_list = []
    for seg in seg_list:
        if len(seg.word) <= 1:
            continue
        elif seg.flag == "eng":
            if len(seg.word) <= 2:
                continue
            else:
                filtered_words_list.append(seg.word)
        elif seg.flag.startswith("n"):
            filtered_words_list.append(seg.word)
        elif seg.flag in ["x", "eng"]:  # 是自定一个词语或者是英文单词
            filtered_words_list.append(seg.word)
    return filtered_words_list


def do_segmentation_remove_stopwords(df_data, stop_words):
    # word segmentation for abstracts
    seg_udf = udf(seg, ArrayType(StringType()))
    df_data = df_data.withColumn('seg', seg_udf(df_data['abstract']))

    # remove stopwords
    remover = StopWordsRemover(inputCol="seg", outputCol='words', stopWords=stop_words)
    df_data = remover.transform(df_data)
    df_words = df_data.select('doi', 'title', 'words')
    df_relate = df_data.select('doi', 'title')
    # words_df.show()
    return df_data, df_words, df_relate


# 2. word2vec
def word2vec(df_words):
    w2v_model = Word2Vec(vectorSize=100, inputCol='words', outputCol='vector', minCount=3)
    model = w2v_model.fit(df_words)
    model.write().overwrite().save("models/word2vec_model/python.word2vec")
    w2v_model = Word2VecModel.load("models/word2vec_model/python.word2vec")
    vectors = w2v_model.getVectors()
    # vectors.show()
    return vectors


# 3. tfidf

# Select the top 20 as keywords. This is only a word index
def sort_by_tfidf(partition):
    TOPK = 20
    for row in partition:
        # Find index and IDF values and sort them
        _dict = list(zip(row.idfFeatures.indices, row.idfFeatures.values))
        _dict = sorted(_dict, key=lambda x: x[1], reverse=True)
        result = _dict[:TOPK]
        for word_index, tfidf in result:
            yield row.doi, row.title, int(word_index), round(float(tfidf), 4)


# append_index
def append_index(data):
    for index in range(len(data)):
        data[index] = list(data[index])
        data[index].append(index)
        data[index][1] = float(data[index][1])


# keyword weight * word vector
def compute_vector(row):
    return row.doi, row.title, row.keywords, row.weights * row.vector


# Calculate the average weight vector
def compute_avg_vectors(row):
    x = 0
    for i in row.vectors:
        x += i
    return row.doi, row.title, x / len(row.vectors)


# do tfidf
def tfidf(spark, vectors, df_words):
    # tf
    cv = CountVectorizer(inputCol="words", outputCol="countFeatures", vocabSize=200 * 10000, minDF=1.0)
    cv_model = cv.fit(df_words)
    cv_model.write().overwrite().save("models/CV.model")
    cv_model = CountVectorizerModel.load("models/CV.model")
    cv_result = cv_model.transform(df_words)

    # idf
    idf = IDF(inputCol="countFeatures", outputCol="idfFeatures")
    idf_model = idf.fit(cv_result)
    idf_model.write().overwrite().save("models/IDF.model")
    idf_model = IDFModel.load("models/IDF.model")
    tfidf_result = idf_model.transform(cv_result)

    # Select the top 20 as keywords. This is only a word index
    keywords_by_tfidf = tfidf_result.rdd.mapPartitions(sort_by_tfidf).toDF(["doi", "title", "index", "weights"])

    # Build keywords and indexes
    keywords_list_with_idf = list(zip(cv_model.vocabulary, idf_model.idf.toArray()))

    # append_index
    append_index(keywords_list_with_idf)
    sc = spark.sparkContext
    rdd = sc.parallelize(keywords_list_with_idf)
    idf_keywords = rdd.toDF(["keywords", "idf", "index"])

    # Find the key words and weight tfidf
    keywords_result = keywords_by_tfidf.join(idf_keywords, idf_keywords.index == keywords_by_tfidf.index).select(
        ["doi", "title", "keywords", "weights"])

    # Join keywords and word vector
    keywords_vector = keywords_result.join(vectors, vectors.word == keywords_result.keywords, 'inner')

    # keyword weight * word vector
    article_keyword_vectors = keywords_vector.rdd.map(compute_vector).toDF(
        ["doi", "title", "keywords", "weightingVector"])

    # Collect set() method is used to combine the word vectors of all keywords in an article into a list
    article_keyword_vectors.registerTempTable('temptable')
    article_keyword_vectors = spark.sql(
        "select doi, min(title) title, collect_set(weightingVector) vectors from temptable group by doi")

    # Calculate the average weight vector
    article_vector = article_keyword_vectors.rdd.map(compute_avg_vectors).toDF(['doi', 'title', 'articlevector'])

    return article_vector


class article_recommend:

    def __init__(self, doi):
        self.filename = 'C:/Users/lenovo/2020-22_2.json'
        self.doi = doi

    def BucketedRandomProjectionLSH(self, spark, article_vector):
        test = spark.sql("select doi, title as search, articlevector from vector_table where doi = '%s'" % self.doi)
        print("----- input ------")
        test.show()
        train = article_vector.select(['doi', 'title', 'articlevector'])

        brp = BucketedRandomProjectionLSH(inputCol='articlevector', outputCol='hashes', numHashTables=4.0,
                                          bucketLength=10.0)
        model = brp.fit(train)
        similar = model.approxSimilarityJoin(test, train, 2.0, distCol='EuclideanDistance')
        return similar

    def recommend(self):
        spark = spark_session()
        df_data = read_data(spark, self.filename)
        stop_words = read_stopwords(spark)
        df_data, df_words, df_relate = do_segmentation_remove_stopwords(df_data, stop_words)
        vectors = word2vec(df_words)
        article_vector = tfidf(spark, vectors, df_words)
        article_vector.registerTempTable('vector_table')
        similar = self.BucketedRandomProjectionLSH(spark, article_vector)

        # whole table
        print("----- whole table ------")
        similar = similar.orderBy('EuclideanDistance', ascending=True).select('datasetA.search', 'datasetB.title',
                                                                              'EuclideanDistance')
        similar.show(11)

        # Nearest Neighbors
        Nearest_Neighbors = similar.select('title').rdd.flatMap(lambda x: x).collect()[1:11]
        print("----- Nearest Neighbors list -----")
        print(Nearest_Neighbors)
        return Nearest_Neighbors


# if __name__ == '__main__':
#     article = article_recommend("10.1109/TII.2019.2930471")
#     article.recommend()

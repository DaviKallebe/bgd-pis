from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rank, avg, lit, count, max
from pyspark.sql.window import Window
import sys

def question_a(dfReviews, Id = '6'):
    print("\n(a) Dado produto, listar os 5 comentario mais uteis e com maior avaliacao.\n")
    dfReviews.filter(dfReviews['Id'] == Id) \
        .orderBy(['helpful', 'rating'], ascending=[False, False]) \
        .limit(5) \
        .show()

    print("\n(a) e os 5 comentarios mais uteis e com menor avaliacao.\n")
    dfReviews.filter(dfReviews['Id'] == Id) \
        .orderBy([dfReviews['helpful'], dfReviews['rating']], ascending=[False, True]) \
        .limit(5) \
        .show()

def question_b(dfSells, dfSimilars, Id = '6'):
    dfSells.filter(dfSells['Id'] == Id) \
        .join(dfSimilars, 'Id') \
        .select(dfSells['Id'], dfSells['salesrank'].alias('salesrank_original'), dfSimilars['similar']) \
        .alias('temp1') \
        .join(dfSells.alias('sellstemp'), col('temp1.similar') == col('sellstemp.ASIN')) \
        .select(['temp1.Id', 'temp1.similar', 'salesrank_original', 'salesrank']) \
        .filter((col('salesrank') > 0) & (col('salesrank') < col('salesrank_original'))) \
        .show()

def question_c(dfReviews, Id = '6'):
    dfReviews.select([col('reviews.date'), col('reviews.rating')]) \
        .groupBy(col('reviews.date')) \
        .avg() \
        .orderBy(col('reviews.date'), ascending=True) \
        .show()

def question_d(dfSells):
    window = Window.partitionBy(col('sells.group')).orderBy(col('sells.salesrank'))

    dfSells.filter(col('sells.salesrank') > 0) \
        .select(col('sells.group'), col('sells.Id'), col('sells.title'), col('sells.salesrank'), rank().over(window).alias('rank')) \
        .orderBy([col('sells.group'), col('sells.salesrank')], ascending=[True, True]) \
        .filter(col('rank') <= 10) \
        .drop(col('rank')) \
        .show()

def question_e(dfReviews, dfSells):
    dfReviews.select(col('reviews.Id'), col('reviews.helpful')) \
        .groupBy(col('reviews.Id')) \
        .agg(avg(col('reviews.helpful')).alias('avghelpful')) \
        .orderBy(col('avghelpful'), ascending = False) \
        .join(dfSells, col('reviews.Id') == col('sells.Id')) \
        .select(col('reviews.Id'), col('sells.ASIN'), col('sells.title'), col('avghelpful')) \
        .orderBy(col('avghelpful'), ascending=False) \
        .limit(10) \
        .show()

def question_g(dfSells):
    pass

def question_f(dfReviews, dfSells):
    window = Window.partitionBy(col('temp1.group'))

    dfReviews.select(col('reviews.Id'), col('reviews.customer')) \
        .join(dfSells, col('reviews.Id') == col('sells.Id')) \
        .select(col('sells.group'), col('reviews.customer')) \
        .groupBy(col('sells.group'), col('reviews.customer')) \
        .agg(count(col('reviews.customer')).alias('comments')) \
        .alias('temp1') \
        .withColumn('maxGroup', max(col('temp1.comments')).over(window)) \
        .filter(col('temp1.comments') == col('maxGroup')) \
        .drop(col('maxGroup')) \
        .show()     

def main(file_name, Id = '6'):
    conf = (SparkConf()
         .setMaster("local")
         .setAppName("Hello"))

    sc = SparkContext(conf = conf)
    sc._jsc.hadoopConfiguration().set("textinputformat.record.delimiter", "\n\r\n")

    spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

    reviews_header_words = ("reviews", "total", "downloaded", "avg")
    reviews_words = ("cutomer", "rating", "helpful")  

    text_file = sc.textFile(file_name)    
    registers = text_file.map(lambda text: [(line.strip().split(":", 1) if all(rhw in line for rhw in reviews_header_words) else
            ['review', line.strip()] + [int(i) if i.isdigit() else i for i in line.split()] if all(rw in line for rw in reviews_words) else
            [x.strip() for x in line.split(':', 1)]) for line in text.strip().split('\n')])\
        .filter(lambda reg: all(("Id" in reg[0], "discontinued product" not in reg[-1])))

    registers.cache()

    sells = registers.map(lambda reg: (reg[0][1], reg[1][1], reg[2][1], reg[3][1], int(reg[4][1]), reg[6][1]))
    similars = registers.flatMap(lambda reg: [(reg[0][1], reg[1][1], w) for w in reg[5][1].split()[1:]])
    reviews = registers.flatMap(lambda reg: [[reg[0][1], reg[1][1]] + [int(x) if x.isdigit() else x for x in w[1].split() if ":" not in x] for w in reg if "review" in w])
    dfSells = sells.toDF(["Id", "ASIN", "title", "group", "salesrank", "categories"]).alias('sells')
    dfSimilars = similars.toDF(["Id", "ASIN", "similar"]).alias('similars')
    dfReviews = reviews.toDF(["Id", "ASIN", "date", "customer", "rating", "votes", "helpful"]).alias('reviews')

    #question_a(dfReviews, Id)
    #question_b(dfSells, dfSimilars, Id)
    #question_c(dfReviews, Id)
    #question_d(dfSells)
    #question_e(dfReviews, dfSells)
    question_f(dfReviews, dfSells)

if __name__ == "__main__":    
    argv = sys.argv

    #sparksql.py caminho_para_o_arquivo_de_text_da_amazon.txt [id_do_produto]
    if len(argv) == 3:
        main(argv[1], argv[2])
    else:
        main(argv[1])
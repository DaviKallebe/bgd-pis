from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rank, avg, lit, count, max
from pyspark.sql.window import Window
import sys

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
    reviews = registers.flatMap(lambda reg: [[reg[0][1], reg[1][1], reg[3][1]] + [int(x) if x.isdigit() else x for x in w[1].split() if ":" not in x] for w in reg if "review" in w])
    dfSells = sells.toDF(["Id", "ASIN", "title", "group", "salesrank", "categories"]).alias('sells')
    dfSimilars = similars.toDF(["Id", "ASIN", "similar"]).alias('similars')
    dfReviews = reviews.toDF(["Id", "ASIN", "group", "date", "customer", "rating", "votes", "helpful"]).alias('reviews')

    dfSells.createOrReplaceTempView("sells")
    dfSimilars.createOrReplaceTempView("similars")
    dfReviews.createOrReplaceTempView("reviews")

    
    question_a1 = spark.sql("SELECT * FROM reviews WHERE Id = '{0}' ORDER BY helpful DESC, rating DESC LIMIT 5".format(Id))
    question_a1.show()
    question_a1.coalesce(1).write.format("com.databricks.spark.csv").save("question_a1.csv")

    question_a2 = spark.sql("SELECT * FROM reviews WHERE Id = '{0}' ORDER BY helpful DESC, rating LIMIT 5".format(Id))
    question_a2.show()
    question_a2.coalesce(1).write.format("com.databricks.spark.csv").save("question_a2.csv")

    question_b = spark.sql("""SELECT s.Id, s.ASIN, s.salesrank AS sells_sales, sim.similar_sales
                              FROM sells AS s, (SELECT sim1.Id, sim1.ASIN, sim1.similar, s1.salesrank AS similar_sales 
                                                FROM similars AS sim1, sells AS s1 
                                                WHERE sim1.Id == '{0}'
                                                AND sim1.similar == s1.ASIN) AS sim
                              WHERE s.Id == '{0}'
                              AND s.Id == sim.Id
                              AND sim.similar_sales < s.salesrank
                              AND sim.similar_sales > 0""".format(Id))
    question_b.show()
    question_b.coalesce(1).write.format("com.databricks.spark.csv").save("question_b.csv")

    question_c = spark.sql("""SELECT date, AVG(rating) AS avgrating 
                              FROM reviews WHERE Id = '{0}' 
                              GROUP BY date 
                              ORDER BY date""".format(Id))
    question_c.show()
    question_c.coalesce(1).write.format("com.databricks.spark.csv").save("question_c.csv")

    question_d = spark.sql("""SELECT s1.group, s1.Id, s1.title, s1.salesrank
                              FROM (SELECT s.group, s.Id, s.title, s.salesrank, 
                                        RANK() OVER (PARTITION BY group ORDER BY salesrank ASC) AS ranked
                                    FROM sells as s) AS s1
                              WHERE s1.ranked < 10""")
    question_d.show()
    question_d.coalesce(1).write.format("com.databricks.spark.csv").save("question_d.csv")

    question_e = spark.sql("""SELECT *
                              FROM (SELECT r.Id, r.ASIN, AVG(helpful) AS avghelpful
                                    FROM reviews AS r
                                    GROUP BY r.id, r.ASIN) AS r1
                              ORDER BY avghelpful DESC
                              LIMIT 10""")
    question_e.show()
    question_e.coalesce(1).write.format("com.databricks.spark.csv").save("question_e.csv")
    
    question_g = spark.sql("""SELECT r2.group, r2.customer, r2.comments
                              FROM (SELECT r1.group, r1.customer, r1.comments, MAX(r1.comments) OVER (PARTITION BY r1.group) AS max_comments
                                    FROM (SELECT r.group, r.customer, COUNT(customer) AS comments
                                          FROM reviews as r
                                          GROUP BY r.group, r.customer) AS r1) AS r2
                              WHERE r2.comments = r2.max_comments""")
    question_g.show()  
    question_g.coalesce(1).write.format("com.databricks.spark.csv").save("question_g.csv")                            

if __name__ == "__main__":    
    argv = sys.argv

    #sparksql.py caminho_para_o_arquivo_de_text_da_amazon.txt [id_do_produto]
    if len(argv) == 3:
        main(argv[1], argv[2])
    else:
        main(argv[1])
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rank, avg, lit, count, max
from pyspark.sql.window import Window
import sys

#o script aceita o caminho_do_arquivo_de_text.txt e Id se o Id não for passado
#só alterar o Id na função main que está abaixo da função question_g
#eu esqueci de comentar a parte A do trabalho, não dessa vez
#rdd foi mais dificil, mas foi mais divertido
#o output vai se guardado em disco e organizado em pastas
#o nome das pastas são df_question_{a,b,c} e question_{a,b,c}.csv
#o output também será mostrado na tela (boa sorte tentando vizualiza-lo)

def question_a(dfReviews, Id = '6'):
    #seleciona o produto pelo Id e ordena por helpful (decrescente) e rating (decrescente)
    df1 = dfReviews.filter(dfReviews['Id'] == Id) \
        .orderBy(['helpful', 'rating'], ascending=[False, False]) \
        .limit(5)

    #seleciona o produto pelo Id e ordena por helpful (decrescente) e rating (crescente)
    df2 = dfReviews.filter(dfReviews['Id'] == Id) \
        .orderBy([dfReviews['helpful'], dfReviews['rating']], ascending=[False, True]) \
        .limit(5)

    #mostra o resultado na tela e salva em arquivo
    df1.show()
    df1.write.format("com.databricks.spark.csv").save("df_question_a1")
    df2.show()
    df2.write.format("com.databricks.spark.csv").save("df_question_a2")

def question_b(dfSells, dfSimilars, Id = '6'):
    #seleciona o produto pelo Id e realiza um join com similares
    #o alias é necessario pra evitar ambiguidades e facilitar o segundo join
    #porque é preciso das vendas dos similares pra fazer a comparação
    df = dfSells.filter(dfSells['Id'] == Id) \
        .join(dfSimilars, 'Id') \
        .select(dfSells['Id'], dfSells['salesrank'].alias('salesrank_original'), dfSimilars['similar']) \
        .alias('temp1') \
        .join(dfSells.alias('sellstemp'), col('temp1.similar') == col('sellstemp.ASIN')) \
        .select(['temp1.Id', 'temp1.similar', 'salesrank_original', 'salesrank']) \
        .filter((col('salesrank') > 0) & (col('salesrank') < col('salesrank_original')))

    df.show()
    df.write.format("com.databricks.spark.csv").save("df_question_b")

def question_c(dfReviews, Id = '6'):
    #simples seleciona os campos e agrupo pela data e chama a função avg
    #para a media das avaliações diarias
    df = dfReviews.select([col('reviews.date'), col('reviews.rating')]) \
        .groupBy(col('reviews.date')) \
        .avg() \
        .orderBy(col('reviews.date'), ascending=True)

    df.show()
    df.write.format("com.databricks.spark.csv").save("df_question_c")

def question_d(dfSells):
    #utiliza a função window para gerar uma partição sobre grupo em vendas
    #necessário para ranquear
    window = Window.partitionBy(col('sells.group')).orderBy(col('sells.salesrank'))

    #existem salesranks com 0 e -1 então é necessário filtra-los
    #seleciona os campos e realiza o ranqueamento com a função window
    #o rank é uma gambiarra que ajuda a filtrar os 10 primeiro ou os 10 maiores
    df = dfSells.filter(col('sells.salesrank') > 0) \
        .select(col('sells.group'), col('sells.Id'), col('sells.title'), col('sells.salesrank'), rank().over(window).alias('rank')) \
        .orderBy([col('sells.group'), col('sells.salesrank')], ascending=[True, True]) \
        .filter(col('rank') <= 10) \
        .drop(col('rank'))

    df.show()
    df.write.format("com.databricks.spark.csv").save("df_question_d")

def question_e(dfReviews, dfSells):
    #seleciona os campos e primeiro realiza um avg sobre helpful
    #para fazer um join com vendas para imprimir o ANSI e o título
    df = dfReviews.select(col('reviews.Id'), col('reviews.helpful')) \
        .groupBy(col('reviews.Id')) \
        .agg(avg(col('reviews.helpful')).alias('avghelpful')) \
        .orderBy(col('avghelpful'), ascending = False) \
        .join(dfSells, col('reviews.Id') == col('sells.Id')) \
        .select(col('reviews.Id'), col('sells.ASIN'), col('sells.title'), col('avghelpful')) \
        .orderBy(col('avghelpful'), ascending=False) \
        .limit(10)

    df.show()
    df.write.format("com.databricks.spark.csv").save("df_question_e")

def question_f(dfSells):
    #rip muito trabalho. Digo, tem que tratar a entrada e parece que nunca sobra tempo
    #eu devia parar de enrolar
    pass

def question_g(dfReviews, dfSells):  
    #particiona somente sobre o groupo enquanto na consulta
    #o agrupamento é por groupo e customer, com isso a função
    #window vai ajudar a achar o max somente em group
    window = Window.partitionBy(col('temp1.group'))

    #primeiro seleciona os campos de reviews desejados
    #e realiza um join com vendas para pegar o campo group
    #agrupa por grupo e customer para realizar a conta
    #aí vem a gambiarra, uso a função window que ta particionando
    #somente em group pra achar o maior valor por group e filtra os
    #registro que não tem os valores iguais assim a consulta mostra todos
    #os cliente que tem o maior número de comentários se tiver empate
    df = dfReviews.select(col('reviews.Id'), col('reviews.customer')) \
        .join(dfSells, col('reviews.Id') == col('sells.Id')) \
        .select(col('sells.group'), col('reviews.customer')) \
        .groupBy(col('sells.group'), col('reviews.customer')) \
        .agg(count(col('reviews.customer')).alias('comments')) \
        .alias('temp1') \
        .withColumn('maxGroup', max(col('temp1.comments')).over(window)) \
        .filter(col('temp1.comments') == col('maxGroup')) \
        .drop(col('maxGroup'))

    df.show()
    df.write.format("com.databricks.spark.csv").save("df_question_g")    

#altere o Id aqui para outros produtos
def main(file_name, Id = '6'):
    conf = (SparkConf()
         .setMaster("local")
         .setAppName("Pi2C"))

    sc = SparkContext(conf = conf)
    #separa o arquivo por registro e não por linhas
    sc._jsc.hadoopConfiguration().set("textinputformat.record.delimiter", "\n\r\n")

    spark = SparkSession \
    .builder \
    .appName("Pi2 Parte C") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

    reviews_header_words = ("reviews", "total", "downloaded", "avg")
    reviews_words = ("cutomer", "rating", "helpful")  

    text_file = sc.textFile(file_name)
    #mapea cada registro em tuplas ("chave", "valor") ou ('review', valor) é uma bagunça que nem eu entendo
    #eu sei que eu pego os valores que estão no review e transformo em número se for digito
    registers = text_file.map(lambda text: [(line.strip().split(":", 1) if all(rhw in line for rhw in reviews_header_words) else
            ['review', line.strip()] + [int(i) if i.isdigit() else i for i in line.split()] if all(rw in line for rw in reviews_words) else
            [x.strip() for x in line.split(':', 1)]) for line in text.strip().split('\n')])\
        .filter(lambda reg: all(("Id" in reg[0], "discontinued product" not in reg[-1])))

    registers.cache()

    #cria os rdds que vão servir como fonte de dados para os dataframes
    sells = registers.map(lambda reg: (reg[0][1], reg[1][1], reg[2][1], reg[3][1], int(reg[4][1]), reg[6][1]))
    similars = registers.flatMap(lambda reg: [(reg[0][1], reg[1][1], w) for w in reg[5][1].split()[1:]])
    reviews = registers.flatMap(lambda reg: [[reg[0][1], reg[1][1], reg[3][1]] + [int(x) if x.isdigit() else x for x in w[1].split() if ":" not in x] for w in reg if "review" in w])

    #cria os dataframes a partir dos rdds, são 3 dataframes de vendas (sells), similaredades (similars) e avaliações (reviews)
    dfSells = sells.toDF(["Id", "ASIN", "title", "group", "salesrank", "categories"]).alias('sells')
    dfSimilars = similars.toDF(["Id", "ASIN", "similar"]).alias('similars')
    dfReviews = reviews.toDF(["Id", "ASIN", "group", "date", "customer", "rating", "votes", "helpful"]).alias('reviews')
    
    #auto explicativo
    question_a(dfReviews, Id)
    question_b(dfSells, dfSimilars, Id)
    question_c(dfReviews, Id)
    question_d(dfSells)
    question_e(dfReviews, dfSells)
    question_g(dfReviews, dfSells)

    #cria as views a partir dos dataframes
    dfSells.createOrReplaceTempView("sells")
    dfSimilars.createOrReplaceTempView("similars")
    dfReviews.createOrReplaceTempView("reviews")

    #eu preciso explicar as querys pro professor de BD? :v
    #mas o que eu posso dizer é que as subquerys estão todas na cláusula FROM
    #porque se eu colocasse uma subquery no select o spark não adicionava a campo
    #na lista de campos válidos mesmo se eu colocasse um alias então se eu colocar
    #a subquery na clausula from as subquery ou os campos agregados podem ser acessados    
    question_a1_sql = spark.sql("SELECT * FROM reviews WHERE Id = '{0}' ORDER BY helpful DESC, rating DESC LIMIT 5".format(Id))
    question_a1_sql.show()
    question_a1_sql.coalesce(1).write.format("com.databricks.spark.csv").save("question_a1.csv")

    question_a2_sql = spark.sql("SELECT * FROM reviews WHERE Id = '{0}' ORDER BY helpful DESC, rating LIMIT 5".format(Id))
    question_a2_sql.show()
    question_a2_sql.coalesce(1).write.format("com.databricks.spark.csv").save("question_a2.csv")

    question_b_sql = spark.sql("""SELECT s.Id, s.ASIN, s.salesrank AS sells_sales, sim.similar_sales
                                    FROM sells AS s, (SELECT sim1.Id, sim1.ASIN, sim1.similar, s1.salesrank AS similar_sales 
                                                        FROM similars AS sim1, sells AS s1 
                                                        WHERE sim1.Id == '{0}'
                                                        AND sim1.similar == s1.ASIN) AS sim
                                    WHERE s.Id == '{0}'
                                    AND s.Id == sim.Id
                                    AND sim.similar_sales < s.salesrank
                                    AND sim.similar_sales > 0""".format(Id))
    question_b_sql.show()
    question_b_sql.coalesce(1).write.format("com.databricks.spark.csv").save("question_b.csv")

    question_c_sql = spark.sql("""SELECT date, AVG(rating) AS avgrating 
                                    FROM reviews WHERE Id = '{0}' 
                                    GROUP BY date 
                                    ORDER BY date""".format(Id))
    question_c_sql.show()
    question_c_sql.coalesce(1).write.format("com.databricks.spark.csv").save("question_c.csv")

    question_d_sql = spark.sql("""SELECT s1.group, s1.Id, s1.title, s1.salesrank
                                    FROM (SELECT s.group, s.Id, s.title, s.salesrank, 
                                                RANK() OVER (PARTITION BY group ORDER BY salesrank ASC) AS ranked
                                            FROM sells as s) AS s1
                                    WHERE s1.ranked < 10""")
    question_d_sql.show()
    question_d_sql.coalesce(1).write.format("com.databricks.spark.csv").save("question_d.csv")

    question_e_sql = spark.sql("""SELECT *
                                    FROM (SELECT r.Id, r.ASIN, AVG(helpful) AS avghelpful
                                            FROM reviews AS r
                                            GROUP BY r.id, r.ASIN) AS r1
                                    ORDER BY avghelpful DESC
                                    LIMIT 10""")
    question_e_sql.show()
    question_e_sql.coalesce(1).write.format("com.databricks.spark.csv").save("question_e.csv")
    
    question_g_sql = spark.sql("""SELECT r2.group, r2.customer, r2.comments
                                    FROM (SELECT r1.group, r1.customer, r1.comments, MAX(r1.comments) OVER (PARTITION BY r1.group) AS max_comments
                                            FROM (SELECT r.group, r.customer, COUNT(customer) AS comments
                                                FROM reviews as r
                                                GROUP BY r.group, r.customer) AS r1) AS r2
                                    WHERE r2.comments = r2.max_comments""")
    question_g_sql.show()  
    question_g_sql.coalesce(1).write.format("com.databricks.spark.csv").save("question_g.csv") 

if __name__ == "__main__":    
    argv = sys.argv

    #sparksql.py caminho_para_o_arquivo_de_text_da_amazon.txt [id_do_produto]
    if len(argv) == 3:
        main(argv[1], argv[2])
    else:
        main(argv[1])
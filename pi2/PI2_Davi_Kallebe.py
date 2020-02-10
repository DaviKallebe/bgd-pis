from pyspark import SparkContext, SparkConf
import sys

def question_a(registers, Id = '6'):
    register_reviews = registers.filter(lambda reg: Id == reg[0][1])\
        .flatMap(lambda line: line)\
        .filter(lambda value: "review" in value)  

    register_reviews_sorted_a1 = register_reviews.sortBy(lambda reg: (reg[10], reg[6]), False)
    register_reviews_sorted_a2 = register_reviews.sortBy(lambda reg: (reg[10], -reg[6]), False)
    sorted_reviews_a1 = register_reviews_sorted_a1.collect()
    sorted_reviews_a2 = register_reviews_sorted_a2.collect()
    
    print("\n(a) Dado produto, listar os 5 comentario mais uteis e com maior avaliacao.\n")
    for review in sorted_reviews_a1[0:5]:
        print("Review:", review[1])    

    print("\n(a) e os 5 comentarios mais uteis e com menor avaliacao.\n")
    for review in sorted_reviews_a2[0:5]:
        print("Review:", review[1])
    

def question_b(registers, Id = '6'):
    similar_asins = registers.filter(lambda reg: Id == reg[0][1])\
        .flatMap(lambda line: line)\
        .filter(lambda value: any(w in value for w in ("similar","salesrank")))\
        .map(lambda value: value[1].split()[1:] if "similar" == value[0] else [int(value[1])])
    
    asin_list = similar_asins.collect()
    similars = registers.filter(lambda reg: any(w == reg[1][1] for w in asin_list[1]) and asin_list[0][0] > int(reg[4][1]) and reg[4][1] != '0')
    similares = similars.collect()

    print("\n(b) Dado um produto, listar os produtos similares com maiores vendas do que ele.\n")
    print("Produto Id:", Id, " Produto salesrank:", asin_list[0][0], '\n')
    print("Similares com maiores vendas.\n")
    for similar in similares:
        print("Id:", similar[0][1], " ASIN:", similar[1][1], " salesrank:", similar[4][1], " Titulo:", similar[2][1])

def question_c(registers, Id = '6'):
    register_reviews = registers.filter(lambda reg: Id == reg[0][1])\
        .flatMap(lambda line: line)\
        .filter(lambda value: "review" in value)\
        .map(lambda value: (value[2], (value[6], 1)))\
        .reduceByKey(lambda va, vb: (va[0]+vb[0], va[1]+vb[1]))\
        .map(lambda value: (value[0], value[1][0] / value[1][1]))

    register_review_list = register_reviews.collect()

    print("\n(c) Dado um produto, mostrar a evolucao diaria das medias de avaliacao ao longo do intervalo de tempo coberto no arquivo de entrada.\n")
    for review in register_review_list:
        print(review)

def question_d(registers):
    groups_and_sales = registers.map(lambda reg: (reg[3][1], (reg[4][1], reg[0][1], reg[1][1], reg[2][1])))\
        .filter(lambda value: value[1][0] != '0')\
        .reduceByKey(lambda va, vb: va if int(va[0]) < int(vb[0]) else vb)

    groups_and_sales_list = groups_and_sales.collect()

    print("\n(d) Listar os 10 produtos lideres de venda em cada grupo de produtos.\n")
    for gns in groups_and_sales_list:
        print("Categoria:", gns[0], " Rank:", gns[1][0], " Id:", gns[1][1], " ASIN:", gns[1][2], " Titulo:", gns[1][3])

def question_e(registers):
    ratings = registers.flatMap(lambda reg: [(reg[0][1], (int(line[-1]), 1, reg[1][1], reg[2][1])) for line in reg if "review" in line])\
        .reduceByKey(lambda va, vb: (va[0] + vb[0], va[1] + vb[1], va[2], va[3]))\
        .map(lambda line: (line[1][0] / line[1][1], (line[0], line[1][2], line[1][3])))\
        .sortByKey(ascending=False)
        
    rating_list = ratings.take(10)

    print("\n(e) Listar os 10 produtos com a maior media de avaliacoes uteis positivas\n")
    for rating in rating_list:
        print("Media Avaliacao util: %.2f" % rating[0],  "Id:", rating[1][0], "ASIN:", rating[1][1], "Titulo:", rating[1][2])

def question_f(registers):
    pass

def question_g(registers):
    group_reviews = registers.map(lambda reg: [reg[3][1]] + [line[4] for line in reg if line[0] == "review"])\
        .flatMap(lambda line: [((line[0], value), 1) for value in line[1:]])\
        .reduceByKey(lambda va, vb: va+vb)\
        .map(lambda line: (line[0][0], (line[1], line[0][1])))\
        .groupByKey()\
        .map(lambda value: (value[0], sorted(list(value[1]), reverse=True)[0:10]))

    group_reviews_list = group_reviews.collect()

    print("\n(g) Listar os 10 clientes que mais fizeram comentarios por grupo de produto.\n")
    for gr in group_reviews_list:
        print("\nGrupo:", gr[0], '\n')
        for client in gr[1]:
            print("Client:", client[1], " Quantidade:", client[0])

def main(file_name, Id = '6'):
    conf = (SparkConf()
         .setMaster("local")
         .setAppName("Hello"))

    sc = SparkContext(conf = conf)
    sc._jsc.hadoopConfiguration().set("textinputformat.record.delimiter", "\n\r\n")

    reviews_header_words = ("reviews", "total", "downloaded", "avg")
    reviews_words = ("cutomer", "rating", "helpful")    

    text_file = sc.textFile(file_name)    
    registers = text_file.map(lambda text: [(line.strip().split(":", 1) if all(rhw in line for rhw in reviews_header_words) else
            ['review', line.strip()] + [int(i) if i.isdigit() else i for i in line.split()] if all(rw in line for rw in reviews_words) else
            [x.strip() for x in line.split(':', 1)]) for line in text.strip().split('\n')])\
        .filter(lambda reg: all(("Id" in reg[0], "discontinued product" not in reg[-1])))
    
    registers.cache()

    question_a(registers)
    question_b(registers)
    question_c(registers)
    question_d(registers)
    question_e(registers)
    question_g(registers)

if __name__ == "__main__":    
    argv = sys.argv

    #spark.py caminho_para_o_arquivo_de_text_da_amazon.txt [id_do_produto]
    if len(argv) == 3:
        main(argv[1], argv[2])
    else:
        main(argv[1])
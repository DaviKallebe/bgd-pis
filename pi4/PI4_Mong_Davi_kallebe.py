from pymongo import MongoClient, ASCENDING
import sys

def parse_file(file_name, database):
    register_list = []
    rank_group = {}
    product_collection = database['product']
    product_collection.create_index([('Id', ASCENDING)], unique=True)

    #iniciando o parse do arquivo
    with open(file_name, 'rb') as f:
        #separa por registros separados por \n\r\n
        for entry in f.read().split(b'\n\r\n'):
            #filtra produtos descontinuados ou inválidos
            if b"discontinued product" in entry or b"Id:" not in entry:
                continue

            #decodifica de bytes para str e separa as linhas do registro
            lines = entry.decode().split("\n")
            #extrai as informações do registros e para cada linha uma informação
            register = [lines[0].split(":")[1].strip(), #Id
                    lines[1].split(":")[1].strip(), #ASIN
                    lines[2].split(":")[1].strip(), #title
                    lines[3].split(":")[1].strip(), #group
                    int(lines[4].split(":")[1].strip())] #salesrank
            #extrai os similares, a lista é vazia se não houver similares
            similars = [w for w in lines[5].split(":")[1].split()[1:]]

            Id = register[0]
            ASIN = str(register[1])            
            #extrai o total de categorias
            categories = int(lines[6].split(":")[1].strip())
            #as reviews começam depois das categories logo
            reviews = 7 + categories
            #extrai as categorias, a lista é vazia se não houver categorias
            categories = [w.strip() for w in lines[7:7+categories] if categories > 0]
            #extrai os números da primeira linha de reviews
            review_stats = [w for w in lines[reviews].split() if w.replace('.', '').isdigit()]
            #extrai as reviews,a lista é vazia se não houver reviews
            reviews = [w.split() for w in lines[reviews+1:] if w.strip() != ""]

            #como o mongodb tem problema para ranks lá vem a gambiarra
            #criar uma lista com os 10 menores ranks positivos por grupo
            if register[3] in rank_group:
                if register[4] > 0:
                    rank_group[register[3]].append(register[4])

                    #a cada 200 registros por grupo ordena a lista e pega os 10 primeiros
                    #para gerar uma lista menor, apenas para otimização
                    if len(rank_group[register[3]]) % 200 == 0:
                        rank_group[register[3]] = sorted(rank_group[register[3]])[0:10]
            else:
                if register[4] > 0:
                    rank_group[register[3]] = [register[4]]
                else:
                    rank_group[register[3]] = []

            register_list.append({
                "Id": int(register[0]),
                "ASIN": register[1],
                "title": register[2],
                "group": register[3],
                "salesrank": register[4],
                "similars": similars,
                "reviews": [{
                        "date": review[0], 
                        "customer": review[2], 
                        "rating": int(review[4]),
                        "vote": int(review[6]),
                        "helpful": int(review[8])} for review in reviews]
            })  

            list_size = sys.getsizeof(register_list)

            if (list_size > (2 << 15)):
                product_collection.insert_many(register_list)

        if register_list:
            product_collection.insert_many(register_list)

        group_rank_collection = database['group_rank']

        for group, salesranks in rank_group.items():
            salesrank_list = sorted(salesranks)
            group_rank_collection.insert_one({"group": group, "salesrank": salesrank_list[9] if len(salesrank_list) > 9 else salesrank_list[-1]})


def question_a(database, Id = 6):
    product_collection = database.product
    product = product_collection.find_one({"Id": Id})
    reviews = sorted(product["reviews"], key=lambda x: (-x["helpful"], -x["rating"]))

    print("(a) Dado produto, listar os 5 comentários mais úteis e com maior avaliação e os 5 comentários mais úteis e com menor avaliação.\n")

    for review in reviews[0:5]:
        print("date:", review["date"], "customer:", review["customer"], "helpful:", review["helpful"], "rating:", review['rating'])

    reviews = sorted(product["reviews"], key=lambda x: (-x["helpful"], x["rating"]))
    print("\n")
    for review in reviews[0:5]:
        print("date:", review["date"], "customer:", review["customer"], "helpful:", review["helpful"], "rating:", review['rating'])

    print("\n\n")

def question_b(database, Id = 6):
    product_collection = database.product
    product = product_collection.find_one({"Id": Id})
    similars = product_collection.find({"ASIN": {"$in": product["similars"]}})

    print("(b) Dado um produto, listar os produtos similares com maiores vendas do que ele.")
    print("Produto Id:", product["Id"], "ASIN:", product["ASIN"], "salesrank:", product["salesrank"], "\n")

    for similar in similars:
        #if similar["'salesrank"] < product["salesrank"] and similar["'salesrank"] > 0:
        print("ASIN", similar['ASIN'], "salesrank", similar["salesrank"])

    print("\n\n")

def question_c(database, Id = 6):
    product_collection = database.product
    results = product_collection.aggregate([#pipeline de comandos
        {"$match": {"Id": Id}}, #match para encontrar o documento
        {"$unwind": "$reviews"}, #ignora os valores nulos
        {"$group": { #agrupa
                "_id": "$reviews.date", #agrupo por data
                "avg_rating": {"$avg": "$reviews.rating"} #média das avaliações
                }
        },
        {"$sort": {"_id": 1}}
    ])

    print("(c) Dado um produto, mostrar a evolução diária das médias de avaliação ao longo do intervalo de tempo coberto no arquivo de entrada.\n")

    for row in list(results):
        print(row["_id"] + ",", "%.2f" % row["avg_rating"])

    print("\n\n")

def question_d(database):
    group_rank_collection = database.group_rank #grupos de produto com o salesrank do 10 elemento
    product_collection = database.product
    group_rank_list = list(group_rank_collection.find())

    print("(d) Listar os 10 produtos lideres de venda em cada grupo de produtos.\n")

    for group_rank in group_rank_list:
        results = product_collection.find({"$and": [{"group": group_rank["group"]}, {"salesrank": {"$lte": group_rank["salesrank"]}}]}).sort([("salesrank", 1)])

        for result in list(results):
            print("group:", result["group"], "salesrank:", result["salesrank"], "Id:", result["Id"], "ASIN:", result["ASIN"])

        print("")

def question_e(database):
    product_collection = database.product
    results = product_collection.aggregate([
        {"$unwind": "$reviews"},
        {"$group": {
            "_id": {"Id": "$Id"},
            "avg_helpful": {"$avg": "$reviews.helpful"}
        }},
        {"$sort": {"avg_helpful": -1}},
        {"$limit": 10}
    ])
    print("(e) Listar os 10 produtos com a maior média de avaliações úteis positivas.\n")

    for result in list(results):
        print("Id:", result["_id"]["Id"], "avg_helpful: %.2f" % result["avg_helpful"])

    print("\n\n")

def question_f(database):
    print("(f) Listar a 5 categorias de produto com a maior média de avaliações úteis positivas.\n")

def question_g(database):
    product_collection = database.product
    group_looked = {}

    results = product_collection.aggregate([
        {"$unwind": "$reviews"},
        {"$group": {
            "_id": {"group": "$group", "customer": "$reviews.customer"},
            "count": {"$sum": 1}
        }},
        {"$sort": {"_id.group": 1, "count": -1}}
    ])

    print("(g) Listar os 10 clientes que mais fizeram comentários por grupo de produto.\n")

    for result in list(results):
        group = result["_id"]["group"]
        customer = result["_id"]["customer"]
        count = result["count"]

        if (group in group_looked):
            group_looked[group] = group_looked[group] + 1
        else:
            group_looked[group] = 1

        if  group_looked[group] <= 10:
            print("Group:", group, "customer:", customer, "count:", count)

def main(file_name, Id = 6):
    client = MongoClient()
    client.drop_database('mongodb_dk')
    
    database = client['mongodb_dk']
    parse_file(file_name, database)

    question_a(database, Id)
    question_b(database, Id)
    question_c(database, Id)
    question_d(database)
    question_e(database)
    question_g(database)

if __name__ == "__main__":
    main(sys.argv[1])
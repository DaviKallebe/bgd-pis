import sys
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

KEYSPACE = "cassandra_dk"
products = 'products'
similars = 'similars'
reviews_v1 = 'reviews_v1'
reviews_v2 = 'reviews_v2'
reviews_v3 = 'reviews_v3'
groups = 'groups'
customer_reviews = 'customer_reviews'

def create_base(session):
    session.execute("DROP KEYSPACE IF EXISTS %s" % KEYSPACE)

    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS %s
        WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '2' }
        """ % KEYSPACE)

    session.execute("USE %s" % KEYSPACE)    

    session.execute("""
                CREATE TABLE IF NOT EXISTS %s (
                    Id int,
                    ASIN text,
                    title text,
                    group text,
                    salesrank int,
                    PRIMARY KEY (Id, salesrank)
                ) WITH CLUSTERING ORDER BY (salesrank ASC)
                """ % products)

    session.execute("""
                CREATE TABLE IF NOT EXISTS %s (
                    Id int,
                    ASIN text,
                    similar text,
                    salesrank int,                    
                    similarsales int,
                    PRIMARY KEY (Id, salesrank, similarsales)
                ) WITH CLUSTERING ORDER BY (salesrank ASC, similarsales ASC)
                """ % similars)

    session.execute("""
                CREATE TABLE IF NOT EXISTS %s (
                    Id int,
                    ASIN text,
                    date text,
                    customer text,
                    rating int,
                    vote int,
                    helpful int,
                    PRIMARY KEY (Id, helpful, rating)
                ) WITH CLUSTERING ORDER BY (helpful DESC, rating DESC)
                """ % reviews_v1)

    session.execute("""
                CREATE TABLE IF NOT EXISTS %s (
                    Id int,
                    ASIN text,
                    date text,
                    total_rating int,
                    count_rating int,
                    PRIMARY KEY (Id, date)
                ) WITH CLUSTERING ORDER BY (date ASC)
                """ % reviews_v2)

    session.execute("""
                CREATE TABLE IF NOT EXISTS %s (
                    group text,
                    Id int,
                    ASIN text,
                    salesrank int,
                    PRIMARY KEY (group, salesrank)
                ) WITH CLUSTERING ORDER BY (salesrank ASC)
                """ % groups)

    session.execute("""
                CREATE TABLE IF NOT EXISTS %s (
                    group text,
                    customer text,
                    total int,
                    PRIMARY KEY (group, total, customer)
                ) WITH CLUSTERING ORDER BY (total DESC, customer ASC)
                """ % customer_reviews)

def parse_file(file_name, session):
    insert_product_query = "INSERT INTO {0}(Id, ASIN, title, group, salesrank) VALUES(?,?,?,?,?)".format(products)
    insert_review_v1_query = "INSERT INTO {0}(Id, ASIN, date, customer, rating, vote, helpful) VALUES(?,?,?,?,?,?,?)".format(reviews_v1)
    insert_review_v2_query = "INSERT INTO {0}(Id, ASIN, date, total_rating, count_rating) VALUES(?,?,?,?,?)".format(reviews_v2)
    insert_similar_query = "INSERT INTO {0}(Id, ASIN, similar, salesrank, similarsales) VALUES(?,?,?,?,?)".format(similars)
    insert_group_query = "INSERT INTO {0}(group, Id, ASIN, salesrank) VALUES(?,?,?,?)".format(groups)
    insert_customer_review_query = "INSERT INTO {0}(group, customer, total) VALUES (?,?,?)".format(customer_reviews)
    insert_product = session.prepare(insert_product_query)
    insert_review_v1 = session.prepare(insert_review_v1_query)
    insert_review_v2 = session.prepare(insert_review_v2_query)
    insert_similar = session.prepare(insert_similar_query)
    insert_group = session.prepare(insert_group_query)
    insert_customer_review = session.prepare(insert_customer_review_query)
    similar_sales = {}
    similar_list = []
    review_group = {}

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
            register = [int(lines[0].split(":")[1].strip()), #Id
                    lines[1].split(":")[1].strip(), #ASIN
                    lines[2].split(":")[1].strip(), #title
                    lines[3].split(":")[1].strip(), #group
                    int(lines[4].split(":")[1].strip())] #salesrank            

            Id = register[0]
            ASIN = register[1]
            similar_sales[ASIN] = register[4]      
            #extrai os similares, a lista é vazia se não houver similares
            similar_list.append([[Id, ASIN, w, register[4]] for w in lines[5].split(":")[1].split()[1:]])
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
            
            #insere na tabela produto
            session.execute(insert_product, register)

            #hash para somar e contar o total das avaliações diárias
            rating = {}

            for review in reviews:
                #insere a review na tabela
                session.execute(insert_review_v1, (Id, ASIN, review[0], review[2], int(review[4]), int(review[6]), int(review[8])))

                date = review[0]
                #somar e conta as avaliações diárias
                if date in rating:
                    rating[date] = (rating[date][0] + int(review[4]), rating[date][1] + 1)
                else:
                    rating[date] = (int(review[4]), 1)

                #questão g, review particionado por grupo
                review_key = register[3] + ":::" + review[2]
                if review_key in review_group:
                    review_group[review_key] = review_group[review_key] + 1
                else:
                    review_group[review_key] = 1                

            #insere a data e a soma e conta das avaliações
            for (date_key, value) in rating.items():
                session.execute(insert_review_v2, [Id, ASIN, date_key, value[0], value[1]])

            #insere o produto na tabela particionada pelo grupo do produto
            #para a questão d
            session.execute(insert_group, [register[3], Id, ASIN, register[4]])

        for sim_list in similar_list:
            for sim in sim_list:
                sim_asin = sim[2]
                if sim_asin in similar_sales:
                    session.execute(insert_similar, sim + [similar_sales[sim_asin]])
                else:
                    session.execute(insert_similar, sim + [-1])

        for key, value in review_group.items():
            session.execute(insert_customer_review, key.split(":::") + [value])

def question_a(session, Id  ='6'):
    query_a = """
            SELECT * 
            FROM {0} 
            WHERE Id = {1}
            """.format(reviews_v1, Id)

    prepared_a = session.prepare(query_a)
    rows = session.execute(prepared_a)

    print("(a) Dado produto, listar os 5 comentários mais úteis e com maior avaliação e os 5 comentários mais úteis e com menor avaliação.\n")

    for row in rows[0:5]:
        print("helpful:", row[1], "rating:", row[2], "customer:", row[4])

    rows = [list(w) for w in rows]
    rows = sorted(rows, key = lambda x: (-x[1], x[2]))

    print("\n")
    for row in rows[0:5]:
        print("helpful:", row[1], "rating:", row[2], "customer:", row[4])

    print("\n\n")

def question_b(session, Id = '6'):
    query_b = """
            SELECT *
            FROM {0}
            WHERE Id = {1}
            """.format(similars, int(Id))
    prepared_b = session.prepare(query_b)

    print("(b) Dado um produto, listar os produtos similares com maiores vendas do que ele.\n")
    
    for row in session.execute(prepared_b):
        if row[1] > row[2] and row[2] > 0:
            print("ASIN: ", row[3], "salesrank:", row[2])

    print("\n\n")

def question_c(session, Id = '6'):
    query_c = """
            SELECT date, total_rating, count_rating
            FROM {0}
            WHERE Id = {1}
            """.format(reviews_v2, Id)
    prepared_c = session.prepare(query_c)

    print("(c) Dado um produto, mostrar a evolução diária das médias de avaliação ao longo do intervalo de tempo coberto no arquivo de entrada.\n")

    for row in session.execute(prepared_c):
        print(row[0] + ",", "%.2f" % (row[1] / row[2]))

    print("\n\n")

def question_d(session):
    query_d = """
            SELECT group, id, ASIN, salesrank
            FROM {0} PER PARTITION LIMIT 10
            """.format(groups)

    prepare_d = session.prepare(query_d)

    print("(d) Listar os 10 produtos lideres de venda em cada grupo de produtos.\n")

    for row in session.execute(prepare_d):
        print("Grupo:", row[0], "salesrank:", row[3], "id:", row[1], "ASIN:", row[2])

    print("\n")

def question_e(session):
    query_e = """
            SELECT Id, ASIN, group, title, salesrank
            FROM {0}
            LIMIT 10
            """.format(products)
    prepared_e = session.prepare(query_e)

    print("(e) Listar os 10 produtos com a maior média de avaliações úteis positivas.\n")

    for row in session.execute(prepared_e):
        print("id:", row[0], "group:", row[2], "title:", row[3], "salesrank:", row[4])

    print("\n\n")

def question_f(session):
    print("(f) Listar a 5 categorias de produto com a maior média de avaliações úteis positivas.\n")

    print("\n\n")

def question_g(session):
    query_g = """
            SELECT group, customer, total
            FROM {0} PER PARTITION LIMIT 10
            """.format(customer_reviews)
    
    prepare_g = session.prepare(query_g)

    print("(g) Listar os 10 clientes que mais fizeram comentários por grupo de produto.\n")

    for row in session.execute(prepare_g):
        print("group:", row[0], "customer:", row[1], "total:", row[2])

    print("\n\n")

def main(file_name, Id = '6'):
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()

    create_base(session)
    parse_file(file_name, session)

    session.execute("USE %s" % KEYSPACE)

    question_a(session, Id)
    question_b(session, Id)
    question_c(session, Id)
    question_d(session)
    question_e(session)
    question_g(session)

    cluster.shutdown()

if __name__ == "__main__":    
    main(sys.argv[1])
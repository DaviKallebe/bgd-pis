import sys
import redis

def parse_file(file_name, rds):
    total_processado = 0
    pipe = rds.pipeline()
    print("Contador será atualizado de 200 em 200 registros processados.")
    print("\rProcessado:", total_processado, end = "")
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

            #a base vai trabalhar com ASIN, logo se a entrada for um Id rapidamente recupera o ASIN
            rds.set(Id, ASIN)            
            rds.set(ASIN + "|salesrank", register[4])

            #criando um conjunto ordernado para pré computar as salesranks para a questão (d)
            #e adicionando os grupos num set para posterior recuperação e assim fica
            #sem a necessidade de iterar sobre as chaves que pode ser custoso
            if (register[4] > 0):
                rds.zadd(register[3], {ASIN: register[4]})
                rds.sadd("groups", register[3])

            #computando a média das avaliações úteis para questão (e) e (f), par (acumulador, contador)
            #e contando a quantidade de comentários
            avg_helpful = (0,0)
            comments = {}
            for review in reviews:
                avg_helpful = (avg_helpful[0] + int(review[-1]), avg_helpful[1] + 1)

                #hash para incrementar em 1 se for encontrar senão cria uma entrada no hash
                if review[1] in comments:
                    comments[review[2]] = comments[review[2]] + 1
                else:
                    comments[review[2]] = 1
            #questão g adicionando os comentários por grupo
            for customer, count in comments.items():
                rds.zincrby("group_comments|" + register[3], count, customer)

            if avg_helpful[1] > 0:
                avg_helpful = avg_helpful[0] / avg_helpful[1]
                #adiciona a média em um conjunto ordenado            
                rds.zadd("avg_helpful", {ASIN: avg_helpful})
                #o mesmo para categorias
                for category in categories:
                    rds.zadd("avg_helpful_category", {category: avg_helpful})                    

            #inserindo os valores na base redis
            for attribute in register:
                rds.rpush(ASIN, attribute)                

            for similar in similars:
                rds.rpush(ASIN + "|similars", similar)

            for category in categories:
                rds.rpush(ASIN + "|categories", category)

            for review in reviews:
                rds.rpush(ASIN + "|reviews", '|'.join([review[0], review[2], review[4], review[6], review[8], review_stats[2]]))

            if total_processado % 200 == 0:
                pipe.execute()
                pipe = rds.pipeline()

                print("\rProcessado:", total_processado, end = "")
                sys.stdout.flush()

            total_processado = total_processado + 1            

        #fim do parse

def question_a(rds, Id = '6'):
    ASIN = rds.get(Id).decode()
    reviews = []

    for item in rds.lrange(ASIN + "|reviews", 0, -1):
        review = item.decode().split("|")        
        reviews.append([(int(w) if w.isdigit() else w) for w in review])
        review[-1] = float(review[-1])

    print("(a) Dado produto, listar os 5 comentários mais úteis e com maior avaliação e os 5 comentários mais úteis e com menor avaliação")

    lista_a1 = sorted(reviews, key=lambda x: (-x[4], x[2]))
    lista_a2 = sorted(reviews, key=lambda x: (-x[4], -x[2]))

    for item in lista_a1[0:5]:
        print('Data:', item[0], 'Cliente:', item[1], 'rating:', item[2], 'votes:', item[3], 'helpful:', item[4])

    print("\n")

    for item in lista_a2[0:5]:
        print('Data:', item[0], 'Cliente:', item[1], 'rating:', item[2], 'votes:', item[3], 'helpful:', item[4])

    print("\n\n")

def question_b(rds, Id = '6'):
    if rds.exists(Id):
        ASIN = rds.get(Id).decode()
    else:
        return

    salesrank = int(rds.get(ASIN + "|salesrank").decode())
    similars = [(item.decode(), rds.get(item.decode() + "|salesrank")) for item in rds.lrange(ASIN + "|similars", 0, -1)]

    print("(b) Dado um produto, listar os produtos similares com maiores vendas do que ele")
    print("Produto Id:", Id, "ASIN:", ASIN, "salesrank:", salesrank, "\n")
    for (similar, similar_rank) in similars:
        rank = int(similar_rank.decode()) if isinstance(similar_rank, bytes) else -1

        if rank > 0 and salesrank > rank:
            print("Similar com mais vendas:", similar, "rank:", rank)

    print("\n\n")

def question_c(rds, Id = '6'):
    if rds.exists(Id):
        ASIN = rds.get(Id).decode()
    else:
        return

    if rds.exists(ASIN + "|reviews"):
        reviews = {}
        temp = []

        for item in rds.lrange(ASIN + "|reviews", 0, -1):
            review = item.decode().split("|")
            day = review[0]

            if day not in reviews:
                reviews[day] = (int(review[2]), 1)
            else:
                reviews[day] = (reviews[day][0] + int(review[2]), reviews[day][1] + 1)

        for (key, value) in reviews.items():
            temp.append((key, value[0] / value[1]))

        temp = sorted(temp, key=lambda x: x[0])        

        print("(c) Dado um produto, mostrar a evolução diária das médias de avaliação ao longo do intervalo de tempo coberto no arquivo de entrada")
        
        for item in temp:
            print(item[0] + ",", "%.2f" % (item[1]))

        print("\n\n")

def question_d(rds):
    groups = rds.smembers("groups")

    print("(d) Listar os 10 produtos lideres de venda em cada grupo de produtos")

    for group in groups:
        print("\nGrupo:", group.decode())
        for key, value in rds.zrange(group, 0, 10, withscores=True):
            print("ASIN:", key.decode(), "salesrank", int(value))

    print("\n\n")

def question_e(rds):    
    print("(e) Listar os 10 produtos com a maior média de avaliações úteis positivas")

    for key, value in rds.zrange("avg_helpful", 0, 10, withscores=True, desc=True):
        print("ASIN:", key, "avg_helpful: %.2f" % (value))

    print("\n\n")

def question_f(rds):
    print("(f) Listar a 5 categorias de produto com a maior média de avaliações úteis positivas")

    for key, value in rds.zrange("avg_helpful_category", 0, 5, withscores=True, desc=True):
        print("Avg_helpful: %.2f" % (value), "Categoria:", key.decode())

    print("\n\n")

def question_g(rds):
    print("(g) Listar os 10 clientes que mais fizeram comentários por grupo de produto")
    groups = rds.smembers("groups")

    for group in groups:
        print("\nGrupo:", group.decode())
        for (customer, total) in rds.zrange("group_comments|" + group.decode(), 0, 10, withscores=True, desc=True):
            print("Customer:", customer.decode(), "Quantidade:", int(total))

#mude o valor do Id aqui
def main(file_name, Id = '6'):
    rds = redis.Redis()
    rds.flushall()
    
    #função para o parse do arquivo
    parse_file(file_name, rds)

    #questões para mudar o Id bastar mudar o parametro na função main
    question_a(rds, Id)
    question_b(rds, Id)
    question_c(rds, Id)
    question_d(rds)
    question_e(rds)
    question_f(rds)
    question_g(rds)

if __name__ == "__main__":    
    main(sys.argv[1])
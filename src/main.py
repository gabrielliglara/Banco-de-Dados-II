import psycopg2

# usamos a funcao connect do psycopg2 para criar uma nova sessão de 
# banco de dados e retornar uma nova instância de conexão
def connect_database():
    return psycopg2.connect(
        host="localhost",
        database="banco_log",
        user="gabriellilara",
        password="Princess2!"
    )

# busca todas as entradas do log organizada por id para executarmos em ordem cronologica
# criamos um cursor dentro da funcao para garantir que ele vai fechar e nao ficara em aberto
# fetchall() -> recupera as linhas da busca
def load_log(connection):
    with connection.cursor() as cursor:
        cursor.execute("SELECT * FROM LOG ORDER BY ID")
        return cursor.fetchall()

# verifica se o cliente existe na tabela clientes em memoria
# usamos count pq só precisamos saber se esta la 
# fetchone() retorna a primeira linha do resultado da consulta como tupla
def client_in_memory(connection, id_cliente):
    with connection.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM clientes_em_memoria WHERE id = %s", (id_cliente,))
        return cursor.fetchone()[0] > 0

def redo(connection, log_data):
    for operacao, id_cliente, nome, saldo in log_data:
        if operacao == "INSERT":
            if not client_in_memory(connection, id_cliente):
                with connection.cursor() as cursor:
                    cursor.execute(
                        "INSERT INTO clientes_em_memoria (id, nome, saldo) VALUES (%s, %s, %s)",
                        (id_cliente, nome, saldo)
                    )
                    print(f"REDO: INSERT aplicado para Cliente {id_cliente}")
        elif operacao == "UPDATE":
            with connection.cursor() as cursor:
                cursor.execute("SELECT saldo FROM clientes_em_memoria WHERE id = %s", (id_cliente,))
                result = cursor.fetchone()
                if result and result[0] != saldo:
                    cursor.execute(
                        "UPDATE clientes_em_memoria SET saldo = %s WHERE id = %s",
                        (saldo, id_cliente)
                    )
                    print(f"REDO: UPDATE aplicado para Cliente {id_cliente}")

    connection.commit()

def main():
    connection = connect_database()
    log_data = load_log(connection)
    redo(connection, log_data)
    connection.close()

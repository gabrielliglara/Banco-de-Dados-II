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

def create_unlogged_table(connection):
  with connection.cursor() as cursor:
    cursor.execute('DROP TABLE IF EXISTS clientes_em_memoria;')

    cursor.execute('''
        CREATE UNLOGGED TABLE clientes_em_memoria (
            id SERIAL PRIMARY KEY,
            nome TEXT,
            saldo NUMERIC
        );
    ''')

def create_table(connection):
    with connection.cursor() as cursor:
        cursor.execute('DROP TABLE IF EXISTS log')

        cursor.execute('''
            CREATE TABLE log(
                operacao TEXT, id_cliente INT, nome TEXT, saldo NUMERIC, 
                id_transacao INT, is_committed boolean)
        ''')
        

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
    for operacao, id_cliente, nome, saldo, id_transacao, is_committed in log_data:
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
        elif operacao == "DELETE":
           with connection.cursor() as cursor:
              if client_in_memory(connection, id_cliente):
                 cursor.execute("DELETE * FROM clientes_em_memoria WHERE id = %s", (id_cliente))
    connection.commit()

def extrair_id_cliente(comando):
    if "WHERE id =" in comando:
        return int(comando.split("WHERE id =")[1].replace(";", "").strip())
    return None

def extrair_nome(comando):
    if "VALUES" in comando:
        partes = comando.split("VALUES")[1]
        if "," in partes:
            nome = partes.split(",")[0].replace("(", "").replace("'", "").strip()
            return nome
    return None

def extrair_saldo(comando):
    if "VALUES" in comando:
        partes = comando.split("VALUES")[1]
        if "," in partes:
            saldo = partes.split(",")[1].replace(")", "").replace(";", "").strip()
            return float(saldo)
    elif "SET saldo" in comando:
        incremento = comando.split("saldo +")[1].split("WHERE")[0].strip()
        return float(incremento)  
    return None

def run_script(connection):
    with open("src/script_entrada.sql", "r") as file:
        linhas = [linha.strip() for linha in file.readlines() if linha.strip()]

    transaction_id = 0
    transacao_em_andamento = False
    comandos_transacao = []

    with connection.cursor() as cursor:
        for linha in linhas:
            if linha == "BEGIN;":
                transaction_id += 1
                transacao_em_andamento = True
                comandos_transacao = []
            elif linha == "END;":
                # Executa os comandos somente se houver um END;
                for comando in comandos_transacao:
                    cursor.execute(comando)
                    # Extrai info do comando para log
                    operacao = comando.split()[0]  # INSERT, UPDATE, etc.
                    id_cliente = extrair_id_cliente(comando)
                    nome = extrair_nome(comando)
                    saldo = extrair_saldo(comando)
                    cursor.execute(
                        "INSERT INTO log (operacao, id_cliente, nome, saldo, id_transacao, is_committed) VALUES (%s, %s, %s, %s, %s, %s)",
                        (operacao, id_cliente, nome, saldo, transaction_id, True)
                    )
                transacao_em_andamento = False
                comandos_transacao = []
            elif transacao_em_andamento:
                comandos_transacao.append(linha)
                # Registra no log como não comitada (vai atualizar depois se tiver END)
                operacao = linha.split()[0]
                id_cliente = extrair_id_cliente(linha)
                nome = extrair_nome(linha)
                saldo = extrair_saldo(linha)
                cursor.execute(
                    "INSERT INTO log (operacao, id_cliente, nome, saldo, id_transacao, is_committed) VALUES (%s, %s, %s, %s, %s, %s)",
                    (operacao, id_cliente, nome, saldo, transaction_id, False)
                )

        connection.commit()
    
def main():
    connection = connect_database()
    create_unlogged_table(connection)
    create_table(connection)
    log_data = load_log(connection)
    redo(connection, log_data)
    connection.close()

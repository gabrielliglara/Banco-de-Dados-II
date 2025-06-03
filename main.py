import psycopg2
import re

# Criamos nova sessão de banco de dados com a função Connect do psycopg2
# Retorna uma nova instância de conexão


def connect_database():
    return psycopg2.connect(
        host="localhost",
        database="banco_log",
        user="postgres",
        password="Princess2!"
    )

# Criamos a tabela na memória


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

# Criamos a tabela de log


def create_table(connection):
    with connection.cursor() as cursor:
        cursor.execute('DROP TABLE IF EXISTS log')

        cursor.execute('''
            CREATE TABLE log(
                id_log SERIAL PRIMARY KEY, operacao TEXT, id_cliente INT, nome TEXT, saldo NUMERIC, 
                id_transacao INT, is_committed boolean)
        ''')


# Busca todas as entradas do log organizada por id para executarmos em ordem cronologica
# fetchall() -> recupera as linhas da busca
def load_log(connection):
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT * FROM LOG WHERE is_committed = TRUE ORDER BY id_transacao, id_log")
        return cursor.fetchall()

# Verifica se o cliente existe na tabela clientes_em_memoria


def client_in_memory(connection, id_cliente):
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT COUNT(*) FROM clientes_em_memoria WHERE id = %s", (id_cliente,))
        return cursor.fetchone()[0] > 0

# Executa o redo com base no log


def redo(connection, log_data):
    print("\nTransações que devem realizar o REDO:")
    processed_transactions = set()
    updated_clients = {}

    for id_log, operacao, id_cliente, nome, saldo, id_transacao, is_committed in log_data:
        if is_committed:
            if id_transacao not in processed_transactions:
                print(f"- Transação {id_transacao}")
                processed_transactions.add(id_transacao)

            if operacao == "INSERT":
                with connection.cursor() as cursor_redo:
                    if not client_in_memory(connection, id_cliente):
                        cursor_redo.execute(
                            "INSERT INTO clientes_em_memoria (id, nome, saldo) VALUES (%s, %s, %s)",
                            (id_cliente, nome, saldo)
                        )
                        print(
                            f"  REDO: INSERT aplicado para Cliente {id_cliente} na Transação {id_transacao}")
                        updated_clients[id_cliente] = (nome, saldo, "INSERT")
            elif operacao == "UPDATE":
                with connection.cursor() as cursor_redo:
                    cursor_redo.execute(
                        "SELECT saldo FROM clientes_em_memoria WHERE id = %s FOR UPDATE", (id_cliente,))
                    result = cursor_redo.fetchone()

                    if result:
                        current_saldo = result[0]
                        if operacao == "UPDATE" and saldo is not None:  # saldo é o incremento aqui
                            new_saldo = current_saldo + saldo
                            print(
                                f"  REDO: Calculando novo saldo para Cliente {id_cliente}: {current_saldo} + {saldo} = {new_saldo}")
                            cursor_redo.execute(
                                "UPDATE clientes_em_memoria SET saldo = %s WHERE id = %s",
                                (new_saldo, id_cliente)
                            )
                            print(
                                f"  REDO: UPDATE aplicado para Cliente {id_cliente} na Transação {id_transacao}. Saldo antigo: {current_saldo}, novo: {new_saldo}")
                            updated_clients[id_cliente] = (
                                nome, new_saldo, "UPDATE")  # Armazena o novo saldo calculado
                        else:
                            print(
                                f"  REDO: UPDATE para Cliente {id_cliente} na Transação {id_transacao} não pode ser aplicado (saldo não especificado ou não é incremento).")
                    else:
                        print(
                            f"  REDO: UPDATE para Cliente {id_cliente} na Transação {id_transacao} pulado (cliente não encontrado).")

            elif operacao == "DELETE":
                with connection.cursor() as cursor_redo:
                    if client_in_memory(connection, id_cliente):
                        cursor_redo.execute(
                            "DELETE FROM clientes_em_memoria WHERE id = %s", (id_cliente,))
                        print(
                            f"  REDO: DELETE aplicado para Cliente {id_cliente} na Transação {id_transacao}")
                        updated_clients[id_cliente] = (None, None, "DELETE")
    connection.commit()

# Atualizando como está o REDO
    print("\n--- Dados atualizados após REDO ---")
    if updated_clients:
        for id_cli, data in updated_clients.items():
            if data[2] == "DELETE":
                print(f"Cliente {id_cli}: DELETADO")
            else:
                print(
                    f"Cliente {id_cli}: Nome={data[0]}, Saldo={data[1]} ({data[2]})")
    else:
        print("Nenhum dado foi atualizado pelo REDO.")


def extrair_id_cliente(comando):
    match_insert = re.search(r'VALUES \((\d+),', comando, re.IGNORECASE)
    if match_insert:
        return int(match_insert.group(1))

    match_where = re.search(r'WHERE id = (\d+)', comando, re.IGNORECASE)
    if match_where:
        return int(match_where.group(1))

    return None


def extrair_nome(comando):
    match_insert = re.search(
        r"VALUES \(\d+,\s*'([^']+)'", comando, re.IGNORECASE)
    if match_insert:
        return match_insert.group(1)
    return None


def extrair_saldo(comando):
    match_insert = re.search(
        r"VALUES \(\d+,\s*'[^']+',\s*(\d+\.?\d*)\)", comando, re.IGNORECASE)
    if match_insert:
        return float(match_insert.group(1))

    match_update = re.search(
        r'SET saldo = saldo \+ (\d+\.?\d*)', comando, re.IGNORECASE)
    if match_update:
        return float(match_update.group(1))

    return None


def run_script(connection):
    with open("src/script_entrada.sql", "r") as file:
        linhas = [linha.strip() for linha in file.readlines() if linha.strip()]

    transaction_id = 0
    transacao_em_andamento = False

    temp_log_entries_for_transaction = []

    with connection.cursor() as cursor:
        for linha in linhas:
            if linha.upper() == "BEGIN;":
                transaction_id += 1
                transacao_em_andamento = True
                temp_log_entries_for_transaction = []
                print(f"--- Iniciando Transação {transaction_id} ---")
            elif linha.upper() == "END;":
                if transacao_em_andamento:
                    print(
                        f"--- Finalizando Transação {transaction_id} (COMMIT) ---")
                    if temp_log_entries_for_transaction:
                        final_log_entries = []
                        for entry in temp_log_entries_for_transaction:

                            final_log_entries.append(entry + (True,))

                        cursor.executemany(
                            "INSERT INTO log (operacao, id_cliente, nome, saldo, id_transacao, is_committed) VALUES (%s, %s, %s, %s, %s, %s)",
                            final_log_entries
                        )
                    transacao_em_andamento = False
                else:
                    print(
                        f"AVISO: END; sem BEGIN; anterior. Ignorando linha: {linha}")
            elif transacao_em_andamento:

                operacao_tipo = linha.split()[0].upper()

                id_cliente = extrair_id_cliente(linha)
                nome = extrair_nome(linha)
                saldo = extrair_saldo(linha)

                if id_cliente is None:
                    print(
                        f"AVISO: Não foi possível extrair ID para a operação: '{linha}'. Esta operação será pulada.")
                    continue

                temp_log_entries_for_transaction.append(
                    (operacao_tipo, id_cliente, nome, saldo, transaction_id)
                )
                print(
                    f"  Registrando para Transação {transaction_id}: {operacao_tipo} Cliente {id_cliente} (linha: '{linha}')")

            else:
                print(
                    f"AVISO: Linha fora de transação ativa ou comando desconhecido: '{linha}'")

        connection.commit()
        print("\nLog de transações populado com operações comitadas.")


def main():

    connection_phase1 = connect_database()
    create_unlogged_table(connection_phase1)
    create_table(connection_phase1)
    run_script(connection_phase1)
    connection_phase1.close()  # simula a queda ao fechar a conexão

    print("Sistema caiu e foi reiniciado")
    connection_phase2 = connect_database()
    log_data = load_log(connection_phase2)
    redo(connection_phase2, log_data)
    connection_phase2.close()
    print("\nExecução finalizada")


if __name__ == "__main__":
    main()

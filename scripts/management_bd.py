import psycopg2
from psycopg2 import sql

#Função para verificar se a tabela existe
def table_exists(cursor, table_name):
    exists_query = sql.SQL("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = %s)")
    cursor.execute(exists_query, (table_name,))
    return cursor.fetchone()[0]

# Função para verificar o valor salvo na tabela
def get_scanned_day(cursor, table_name, column_name):
    # Consultar o valor do campo scannedday
    select_query = f"SELECT {column_name} FROM {table_name} WHERE id = 1"
    cursor.execute(select_query)
    racing_date = cursor.fetchone()[0]
    return racing_date

def has_values(cursor, table_name, column_name):
    try:
        query = sql.SQL("SELECT EXISTS(SELECT 1 FROM {} WHERE {} IS NOT NULL)").format(
            sql.Identifier(table_name),
            sql.Identifier(column_name)
        )
        cursor.execute(query)
        return cursor.fetchone()[0]
    except psycopg2.Error as e:
        print("Erro ao verificar se a tabela tem valores:", e)
        return False

# Função para inserir ou atualizar o valor
def insert_or_update_value(conn, cursor, table_name, column_name, value):
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]
        
        if row_count > 0:
            update_query = f"UPDATE {table_name} SET {column_name} = %s WHERE id = 1"
            cursor.execute(update_query, (value,))
            print(f"Valor atualizado para {value} na coluna {column_name} da tabela {table_name}.")
        else:
            insert_query = f"INSERT INTO {table_name} ({column_name}) VALUES (%s)"
            cursor.execute(insert_query, (value,))
            print(f"Valor inserido {value} na coluna {column_name} da tabela {table_name}.")
        
        conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        print("Erro ao inserir ou atualizar valor:", e)

def drop_table(conn, table_name):
    try:
        cursor = conn.cursor()
        drop_table_query = f"DROP TABLE IF EXISTS {table_name}"
        cursor.execute(drop_table_query)
        conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        print("Erro ao excluir a tabela:", e)

# Função para verificar se a URL já existe na tabela
def url_exists(conn, url):
    try:
        cursor = conn.cursor()
        select_query = "SELECT EXISTS(SELECT 1 FROM table_linkstoscam WHERE url = %s)"
        cursor.execute(select_query, (url,))
        return cursor.fetchone()[0]
    except psycopg2.Error as e:
        print("Erro ao verificar a existência da URL na tabela:", e)

# Função para inserir dados na tabela
def insert_data(conn, url, website):
    try:
        cursor = conn.cursor()
        insert_query = "INSERT INTO table_linkstoscam (url, website, scanned) VALUES (%s, %s, FALSE)"
        cursor.execute(insert_query, (url, website))
        conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        print("Erro ao inserir dados:", e)
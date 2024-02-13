import psycopg2
from psycopg2 import sql

#Função para verificar se a tabela existe
def table_exists(cursor, table_name):
    exists_query = sql.SQL("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = %s)")
    cursor.execute(exists_query, (table_name,))
    return cursor.fetchone()[0]

# Função para verificar o valor salvo na tabela
def get_scanned_day(cursor, table_name):
    # Consultar o valor do campo scannedday
    select_query = f"SELECT scannedday FROM {table_name} WHERE id = 1"
    cursor.execute(select_query)
    racing_date = cursor.fetchone()[0]
    return racing_date
    
# Função para verificar se a tabela tem algum valor
def has_values(cursor, table_name):
    count_query = sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table_name))
    cursor.execute(count_query)
    return cursor.fetchone()[0] > 0

# Função para inserir ou atualizar o valor
def insert_or_update_value(cursor, table_name, value):
    if has_values(cursor, table_name):
        update_query = sql.SQL("UPDATE {} SET scannedday = %s WHERE id = 1").format(sql.Identifier(table_name))
        cursor.execute(update_query, (value,))
    else:
        insert_query = sql.SQL("INSERT INTO {} (scannedday) VALUES (%s)").format(sql.Identifier(table_name))
        cursor.execute(insert_query, (value,))

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
def insert_data(conn, url):
    try:
        cursor = conn.cursor()
        insert_query = "INSERT INTO table_linkstoscam (url, scanned) VALUES (%s, FALSE)"
        cursor.execute(insert_query, (url,))
        conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        print("Erro ao inserir dados:", e)
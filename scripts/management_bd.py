import psycopg2
from psycopg2 import sql

def get_url_to_scrap(conn, table_name, column_name):
    try:
        cursor = conn.cursor()
        select_query = sql.SQL("SELECT url FROM {} WHERE website = %s AND scanned = false order by id asc limit 1").format(
            sql.Identifier(table_name)
        )
        cursor.execute(select_query, (column_name,))
        url_to_scrap = cursor.fetchone()
        if url_to_scrap:
            return url_to_scrap[0]
        else:
            print("Nenhuma URL encontrada com as condições especificadas.")
            return None
    except psycopg2.Error as e:
        print("Erro ao selecionar a URL:", e)
        return None
# Funções importadas para o funcionamento do script
from db import connect
from utils import *
import sys
import psycopg2
from psycopg2 import sql

# Instalar e configurar selenium e chrome driver.
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

options = Options()
options.add_argument('--headless')
#options.add_argument('--no-sandbox')
options.add_argument('log-level=3') # INFO = 0 / WARNING = 1 / LOG_ERROR = 2 / LOG_FATAL = 3
options.add_argument('--disable-dev-shm-usage')
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

# Função para verificar se a tabela existe
def table_exists(cursor, table_name):
    exists_query = sql.SQL("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = %s)")
    cursor.execute(exists_query, (table_name,))
    return cursor.fetchone()[0]

# Dicionário contendo o nome das tabelas e suas definições
tables = {
    'lastscannedday': """
        CREATE TABLE lastscannedday (
            id SERIAL PRIMARY KEY,
            timeform_scannedday VARCHAR(10),
            racingpost_scannedday VARCHAR(10)
        )
    """,
    'linkstoscam': """
        CREATE TABLE IF NOT EXISTS linkstoscam (
            id SERIAL PRIMARY KEY,
            url VARCHAR,
            website VARCHAR(25),
            scanned BOOLEAN
        )
    """
}

websites = {
    'timeform' : 'timeform_scannedday',
    'racingpost' : 'racingpost_scannedday'
}

# Função para verificar o valor salvo na tabela
def get_scanned_day(cursor, column_name):
    # Consultar o valor do campo scannedday
    select_query = f"SELECT {column_name} FROM lastscannedday WHERE id = 1"
    cursor.execute(select_query)
    racing_date = cursor.fetchone()[0]
    return racing_date

def has_values(cursor, column_name):
    try:
        query = sql.SQL("SELECT EXISTS(SELECT 1 FROM lastscannedday WHERE {} IS NOT NULL)").format(
            sql.Identifier(column_name)
        )
        cursor.execute(query)
        return cursor.fetchone()[0]
    except psycopg2.Error as e:
        print("Erro ao verificar se a tabela tem valores:", e)
        return False

# Função para inserir ou atualizar o valor
def insert_or_update_value(conn, cursor, column_name, value):
    try:
        cursor.execute("SELECT COUNT(*) FROM lastscannedday")
        row_count = cursor.fetchone()[0]
        
        if row_count > 0:
            update_query = f"UPDATE lastscannedday SET {column_name} = %s WHERE id = 1"
            cursor.execute(update_query, (value,))
        else:
            insert_query = f"INSERT INTO lastscannedday ({column_name}) VALUES (%s)"
            cursor.execute(insert_query, (value,))
        
        conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        print("Erro ao inserir ou atualizar valor:", e)

def update_field_to_null(conn, column_name):
    try:
        cursor = conn.cursor()
        update_query = f"UPDATE lastscannedday SET {column_name} = NULL"
        cursor.execute(update_query)
        conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        print("Erro ao atualizar campo:", e)

# Função para verificar se a URL já existe na tabela
def url_exists(conn, url):
    try:
        cursor = conn.cursor()
        select_query = "SELECT EXISTS(SELECT 1 FROM linkstoscam WHERE url = %s)"
        cursor.execute(select_query, (url,))
        return cursor.fetchone()[0]
    except psycopg2.Error as e:
        print("Erro ao verificar a existência da URL na tabela:", e)

# Função para inserir dados na tabela
def insert_data(conn, url, website):
    try:
        cursor = conn.cursor()
        insert_query = "INSERT INTO linkstoscam (url, website, scanned) VALUES (%s, %s, FALSE)"
        cursor.execute(insert_query, (url, website))
        conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        print("Erro ao inserir dados:", e)

# Usar a função connect() para obter uma conexão com o banco de dados e criação de um cursor
conn = connect()
cursor = conn.cursor()

# Verificar se as tabelas existem e criar se não existirem
for table_name, create_table_query in tables.items():
    if not table_exists(cursor, table_name):
        cursor.execute(create_table_query)
        conn.commit()
        print(f"Tabela {table_name} criada com sucesso.")
    else:
        print(f"A tabela {table_name} já existe.")

def arrayoflinks(listoflinks):
    if len(listoflinks) != 0:
        for i in listoflinks:
            hrefCaptured = i.get_attribute('href')
            # Verificar se a URL já existe na tabela
            if not url_exists(conn, hrefCaptured):
                # Inserir a URL na tabela se não existir
                insert_data(conn, hrefCaptured, website_scanned)
            else:
                print('A URL já existe na tabela. Ignorando a inserção.')

# Verificar se as tabelas existem e criar se não existirem
for website_name, column_value in websites.items():
    if website_name == 'timeform':
        ######################
        #   TIMEFORM SCRAP   #
        ######################

        # Váriaveis de banco de dados
        column_lastscannedday = 'timeform_scannedday'
        website_scanned = 'timeform'

        # Parte do link para rastrear
        partoflink = 'https://www.timeform.com/greyhound-racing/results/'

        def getlinksatscannedday(daytoscrap):
            # Link com variavel racingpost para rastrear
            driver.get(partoflink + daytoscrap)
            driver.implicitly_wait(0.5)
            fullpage = driver.find_elements(By.XPATH, "//a[@class='waf-header hover-opacity']")
            return fullpage

        # Verificar se a tabela tem algum valor
        if not has_values(cursor, column_lastscannedday):
            # Chamar a função e salvar a data de ontem em uma variável
            racingDate = getlastday()
            listoflinks = getlinksatscannedday(racingDate)
            arrayoflinks(listoflinks)
            # Inserir valor se não houver nenhum
            insert_or_update_value(conn, cursor, column_lastscannedday, racingDate)
        else:
            # Chamar a função e salvar o valor retornado em uma variável
            racingDate = get_scanned_day(cursor, column_lastscannedday)
            if racingDate == '2013-01-01':
                update_field_to_null(conn, column_lastscannedday)
                sys.exit()
            else:
                racingDate = retrocederData(racingDate)
                listoflinks = getlinksatscannedday(racingDate)
                arrayoflinks(listoflinks)
            # Atualizar valor se houver algum
            insert_or_update_value(conn, cursor, column_lastscannedday, racingDate)
    else:
        ########################
        #   RACINGPOST SCRAP   #
        ########################

        # Váriaveis de banco de dados 
        column_lastscannedday = 'racingpost_scannedday'
        website_scanned = 'racingpost'

        # Parte do link para rastrear
        partoflink = 'https://greyhoundbet.racingpost.com/#results-list/r_date='
        fulllinksscanned = []

        def getlinksatscannedday(daytoscrap):
            # Link com variavel racingpost para rastrear
            driver.get(partoflink + daytoscrap)
            driver.implicitly_wait(0.5)
        #    fullpage = driver.find_elements(By.XPATH, "//a[@class='waf-header hover-opacity']")
            partialHTML = driver.find_elements(By.CLASS_NAME, 'results-race-list-row')
            for i in partialHTML:
                linksscanned = i.find_elements(By.TAG_NAME, 'a')
                for n in linksscanned:
                    fulllinksscanned.append(n)
            return fulllinksscanned

        # Verificar se a tabela tem algum valor
        if not has_values(cursor, column_lastscannedday):
            # Chamar a função e salvar a data de ontem em uma variável
            racingDate = getlastday()
            listoflinks = getlinksatscannedday(racingDate)
            arrayoflinks(listoflinks)
            # Inserir valor se não houver nenhum
            insert_or_update_value(conn, cursor, column_lastscannedday, racingDate)
        else:
            # Chamar a função e salvar o valor retornado em uma variável
            racingDate = get_scanned_day(cursor, column_lastscannedday)
            if racingDate == '1997-01-01':
                update_field_to_null(conn, column_lastscannedday)
                sys.exit()
            else:
                racingDate = retrocederData(racingDate)
                listoflinks = getlinksatscannedday(racingDate)
                arrayoflinks(listoflinks)
            # Atualizar valor se houver algum
            insert_or_update_value(conn, cursor, column_lastscannedday, racingDate)

    # Salvar alterações no banco de dados
    conn.commit()

# Fechar o cursor e a conexão
cursor.close()
conn.close()
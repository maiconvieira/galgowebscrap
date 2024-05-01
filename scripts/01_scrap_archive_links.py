import datetime
import time
import sys
import logging
import psycopg2
from db import connect
from tables import tables, table_exists
from psycopg2 import sql
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

logging.basicConfig(level=logging.ERROR)

def create_tables_if_not_exist():
    with connect() as conn:
        with conn.cursor() as cursor:
            for table_name, create_table_query in tables.items():
                if not table_exists(table_name):
                    cursor.execute(create_table_query)
                    conn.commit()

def get_driver():
    options = Options()
    options.add_argument('--headless')
    options.add_argument('log-level=3')
    options.add_argument('--disable-dev-shm-usage')
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

# Função para verificar se a tabela tem valores
def has_values(cursor, column_name):
    query = f"SELECT EXISTS(SELECT 1 FROM lastscannedday WHERE {column_name} IS NOT NULL)"
    cursor.execute(query)
    return cursor.fetchone()[0]

def get_scanned_day(cursor, column_name):
    try:
        # Consultar o valor do campo scannedday
        select_query = f"SELECT {column_name} FROM lastscannedday WHERE id = 1"
        cursor.execute(select_query)
        result = cursor.fetchone()
        if result is not None:
            return result[0]
        else:
            logging.warning("Nenhum resultado retornado da consulta.")
            return None
    except psycopg2.Error as e:
        logging.error("Erro ao consultar o campo scannedday: %s", e)
        sys.exit()

def insert_or_update_value(cursor, column_name, value):
    try:
        with connect() as conn:
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
        logging.error("Erro ao inserir ou atualizar valor: %s", e)

# Função para atualizar um campo para NULL
def update_field_to_null(cursor, column_name):
    update_query = f"UPDATE lastscannedday SET {column_name} = NULL"
    cursor.execute(update_query)

def url_exists_in_table(cursor, url):
    try:
        select_query = "SELECT EXISTS(SELECT 1 FROM linkstoscam WHERE url = %s)"
        cursor.execute(select_query, (url,))
        return cursor.fetchone()[0]
    except psycopg2.Error as e:
        logging.error("Erro ao verificar a existência da URL na tabela: %s", e)
        return False

def insert_url_into_table(conn, url, website):
    try:
        with conn.cursor() as cursor:
            insert_query = "INSERT INTO linkstoscam (url, website, scanned) VALUES (%s, %s, FALSE)"
            cursor.execute(insert_query, (url, website))
            conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        logging.error("Erro ao inserir dados: %s", e)

def scrape_page(driver, racing_date):
    driver.get(f"https://www.timeform.com/greyhound-racing/results/{racing_date}")
    driver.implicitly_wait(3)
    try:
        return driver.find_elements(By.XPATH, "//a[@class='waf-header hover-opacity']")
    except NoSuchElementException:
        logging.error('Elemento: scrapedPage não localizado')
        return []
    
# Função para obter o último dia
def get_last_day():
    date_now = datetime.datetime.now()
    last_day = date_now - datetime.timedelta(days=1)
    return last_day.strftime('%Y-%m-%d')

# Função para voltar um dia
def go_back_day(parameter):
    date = datetime.datetime.strptime(parameter, '%Y-%m-%d')
    previous_date = date - datetime.timedelta(days=1)
    return previous_date.strftime('%Y-%m-%d')

start_time = time.time()
with connect() as conn:
    create_tables_if_not_exist()
    with conn.cursor() as cursor:
        if not has_values(cursor, 'timeform_scannedday'):
            racing_date = get_last_day()
        else:
            racing_date = get_scanned_day(cursor, 'timeform_scannedday')
            if racing_date == '2013-01-01':
                update_field_to_null(cursor, 'timeform_scannedday')
                sys.exit()
            else:
                racing_date = go_back_day(racing_date)
        driver = get_driver()
        scraped_page = scrape_page(driver, racing_date)
        for i in scraped_page:
            href_captured = i.get_attribute('href')
            print(href_captured)
            if not url_exists_in_table(cursor, href_captured):
                insert_url_into_table(conn, href_captured, 'timeform')
        insert_or_update_value(cursor, 'timeform_scannedday', racing_date)
        logging.info('Timeform - Archive')

#driver.get("https://www.google.com/")
# identify elements with tagname <a>
#lnks=driver.find_elements_by_tag_name("a")
# traverse list
#for lnk in lnks:
   # get_attribute() to get all href
#   print(lnk.get_attribute(href))
#driver.quit()

driver.quit()

logging.info('OK!')
end_time = time.time()
execution_time = end_time - start_time
logging.info(f"Tempo de execução: {execution_time} segundos")

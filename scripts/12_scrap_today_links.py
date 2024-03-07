import datetime
import time
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

logging.basicConfig(level=logging.INFO)

scrapedPage = []

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
    driver.get(f"https://greyhoundbet.racingpost.com/#results-list/r_date={racing_date}")
    driver.implicitly_wait(2)
    partialHTML = driver.find_elements(By.CLASS_NAME, 'results-race-list-row')
    for i in partialHTML:
        scannedLinks = i.find_elements(By.TAG_NAME, 'a')
        for n in scannedLinks:
            scrapedPage.append(n)
    return scrapedPage

start_time = time.time()

racing_date = datetime.datetime.now().strftime('%Y-%m-%d')
with connect() as conn:
    create_tables_if_not_exist()
    with conn.cursor() as cursor:
        driver = get_driver()
        scraped_page = scrape_page(driver, racing_date)
        for i in scraped_page:
            href_captured = i.get_attribute('href')
            if not url_exists_in_table(cursor, href_captured):
                insert_url_into_table(conn, href_captured, 'timeform')
        logging.info('Timeform - Today')
driver.quit()

logging.info('OK!')

end_time = time.time()
execution_time = end_time - start_time
logging.info(f"Tempo de execução: {execution_time} segundos")

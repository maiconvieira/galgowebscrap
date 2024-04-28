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

def has_values(cursor,column_name):
    try:
        query = sql.SQL(f"SELECT EXISTS(SELECT 1 FROM lastscannedday WHERE {column_name} IS NOT NULL)")
        cursor.execute(query)
        return cursor.fetchone()[0]
    except psycopg2.Error as e:
        logging.error("Erro ao verificar se a tabela tem valores: %s", e)
        return False

def get_scanned_day(cursor, column_name):
    # Consultar o valor do campo scannedday
    select_query = f"SELECT {column_name} FROM lastscannedday WHERE id = 1"
    cursor.execute(select_query)
    racing_date = cursor.fetchone()[0]
    return racing_date

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

def update_field_to_null(cursor, column_name):
    try:
        with connect() as conn:
            update_query = f"UPDATE lastscannedday SET {column_name} = NULL"
            cursor.execute(update_query)
            conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        logging.error("Erro ao atualizar campo: %s", e)

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

def get_last_day():
    date_now = datetime.datetime.now()
    last_day = date_now - datetime.timedelta(days=1)
    return last_day.strftime('%Y-%m-%d')

def go_back_day(parameter):
    partsOfDate = parameter.split('-')
    lastLineYear = int(partsOfDate[0])
    lastLineMonth = int(partsOfDate[1])
    lastLineDay = int(partsOfDate[2])
    if lastLineDay == 26 and lastLineMonth == 12:
        dayToScrap = str(lastLineDay - 2).zfill(2)
        monthToScrap = str(lastLineMonth).zfill(2)
        yearToScrap = str(lastLineYear).zfill(4)
    elif lastLineDay > 1 and lastLineMonth > 1:
        dayToScrap = str(lastLineDay - 1).zfill(2)
        monthToScrap = str(lastLineMonth).zfill(2)
        yearToScrap = str(lastLineYear).zfill(4)
    elif lastLineDay > 1 and lastLineMonth == 1:
        dayToScrap = str(lastLineDay - 1).zfill(2)
        monthToScrap = str(lastLineMonth).zfill(2)
        yearToScrap = str(lastLineYear).zfill(4)
    elif lastLineDay == 1 and lastLineMonth == 3 and ((lastLineYear%4 == 0 and lastLineYear%100 != 0) or (lastLineYear%400 == 0)):
        dayToScrap = str(29)
        monthToScrap = str(lastLineMonth - 1).zfill(2)
        yearToScrap = str(lastLineYear).zfill(4)
    elif lastLineDay == 1 and lastLineMonth == 3 and ((lastLineYear%4 != 0 and lastLineYear%100 == 0) or (lastLineYear%400 != 0)):
        dayToScrap = str(28)
        monthToScrap = str(lastLineMonth - 1).zfill(2)
        yearToScrap = str(lastLineYear).zfill(4)
    elif lastLineDay == 1 and lastLineMonth in(2, 4, 6, 8, 9, 11):
        dayToScrap = str(31)
        monthToScrap = str(lastLineMonth - 1).zfill(2)
        yearToScrap = str(lastLineYear).zfill(4)
    elif lastLineDay == 1 and lastLineMonth in(5, 7, 10, 12):
        dayToScrap = str(30)
        monthToScrap = str(lastLineMonth - 1).zfill(2)
        yearToScrap = str(lastLineYear).zfill(4)
    elif lastLineDay == 1 and lastLineMonth == 1:
        dayToScrap = str(31)
        monthToScrap = str(12)
        yearToScrap = str(lastLineYear -1).zfill(4)
    racingDate = yearToScrap + '-' + monthToScrap + '-' + dayToScrap
    return racingDate

start_time = time.time()
with connect() as conn:
    create_tables_if_not_exist()
    with conn.cursor() as cursor:
        if not has_values(cursor, 'racingpost_scannedday'):
            racing_date = get_last_day()
        else:
            racing_date = get_scanned_day(cursor, 'racingpost_scannedday')
            if racing_date == '1997-01-01':
                update_field_to_null(cursor, 'racingpost_scannedday')
                sys.exit()
            else:
                racing_date = go_back_day(racing_date)
        driver = get_driver()
        scraped_page = scrape_page(driver, racing_date)
        for i in scraped_page:
            href_captured = i.get_attribute('href')
            if not url_exists_in_table(cursor, href_captured):
                insert_url_into_table(conn, href_captured, 'racingpost')
        insert_or_update_value(cursor, 'racingpost_scannedday', racing_date)
        logging.info('Racingpost - Archive')
driver.quit()

logging.info('OK!')

end_time = time.time()
execution_time = end_time - start_time
logging.info(f"Tempo de execução: {execution_time} segundos")

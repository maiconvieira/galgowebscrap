# Funções importadas para o funcionamento do script
from db import connect
import datetime
import sys
import psycopg2
from psycopg2 import sql
from selenium import webdriver
from selenium.webdriver.common.by import By
#from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
#from selenium.common.exceptions import StaleElementReferenceException
#from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager

options = Options()
options.add_argument('--headless')
#options.add_argument('--no-sandbox')
options.add_argument('log-level=3') # INFO = 0 / WARNING = 1 / LOG_ERROR = 2 / LOG_FATAL = 3
options.add_argument('--disable-dev-shm-usage')
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

# Usar a função connect() para obter uma conexão com o banco de dados e criação de um cursor
with connect() as conn:
    with conn.cursor() as cursor:
        # Verificar se tabelas existem e criar no banco de dados
        def table_exists(table_name):
            exists_query = sql.SQL("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = %s)")
            cursor.execute(exists_query, (table_name,))
            return cursor.fetchone()[0]

        # Dicionário contendo o nome das tabelas e suas definições
        tables = {
            'lastscannedday': """
                CREATE TABLE IF NOT EXISTS lastscannedday (
                    id SERIAL PRIMARY KEY,
                    timeform_scannedday VARCHAR(10),
                    racingpost_scannedday VARCHAR(10)
                )
            """,
            'linkstoscam': """
                CREATE TABLE IF NOT EXISTS linkstoscam (
                    id SERIAL PRIMARY KEY,
                    url VARCHAR NOT NULL UNIQUE,
                    website VARCHAR(25),
                    scanned BOOLEAN
                )
            """
        }

        # Verificar se as tabelas existem se não existir criar
        for table_name, create_table_query in tables.items():
            if not table_exists(table_name):
                cursor.execute(create_table_query)
                conn.commit()
                print(f"Tabela {table_name} criada com sucesso.")

        # Variaveis criados para o script
        partOfLinkTimeform = 'https://www.timeform.com/greyhound-racing/results/'
        partOfLinkRacingpost = 'https://greyhoundbet.racingpost.com/#results-list/r_date='
        scrapedPage = []
        limitOfDateTimeform = '2013-01-01'
        limitOfDateRacingpost = '1997-01-01'

        # Funções criadas para o script
        def getLinksAtScannedDayAtTimeform(daytoscrap):
            driver.get(partOfLinkTimeform + daytoscrap)
            driver.implicitly_wait(0.5)
            scrapedPage = driver.find_elements(By.XPATH, "//a[@class='waf-header hover-opacity']")
            return scrapedPage

        def getLinksAtScannedDayAtRacingpost(daytoscrap):
            # Link com variavel racingpost para rastrear
            driver.get(partOfLink + daytoscrap)
            driver.implicitly_wait(0.5)
            partialHTML = driver.find_elements(By.CLASS_NAME, 'results-race-list-row')
            for i in partialHTML:
                scannedLinks = i.find_elements(By.TAG_NAME, 'a')
                for n in scannedLinks:
                    scrapedPage.append(n)
            return scrapedPage

        def arrayOfLinks(listOfLinks):
            if len(listOfLinks) != 0:
                for i in listOfLinks:
                    hrefCaptured = i.get_attribute('href')
                    # Verificar se a URL já existe na tabela
                    if not url_exists(hrefCaptured):
                        # Inserir a URL na tabela se não existir
                        insert_data(hrefCaptured, website_name)
                    else:
                        print('A URL já existe na tabela. Ignorando a inserção.')

        def getLastDay():
            date_now = datetime.datetime.now()
            last_day = date_now - datetime.timedelta(days=1)
            racingDate = last_day.strftime('%Y-%m-%d')
            return racingDate

        def goBackDay(parameter):
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

        # Funções relacionadas ao banco de dados
        def has_values(column_name):
            try:
                query = sql.SQL(f"SELECT EXISTS(SELECT 1 FROM lastscannedday WHERE {column_name} IS NOT NULL)")
                cursor.execute(query)
                return cursor.fetchone()[0]
            except psycopg2.Error as e:
                print("Erro ao verificar se a tabela tem valores:", e)
                return False

        # Função para verificar o valor salvo na tabela
        def get_scanned_day(column_name):
            # Consultar o valor do campo scannedday
            select_query = f"SELECT {column_name} FROM lastscannedday WHERE id = 1"
            cursor.execute(select_query)
            racing_date = cursor.fetchone()[0]
            return racing_date

        # Função para inserir ou atualizar a data escaneada
        def insert_or_update_value(column_name, value):
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

        def update_field_to_null(column_name):
            try:
                update_query = f"UPDATE lastscannedday SET {column_name} = NULL"
                cursor.execute(update_query)
                conn.commit()
            except psycopg2.Error as e:
                conn.rollback()
                print("Erro ao atualizar campo:", e)

        # Função para verificar se a URL já existe na tabela
        def url_exists(url):
            try:
                select_query = "SELECT EXISTS(SELECT 1 FROM linkstoscam WHERE url = %s)"
                cursor.execute(select_query, (url,))
                return cursor.fetchone()[0]
            except psycopg2.Error as e:
                print("Erro ao verificar a existência da URL na tabela:", e)

        # Função para inserir dados na tabela
        def insert_data(url, website):
            try:
                cursor = conn.cursor()
                insert_query = "INSERT INTO linkstoscam (url, website, scanned) VALUES (%s, %s, FALSE)"
                cursor.execute(insert_query, (url, website))
                conn.commit()
            except psycopg2.Error as e:
                conn.rollback()
                print("Erro ao inserir dados:", e)

        # Loop com informações referente ao websites a serem escavados
        websites = {
            'timeform': partOfLinkTimeform,
            'racingpost': partOfLinkRacingpost
        }

        for website_name, partOfLink in websites.items():
            scannedDayColumn = website_name + '_scannedday'

            # RACINGPOST SCRAP
            if website_name == 'racingpost':
                if not has_values(scannedDayColumn):
                        listOfLinks = getLinksAtScannedDayAtRacingpost(racingDate)
                        arrayOfLinks(listOfLinks)
                        insert_or_update_value(scannedDayColumn, racingDate)
                else:
                    racingDate = get_scanned_day(scannedDayColumn)
                    if not racingDate == limitOfDateRacingpost:
                        racingDate = goBackDay(racingDate)
                        listOfLinks = getLinksAtScannedDayAtRacingpost(racingDate)
                        arrayOfLinks(listOfLinks)
                        insert_or_update_value(scannedDayColumn, racingDate)
                    else:
                        update_field_to_null(scannedDayColumn)
                        sys.exit()
            # TIMEFORM SCRAP
            elif website_name == 'timeform':
                if not has_values(scannedDayColumn):
                    racingDate = getLastDay()
                    listOfLinks = getLinksAtScannedDayAtTimeform(racingDate)
                    arrayOfLinks(listOfLinks)
                    insert_or_update_value(scannedDayColumn, racingDate)
                else:
                    racingDate = get_scanned_day(scannedDayColumn)
                    if not racingDate == limitOfDateTimeform:
                        racingDate = goBackDay(racingDate)
                        listOfLinks = getLinksAtScannedDayAtTimeform(racingDate)
                        arrayOfLinks(listOfLinks)
                        insert_or_update_value(scannedDayColumn, racingDate)
                    else:
                        update_field_to_null(scannedDayColumn)
                        sys.exit()
            driver.quit()

        # Salvar alterações no banco de dados
        conn.commit()

# Fechar o cursor e a conexão
cursor.close()
conn.close()
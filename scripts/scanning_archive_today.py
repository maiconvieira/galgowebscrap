# Funções importadas para o funcionamento do script
from db import connect
import datetime
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
#                print(f"Tabela {table_name} criada com sucesso.")

        # Variaveis criados para o script
        partOfLinkTimeform = 'https://www.timeform.com/greyhound-racing/results/'
        partOfLinkRacingpost = 'https://greyhoundbet.racingpost.com/#results-list/r_date='
        scrapedPage = []
        racingDateToday = datetime.datetime.now().strftime('%Y-%m-%d')

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

            # TIMEFORM SCRAP
            if website_name == 'timeform':
                listOfLinks = getLinksAtScannedDayAtTimeform(racingDateToday)
                arrayOfLinks(listOfLinks)
            # RACINGPOST SCRAP
            elif website_name == 'racingpost':
                listOfLinks = getLinksAtScannedDayAtRacingpost(racingDateToday)
                arrayOfLinks(listOfLinks)

        # Salvar alterações no banco de dados
        conn.commit()

# Fechar o cursor e a conexão
cursor.close()
conn.close()
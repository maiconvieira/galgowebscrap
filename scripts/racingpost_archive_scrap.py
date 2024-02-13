from db import connect
from retrocederData import *
from management_bd import *
import sys

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

# Usar a função connect() para obter uma conexão com o banco de dados
conn = connect()
cursor = conn.cursor()

# Váriaveis de banco de dados 
table_lastscannedday = 'lastscannedday'
table_linkstoscam = 'linkstoscam'
column_lastscannedday = 'racingpost_scannedday'
website_scanned = 'racingpost'

# Verificar se as tabelas existem
if not table_exists(cursor, table_lastscannedday):
    # Criar a tabela table_lastscannedday se ela não existir
    create_table_query = """
        CREATE TABLE lastscannedday (
            id SERIAL PRIMARY KEY,
            timeform_scannedday VARCHAR(100),
            racingpost_scannedday VARCHAR(100)
        )
    """
    cursor.execute(create_table_query)
    conn.commit()

if not table_exists(cursor, table_linkstoscam):
    # Criar a tabela table_linkstoscam se ela não existir
    create_table_query = """
        CREATE TABLE IF NOT EXISTS table_linkstoscam (
            id SERIAL PRIMARY KEY,
            url VARCHAR(100),
            website VARCHAR(25),
            scanned BOOLEAN
        )
    """
    cursor.execute(create_table_query)
    conn.commit()

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

def arrayoflinks(listoflinks):
    if len(listoflinks) != 0:
        for i in listoflinks:
            hrefCaptured = i.get_attribute('href')
            # Verificar se a URL já existe na tabela
#            if not url_exists(conn, hrefCaptured):
                # Inserir a URL na tabela se não existir
#                insert_data(conn, hrefCaptured)
#            else:
#                print("A URL já existe na tabela. Ignorando a inserção.")
            print(hrefCaptured)

# Verificar se a tabela tem algum valor
if not has_values(cursor, table_lastscannedday, column_lastscannedday):
    # Chamar a função e salvar a data de ontem em uma variável
    racingDate = getlastday()
    listoflinks = getlinksatscannedday(racingDate)
    arrayoflinks(listoflinks)
    # Inserir valor se não houver nenhum
    insert_or_update_value(conn, cursor, table_lastscannedday, column_lastscannedday, racingDate)
else:
    # Chamar a função e salvar o valor retornado em uma variável
    racingDate = get_scanned_day(cursor, table_lastscannedday, column_lastscannedday)
    if racingDate == '2013-01-01':
        drop_table(conn, table_lastscannedday)
        sys.exit()
    else:
        racingDate = retrocederData(racingDate)
        listoflinks = getlinksatscannedday(racingDate)
        arrayoflinks(listoflinks)
    # Atualizar valor se houver algum
    insert_or_update_value(conn, cursor, table_lastscannedday, column_lastscannedday, racingDate)

# Salvar alterações no banco de dados
#conn.commit()

# Fechar o cursor e a conexão
#cursor.close()
#conn.close()

#dayscanned = '2024-02-10'

#driver.get(partoflink + dayscanned)
#driver.implicitly_wait(0.5)
#fullpage = driver.find_elements(By.XPATH, "//a[@class='waf-header hover-opacity']")
#partialHTML = driver.find_elements(By.CLASS_NAME, 'results-race-list-row')

#num = 0

#for i in partialHTML:
#    linksscanned = i.find_elements(By.TAG_NAME, 'a')

#for n in linksscanned:
#    hrefCaptured = n.get_attribute('href')
#    print(hrefCaptured)

#print(fullpage)
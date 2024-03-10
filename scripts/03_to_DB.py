# Funções importadas para o funcionamento do script
from db import connect
from tables import tables, table_exists
import psycopg2
import time
import logging
import requests
import requests_cache
from psycopg2 import sql
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

requests_cache.install_cache('race_cache', backend='sqlite', expire_after=3600)  # Cache expira após 1 hora
logging.basicConfig(level=logging.INFO)

options = Options()
options.add_argument('--headless')
options.add_argument('log-level=3')
options.add_argument('--disable-dev-shm-usage')
driver =  webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

# Funções Auxiliares:
def create_tables_if_not_exist():
    with connect() as conn:
        with conn.cursor() as cursor:
            for table_name, create_table_query in tables.items():
                if not table_exists(table_name):
                    cursor.execute(create_table_query)
                    conn.commit()

def contar_caracter(texto):
    contador = 1
    for caractere in texto:
        if caractere == ' ':
            break
        contador += 1
    return contador

def capitalize_words(sentence):
    words = sentence.split()
    capitalized_words = [word.capitalize() for word in words]
    return ' '.join(capitalized_words)

# Chama a função connect para obter uma conexão com o banco de dados
with connect() as conn:
    with conn.cursor() as cursor:
        # Verifica se as tabelas existem e, se não existirem, as cria
        for table_name, create_table_query in tables.items():
            if not table_exists(table_name):
                cursor.execute(create_table_query)
                conn.commit()                
    cursor.close()
conn.close()

start_time = time.time()

with connect() as conn:
    with conn.cursor() as cursor:    
        cursor.execute("SELECT url FROM linkstoscam WHERE website = 'timeform' AND scanned = true AND CAST(split_part(url, '/', 9) AS INTEGER) NOT IN (SELECT timeform_id FROM race)")
        results = cursor.fetchall()
        if results:
            update_query = """
            UPDATE linkstoscam SET scanned = false WHERE
            website = 'timeform' AND scanned = true AND CAST(split_part(url, '/', 9) AS INTEGER) NOT IN (SELECT timeform_id FROM race);
            """
            cursor.execute(update_query)
            conn.commit()
        cursor.execute("SELECT url, website, scanned FROM linkstoscam WHERE website = 'timeform' AND scanned = FALSE ORDER BY SUBSTRING(url FROM '[0-9]{4}-[0-9]{2}-[0-9]{2}')::DATE LIMIT 1")       
        result = cursor.fetchone()

        # Função para inserir dados na tabela
        def update_scanned(url):
            try:
                update_query = "UPDATE linkstoscam SET scanned = TRUE WHERE url = %s"
                cursor.execute(update_query, (url,))
                conn.commit()
            except psycopg2.Error as e:
                logging.error('Erro ao atualizar o status scanned: %s', e)

        def insert_data(name, website_id, url):
            try:
                # Verifica se a URL já existe na tabela
                select_query = "SELECT timeform_id FROM greyhoundlinkstoscam WHERE url = %s"
                cursor.execute(select_query, (url,))
                result = cursor.fetchone()
                if result is not None:
                    logging.error('URL já existe na tabela. Ignorando inserção.')
                    return
                
                # Insere os dados na tabela
                insert_query = "INSERT INTO greyhoundlinkstoscam (name, timeform_id, url, website, scanned) VALUES (%s, %s, %s, 'timeform', FALSE)"
                cursor.execute(insert_query, (name, website_id, url))
                conn.commit()
            except psycopg2.Error as e:
                conn.rollback()
                logging.error('Erro ao inserir dados: %s', e)

        def insert_or_get_id(table_name, column_name, value):
            try:
                select_query = sql.SQL("""
                    SELECT id FROM {}
                    WHERE {} = %s
                """).format(sql.Identifier(table_name), sql.Identifier(column_name))
                cursor.execute(select_query, (value,))
                result = cursor.fetchone()

                if result:
                    return result[0]

                insert_query = sql.SQL("""
                    INSERT INTO {} ({})
                    VALUES (%s)
                    RETURNING id
                """).format(sql.Identifier(table_name), sql.Identifier(column_name))
                cursor.execute(insert_query, (value,))
                conn.commit()
                return cursor.fetchone()[0]

            except psycopg2.Error as e:
                conn.rollback()
                logging.error(f'Erro ao inserir ou obter ID da tabela {table_name}: %s', e)
                return None
        
        def insert_race_if_not_exists(race_date, race_time, grade, distance, race_type, tf_going, going, prizes, forecast, tricast, race_comment, timeform_id, stadium_id):
            try:
                select_query = sql.SQL("""
                    SELECT id FROM race
                    WHERE race_date = %s AND race_time = %s AND grade = %s AND distance = %s AND race_type = %s AND tf_going = %s AND going = %s AND prize = %s 
                                        AND forecast = %s AND tricast = %s AND race_comment = %s AND timeform_id = %s AND stadium_id = %s
                """)
                cursor.execute(select_query, (race_date, race_time, grade, distance, race_type, tf_going, going, prizes, forecast, tricast, race_comment, timeform_id, stadium_id))
                existing_race_id = cursor.fetchone()

                if existing_race_id:
                    return existing_race_id[0]

                insert_query = sql.SQL("""
                    INSERT INTO race (race_date, race_time, grade, distance, race_type, tf_going, going, prize, forecast, tricast, race_comment, timeform_id, stadium_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """)
                cursor.execute(insert_query, (race_date, race_time, grade, distance, race_type, tf_going, going, prizes, forecast, tricast, race_comment, timeform_id, stadium_id))
                race_id = cursor.fetchone()[0]
                conn.commit()

                return race_id

            except Exception as e:
                # Trata qualquer exceção e imprime uma mensagem de erro
                print(f"Erro ao inserir corrida: {e}")
                conn.rollback()
                return None

        def insert_race_if_not_exists(race_date, race_time, grade, distance, race_type, tf_going, going, prizes, forecast, tricast, race_comment, timeform_id, stadium_id):
            try:
                select_query = sql.SQL("""
                    SELECT id FROM race
                    WHERE race_date = %s AND race_time = %s AND grade = %s AND distance = %s AND timeform_id = %s AND stadium_id = %s
                """)
                cursor.execute(select_query, (race_date, race_time, grade, distance, timeform_id, stadium_id))
                existing_race_id = cursor.fetchone()

                if existing_race_id:
                    return existing_race_id[0]

                insert_query = sql.SQL("""
                    INSERT INTO race (race_date, race_time, grade, distance, race_type, tf_going, going, prize, forecast, tricast, race_comment, timeform_id, stadium_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """)
                cursor.execute(insert_query, (race_date, race_time, grade, distance, race_type, tf_going, going, prizes, forecast, tricast, race_comment, timeform_id, stadium_id))
                race_id = cursor.fetchone()[0]
                conn.commit()

                return race_id

            except Exception as e:
                # Trata qualquer exceção e imprime uma mensagem de erro
                print(f"Erro ao inserir corrida: {e}")
                conn.rollback()
                return None

        def insert_or_get_greyhound_id(name, genre, timeform_id):
            try:
                # Verifica se já existe uma linha com os valores especificados
                select_query = sql.SQL("""
                    SELECT id FROM greyhound
                    WHERE name = %s AND genre = %s AND timeform_id = %s
                """)
                cursor.execute(select_query, (name, genre, timeform_id))
                result = cursor.fetchone()

                if result:
                    greyhound_id = result[0]
                    return greyhound_id

                # Se não existir, insere os dados na tabela
                insert_query = sql.SQL("""
                    INSERT INTO greyhound (name, genre, timeform_id)
                    VALUES (%s, %s, %s)
                    RETURNING id
                """)
                cursor.execute(insert_query, (name, genre, timeform_id))
                conn.commit()
                greyhound_id = cursor.fetchone()[0]
                return greyhound_id

            except psycopg2.Error as e:
                conn.rollback()
                print("Erro ao inserir ou obter greyhound:", e)
                return None

        def insert_or_get_race_result_id(position, bnt, trap, run_time, sectional, bend, remarks_acronym, isp, bsp, tfr, greyhound_id, race_id):
            try:
                # Verifica se já existe uma linha com os valores especificados
                select_query = sql.SQL("""
                    SELECT id FROM race_result
                    WHERE greyhound_id = %s AND race_id = %s
                """)
                cursor.execute(select_query, (greyhound_id, race_id))
                result = cursor.fetchone()

                if result:
                    race_result_id = result[0]
                    print("Esses dados já existem na tabela.")
                    return race_result_id

                # Se não existir, insere os dados na tabela
                insert_query = sql.SQL("""
                    INSERT INTO race_result (position, bnt, trap, run_time, sectional, bend, remarks_acronym, isp, bsp, tfr, greyhound_id, race_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """)
                cursor.execute(insert_query, (position, bnt, trap, run_time, sectional, bend, remarks_acronym, isp, bsp, tfr, greyhound_id, race_id))
                conn.commit()
                race_result_id = cursor.fetchone()[0]
                return race_result_id

            except psycopg2.Error as e:
                conn.rollback()
                print("Erro ao inserir ou obter resultado da corrida:", e)

        def insert_trainer_greyhound(trainer_id, greyhound_id):
            try:
                select_query = sql.SQL("""
                    SELECT 1 FROM trainer_greyhound
                    WHERE trainer_id = %s AND greyhound_id = %s
                """)
                cursor.execute(select_query, (trainer_id, greyhound_id))
                if cursor.fetchone():
                    print("Relacionamento já existe em trainer_greyhound.")
                    return

                insert_query = sql.SQL("""
                    INSERT INTO trainer_greyhound (trainer_id, greyhound_id)
                    VALUES (%s, %s)
                """)
                cursor.execute(insert_query, (trainer_id, greyhound_id))
                conn.commit()
                print("Relacionamento inserido com sucesso em trainer_greyhound.")

            except psycopg2.Error as e:
                conn.rollback()
                print("Erro ao inserir relacionamento em trainer_greyhound:", e)

        # Verificar se encontrou um resultado e imprimir os valores
        if result:
            url, website, scanned = result
            partsOfRaceURL = url.split('/')
            race_date = partsOfRaceURL[7]
            timeform_id = partsOfRaceURL[8]
            response = requests.get(url)
            driver.get(url)
            driver.implicitly_wait(0.5)

            body_element = driver.find_element(By.TAG_NAME, 'body')
            if not body_element.text == 'For data, please visit https://www.globalsportsapi.com/':
                sectionHTML = driver.find_element(By.XPATH, '/html/body/main/section[2]')
                try:
                    trValue = sectionHTML.find_element(By.XPATH, 'section[2]/table/tbody')
                except NoSuchElementException:
                    print('Elemento: trValue não localizado')
                    trValue = None
                try:
                    raceH1 = driver.find_element(By.XPATH, '//h1[@class="w-header"]').text
                    numOfSpace = contar_caracter(raceH1)
                    race_time = raceH1[0:numOfSpace].strip()
                    stadium = capitalize_words(raceH1[numOfSpace:].strip())
                    stadium_id = insert_or_get_id('stadium', 'name', stadium)
                except NoSuchElementException:
                    print('Elemento: raceH1 não localizado')
                    raceH1 = None

                # Variáveis raspadas do website referente a corrida
                try:
                    grade = sectionHTML.find_element(By.XPATH, 'section[1]/div[2]/div[3]/b[1]').text
                    grade = grade[1:-1]
                except NoSuchElementException:
                    print('Elemento: grade não localizado')
                    grade = None
                try:
                    distance = sectionHTML.find_element(By.XPATH, 'section[1]/div[2]/div[3]/b[2]').text
                    distance = distance[0:-1]
                except NoSuchElementException:
                    print('Elemento: distance não localizado')
                    distance = None
                try:
                    race_type = sectionHTML.find_element(By.XPATH, 'section[1]/div[2]/div[3]/b[3]').text
                except NoSuchElementException:
                    print('Elemento: racing não localizado')
                    race_type = None
                try:
                    tf_going = sectionHTML.find_element(By.XPATH, 'section[1]/div[2]/div[5]/b[1]').text
                except NoSuchElementException:
                    print('Elemento: tfGoing não localizado')
                    tf_going = None
                try:
                    going = sectionHTML.find_element(By.XPATH, 'section[1]/div[2]/div[5]/b[2]').text
                except NoSuchElementException:
                    print('Elemento: going não localizado')
                    going = None
                try:
                    prizes = sectionHTML.find_element(By.XPATH, 'section[1]/div[2]/div[4]/b[2]').text
                except NoSuchElementException:
                    print('Elemento: prizeTotal não localizado')
                    prizes = None
                try:
                    forecast = sectionHTML.find_element(By.XPATH, 'section[1]/div[2]/div[6]/b[1]').text
                except NoSuchElementException:
                    print('Elemento: forecast não localizado')
                    forecast = None
                try:
                    tricast = sectionHTML.find_element(By.XPATH, 'section[1]/div[2]/div[6]/b[2]').text
                except NoSuchElementException:
                    print('Elemento: tricast não localizado')
                    tricast = None
                try:
                    race_comment = sectionHTML.find_element(By.XPATH, 'section[3]/div').text
                except NoSuchElementException:
                    print('Elemento: raceComments não localizado')
                    race_comment = None

                race_id = insert_race_if_not_exists(race_date, race_time, grade, distance, race_type, tf_going, going, prizes, forecast, tricast, race_comment, timeform_id, stadium_id)

                listaGeral = []
                # Variáveis raspadas do website referente ao resultado da corrida
                try:
                    positions = trValue.find_elements(By.XPATH, '//td[@class="rrb-pos al-center"]/span')
                    positionList = []
                    if positions:
                        for position in positions:
                            position = position.text[0:1]
                            positionList.append(position)
                    else:
                        print("Nenhum elemento encontrado com o XPath especificado")
                except NoSuchElementException:
                    print('Elemento: positions não localizado')
                    positions = None
                try:
                    btns = trValue.find_elements(By.XPATH, '//td[@class="rrb-hide-1 al-center"]')
                    btnList = []
                    if btns:
                        for btn in btns:
                            btnList.append(btn.text)
                        del(btnList[0])
                    else:
                        print("Nenhum elemento encontrado com o XPath especificado")
                except NoSuchElementException:
                    print('Elemento: btns não localizado')
                    btns = None
                try:
                    imgs = trValue.find_elements(By.XPATH, '//td/img')
                    imgList = []
                    if imgs:
                        for img in imgs:
                            imgList.append(img.get_attribute('alt'))
                    else:
                        print("Nenhum elemento encontrado com o XPath especificado")
                except NoSuchElementException:
                    print('Elemento: imgs não localizado')
                    imgs = None
                try:
                    greyhoundNames = trValue.find_elements(By.XPATH, '//td/a')
                    greyhoundNameList = []
                    if greyhoundNames:
                        for greyhoundName in greyhoundNames:
                            greyhoundNameList.append(capitalize_words(greyhoundName.text.strip()))
                    else:
                        print("Nenhum elemento encontrado com o XPath especificado")
                except NoSuchElementException:
                    print('Elemento: greyhoundNames não localizado')
                    greyhoundNames = None
                try:
                    greyhoundURLs = trValue.find_elements(By.XPATH, '//td/a')
                    greyhoundURLList = []
                    idTimeformList = []
                    if greyhoundURLs:
                        for greyhoundURL in greyhoundURLs:
                            greyhoundURL = greyhoundURL.get_attribute('href')
                            greyhoundURLList.append(greyhoundURL)
                            partsOfGreyhoundURL = greyhoundURL.split('/')
                            id_timeform = partsOfGreyhoundURL[6]
                            idTimeformList.append(id_timeform)
                    else:
                        print("Nenhum elemento encontrado com o XPath especificado")
                except NoSuchElementException:
                    print('Elemento: greyhoundURLs não localizado')
                    greyhoundURLs = None
                try:
                    genres = trValue.find_elements(By.XPATH, '//td[@title="The age and sex of the greyhound"]')
                    genreList = []
                    if genres:
                        for genre in genres:
                            genre = genre.text
                            genre = genre[-1:]
                            genreList.append(genre)
                        del(genreList[0])
                    else:
                        print("Nenhum elemento encontrado com o XPath especificado")
                except NoSuchElementException:
                    print('Elemento: genres não localizado')
                    genres = None
                try:
                    bends = trValue.find_elements(By.XPATH, '//td[@class="rrb-hide-1"]/span')
                    bendList = []
                    if bends:
                        for bend in bends:
                            bendList.append(bend.text)
                    else:
                        print("Nenhum elemento encontrado com o XPath especificado")
                except NoSuchElementException:
                    print('Elemento: bends não localizado')
                    bends = None
                try:
                    remarks = trValue.find_elements(By.XPATH, '//td[@class="rrb-hide-1 rowspan-two"]/span')
                    remarkList = []
                    if remarks:
                        for remark in remarks:
                            remarkList.append(remark.text)
                    else:
                        print("Nenhum elemento encontrado com o XPath especificado")
                except NoSuchElementException:
                    print('Elemento: remarks não localizado')
                    remarks = None
                try:
                    isps = trValue.find_elements(By.XPATH, '//td/span[@title="The official starting price of the greyhound in this race"]')
                    ispList = []
                    if isps:
                        for isp in isps:
                            ispList.append(isp.text)
                    else:
                        print("Nenhum elemento encontrado com o XPath especificado")
                except NoSuchElementException:
                    print('Elemento: isps não localizado')
                    isps = None
                try:
                    tfrs = trValue.find_elements(By.XPATH, '//td[@class="al-center rowspan-two"]/span')
                    tfrList = []
                    if tfrs:
                        for tfr in tfrs:
                            tfrList.append(tfr.text)
                    else:
                        print("Nenhum elemento encontrado com o XPath especificado")
                except NoSuchElementException:
                    print('Elemento: tfrs não localizado')
                    tfrs = None
                try:
                    runTimes = trValue.find_elements(By.XPATH, '//td/span[@title="The official run time of the greyhound in this race" or @title="The official run time of the greyhound in this race (official sectional)"]')
                    runTimeList = []
                    sectionalList = []
                    if runTimes:
                        for runTime in runTimes:
                            runTime = runTime.text
                            runTimeSplit = runTime.split('(')
                            if len(runTimeSplit) == 2:
                                runTimeList.append(runTimeSplit[0][0:5])
                                sectionalList.append(runTimeSplit[1][0:-1])
                            else:
                                runTimeList.append(runTime)
                                sectionalList.append(None)
                    else:
                        print("Nenhum elemento encontrado com o XPath especificado")
                except NoSuchElementException:
                    print('Elemento: runTimes não localizado')
                    runTimes = None
                try:
                    trainers = trValue.find_elements(By.XPATH, '//td/span[@title="The full name of the greyhound\'s trainer"]')
                    trainerList = []
                    if trainers:
                        for trainer in trainers:
                            trainerList.append(trainer.text.strip())
                    else:
                        print("Nenhum elemento encontrado com o XPath especificado")
                except NoSuchElementException:
                    print('Elemento: trainers não localizado')
                    trainers = None
                try:
                    bsps = trValue.find_elements(By.XPATH, '//td/span[@title="The Betfair starting price of the greyhound in this race"]')
                    bspList = []
                    if bsps:
                        for bsp in bsps:
                            bspList.append(bsp.text)
                    else:
                        print("Nenhum elemento encontrado com o XPath especificado")
                except NoSuchElementException:
                    print('Elemento: bsps não localizado')
                    bsps = None

                listaGeral.append(positionList)
                listaGeral.append(btnList)
                listaGeral.append(imgList)
                listaGeral.append(runTimeList)
                listaGeral.append(sectionalList)
                listaGeral.append(bendList)
                listaGeral.append(remarkList)
                listaGeral.append(ispList)
                listaGeral.append(bspList)
                listaGeral.append(tfrList)
                listaGeral.append(genreList)
                listaGeral.append(trainerList)
                listaGeral.append(greyhoundURLList)
                listaGeral.append(idTimeformList)
                listaGeral.append(greyhoundNameList)

                elementos_sequenciais = []
                for i in range(len(listaGeral[0])):
                    elementos_sequenciais.append([sublista[i] for sublista in listaGeral])
                for elemento in elementos_sequenciais:
                    try:
                        insert_data(elemento[14], elemento[13], elemento[12])
                    except psycopg2.IntegrityError as e:
                        print("A URL já existe na tabela:", e)
                    trainer_id = insert_or_get_id('trainer', 'name', elemento[11])
                    greyhound_id = insert_or_get_greyhound_id(elemento[14], elemento[10], elemento[13])
                    insert_or_get_race_result_id(elemento[0], elemento[1], elemento[2], elemento[3], elemento[4], elemento[5], elemento[6], elemento[7], elemento[8], elemento[9], greyhound_id, race_id)
                    insert_trainer_greyhound(trainer_id, greyhound_id)
            else:
                print('For data, please visit https://www.globalsportsapi.com/')

        driver.quit()
        update_scanned(url)

    cursor.close()
conn.close()

end_time = time.time()
execution_time = end_time - start_time
logging.info(f"Tempo de execução: {execution_time} segundos")
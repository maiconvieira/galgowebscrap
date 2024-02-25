# Funções importadas para o funcionamento do script
from db import connect
from tables import tables, connect, table_exists
import psycopg2
import time
from psycopg2 import sql
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# Captura o tempo de início
start_time = time.time()
###########################

# Configurações do Selenium
options = Options()
options.add_argument('--headless')
options.add_argument('log-level=3') # INFO = 0 / WARNING = 1 / LOG_ERROR = 2 / LOG_FATAL = 3
options.add_argument('--disable-dev-shm-usage')

# Funções Auxiliares:
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


def loopScam():
    with connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT url, website, scanned FROM linkstoscam WHERE website = 'timeform' AND scanned = FALSE ORDER BY SUBSTRING(url FROM '[0-9]{4}-[0-9]{2}-[0-9]{2}')::DATE LIMIT 1")       
            result = cursor.fetchone()

            # Função para inserir dados na tabela
            def update_scanned(url):
                try:
                    # Verifica se a URL existe e atualiza o campo scanned para True
                    update_query = "UPDATE linkstoscam SET scanned = TRUE WHERE url = %s"
                    cursor.execute(update_query, (url,))
                    conn.commit()
                except psycopg2.Error as e:
                    print("Erro ao atualizar o status scanned:", e)

            def insert_data(name, website_id, url):
                try:
                    # Verifica se a URL já existe na tabela
                    select_query = "SELECT id_timeform FROM greyhoundlinkstoscam WHERE url = %s"
                    cursor.execute(select_query, (url,))
                    result = cursor.fetchone()
                    if result is not None:
#                        print("URL já existe na tabela. Ignorando inserção.")
                        return
                    
                    # Insere os dados na tabela
                    insert_query = "INSERT INTO greyhoundlinkstoscam (name, id_timeform, url, website, scanned) VALUES (%s, %s, %s, 'timeform', FALSE)"
                    cursor.execute(insert_query, (name, website_id, url))
                    conn.commit()
                except psycopg2.Error as e:
                    conn.rollback()
                    print("Erro ao inserir dados:", e)

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
                    print(f"Erro ao inserir ou obter ID da tabela {table_name}: {e}")
                    return None
            
            def insert_race_if_not_exists(date_race, time_race, grade, distance, racing_type, tf_going, going, prizes, forecast, tricast, race_comment, id_timeform, id_stadium):
                try:
                    # Verifica se já existe uma linha com os valores especificados
                    select_query = sql.SQL("""
                        SELECT id FROM race
                        WHERE date_race = %s AND time_race = %s AND id_stadium = %s
                    """)
                    cursor.execute(select_query, (date_race, time_race, id_stadium))
                    existing_race_id = cursor.fetchone()

                    if existing_race_id:
                        # Retorna o ID da corrida existente
                        return existing_race_id[0]

                    # Se não existir, insere os dados na tabela
                    insert_query = sql.SQL("""
                        INSERT INTO race (date_race, time_race, grade, distance, racing_type, tf_going, going, prize_total, forecast, tricast, race_comment, id_timeform, id_stadium)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING id
                    """)
                    cursor.execute(insert_query, (date_race, time_race, grade, distance, racing_type, tf_going, going, prizes, forecast, tricast, race_comment, id_timeform, id_stadium))
                    race_id = cursor.fetchone()[0]
                    conn.commit()

                    return race_id

                except Exception as e:
                    # Trata qualquer exceção e imprime uma mensagem de erro
                    print(f"Erro ao inserir corrida: {e}")
                    conn.rollback()  # Desfaz qualquer mudança no banco de dados
                    return None

            def insert_or_get_greyhound_id(name, sex, id_timeform, id_trainer):
                try:
                    # Verifica se já existe uma linha com os valores especificados
                    select_query = sql.SQL("""
                        SELECT id FROM greyhound
                        WHERE name = %s AND sex = %s AND id_timeform = %s AND id_trainer = %s
                    """)
                    cursor.execute(select_query, (name, sex, id_timeform, id_trainer))
                    result = cursor.fetchone()

                    if result:
                        greyhound_id = result[0]
                        return greyhound_id

                    # Se não existir, insere os dados na tabela
                    insert_query = sql.SQL("""
                        INSERT INTO greyhound (name, sex, id_timeform, id_trainer)
                        VALUES (%s, %s, %s, %s)
                        RETURNING id
                    """)
                    cursor.execute(insert_query, (name, sex, id_timeform, id_trainer))
                    conn.commit()
                    greyhound_id = cursor.fetchone()[0]
                    return greyhound_id

                except psycopg2.Error as e:
                    conn.rollback()
                    print("Erro ao inserir ou obter greyhound:", e)
                    return None

            def insert_or_get_race_result_id(position, bnt, trap, id_greyhound, run_time, sectional, bend, remarks_acronym, start_price, betfair_price, tf_rating, id_race, id_trainer):
                try:
                    # Verifica se já existe uma linha com os valores especificados
                    select_query = sql.SQL("""
                        SELECT id FROM race_result
                        WHERE id_greyhound = %s AND id_race = %s
                    """)
                    cursor.execute(select_query, (id_greyhound, id_race))
                    result = cursor.fetchone()

                    if result:
                        race_result_id = result[0]
#                        print("Esses dados já existem na tabela.")
                        return race_result_id

                    # Se não existir, insere os dados na tabela
                    insert_query = sql.SQL("""
                        INSERT INTO race_result (position, bnt, trap, id_greyhound, run_time, sectional, bend, remarks_acronym, start_price, betfair_price, tf_rating, id_race, id_trainer)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING id
                    """)
                    cursor.execute(insert_query, (position, bnt, trap, id_greyhound, run_time, sectional, bend, remarks_acronym, start_price, betfair_price, tf_rating, id_race, id_trainer))
                    conn.commit()
                    race_result_id = cursor.fetchone()[0]
                    return race_result_id

                except psycopg2.Error as e:
                    conn.rollback()
                    print("Erro ao inserir ou obter resultado da corrida:", e)

            def insert_race_result_trainer(id_race_result, id_trainer):
                try:
                    # Verifica se o relacionamento já existe
                    select_query = sql.SQL("""
                        SELECT 1 FROM race_result_trainer
                        WHERE id_race_result = %s AND id_trainer = %s
                    """)
                    cursor.execute(select_query, (id_race_result, id_trainer))
                    if cursor.fetchone():
 #                       print("Relacionamento já existe em race_result_trainer.")
                        return

                    # Se não existir, insere o relacionamento
                    insert_query = sql.SQL("""
                        INSERT INTO race_result_trainer (id_race_result, id_trainer)
                        VALUES (%s, %s)
                    """)
                    cursor.execute(insert_query, (id_race_result, id_trainer))
                    conn.commit()
#                    print("Relacionamento inserido com sucesso em race_result_trainer.")

                except psycopg2.Error as e:
                    conn.rollback()
                    print("Erro ao inserir relacionamento em race_result_trainer:", e)

            def insert_race_result_greyhound(id_race_result, id_greyhound):
                try:
                    # Verifica se o relacionamento já existe
                    select_query = sql.SQL("""
                        SELECT 1 FROM race_result_greyhound
                        WHERE id_race_result = %s AND id_greyhound = %s
                    """)
                    cursor.execute(select_query, (id_race_result, id_greyhound))
                    if cursor.fetchone():
#                        print("Relacionamento já existe em race_result_greyhound.")
                        return

                    # Se não existir, insere o relacionamento
                    insert_query = sql.SQL("""
                        INSERT INTO race_result_greyhound (id_race_result, id_greyhound)
                        VALUES (%s, %s)
                    """)
                    cursor.execute(insert_query, (id_race_result, id_greyhound))
                    conn.commit()
#                    print("Relacionamento inserido com sucesso em race_result_greyhound.")

                except psycopg2.Error as e:
                    conn.rollback()
                    print("Erro ao inserir relacionamento em race_result_greyhound:", e)

            # Verificar se encontrou um resultado e imprimir os valores
            if result:
                url, website, scanned = result
                partsOfRaceURL = url.split('/')
                dateRace = partsOfRaceURL[7]
                tfRace_id = partsOfRaceURL[8]
                driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
                driver.get(url)

                print(f'{url}')

                body_element = driver.find_element(By.TAG_NAME, 'body')
                if not body_element.text == 'For data, please visit https://www.globalsportsapi.com/':

                    try:
                        sectionHTML = driver.find_element(By.XPATH, '/html/body/main/section[2]')
                    except NoSuchElementException:
                        print('Elemento: sectionHTML não localizado')
                        sectionHTML = None
                    try:
                        trValue = sectionHTML.find_element(By.XPATH, 'section[2]/table/tbody')
                    except NoSuchElementException:
                        print('Elemento: trValue não localizado')
                        trValue = None
                    try:
                        raceH1 = driver.find_element(By.XPATH, '//h1[@class="w-header"]').text
                        numOfSpace = contar_caracter(raceH1)
                        raceTime = raceH1[0:numOfSpace].strip()
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
                        racing = sectionHTML.find_element(By.XPATH, 'section[1]/div[2]/div[3]/b[3]').text
                    except NoSuchElementException:
                        print('Elemento: racing não localizado')
                        racing = None
                    try:
                        tfGoing = sectionHTML.find_element(By.XPATH, 'section[1]/div[2]/div[5]/b[1]').text
                    except NoSuchElementException:
                        print('Elemento: tfGoing não localizado')
                        tfGoing = None
                    try:
                        going = sectionHTML.find_element(By.XPATH, 'section[1]/div[2]/div[5]/b[2]').text
                    except NoSuchElementException:
                        print('Elemento: going não localizado')
                        going = None
                    try:
                        prizeTotal = sectionHTML.find_element(By.XPATH, 'section[1]/div[2]/div[4]/b[2]').text
                    except NoSuchElementException:
                        print('Elemento: prizeTotal não localizado')
                        prizeTotal = None
                    try:
                        forecast = sectionHTML.find_element(By.XPATH, 'section[1]/div[2]/div[6]/b[1]').text
                    except NoSuchElementException:
                        print('Elemento: forecast não localizado')
                        tricast = None
                    try:
                        tricast = sectionHTML.find_element(By.XPATH, 'section[1]/div[2]/div[6]/b[2]').text
                    except NoSuchElementException:
                        print('Elemento: tricast não localizado')
                        tricast = None
                    try:
                        raceComments = sectionHTML.find_element(By.XPATH, 'section[3]/div').text
                    except NoSuchElementException:
                        print('Elemento: raceComments não localizado')
                        raceComments = None

                    raceId = insert_race_if_not_exists(dateRace, raceTime, grade, distance, racing, tfGoing, going, prizeTotal, forecast, tricast, raceComments, tfRace_id, stadium_id)

                    listaGeral = []
                    # Variáveis raspadas do website referente ao resultado da corrida
                    try:
                        positions = trValue.find_elements(By.XPATH, '//td[@class="rrb-pos al-center"]/span')
                        positionList = []
                        if positions:
                            for position in positions:
                                positionList.append(position.text)
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
                    listaGeral.append(greyhoundNameList)
                    listaGeral.append(greyhoundURLList)
                    listaGeral.append(idTimeformList)
                    listaGeral.append(genreList)
                    listaGeral.append(bendList)
                    listaGeral.append(remarkList)
                    listaGeral.append(ispList)
                    listaGeral.append(tfrList)
                    listaGeral.append(runTimeList)
                    listaGeral.append(sectionalList)
                    listaGeral.append(trainerList)
                    listaGeral.append(bspList)

                    elementos_sequenciais = []
                    for i in range(len(listaGeral[0])):
                        elementos_sequenciais.append([sublista[i] for sublista in listaGeral])
                    for elemento in elementos_sequenciais:
                        # Verificar se a URL existe antes de inseri-la
                        try:
                            # greyhoundName = elemento[3], id_timeform = elemento[5] e greyhoundURL = elemento[4]
                            insert_data(elemento[3], elemento[5], elemento[4])
                        except psycopg2.IntegrityError as e:
                            print("A URL já existe na tabela:", e)
                    # trainer = elemento[13]
                        trainer_id = insert_or_get_id('trainer', 'name', elemento[13])
                        # greyhoundName = elemento[3], genre = elemento[6] e id_timeform = elemento[5]
                        greyhound_id = insert_or_get_greyhound_id(elemento[3], elemento[6], elemento[5], trainer_id)
                        # position = elemento[0], bnt = elemento[1], trap = elemento[2], bend = elemento[7], remarks_acronym = elemento[8], 
                        # start_price = elemento[9], tf_rating = elemento[10], run_time = elemento[11], sectional = elemento[12], betfair_price = elemento[14]
                        raceResult_id = insert_or_get_race_result_id(elemento[0], elemento[1], elemento[2], greyhound_id, elemento[11], elemento[12], elemento[7], elemento[8], elemento[9], elemento[10], elemento[11], raceId, trainer_id)

                        insert_race_result_greyhound(raceResult_id, greyhound_id)
                        insert_race_result_trainer(raceResult_id, trainer_id)
                else:
                    print('For data, please visit https://www.globalsportsapi.com/')

            driver.quit()

            # Atualiza o estados do link para escaneado igual True
            update_scanned(url)

    # Fechar o cursor e a conexão com o banco de dados
        cursor.close()
    conn.close()

x = 0
for _ in range(20):
    x += 1
    loopScam()
    print(f'{x} - OK!')

# Captura o tempo de fim
end_time = time.time()

# Calcula o tempo total de execução
execution_time = end_time - start_time

print(f"Tempo de execução: {execution_time} segundos")
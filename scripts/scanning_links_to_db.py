# Funções importadas para o funcionamento do script
from db import connect
#import datetime
#import sys
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
        # Consultar a primeira linha da tabela linkstoscam onde website é igual a 'timeform' e scanned é igual a False
        #cursor.execute("SELECT url, website, scanned FROM linkstoscam WHERE scanned = FALSE LIMIT 1")
        cursor.execute("SELECT url, website, scanned FROM linkstoscam WHERE website = 'timeform' AND scanned = FALSE LIMIT 1")
        #cursor.execute("SELECT url, website, scanned FROM linkstoscam WHERE (website = 'timeform' OR website = 'racingpost') AND scanned = FALSE LIMIT 1")        
        result = cursor.fetchone()

        # Verificar se tabelas existem e criar no banco de dados
        def table_exists(table_name):
            exists_query = sql.SQL("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = %s)")
            cursor.execute(exists_query, (table_name,))
            return cursor.fetchone()[0]

        # Dicionário contendo o nome das tabelas e suas definições
        tables = {
            'stadium': """
                CREATE TABLE IF NOT EXISTS stadium (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(25) NOT NULL UNIQUE,
                    url VARCHAR,
                    address VARCHAR(100),
                    email VARCHAR,
                    location VARCHAR
                )
            """,
            'trainer': """
                CREATE TABLE IF NOT EXISTS trainer (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(25) NOT NULL UNIQUE
                )
            """,
            'greyhound': """
                CREATE TABLE IF NOT EXISTS greyhound (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(60) NOT NULL,
                    born_date VARCHAR(12),
                    sex VARCHAR(10),
                    colour VARCHAR(15),
                    dam VARCHAR(20),
                    sire VARCHAR(20),
                    owner VARCHAR,
                    id_timeform INT,
                    id_racingpost INT,
                    id_trainer INT,
                    CONSTRAINT fk_trainer FOREIGN KEY (id_trainer) REFERENCES trainer (id)
                )
            """,
            'race': """
                CREATE TABLE IF NOT EXISTS race (
                    id SERIAL PRIMARY KEY,
                    date_race VARCHAR(10),
                    time_race VARCHAR(5),
                    grade VARCHAR(5),
                    distance INT,
                    racing_type VARCHAR,
                    tf_going NUMERIC,
                    going NUMERIC,
                    prize_total VARCHAR(6),
                    forecast VARCHAR,
                    tricast VARCHAR,
                    id_timeform INT,
                    id_racingpost INT,
                    race_comment VARCHAR,
                    race_comment_ptbr VARCHAR,
                    id_stadium INT NOT NULL,
                    CONSTRAINT fk_stadium FOREIGN KEY (id_stadium) REFERENCES stadium (id)
                )
            """,
            'race_result': """
                CREATE TABLE IF NOT EXISTS race_result (
                    id SERIAL PRIMARY KEY,
                    position VARCHAR(3),
                    bnt VARCHAR(5),
                    trap INT,
                    id_greyhound INT NOT NULL,
                    run_time NUMERIC,
                    sectional NUMERIC,
                    bend VARCHAR,
                    remarks_acronym VARCHAR,
                    remarks VARCHAR,
                    start_price VARCHAR,
                    betfair_price NUMERIC,
                    tf_rating INT,
                    id_race INT NOT NULL,
                    id_trainer INT,
                    CONSTRAINT fk_greyhound FOREIGN KEY (id_greyhound) REFERENCES greyhound (id),
                    CONSTRAINT fk_race FOREIGN KEY (id_race) REFERENCES race (id),
                    CONSTRAINT fk_trainer_race_result FOREIGN KEY (id_trainer) REFERENCES trainer (id)
                )
            """,
            'race_result_trainer': """
                CREATE TABLE IF NOT EXISTS race_result_trainer (
                    id_race_result INT NOT NULL,
                    id_trainer INT NOT NULL,
                    PRIMARY KEY (id_race_result, id_trainer),
                    CONSTRAINT fk_race_result FOREIGN KEY (id_race_result) REFERENCES race_result (id),
                    CONSTRAINT fk_trainer FOREIGN KEY (id_trainer) REFERENCES trainer (id)
                )
            """,
            'race_result_greyhound': """
                CREATE TABLE IF NOT EXISTS race_result_greyhound (
                    id_race_result INT NOT NULL,
                    id_greyhound INT NOT NULL,
                    PRIMARY KEY (id_race_result, id_greyhound),
                    CONSTRAINT fk_race_result FOREIGN KEY (id_race_result) REFERENCES race_result (id),
                    CONSTRAINT fk_greyhound FOREIGN KEY (id_greyhound) REFERENCES greyhound (id)
                )
            """,
            'greyhoundlinkstoscam': """
                CREATE TABLE IF NOT EXISTS greyhoundlinkstoscam (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR,
                    id_timeform int UNIQUE,
                    id_racingpost int UNIQUE,
                    url VARCHAR NOT NULL UNIQUE,
                    website VARCHAR(25),
                    scanned BOOLEAN
                )
            """
        }
        
        # Função para inserir dados na tabela
        def url_exists(url):
            try:
                select_query = "SELECT EXISTS(SELECT 1 FROM greyhoundlinkstoscam WHERE url = %s)"
                cursor.execute(select_query, (url,))
                return cursor.fetchone()[0]
            except psycopg2.Error as e:
                print("Erro ao verificar a existência da URL na tabela:", e)

        def insert_data(name, website_id, url):
            try:
                cursor = conn.cursor()
                insert_query = "INSERT INTO greyhoundlinkstoscam (name, id_timeform, url, website, scanned) VALUES (%s, %s, %s, 'timeform', FALSE)"
                cursor.execute(insert_query, (name, website_id, url))
                conn.commit()
            except psycopg2.Error as e:
                conn.rollback()
                print("Erro ao inserir dados:", e)

        # Verificar se as tabelas existem se não existir criar
        for table_name, create_table_query in tables.items():
            if not table_exists(table_name):
                cursor.execute(create_table_query)
                conn.commit()

        def update_scanned_status(url):
            try:
                # Verifica se a URL existe e atualiza o campo scanned para True
                update_query = "UPDATE linkstoscam SET scanned = TRUE WHERE url = %s"
                cursor.execute(update_query, (url,))
                conn.commit()
            except psycopg2.Error as e:
                print("Erro ao atualizar o status scanned:", e)

        # Funções criadas para o script
        def check_and_create_stadium(name):
            cursor.execute("SELECT id FROM stadium WHERE name = %s", (name,))
            result = cursor.fetchone()
            if result:
                return
            else:
                insert_query = """
                    INSERT INTO stadium (name) 
                    VALUES (%s)
                """
                cursor.execute(insert_query, (name,))
                conn.commit()

        def get_id_from_stadium(name):
            cursor.execute("SELECT id FROM stadium WHERE name = %s", (name,))
            result = cursor.fetchone()
            if result:
                return result[0]
            else:
                return None
        
        def check_and_create_trainer(name):
            cursor.execute("SELECT id FROM trainer WHERE name = %s", (name,))
            result = cursor.fetchone()
            if result:
                return
            else:
                insert_query = """
                    INSERT INTO trainer (name) 
                    VALUES (%s)
                """
                cursor.execute(insert_query, (name,))
                conn.commit()

        def get_id_from_trainer(name):
            cursor.execute("SELECT id FROM trainer WHERE name = %s", (name,))
            result = cursor.fetchone()
            if result:
                return result[0]
            else:
                return None

        def insert_race_if_not_exists(date_race, time_race, grade, distance, racing_type, tf_going, going, prizes, forecast, tricast, id_timeform, id_stadium):
            try:
                # Verifica se já existe uma linha com os valores especificados
                select_query = sql.SQL("""
                    SELECT id FROM race
                    WHERE date_race = %s AND time_race = %s AND id_stadium = %s
                """)
                cursor.execute(select_query, (date_race, time_race, id_stadium))
                result = cursor.fetchone()
                if result is not None:
                    # Retorna o ID da corrida existente
                    race_id = result[0]
                    return race_id
                    
                # Se não existir, insere os dados na tabela
                insert_query = sql.SQL("""
                    INSERT INTO race (date_race, time_race, grade, distance, racing_type, tf_going, going, prize_total, forecast, tricast, id_timeform, id_stadium)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """)
                cursor.execute(insert_query, (date_race, time_race, grade, distance, racing_type, tf_going, going, prizes, forecast, tricast, id_timeform, id_stadium))
                race_id = cursor.fetchone()[0]
                conn.commit()

                return race_id

            except psycopg2.Error as e:
                conn.rollback()
                print("Erro ao inserir corrida:", e)

        def update_race_comment(race_id, race_comment):
            try:
                # Seleciona o race_comment atual da corrida
                select_query = sql.SQL("""
                    SELECT race_comment FROM race
                    WHERE id = %s
                """)
                cursor.execute(select_query, (race_id,))
                result = cursor.fetchone()

                if not result or not result[0]:  # Verifica se o race_comment está vazio
                    # Atualiza o campo race_comment na linha com o ID especificado
                    update_query = sql.SQL("""
                        UPDATE race
                        SET race_comment = %s
                        WHERE id = %s
                    """)
                    cursor.execute(update_query, (race_comment, race_id))
                    conn.commit()

            except psycopg2.Error as e:
                conn.rollback()
                print("Erro ao atualizar comentário da corrida:", e)

        def insert_or_get_greyhound_id(name, sex, id_timeform):
            try:
                # Verifica se já existe uma linha com os valores especificados
                select_query = sql.SQL("""
                    SELECT id FROM greyhound
                    WHERE name = %s AND sex = %s AND id_timeform = %s
                """)
                cursor.execute(select_query, (name, sex, id_timeform))
                result = cursor.fetchone()

                if result:
                    greyhound_id = result[0]
                    return greyhound_id

                # Se não existir, insere os dados na tabela
                insert_query = sql.SQL("""
                    INSERT INTO greyhound (name, sex, id_timeform)
                    VALUES (%s, %s, %s)
                    RETURNING id
                """)
                cursor.execute(insert_query, (name, sex, id_timeform))
                conn.commit()
                greyhound_id = cursor.fetchone()[0]
                return greyhound_id

            except psycopg2.Error as e:
                conn.rollback()
                print("Erro ao inserir ou obter greyhound:", e)
                return None
            
        def update_trainer_id_for_greyhound(greyhound_id, trainer_id):
            try:
                # Verifica se já existe uma linha com greyhound_id e trainer_id
                select_query = sql.SQL("""
                    SELECT id FROM greyhound
                    WHERE id = %s AND id_trainer = %s
                """)
                cursor.execute(select_query, (greyhound_id, trainer_id))
                result = cursor.fetchone()

                if result:
                    return

                # Verifica se existe uma linha com greyhound_id sem trainer_id
                select_query = sql.SQL("""
                    SELECT id FROM greyhound
                    WHERE id = %s AND id_trainer IS NULL
                """)
                cursor.execute(select_query, (greyhound_id,))
                result = cursor.fetchone()

                if not result:
                    return

                # Atualiza a linha com greyhound_id e sem trainer_id
                update_query = sql.SQL("""
                    UPDATE greyhound
                    SET id_trainer = %s
                    WHERE id = %s
                """)
                cursor.execute(update_query, (trainer_id, greyhound_id))
                conn.commit()

            except psycopg2.Error as e:
                conn.rollback()
                print("Erro ao atualizar ID do treinador para o greyhound:", e) 

        def insert_or_get_race_result_id(position, bnt, trap, id_greyhound, bend, remarks_acronym, start_price, tf_rating, id_race):
            try:
                # Verifica se já existe uma linha com os valores especificados
                select_query = sql.SQL("""
                    SELECT id FROM race_result
                    WHERE position = %s AND bnt = %s AND trap = %s AND id_greyhound = %s AND bend = %s AND remarks_acronym = %s AND start_price = %s AND tf_rating = %s AND id_race = %s
                """)
                cursor.execute(select_query, (position, bnt, trap, id_greyhound, bend, remarks_acronym, start_price, tf_rating, id_race))
                result = cursor.fetchone()

                if result:
                    race_result_id = result[0]
                    print("Esses dados já existem na tabela.")
                    return race_result_id

                # Se não existir, insere os dados na tabela
                insert_query = sql.SQL("""
                    INSERT INTO race_result (position, bnt, trap, id_greyhound, bend, remarks_acronym, start_price, tf_rating, id_race)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """)
                cursor.execute(insert_query, (position, bnt, trap, id_greyhound, bend, remarks_acronym, start_price, tf_rating, id_race))
                conn.commit()
                greyhound_id = cursor.fetchone()[0]
                return greyhound_id

            except psycopg2.Error as e:
                conn.rollback()
                print("Erro ao inserir ou obter greyhound:", e)
                return None
            
        def update_for_race_result(idRace_result, runTime, sectional, betfairPrice, trainer_id):
            try:
                # Verifica se já existe uma linha com os dados selecionados
                select_query = sql.SQL("""
                    SELECT id FROM race_result
                    WHERE id = %s AND run_time = %s AND sectional = %s AND betfair_price = %s AND id_trainer = %s
                """)
                cursor.execute(select_query, (idRace_result, runTime, sectional, betfairPrice, trainer_id))
                result = cursor.fetchone()

                if result:
                    return

                # Atualiza a linha com os novos dados
                update_query = sql.SQL("""
                    UPDATE race_result
                    SET run_time = %s, sectional = %s, betfair_price = %s, id_trainer = %s
                    WHERE id = %s
                """)
                cursor.execute(update_query, (runTime, sectional, betfairPrice, trainer_id, idRace_result))
                conn.commit()

            except psycopg2.Error as e:
                conn.rollback()
                print("Erro ao atualizar dados:", e)
                
        def insert_race_result_trainer(id_race_result, id_trainer):
            try:
                # Verifica se o relacionamento já existe
                select_query = """
                    SELECT 1 FROM race_result_trainer
                    WHERE id_race_result = %s AND id_trainer = %s
                """
                cursor.execute(select_query, (id_race_result, id_trainer))
                if cursor.fetchone():
                    print("Relacionamento já existe em race_result_trainer.")
                    return

                # Se não existir, insere o relacionamento
                insert_query = """
                    INSERT INTO race_result_trainer (id_race_result, id_trainer)
                    VALUES (%s, %s)
                """
                cursor.execute(insert_query, (id_race_result, id_trainer))
                conn.commit()
                print("Relacionamento inserido com sucesso em race_result_trainer.")

            except psycopg2.Error as e:
                conn.rollback()
                print("Erro ao inserir relacionamento em race_result_trainer:", e)

        def insert_race_result_greyhound(id_race_result, id_greyhound):
            try:
                # Verifica se o relacionamento já existe
                select_query = """
                    SELECT 1 FROM race_result_greyhound
                    WHERE id_race_result = %s AND id_greyhound = %s
                """
                cursor.execute(select_query, (id_race_result, id_greyhound))
                if cursor.fetchone():
                    print("Relacionamento já existe em race_result_greyhound.")
                    return

                # Se não existir, insere o relacionamento
                insert_query = """
                    INSERT INTO race_result_greyhound (id_race_result, id_greyhound)
                    VALUES (%s, %s)
                """
                cursor.execute(insert_query, (id_race_result, id_greyhound))
                conn.commit()
                print("Relacionamento inserido com sucesso em race_result_greyhound.")

            except psycopg2.Error as e:
                conn.rollback()
                print("Erro ao inserir relacionamento em race_result_greyhound:", e)

        # Verificar se encontrou um resultado e imprimir os valores
        if result:
            url, website, scanned = result
            if website == 'timeform':
                partsOfRaceURL = url.split('/')
                dateRace = partsOfRaceURL[7]
                id_timeformRace = partsOfRaceURL[8]

                driver.get(url)
                driver.implicitly_wait(0.5)
                partialHTML01 = driver.find_element(By.XPATH, '/html/body/main/section[2]/section[1]/div[2]')
                partialHTML02 = driver.find_element(By.XPATH, '/html/body/main/section[2]/section[2]/table/tbody')
                trValue = partialHTML02.find_elements(By.XPATH, 'tr')
                trDetails01 = partialHTML02.find_elements(By.XPATH, 'tr[@class="rrb-runner-details rrb-runner-details-1"]')
                trDetails02 = partialHTML02.find_elements(By.XPATH, 'tr[@class="rrb-runner-details rrb-runner-details-2"]')

                # Variáveis raspadas do website
                raceH1 = driver.find_element(By.XPATH, '//h1[@class="w-header"]').text
                raceTime = raceH1[0:5]
                stadium = raceH1[6:].capitalize()
                check_and_create_stadium(stadium)
                id_stadium = get_id_from_stadium(stadium)

                grade = partialHTML01.find_element(By.XPATH, 'div[3]/b[1]').text
                grade = grade[1:-1]
                distance = partialHTML01.find_element(By.XPATH, 'div[3]/b[2]').text
                distance = distance[0:-1]
                racing = partialHTML01.find_element(By.XPATH, 'div[3]/b[3]').text
                prizeTotal = partialHTML01.find_element(By.XPATH, 'div[4]/b[2]').text
                tfGoing = partialHTML01.find_element(By.XPATH, 'div[5]/b[1]').text
                going = partialHTML01.find_element(By.XPATH, 'div[5]/b[2]').text
                forecast = partialHTML01.find_element(By.XPATH, 'div[6]/b[1]').text
                tricast = partialHTML01.find_element(By.XPATH, 'div[6]/b[2]').text

                idRace = insert_race_if_not_exists(dateRace, raceTime, grade, distance, racing, tfGoing, going, prizeTotal, forecast, tricast, int(id_timeformRace), id_stadium)

                for tr in trValue:
                    for detail01 in trDetails01:
                        if tr.get_attribute('innerHTML') == detail01.get_attribute('innerHTML'):
                            position = tr.find_element(By.XPATH, 'td[1]/span').text
                            lengthsBehind = tr.find_element(By.XPATH, 'td[2]').text
                            trap = tr.find_element(By.XPATH, 'td[3]/img').get_attribute('alt')
                            greyhoundName = tr.find_element(By.XPATH, 'td[4]/a').text
                            greyhoundName = greyhoundName.capitalize()
                            greyhoundURL = tr.find_element(By.XPATH, 'td[4]/a').get_attribute('href')
                            partsOfGreyhoundURL = greyhoundURL.split('/')
                            id_timeform = partsOfGreyhoundURL[6]

                            if not url_exists(greyhoundURL):
                                # Inserir a URL na tabela se não existir
                                insert_data(greyhoundName, id_timeform, greyhoundURL)

                            ageAndSex = tr.find_element(By.XPATH, 'td[5]').text
                            sex = ageAndSex[1]
                            bends = tr.find_element(By.XPATH, 'td[6]/span').text
                            remarks = tr.find_element(By.XPATH, 'td[7]/span').text
                            startingPrice = tr.find_element(By.XPATH, 'td[9]/span').text
                            timeformRating = tr.find_element(By.XPATH, 'td[10]/span').text
                    greyhound_id = insert_or_get_greyhound_id(greyhoundName, sex, id_timeform)
                    idRace_result = insert_or_get_race_result_id(position, lengthsBehind, trap, greyhound_id, bends, remarks, startingPrice, timeformRating, idRace)

                    insert_race_result_greyhound(idRace_result, greyhound_id)  # Insere um relacionamento entre race_result com id 1 e greyhound com id 1
                    for detail02 in trDetails02:
                        if tr.get_attribute('innerHTML') == detail02.get_attribute('innerHTML'):
                            runTime = tr.find_element(By.XPATH, 'td[1]/span').text
                            sectional = runTime[7:-1]
                            runTime = runTime[0:5]
                            trainer = tr.find_element(By.XPATH, 'td[2]/span').text
                            check_and_create_trainer(trainer)
                            update_trainer_id_for_greyhound(greyhound_id, get_id_from_trainer(trainer))
                            betfairPrice = tr.find_element(By.XPATH, 'td[3]/span').text
                            update_for_race_result(idRace_result, runTime, sectional, betfairPrice, get_id_from_trainer(trainer))
                            #insert_race_result_trainer(idRace_result, get_id_from_trainer(trainer))  # Insere um relacionamento entre race_result com id 1 e trainer com id 1

                raceComments = driver.find_element(By.XPATH, '/html/body/main/section[2]/section[3]/div').text
                update_race_comment(idRace, raceComments)
            elif website == 'racingpost':
                pass
        else:
            print("Nenhum resultado encontrado.")
        
        driver.close()
        update_scanned_status(url)

# Fechar o cursor e a conexão com o banco de dados
cursor.close()
conn.close()
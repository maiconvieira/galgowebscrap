from db import connect
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from sqlalchemy import create_engine, exists
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from sqlalchemy.orm import sessionmaker, declarative_base
from tables import Base, engine, PageSource, Stadium, Trainer, Greyhound, Race, DogToScam, TrainerGreyhound, RaceResult
import re, logging, time, platform
import pandas as pd

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql import text

# Verifica o sistema operacional
log_dir, driver_path = '', ''
if platform.system() == 'Windows':
    log_dir = 'D:/Projetos/galgowebscrap/logs'
    driver_path = 'C:/Users/maico/.wdm/drivers/chromedriver/win64/124.0.6367.155/chromedriver-win32/chromedriver.exe'
elif platform.system() == 'Linux':
    log_dir = '/home/maicon/galgowebscrap/logs'
    if platform.node() == 'scraping':
        driver_path = '/home/maicon/.wdm/drivers/chromedriver/linux64/124.0.6367.155/chromedriver-linux64/chromedriver'
    else:
        driver_path = '/home/maicon/.wdm/drivers/chromedriver/linux64/124.0.6367.91/chromedriver'
else:
    print('Sistema operacional não reconhecido')
    exit(1)

# Cria as tabelas
Base.metadata.create_all(engine)

chrome_options = Options()
chrome_options.add_argument('--headless')
chrome_options.add_argument('--disable-gpu')
chrome_options.add_argument('--disable-extensions')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('log-level=3')
chrome_options.add_argument('--disable-dev-shm-usage')

service = Service(driver_path)

# Criar a conexão com o banco de dados usando SQLAlchemy
engine = create_engine('postgresql+psycopg2://', creator=connect)
Base = declarative_base()

Session = sessionmaker(bind=engine)
session = Session()

# Iniciar cronometro
start_time = time.time()

def capitalize_words(sentence):
    words = sentence.split()
    capitalized_words = [word.capitalize() for word in words]
    capitalized_sentence = ' '.join(capitalized_words)
    parts = capitalized_sentence.split("'")
    for i in range(len(parts)):
        parts[i] = parts[i][0].capitalize() + parts[i][1:]
    return "'".join(parts)

def get_page_content(pattern, text, error_message):
    match = re.search(pattern, text, re.MULTILINE)
    if match:
        return match.group(1)
    else:
        print(error_message)
        return None
    
# Definir a consulta SQL
sql_query = text('SELECT id, dia, hora, track, tf_id, tf_url, rp_id, rp_url FROM racetoscam WHERE tf_scanned = FALSE and rp_scanned = FALSE ORDER BY dia asc, hora asc, track asc LIMIT 1')

# Executar a consulta diretamente usando a sessão
result = session.execute(sql_query).fetchone()

# Verificar se a consulta retornou algum resultado
if not result:
    print("Nenhum resultado encontrado.")
else:
    id_, dia, hora, track, tf_id, tf_url, rp_id, rp_url = result

    hora_str = str(hora)[:5]
    hora_formatada = str(hora_str).replace(":", "_")
    track_log = str(track).replace(' ', '_').lower()

    # Configuração do logger
    logging.basicConfig(filename=f'{log_dir}/{dia}-{hora_formatada}-{track}-GetInfo.log', format='%(asctime)s %(message)s', filemode='w', level=logging.INFO, encoding='utf-8')

    session.execute(text('UPDATE racetoscam SET rp_scanned=TRUE, tf_scanned=TRUE WHERE id=:id'), {'id': id_})
    session.commit()

    logging.info(f'Corrida escaneada: {dia} {hora_str} {track}')
    print(f'Corrida escaneada: {dia} {hora_str} {track}')

    driver1 = webdriver.Chrome(service=service, options=chrome_options)
    driver1.get(rp_url)
    driver1.implicitly_wait(5)
    src1 = driver1.find_element(By.XPATH, "//div[@class='level level-3 list']").get_attribute('outerHTML')
    soup1 = BeautifulSoup(src1, 'html.parser')
    rp_content = soup1.find('div', class_='meetingResultsList')
    driver1.quit()

    logging.info(f'Racingpost Link: {rp_url}')
    print(f'Racingpost Link: {rp_url}')

    existsPageSource = session.query(exists().where(
        (PageSource.dia == dia) &
        (PageSource.url == rp_url) &
        (PageSource.site == 'rp') &
        (PageSource.scanned_level == 'obter_info') &
        (PageSource.html_source == str(soup1))
    )).scalar()

    if not existsPageSource:
        pageSource = PageSource(
            dia=dia,
            url=rp_url,
            site='rp',
            scanned_level='obter_info',
            html_source=str(soup1)
        )
        session.add(pageSource)

    driver2 = webdriver.Chrome(service=service, options=chrome_options)
    driver2.get(tf_url)
    driver2.implicitly_wait(3)
    src2 = driver2.find_element(By.XPATH, "//section[@class='mb-bfw-result mb-bfw']").get_attribute('outerHTML')
    soup2 = BeautifulSoup(src2, 'html.parser')
    tf_content = soup2.find('tbody', class_='rrb')
    driver2.quit()

    logging.info(f'Timeform Link: {tf_url}')
    print(f'Timeform Link: {tf_url}')
    print('')

    existsPageSource = session.query(exists().where(
        (PageSource.dia == dia) &
        (PageSource.url == tf_url) &
        (PageSource.site == 'tf') &
        (PageSource.scanned_level == 'obter_info') &
        (PageSource.html_source == str(soup2))
    )).scalar()

    if not existsPageSource:
        pageSource = PageSource(
            dia=dia,
            url=tf_url,
            site='tf',
            scanned_level='obter_info',
            html_source=str(soup2)
        )
        session.add(pageSource)

    session.commit()
    session.close()

    try:
        existingStadium = session.query(Stadium).filter_by(name=track).one()
        stadiumId = existingStadium.id
    except NoResultFound:
        newStadium = Stadium(track)
        session.add(newStadium)
        session.commit()
        stadiumId = newStadium.id

    rp_head = str(soup1.find('span', class_='button').get_text())
    rp_footer = str(soup1.find('div', class_='commentsContainer clearfix').get_text())
    tf_head = str(soup2.find('div', class_='rph-race-details w-content rp-content rp-setting-race-details').get_text())
    tf_footer = soup2.find('section', class_='w-seo-content w-container')

#    print('')
#    print(rp_head)
#    print(rp_footer)
#    print('')

    race_num1 = get_page_content(r'^.*Race\s(\d*).*$', rp_head, "'race_num1' não tem uma combinação válida.")
    race_num = get_page_content(r'^\d+:\d+\s\(R(\d+)\).*$', tf_head, "'race_num' não tem uma combinação válida.")
    if race_num1 == race_num:
        race_num = race_num
    elif race_num1 is None and race_num is not None:
        race_num = race_num
    elif race_num1 is not None and race_num is None:
        race_num = race_num1
    elif race_num1 is not None and race_num is not None:
        race_num = race_num
    else:
        race_num = None

    grade1 = get_page_content(r'^.*\s\((.*)\)\s.*$', rp_head, "'grade1' não tem uma combinação válida.")
    grade = get_page_content(r'^Grade:\s?$\n^\((.*)\)$', tf_head, "'grade' não tem uma combinação válida.")
    if grade1 == grade:
        grade = grade
    elif grade1 is None and grade is not None:
        grade = grade
    elif grade1 is not None and grade is None:
        grade = grade1
    elif grade1 is not None and grade is not None:
        grade = grade
    else:
        grade = None

    prizes1 = get_page_content(r'^.*\s(£\d*)\s.*$', rp_head, "'prizes1' não tem uma combinação válida.")
    prizes = get_page_content(r'^Prizes:\s?$\n^1st\s(£\d*),.*$', tf_head, "'prizes' não tem uma combinação válida.")
    if prizes1 == prizes:
        prizes = prizes
    elif prizes1 is None and prizes is not None:
        prizes = prizes
    elif prizes1 is not None and prizes is None:
        prizes = prizes1
    elif prizes1 is not None and prizes is not None:
        prizes = prizes
    else:
        prizes = None

    distance1 = get_page_content(r'^.*\(.*\)\s(\d*)m.*$', rp_head, "'distance1' não tem uma combinação válida.")
    distance = get_page_content(r'^Distance:\s?$\n^(.*)m$', tf_head, "'distance' não tem uma combinação válida.")
    if distance1 == distance:
        distance = distance
    elif distance1 is None and distance is not None:
        distance = distance
    elif distance1 is not None and distance is None:
        distance = distance1
    elif distance1 is not None and distance is not None:
        distance = distance
    else:
        distance = None

    forecast1 = get_page_content(r'^\s+.*F\/C:\s\(\dx\d\)\s(£\d*.\d*).*\s+$', rp_footer, "'forecast1' não tem uma combinação válida.")
    forecast = get_page_content(r'^Forecast:\s*$\n^(£\d*.\d*)$', tf_head, "'forecast' não tem uma combinação válida.")
    if forecast1 == forecast:
        forecast = forecast
    elif forecast1 is None and forecast is not None:
        forecast = forecast
    elif forecast1 is not None and forecast is None:
        forecast = forecast1
    elif forecast1 is not None and forecast is not None:
        forecast = forecast
    else:
        forecast = None

    tricast1 = get_page_content(r'^\s+.*T\/C:\s\(\dx\dx\d\)\s(£\d*.\d*).*\s+$', rp_footer, "'tricast1' não tem uma combinação válida.")
    tricast = get_page_content(r'^Tricast:\s*$\n^(£\d*.\d*)$', tf_head, "'tricast' não tem uma combinação válida.")
    if tricast1 == tricast:
        tricast = tricast
    elif tricast1 is None and tricast is not None:
        tricast = tricast
    elif tricast1 is not None and tricast is None:
        tricast = tricast1
    elif tricast1 is not None and tricast is not None:
        tricast = tricast
    else:
        tricast = None

    rp_going = get_page_content(r'^.*\sGoing:\s+(.*)$', rp_head, "'rp_going' não tem uma combinação válida.")
    total_porc = get_page_content(r'^\s+.*Total\sSP%:\s(\d*.\d*).*\s+$', rp_footer, "'totalPorc1' não tem uma combinação válida.")

    prize = get_page_content(r'^Prizes:\s?$\n^.*Others\s(.*)$', tf_head, "'prize' não tem uma combinação válida.")
    tf_going = get_page_content(r'^Tf\sGoing:\s?$\n^(.*)$', tf_head, "'tf_going' não tem uma combinação válida.")
    going = get_page_content(r'^Going:\s?$\n^(.*)$', tf_head, "'going' não tem uma combinação válida.")
    race_type = get_page_content(r'^Racing:\s?$\n^(.*)$', tf_head, "'race_type' não tem uma combinação válida.")
    
    tf_footer2 = str(tf_footer.find('div', class_='w-content').get_text())
    tf_footer2 = tf_footer2.strip()
    lines = tf_footer2.splitlines()
    cleaned_lines = [line.strip() for line in lines if line.strip()]
    race_comment = ' '.join(cleaned_lines)
    race_comment = race_comment.strip()
    race_comment = re.sub(r'\s{2,}', ' ', race_comment)

#    print(f'Dia: {dia}')
#    print(f'Hora: {hora}')
#    print(f'Race Number: {race_num}')
#    print(f'Grade: {grade}')
#    print(f'Distance: {distance}')
#    print(f'Race Type: {race_type}')
#    print(f'Going TF: {tf_going}')
#    print(f'Going RP: {rp_going}')
#    print(f'Total% RP: {total_porc}')
#    print(f'Going: {going}')
#    print(f'Prizes: {prizes}')
#    print(f'Prize: {prize}')
#    print(f'Forecast: {forecast}')
#    print(f'Tricast: {tricast}')
#    print(f'TF ID: {tf_id}')
#    print(f'RP ID: {rp_id}')
#    print(f'Comment: {race_comment}')
#    print(f'Stadiun ID: {stadiumId}')

    existsRace = session.query(exists().where(
        (Race.dia == dia) &
        (Race.hora == hora) &
        (Race.race_num == race_num) &
        (Race.grade == grade) &
        (Race.distance == distance) &
        (Race.race_type == race_type) &
        (Race.tf_going == tf_going) &
        (Race.rp_going == rp_going) &
        (Race.total_porc == total_porc) &
        (Race.going == going) &
        (Race.prizes == prizes) &
        (Race.prize == prize) &
        (Race.forecast == forecast) &
        (Race.tricast == tricast) &
        (Race.tf_id == tf_id) &
        (Race.rp_id == rp_id) &
        (Race.race_comment == race_comment) &
        (Race.stadium_id == stadiumId)
    )).scalar()

    if not existsRace:
        new_race = Race(
            dia = dia,
            hora = hora,
            race_num = race_num,
            grade = grade,
            distance = distance,
            race_type = race_type,
            tf_going = tf_going,
            rp_going = rp_going,
            total_porc = total_porc,
            going = going,
            prizes = prizes,
            prize = prize,
            forecast = forecast,
            tricast = tricast,
            tf_id = tf_id,
            rp_id = rp_id,
            race_comment = race_comment,
            stadium_id = stadiumId
        )
        session.add(new_race)
        session.commit()
        raceId = new_race.id
    else:
        # Se já existir, obtenha o ID do registro existente
        existing_race = session.query(Race).filter(
            (Race.dia == dia) &
            (Race.hora == hora) &
            (Race.race_num == race_num) &
            (Race.grade == grade) &
            (Race.distance == distance) &
            (Race.race_type == race_type) &
            (Race.tf_going == tf_going) &
            (Race.rp_going == rp_going) &
            (Race.going == going) &
            (Race.prizes == prizes) &
            (Race.prize == prize) &
            (Race.total_porc == total_porc) &
            (Race.forecast == forecast) &
            (Race.tricast == tricast) &
            (Race.tf_id == tf_id) &
            (Race.rp_id == rp_id) &
            (Race.race_comment == race_comment) &
            (Race.stadium_id == stadiumId)
        ).one()
#        print("Registro de corrida duplicado detectado, operação ignorada.")
        raceId = existing_race.id

#    print(f'ID da corrida: {race_id}')

    runner_details_1 = tf_content.find_all('tr', class_='rrb-runner-details-1')
    runner_details_2 = tf_content.find_all('tr', class_='rrb-runner-details-2')
    runner_details_3 = rp_content.find_all('div', class_='container')
    if len(runner_details_1) == len(runner_details_2) == len(runner_details_3):
        for detail1, detail2, detail3 in zip(runner_details_1, runner_details_2, runner_details_3):
            runner_content = str(detail1.prettify()) + str(detail2.prettify()) + str(detail3.prettify())

#            print('')
#            print(runner_content)
#            print('')

            position = get_page_content(r'^\s+<span>$\n^\s+(\d)$\n^\s+<sup>$', runner_content, "'position' não tem uma combinação válida.")
            position1 = get_page_content(r'^\s+<div.*>$\n^\s+(\d)$\n^\s+<sup>$', runner_content, "'position1' não tem uma combinação válida.")
            if position1 == position:
                position = position
            elif position1 is None and position is not None:
                position = position
            elif position1 is not None and position is None:
                position = position1
            elif dog_sectional_time1 is not None and dog_sectional_time is not None:
                dog_sectional_time = dog_sectional_time
            else:
                position = None

            btn = get_page_content(r'^\s+<td class="rrb-hide.*>$\n^\s+(.*)$\n^\s+<\/td>$', runner_content, "'btn' não tem uma combinação válida.")
            if position1 == '1':
                btn1 = '-'
            else:
                btn1 = get_page_content(r'^\s+<div.*dog-cols">$\n^\s+<div.*>$\n^\s+(.*)$\n^\s+<\/div>$', runner_content, "'btn1' não tem uma combinação válida.")
            if btn1 == btn:
                btn = btn
            elif btn1 is None and btn is not None:
                btn = btn
            elif btn1 is not None and btn is None:
                btn = btn1
            elif btn1 is not None and btn is not None:
                btn = btn
            else:
                btn = None

            trap = get_page_content(r'^\s+<img alt="(\d)".*$', runner_content, "'trap' não tem uma combinação válida.")
            trap1 = get_page_content(r'^\s+<div.*trap(\d)">$', runner_content, "'trap1' não tem uma combinação válida.")
            if trap1 == trap:
                trap = trap
            elif trap1 is None and trap is not None:
                trap = trap
            elif trap1 is not None and trap is None:
                trap = trap1
            elif trap1 is not None and trap is not None:
                trap = trap
            else:
                trap = None

            dog_name = capitalize_words(get_page_content(r'^\s+<a.*>$\n^\s+(.*)$\n^\s+<\/a>$', runner_content, "'dog_name' não tem uma combinação válida."))
            dog_name1 = capitalize_words(get_page_content(r'^\s+<div class="name">$\n^\s+(.*)$\n^\s+<\/div>$', runner_content, "'dog_name' não tem uma combinação válida."))
            if dog_name1 == dog_name:
                dog_name = dog_name
            elif dog_name1 is None and dog_name is not None:
                dog_name = dog_name
            elif dog_name1 is not None and dog_name is None:
                dog_name = dog_name1
            elif dog_sectional_time1 is not None and dog_sectional_time is not None:
                dog_sectional_time = dog_sectional_time
            else:
                dog_name = None

            dog_sex = get_page_content(r'^\s+<td.*age and sex.*>$\n^\s+\d(\w)$\n^\s+<\/td>$', runner_content, "'dog_sex' não tem uma combinação válida.")
            dog_sex1 = get_page_content(r'^\s+<span.*dog-sex">$\n^\s+(\w)$\n^\s+<\/span>$', runner_content, "'dog_sex1' não tem uma combinação válida.")
            if dog_sex1 == dog_sex:
                dog_sex = dog_sex
            elif dog_sex1 is None and dog_sex is not None:
                dog_sex = dog_sex
            elif dog_sex1 is not None and dog_sex is None:
                dog_sex = dog_sex1
            elif dog_sex1 is not None and dog_sex is not None:
                dog_sex = dog_sex
            else:
                dog_sex = None

            dog_isp = get_page_content(r'^\s+<span.*starting price.*">$\n^\s+(.*)$\n^\s+<\/span>$', runner_content, "'dog_isp' não tem uma combinação válida.")
            dog_isp1 = get_page_content(r'^\s+<div.*col col2">$\n^\s+(.*)$\n^\s+<\/div>$', runner_content, "'dog_isp1' não tem uma combinação válida.")
            if dog_isp1 == dog_isp:
                dog_isp = dog_isp
            elif dog_isp1 is None and dog_isp is not None:
                dog_isp = dog_isp
            elif dog_isp1 is not None and dog_isp is None:
                dog_isp = dog_isp1
            elif dog_isp1 is not None and dog_isp is not None:
                dog_isp = dog_isp
            else:
                dog_isp = None

            dog_sectional_time = get_page_content(r'^\s+<span.*official run.*">$\n^\s+.*\((.*)\)$\n^\s+<\/span>$', runner_content, "'dog_sectional_time' não tem uma combinação válida.")
            dog_sectional_time1 = get_page_content(r'^\s+<p.*comment">$\n^\s+.*\((.*)\).*$\n^\s+<\/p>$', runner_content, "'dog_sectional_time1' não tem uma combinação válida.")
            if dog_sectional_time1 == dog_sectional_time:
                dog_sectional_time = dog_sectional_time
            elif dog_sectional_time1 is None and dog_sectional_time is not None:
                dog_sectional_time = dog_sectional_time
            elif dog_sectional_time1 is not None and dog_sectional_time is None:
                dog_sectional_time = dog_sectional_time1
            elif dog_sectional_time1 is not None and dog_sectional_time is not None:
                dog_sectional_time = dog_sectional_time
            else:
                dog_sectional_time = None

            dog_trainer = capitalize_words(get_page_content(r'^\s+<span.*trainer">$\n^\s+(.*)$\n^\s+<\/span>$', runner_content, "'dog_trainer' não tem uma combinação válida."))
            dog_trainer1 = capitalize_words(get_page_content(r'^\s+T:$\n^\s+<\/span>$\n^\s+(.*)$\n^\s+<\/div>$', runner_content, "'dog_trainer1' não tem uma combinação válida."))
            if dog_trainer1 == dog_trainer:
                dog_trainer = dog_trainer
            elif dog_trainer1 is None and dog_trainer is not None:
                dog_trainer = dog_trainer
            elif dog_trainer1 is not None and dog_trainer is None:
                dog_trainer = dog_trainer1
            elif dog_trainer1 is not None and dog_trainer is not None:
                dog_trainer = dog_trainer
            else:
                dog_trainer = None

            dog_sire = get_page_content(r'^\s+<span.*dog-sire-dam">$\n^\s+(.*)-.*$\n^\s+<\/span>$', runner_content, "'dog_sire' não tem uma combinação válida.")
            dog_dam = get_page_content(r'^\s+<span.*dog-sire-dam">$\n^\s+.*-(.*)$\n^\s+<\/span>$', runner_content, "'dog_dam' não tem uma combinação válida.")
            dog_age = get_page_content(r'^\s+<td.*age and sex.*>$\n^\s+(\d)\w$\n^\s+<\/td>$', runner_content, "'dog_age' não tem uma combinação válida.")
            dog_birth_day1 = get_page_content(r'^\s+<span.*birth">$\n^\s+(.*)$\n^\s+<\/span>$', runner_content, "'dog_birth_day1' não tem uma combinação válida.")
            dog_tfr = get_page_content(r'^\s+<span.*rating based.*">$\n^\s+(.*)$\n^\s+<\/span>$', runner_content, "'dog_tfr' não tem uma combinação válida.")
            dog_bsp = get_page_content(r'^\s+<span.*Betfair starting.*">$\n^\s+(.*)$\n^\s+<\/span>$', runner_content, "'dog_bsp' não tem uma combinação válida.")
            if get_page_content(r'^\s+<span.*official run.*">$\n^\s+(.*) \(.*\)$\n^\s+<\/span>$', runner_content, '') is not None:
                dog_run_time = get_page_content(r'^\s+<span.*official run.*">$\n^\s+(.*) \(.*\)$\n^\s+<\/span>$', runner_content, "'dog_run_time' não tem uma combinação válida.")
            else:
                dog_run_time = get_page_content(r'^\s+<div.*dog-cols">$\n^\s+<div.*col1">$\n^\s+(.*)$', runner_content, "'dog_run_time' não tem uma combinação válida.")
            dog_bends = get_page_content(r'^\s+<td.*age and sex.*>$\n^\s+\d(\w)$\n^\s+<\/td>$', runner_content, "'dog_bends' não tem uma combinação válida.")
            dog_color1 = get_page_content(r'^\s+<span.*dog-color">$\n^\s+(.*)$\n^\s+<\/span>$', runner_content, "'dog_color1' não tem uma combinação válida.")
            dog_remarks = get_page_content(r'^\s+<td.*rrb-show.*\srace">$\n^\s+<span.*\srace">$\n^\s+(.*)$\n^\s+<\/span>$', runner_content, "'dog_remarks' não tem uma combinação válida.")
            if get_page_content(r'^\s+<p.*comment">$\n^\s+\(.*\)\s+(.*)$\n^\s+<\/p>$', runner_content, '') is not None:
                dog_remarks1 = get_page_content(r'^\s+<p.*comment">$\n^\s+\(.*\)\s+(.*)$\n^\s+<\/p>$', runner_content, "'dog_remarks1' não tem uma combinação válida.")
            else:
                dog_remarks1 = get_page_content(r'^\s+<p.*comment">$\n^\s+(.*)$\n^\s+<\/p>$', runner_content, "'dog_remarks1' não tem uma combinação válida.")
            dog_link = get_page_content(r'^\s+<a.*href="(.*)" title.*>$', runner_content, "'dog_link' não tem uma combinação válida.")
            dog_link1 = get_page_content(r'^\s+<a.*details.*href="(.*)".*>$', runner_content, "'dog_link1' não tem uma combinação válida.")

            dog_remarks = dog_remarks.replace('&amp;', '&')
            dog_id = dog_link.split('/')[-1]
            dog_link1 = dog_link1.replace('&amp;', '&')
            dog_id1 = dog_link1.split('&')[1]
            dog_id1 = dog_id1[7:]

#            print(f'Dog Name: {dog_name}')
#            print(f'Dog ID: {dog_id}')
#            print(f'Dog Link: https://www.timeform.com{dog_link}')
#            print(f'Dog ID: {dog_id1}')
#            print(f'Dog Link: https://greyhoundbet.racingpost.com/{dog_link1}')

            try:
                existsDogToScam = session.query(exists().where(
                    (DogToScam.dogName == dog_name) &
                    (DogToScam.tfDogId == dog_id) &
                    (DogToScam.tf_url == dog_link) &
                    (DogToScam.rpDogId == dog_id1) &
                    (DogToScam.rp_url == dog_link1)
                )).scalar()

                if not existsDogToScam:
                    dogToScam = DogToScam(
                        dogName=dog_name,
                        tfDogId=dog_id,
                        tf_url=f'https://www.timeform.com{dog_link}',
                        tf_scanned=False,
                        rpDogId=dog_id1,
                        rp_url=f'https://greyhoundbet.racingpost.com/{dog_link}',
                        rp_scanned=False
                    )
                    session.add(dogToScam)
                    session.commit()
            except IntegrityError:
                # Se um erro de integridade ocorrer, faça rollback para evitar que a sessão fique em um estado inválido
                session.rollback()
#                print("Registro de cachorro para escanear duplicado detectado, operação ignorada.")

#            print(f'Dog Trainer: {dog_trainer}')

            try:
                existingTrainer = session.query(Trainer).filter_by(name=dog_trainer).one()
#                print("Registro de treinador duplicado detectado, operação ignorada.")
                trainerId = existingTrainer.id
            except NoResultFound:
                newTrainer = Trainer(name=dog_trainer)
                session.add(newTrainer)
                session.commit()
                trainerId = newTrainer.id

#            print(f'Dog Name: {dog_name}')
#            print(f'Dog Birth Day: {dog_birth_day1}')
#            print(f'Dog Sex: {dog_sex}')
#            print(f'Dog Color: {dog_color1}')
#            print(f'Dog Dam: {dog_dam}')
#            print(f'Dog Sire: {dog_sire}')
#            print(f'Dog ID: {dog_id}')
#            print(f'Dog ID: {dog_id1}')

            existsGreyhound = session.query(exists().where(
                (Greyhound.name == dog_name) &
                (Greyhound.born_date == dog_birth_day1) &
                (Greyhound.genre == dog_sex) &
                (Greyhound.colour == dog_color1) &
                (Greyhound.dam == dog_dam) &
                (Greyhound.sire == dog_sire) &
                (Greyhound.tf_id == dog_id) &
                (Greyhound.rp_id == dog_id1)
            )).scalar()

            if not existsGreyhound:
                new_greyhound = Greyhound(
                    name = dog_name,
                    born_date = dog_birth_day1,
                    genre = dog_sex,
                    colour = dog_color1,
                    dam = dog_dam,
                    sire = dog_sire,
                    tf_id = dog_id,
                    rp_id = dog_id1
                )
                session.add(new_greyhound)
                session.commit()
                greyhoundId = new_greyhound.id
            else:
                # Se já existir, obtenha o ID do registro existente
                existing_greyhound = session.query(Greyhound).filter(
                    (Greyhound.name == dog_name) &
                    (Greyhound.born_date == dog_birth_day1) &
                    (Greyhound.genre == dog_sex) &
                    (Greyhound.colour == dog_color1) &
                    (Greyhound.dam == dog_dam) &
                    (Greyhound.sire == dog_sire) &
                    (Greyhound.tf_id == dog_id) &
                    (Greyhound.rp_id == dog_id1)
                ).one()
#                print("Registro de cachorro duplicado detectado, operação ignorada.")
                greyhoundId = existing_greyhound.id

            # Verificar se a relação já existe
            existsTrainerGreyhound = session.query(exists().where(
                (TrainerGreyhound.trainer_id == trainerId) &
                (TrainerGreyhound.greyhound_id == greyhoundId)
            )).scalar()

            if not existsTrainerGreyhound:
                # Se não existir, cria um novo registro
                new_trainer_greyhound = TrainerGreyhound(
                    trainer_id=trainerId,
                    greyhound_id=greyhoundId
                )
                session.add(new_trainer_greyhound)
                session.commit()
#                print("Nova relação Trainer-Greyhound adicionada.")

#            print(f'Dog Position: {position}')
#            print(f'Dog BTN: {btn}')
#            print(f'Dog Trap: {trap}')
#            print(f'Dog Run Time: {dog_run_time}')
#            print(f'Dog Sectional Time: {dog_sectional_time}')
#            print(f'Dog Bends: {dog_bends}')
#            print(f'Dog Remarks: {dog_remarks}')
#            print(f'Dog Remarks: {dog_remarks1}')
#            print(f'Dog ISP: {dog_isp}')
#            print(f'Dog BSP: {dog_bsp}')
#            print(f'Dog TFR: {dog_tfr}')
#            print(f'Dog Age: {dog_age}')
#            print(f'Dog Id: {greyhoundId}')
#            print(f'Race Id: {raceId}')

            try:
                # Verificar se o registro já existe
                existsRaceResult = session.query(RaceResult).filter_by(
                    greyhound_id=greyhoundId,
                    race_id=raceId
                ).first()

                if not existsRaceResult:
                    # Se não existir, cria um novo registro
                    new_race_result = RaceResult(
                        position=position,
                        btn=btn,
                        trap=trap,
                        run_time=dog_run_time,
                        sectional=dog_sectional_time,
                        bend=dog_bends,
                        remarks_acronym=dog_remarks,
                        remarks=dog_remarks1,
                        isp=dog_isp,
                        bsp=dog_bsp,
                        tfr=dog_tfr,
                        greyhound_age=dog_age,
                        greyhound_id=greyhoundId,
                        race_id=raceId
                    )
                    session.add(new_race_result)
                    session.commit()
#                    print("Novo resultado de corrida adicionado.")

            except Exception as e:
                session.rollback()
#                print(f"Ocorreu um erro: {e}")

    else:
        print("O número de linhas nas duas listas não é igual.")

session.close()

print('')
logging.info(f'Corrida escaneada: {dia} {hora_str} {track}')
print(f'Corrida escaneada: {dia} {hora_str} {track}')
logging.info('Finalizado!')
print('Finalizado!')

end_time = time.time()
execution_time = end_time - start_time
logging.info(f'Tempo de execução: {execution_time} segundos')
print(f'Tempo de execução: {execution_time} segundos')

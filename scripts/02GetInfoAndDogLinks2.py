import logging
import platform
import re
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from sqlalchemy import create_engine, exists
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql import text

from db import connect
from tables import Base, engine, PageSource, Stadium, Trainer, Greyhound, Race, DogToScam, DogToScamSemPar

# Configuração de logs e driver_path com base no sistema operacional
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

# Inicialização do SQLAlchemy e criação de tabelas
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)

# Configuração do Chrome WebDriver
chrome_options = Options()
chrome_options.add_argument('--headless')
chrome_options.add_argument('--disable-gpu')
chrome_options.add_argument('--disable-extensions')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('log-level=3')
chrome_options.add_argument('--disable-dev-shm-usage')
service = Service(driver_path)

# Função para capitalizar palavras
def capitalize_words(sentence):
    capitalized_sentence = ' '.join(word.capitalize() for word in sentence.split())
    parts = capitalized_sentence.split("'")
    for i in range(len(parts)):
        parts[i] = parts[i][0].capitalize() + parts[i][1:]
    return "'".join(parts) 

# Função para executar uma consulta SQL e obter o resultado
def execute_query(session, query):
    with engine.connect() as connection:
        return connection.execute(query).fetchone()

# Função para verificar e adicionar novo estádio
def get_or_add_stadium(session, track):
    try:
        existing_stadium = session.query(Stadium).filter_by(name=track).one()
        return existing_stadium.id
    except NoResultFound:
        new_stadium = Stadium(track)
        session.add(new_stadium)
        session.commit()
        return new_stadium.id

# Função para verificar e adicionar novo treinador
def get_or_add_trainer(session, name):
    try:
        existing_trainer = session.query(Trainer).filter_by(name=name).one()
        return existing_trainer.id
    except NoResultFound:
        new_trainer = Trainer(name=name)
        session.add(new_trainer)
        session.commit()
        return new_trainer.id

# Função para adicionar PageSource se não existir
def add_page_source_if_not_exists(session, dia, url, site, scanned_level, src):
    exists_query = session.query(exists().where(
        (PageSource.dia == dia) & (PageSource.url == url) & (PageSource.site == site) & (PageSource.scanned_level == scanned_level) & (PageSource.html_source == src)
    )).scalar()
    
    if not exists_query:
        page_source = PageSource(dia=dia, url=url, site=site, scanned_level=scanned_level, html_source=src)
        session.add(page_source)
        session.commit()
    else:
        print('Dados já estão na tabela!')

# Função para inicializar o WebDriver e obter a página HTML
def get_page_source(url):
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.get(url)
    driver.implicitly_wait(5)
    src = driver.find_element(By.XPATH, "//div[@class='level level-3 list']").get_attribute('outerHTML')
    driver.quit()
    return src

# Função principal para executar o script
def main():
    session = Session()
    
    sql_query = text('SELECT id, dia, hora, track, tf_id, tf_url, rp_id, rp_url FROM racetoscam WHERE tf_scanned = FALSE and rp_scanned = FALSE ORDER BY dia ASC LIMIT 1')
    result = execute_query(session, sql_query)

    if not result:
        print("Nenhum resultado encontrado.")
        return
    
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
    
    stadium_id = get_or_add_stadium(session, track)
    
    rp_src = get_page_source(rp_url)
    logging.info(f'Racingpost Link: {rp_url}')
    add_page_source_if_not_exists(session, dia, rp_url, 'rp', 'obter_corrida', rp_src)
    
    soup = BeautifulSoup(rp_src, 'html.parser')
    race_info = str(soup.find('span', class_='button'))
    match_race = re.search(r'^.+Race (\d).+Going: (.+)<\/span>$', race_info)
    raceNum, rpGoing = (match_race.group(1), match_race.group(2)) if match_race else (None, None)
    
    race_result = pd.DataFrame()
    
    container_elements = soup.find_all('div', class_='container')
    for element in container_elements:
        place = str(element.find('div', class_='place').text.strip())
        dog_color = str(element.find('span', class_='dog-color').text.strip())
        sire_dam = str(element.find('span', class_='dog-sire-dam').text.strip())
        born = str(element.find('span', class_='dog-date-of-birth').text.strip())
        comment = str(element.find('p', class_='comment').text.strip())
        href = str(element.find('a').get('href'))
        
        match_place = re.search(r'^(\d).*$', place)
        match_dog_color = re.search(r'^(.+)$', dog_color)
        match_sire_dam = re.search(r'^(.+)-(.+)$', sire_dam)
        match_born = re.search(r'^(.+)$', born)
        match_comment = re.search(r'^\(\d+.\d+\)\s+([\w\s,]+)$', comment)
        match_href = re.search(r'^(.+dog_id=(\d+)&.+)$', href)
        
        place = match_place.group(1) if match_place else None
        dog_color = match_dog_color.group(1) if match_dog_color else None
        sire = capitalize_words(match_sire_dam.group(1)) if match_sire_dam else None
        dam = capitalize_words(match_sire_dam.group(2)) if match_sire_dam else None
        born = match_born.group(1) if match_born else None
        comment = match_comment.group(1) if match_comment else None
        href = match_href.group(1) if match_href else None
        rpDogId = match_href.group(2) if match_href else None
        
        new_row = pd.DataFrame([{
            'position': place,
            'dog_color': dog_color,
            'sire': sire,
            'dam': dam,
            'born': born,
            'comment': comment,
            'href': href,
            'rpDogId': rpDogId
        }])
        
        race_result = pd.concat([race_result, new_row])
    
    race_result.reset_index(drop=True, inplace=True)
    
    for i, row in race_result.iterrows():
        greyhound_name = row['dog_color']
        if dog_color:
            dog_color_parts = dog_color.split()
            if len(dog_color_parts) > 1:
                greyhound_name = dog_color_parts[1]
            else:
                greyhound_name = None
        else:
            greyhound_name = None

        if not greyhound_name:
            continue
        
        try:
            existing_greyhound = session.query(Greyhound).filter_by(name=greyhound_name).one()
            greyhound_id = existing_greyhound.id
        except NoResultFound:
            new_greyhound = Greyhound(
                name=greyhound_name,
                born_date=row['born'],
                colour=row['dog_color'],
                dam=row['dam'],
                sire=row['sire'],
                rp_id=row['rpDogId']
            )
            session.add(new_greyhound)
            session.commit()
            greyhound_id = new_greyhound.id
        
        race_id = session.query(Race).filter_by(tf_id=tf_id).first().id
        dog_to_scam_entry = DogToScam(
            race_id=race_id,
            greyhound_id=greyhound_id,
            position=row['position'],
            comment=row['comment']
        )
        session.add(dog_to_scam_entry)
        session.commit()
    
    session.close()
    print("Script executado com sucesso.")

if __name__ == '__main__':
    main()

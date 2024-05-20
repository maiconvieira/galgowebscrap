import re, logging, platform
import pandas as pd
from db import connect
from bs4 import BeautifulSoup
from selenium import webdriver
from sqlalchemy.sql import text 
from selenium.webdriver.common.by import By
from sqlalchemy import create_engine, exists
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import IntegrityError
from tables import Base, engine, PageSource, Stadium, Trainer, Greyhound, Race, DogToScam, RaceResult, TrainerGreyhound, DogToScamSemPar
from sqlalchemy.orm.exc import NoResultFound

# Verifica o sistema operacional
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

estadio = {
    '1'   : 'Crayford',
    '4'   : 'Monmore',
    '5'   : 'Hove',
    '6'   : 'Newcastle',
    '7'   : 'Oxford',
    '9'   : 'Wimbledon',
    '11'  : 'Romford',
    '12'  : 'Walthamstow',
    '13'  : 'Henlow',
    '16'  : 'Yarmouth',
    '17'  : 'Hall-green',
    '18'  : 'Belle-vue',
    '21'  : 'Shelbourne Park',
    '25'  : 'Peterborough',
    '33'  : 'Nottingham',
    '34'  : 'Sheffield',
    '35'  : 'Poole',
    '36'  : 'Reading',
    '38'  : 'Shawfield',
    '39'  : 'Swindon',
    '40'  : 'Limerick',
    '41'  : 'Clonmel',
    '42'  : 'Cork',
    '43'  : 'Harolds Cross',
    '45'  : 'Dundalk',
    '48'  : 'Enniscorthy',
    '49'  : 'Galway',
    '50'  : 'Kilkenny',
    '51'  : 'Lifford',
    '52'  : 'Longford',
    '53'  : 'Mullingar',
    '55'  : 'Newbridge',
    '56'  : 'Thurles',
    '57'  : 'Tralee',
    '58'  : 'Waterford',
    '59'  : 'Youghal',
    '61'  : 'Sunderland',
    '62'  : 'Perry-barr',
    '63'  : 'Suffolk-downs',
    '66'  : 'Doncaster',
    '69'  : 'Harlow',
    '70'  : 'Central-park',
    '73'  : 'Valley',
    '76'  : 'Kinsley',
    '83'  : 'Coventry',
    '86'  : 'Pelaw-grange',
    '88'  : 'Drumbo Park',
    '98'  : 'Towcester'
}

# Criar a conexão com o banco de dados usando SQLAlchemy
engine = create_engine('postgresql+psycopg2://', creator=connect)
Base = declarative_base()

# Cria a sessão
Session = sessionmaker(bind=engine)
session = Session()

# Consulta SQL
sql_query = text('SELECT id, dia, hora, track, tf_id, tf_url, rp_id, rp_url FROM racetoscam WHERE tf_scanned = FALSE and rp_scanned = FALSE ORDER BY dia ASC LIMIT 1')

# Executar a query e salvar o resultado em variáveis
with engine.connect() as connection:
    result = connection.execute(sql_query).fetchone()
    
    if result:
        id_, dia, hora, track, tf_id, tf_url, rp_id, rp_url = result
    else:
        print("Nenhum resultado encontrado.")

# Inicializar race_result2 como um DataFrame vazio
race_result = pd.DataFrame()
race_result2 = pd.DataFrame()

hora = str(hora)[:5]
hora2 = str(hora).replace(':', '_')
track_log = str(track).replace(' ', '_').lower()

# Atualizar campos tf_scanned e rp_scanned para TRUE
session.execute(
    text('UPDATE racetoscam SET rp_scanned=true, tf_scanned=TRUE WHERE id=:id'),
    {'id': id_}
)
session.commit()

def capitalize_words(sentence):
    words = sentence.split()
    capitalized_words = [word.capitalize() for word in words]
    capitalized_sentence = ' '.join(capitalized_words)
    parts = capitalized_sentence.split("'")
    for i in range(len(parts)):
        parts[i] = parts[i][0].capitalize() + parts[i][1:]
    return "'".join(parts)

# Configura o logger para escrever logs em um arquivo com nível INFO
logging.basicConfig(filename=f'{log_dir}/{dia}-{hora2}-{track_log}-02GetInfoAndDogLinks.log', 
                    format='%(asctime)s %(message)s', 
                    filemode='w',
                    level=logging.INFO,
                    encoding='utf-8')

logging.info(f'Corrida escaneada: {dia} {hora} {track}')
print(f'Corrida escaneada: {dia} {hora} {track}')

try:
    # Tente buscar a linha que já existe no banco de dados
    existing_stadium = session.query(Stadium).filter_by(name=track).one()
    # Se encontrar, retorne o id da linha existente
    stadium_id = existing_stadium.id
    print(f"Estádio já estão na tabela! ID: {stadium_id}")
except NoResultFound:
    new_stadium = Stadium(track)
    session.add(new_stadium)
    session.commit()
    stadium_id = new_stadium.id
    print(f"Novo estádio adicionado com ID: {stadium_id}")

driver1 = webdriver.Chrome(service=service, options=chrome_options)
driver1.get(rp_url)
print(rp_url)
driver1.implicitly_wait(5)
logging.info(f'Racingpost Link: {rp_url}')
src1 = driver1.find_element(By.XPATH, "//div[@class='level level-3 list']").get_attribute('outerHTML')
driver1.quit()

# Cria um objeto BeautifulSoup
soup = BeautifulSoup(src1, 'html.parser')

race_info = str(soup.find('span', class_='button'))
match_race = re.search(r'^.+Race (\d).+Going: (.+)<\/span>$', race_info)
raceNum = match_race.group(1) if match_race else None
rpGoing = match_race.group(2) if match_race else None

containner_elements = soup.find_all('div', class_='container')
for element in containner_elements:
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
    sire = match_sire_dam.group(1) if match_sire_dam.group(1) else None
    dam = match_sire_dam.group(2) if match_sire_dam.group(2) else None
    born = match_born.group(1) if match_born else None
    comment = match_comment.group(1) if match_comment else None
    href = match_href.group(1) if match_href.group(1) else None
    rpDogId = match_href.group(2) if match_href.group(2) else None

    # Adicionar o resultado como uma nova linha no DataFrame
    new_row = pd.DataFrame([{
        'position': place,
        'dog_color': dog_color,
        'sire': capitalize_words(sire),
        'dam': capitalize_words(dam),
        'born': born,
        'comment': comment,
        'rpDogId': rpDogId,
        'rp_url': 'https://greyhoundbet.racingpost.com/' + href
    }])
    
    race_result2 = pd.concat([race_result2, new_row], ignore_index=True)

# Verifica se a linha já existe no banco de dados
exists_query = session.query(exists().where(
    (PageSource.dia == dia) &
    (PageSource.url == rp_url) &
    (PageSource.site == 'rp') &
    (PageSource.scanned_level == 'obter_corrida') &
    (PageSource.html_source == src1)
)).scalar()

if not exists_query:
    link = PageSource(
        dia=dia,
        url=rp_url,
        site='rp',
        scanned_level='obter_corrida',
        html_source=src1
    )
    session.add(link)
    session.commit()
else:
    print('Dados já estão na tabela!')

driver2 = webdriver.Chrome(service=service, options=chrome_options)
driver2.get(tf_url)
print(tf_url)
driver2.implicitly_wait(3)
logging.info(f'Timeform Link: {tf_url}')
src2 = driver2.find_element(By.XPATH, "//section[@class='mb-bfw-result mb-bfw']").get_attribute('outerHTML')
driver2.quit()

# Verifica se a linha já existe no banco de dados
exists_query = session.query(exists().where(
    (PageSource.dia == dia) &
    (PageSource.url == tf_url) &
    (PageSource.site == 'tf') &
    (PageSource.scanned_level == 'obter_corrida') &
    (PageSource.html_source == src2)
)).scalar()

if not exists_query:
    link = PageSource(
        dia=dia,
        url=tf_url,
        site='tf',
        scanned_level='obter_corrida',
        html_source=src2
    )
    session.add(link)
    session.commit()
else:
    print('Dados já estão na tabela!')

print('')

# Cria um objeto BeautifulSoup
soup = BeautifulSoup(src2, 'html.parser')

# Encontra a seção com a classe específica
section = soup.find('section', class_='mb-bfw-result mb-bfw')
head_section = str(section.find('div', class_='rph-race-details w-content rp-content rp-setting-race-details'))

match_grade = re.search(r'<span title=".*">Grade: <\/span>\n<b title=".*">\(([A-Z0-9\s-]+)\)<\/b>', head_section)
match_distance = re.search(r'<span title=".*">Distance: <\/span>\n<b title=".*">([0-9]{2,}).+<\/b>', head_section)
match_raceType = re.search(r'<span title=".*">Racing: <\/span>\n<b title=".*">([A-Z][a-z]+)<\/b>', head_section)
match_tfGoing = re.search(r'<span title=".*">Tf Going: <\/span>\n<b title=".*">([0-9]+\.[0-9]+)<\/b>', head_section)
match_going = re.search(r'<span title=".*">Going: <\/span>\n<b title=".*">([0-9]+\.[0-9]+)<\/b>', head_section)
match_prizes = re.search(r'<span title=".*">Prizes: <\/span>\n<b title=".*">([0-9][a-z]{2}.*[0-9]*)\s<\/b>', head_section)
match_prize = re.search(r'<span title=".*">Total: <\/span>\n<b title=".*">\s((€|£)([0-9]+|[0-9]+\.[0-9]+))<\/b>', head_section)
match_forecast = re.search(r'<span title=".*">Forecast: <\/span>\n<b title=".*">((€|£)([0-9]+|[0-9]+\.[0-9]+))<\/b>', head_section)
match_tricast = re.search(r'<span title=".*">Tricast: <\/span>\n<b title=".*">((€|£)([0-9]+|[0-9]+\.[0-9]+))<\/b>', head_section)

grade = match_grade.group(1) if match_grade else None
distance = match_distance.group(1) if match_distance else None
raceType = match_raceType.group(1) if match_raceType else None
tfGoing = match_tfGoing.group(1) if match_tfGoing else None
going = match_going.group(1) if match_going else None
prizes = match_prizes.group(1) if match_prizes else None
prize = match_prize.group(1) if match_prize else None
forecast = match_forecast.group(1) if match_forecast else None
tricast = match_tricast.group(1) if match_tricast else None

# Encontra a seção do rodapé
footer_section = section.find('section', class_='w-seo-content w-container')
p_elements = footer_section.findAll('p')
comments = ''

for p in p_elements:
    comments += p.get_text() + ' '

comments = comments.replace('\n', '')
comments = re.sub(r'\s{2,}', ' ', comments)
comments = comments.strip()

try:
    # Tente buscar a linha que já existe no banco de dados
    existing_race = session.query(Race).filter_by(
        dia=dia,
        hora=hora,
        race_num=raceNum,
        grade=grade,
        distance=distance,
        race_type=raceType,
        tf_going=tfGoing,
        rp_going=rpGoing,
        going=going,
        prizes=prizes,
        prize=prize,
        forecast=forecast,
        tricast=tricast,
        race_comment=comments,
        tf_id=tf_id,
        rp_id=rp_id,
        stadium_id=stadium_id
    ).one()
    # Se encontrar, retorne o id da linha existente
    race_id = existing_race.id
    print(f"Dados da corrida já estão na tabela! ID: {race_id}")
except NoResultFound:
    # Se não encontrar, adicione a nova linha
    new_race = Race(
        dia=dia,
        hora=hora,
        race_num=raceNum,
        grade=grade,
        distance=distance,
        race_type=raceType,
        tf_going=tfGoing,
        rp_going=rpGoing,
        going=going,
        prizes=prizes,
        prize=prize,
        forecast=forecast,
        tricast=tricast,
        race_comment=comments,
        race_comment_ptbr=None,
        tf_id=tf_id,
        rp_id=rp_id,
        stadium_id=stadium_id
    )
    session.add(new_race)
    session.commit()
    race_id = new_race.id
    print(f"Novo dado adicionado com ID: {race_id}")

print('')

# Pega dados da tag Table
tbody = str(section.find('tbody', class_='rrb'))

# Sua expressão regular
regex = r'((^.+span>(\d)<.+$\n^.+">(.+)<\/td>$\n^.+$\n^.+$\n^.+-(\d) hover-opacity" href="(.+\/(\d+))".+$\n^\s+(.+)$\n^.+$\n^.+$\n^.+">(\d)*(\w)*<\/td>$\n^.+">([\d\w\s-]*)<\/span><\/td>$\n^.+$\n^\s+(.*)$\n^.+$\n^.+$\n^.+$\n^.*$\n^.+$\n^.+$\n^<td.+">(.*)<\/span><\/td>$\n^<td.+">(.*)<\/span>$\n^.+$\n^.+$\n^.+$\n^<td.*">(.*)<\/span>$\n^.+$\n^<td.*">(.*)<\/span>$\n^.+$\n^<td.*">(.*)<\/span><\/td>$))'

# Procurando padrões na string de texto
matches = re.findall(regex, tbody, re.MULTILINE)

# Iterando sobre os resultados e imprimindo os grupos capturados
for match in matches:
    if re.search(r'^\d+\.\d+\s\(\d+\.\d+\)$', match[14]):
        run_time_group = re.match(r'^(\d+\.\d+)\s\((\d+\.\d+)\)$', match[14])
        runTime = run_time_group.group(1) if run_time_group else match[14]
        sectional = run_time_group.group(2) if run_time_group else None
    else:
        runTime = match[14]
        sectional = None

    try:
        # Verifique se o treinador já existe no banco de dados com base no nome
        existing_trainer = session.query(Trainer).filter_by(name=match[15]).one()
        # Se encontrar, retorne o id da linha existente
        trainer_id = existing_trainer.id
        print(f"Treinador já está na tabela! ID: {trainer_id}")
    except NoResultFound:
        # Se não encontrar, adicione a nova linha
        new_trainer = Trainer(name=match[15])
        session.add(new_trainer)
        session.commit()
        trainer_id = new_trainer.id
        print(f"Novo treinador adicionado com ID: {trainer_id}")

    # Adicionar o resultado como uma nova linha no DataFrame
    new_row = pd.DataFrame([{
        'dia': dia,
        'hora': hora,
        'track': track,
        'position': match[2],
        'btn': match[3],
        'trap': match[4],
        'tf_url': 'https://www.timeform.com' + match[5],
        'tfDogId': match[6],
        'dogName': capitalize_words(match[7]),
        'dogAge': match[8],
        'dogSex': match[9],
        'bend': match[10],
        'comments': match[11],
        'startPrice': match[12],
        'tfRate': match[13],
        'runTime': runTime,
        'sectional': sectional,
        'trainerId': trainer_id,
        'bsp': match[16],
        'raceId': race_id
    }])

    race_result = pd.concat([race_result, new_row], ignore_index=True)

# Realizar a junção com base na coluna 'position'
race_result = pd.merge(race_result, race_result2, on='position', how='inner')

dogScamColumns = ['dogName', 'tfDogId', 'tf_url', 'rpDogId', 'rp_url']
dogToScam = race_result.loc[:, dogScamColumns]
ignored_count = 0
for index, row in dogToScam.iterrows():
    # Verifique se a linha já está no banco de dados
    existing_entry = session.query(DogToScam).filter_by(dogName=row['dogName'], tfDogId=row['tfDogId'], rpDogId=row['rpDogId']).first()
    if not existing_entry:
        dog_to_scam = DogToScam(
            dogName=row['dogName'],
            tfDogId=row['tfDogId'],
            tf_url=row['tf_url'],
            tf_scanned=False,
            rpDogId=row['rpDogId'],
            rp_url=row['rp_url'],
            rp_scanned=False
        )
        try:
            session.add(dog_to_scam)
            session.commit()
            print(f"{row['dogName']} inserido com sucesso.")
        except IntegrityError:
            session.rollback()
    else:
        ignored_count += 1
if ignored_count > 0:
    print(f'Número de link combinados, que serão ignorados: {ignored_count}')

for index, row in race_result.iterrows():
    try:
        # Verifique se o greyhound já existe no banco de dados com base no nome
        existing_greyhound = session.query(Greyhound).filter_by(name=row['dogName']).first()
        if existing_greyhound:
            # Se encontrar, retorne o id da linha existente
            greyhound_id = existing_greyhound.id
            print(f"Greyhound já está na tabela! ID: {greyhound_id}")
        else:
            raise NoResultFound
    except NoResultFound:
        # Se não encontrar, adicione a nova linha
        new_greyhound = Greyhound(
            name = row['dogName'],
            born_date = row['born'],
            genre = row['dogSex'],
            colour = row['dog_color'],
            dam = row['dam'],
            sire = row['sire'],
            owner = None,
            tf_id = row['tfDogId'],
            rp_id = row['rpDogId']
        )
        session.add(new_greyhound)
        session.commit()
        greyhound_id = new_greyhound.id
        print(f"Novo greyhound adicionado com ID: {greyhound_id}")

#    # Atualize a coluna 'dogId' no DataFrame
#    race_result.at[index, 'dogId'] = greyhound_id

    try:
        existing_race_result = session.query(RaceResult).filter_by(
            position=row['position'],
            btn=row['btn'],
            trap=row['trap'],
            run_time=row['runTime'],
            sectional=row['sectional'],
            bend=row['bend'],
            remarks_acronym=row['comments'],
            remarks=row['comment'],
            isp=row['startPrice'],
            bsp=row['bsp'],
            tfr=row['tfRate'],
            greyhound_weight=None,
            greyhound_id=greyhound_id,
            race_id=row['raceId']
        ).first()
        if existing_race_result:
            race_result_id = existing_race_result.id
            print(f"Resultado da corrida já está na tabela! ID: {race_result_id}")
        else:
            raise NoResultFound
    except NoResultFound:
        race_result_instance = RaceResult(
            position=row['position'],
            btn=row['btn'],
            trap=row['trap'],
            run_time=row['runTime'],
            sectional=row['sectional'],
            bend=row['bend'],
            remarks_acronym=row['comments'],
            remarks=row['comment'],
            isp=row['startPrice'],
            bsp=row['bsp'],
            tfr=row['tfRate'],
            greyhound_weight=None,
            greyhound_id=greyhound_id,
            race_id=row['raceId']
        )
        session.add(race_result_instance)
        session.commit()
        print(f"Novo resultado da corrida adicionado com ID: {race_result_instance.id}")

    try:
        existing_pair = session.query(TrainerGreyhound).filter_by(trainer_id=row['trainerId'], greyhound_id=greyhound_id).first()
        if existing_pair:
            pair_id = (existing_pair.trainer_id, existing_pair.greyhound_id)  # Usar a tupla como ID
            print(f"Par (treinador/greyhound) já está na tabela! ID: {pair_id}")
        else:
            raise NoResultFound
    except NoResultFound:
        trainer_greyhound_instance = TrainerGreyhound(
            trainer_id=row['trainerId'],
            greyhound_id=greyhound_id
        )
        session.add(trainer_greyhound_instance)
        session.commit()
        print(f"Novo par (treinador/greyhound) da corrida adicionado com ID: {race_result_instance.id}")

# Confirma a transação
session.commit()
# Fecha a sessão
session.close()

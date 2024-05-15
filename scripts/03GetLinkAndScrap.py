import re, logging, time, platform
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
from tables import Base, engine, PageSource, Stadium, Race
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
sql_query = text('SELECT id, dia, hora, track, tf_id, tf_url, rp_id, rp_url FROM linkstoscam WHERE tf_scanned = FALSE and rp_scanned = FALSE ORDER BY dia ASC LIMIT 1')

# Executar a query e salvar o resultado em variáveis
with engine.connect() as connection:
    result = connection.execute(sql_query).fetchone()
    
    if result:
        id_, dia, hora, track, tf_id, tf_url, rp_id, rp_url = result
    else:
        print("Nenhum resultado encontrado.")

# Inicializar race_result2 como um DataFrame vazio
race_result = pd.DataFrame(columns=[
    'dia', 'hora', 'track', 'position', 'btn', 'trap', 'tfDogId', 'dogName', 'dogAge', 'dogSex', 'bend', 'comments', 'startPrice', 'tfRate', 'runTime', 'sectional', 'trainer', 'bsp'
])
race_result2 = pd.DataFrame(columns=['dog_color', 'sire', 'dam', 'born', 'comment'])

hora2 = str(hora).replace(':', '_')
track_log = str(track).replace(' ', '_').lower()

## Atualizar campos tf_scanned e rp_scanned para TRUE
#session.execute(
#    text('UPDATE linkstoscam SET rp_scanned=true, tf_scanned=TRUE WHERE id=:id'),
#    {'id': links_banco['id'].iloc[0]}
#)

def capitalize_words(sentence):
    words = sentence.split()
    capitalized_words = [word.capitalize() for word in words]
    capitalized_sentence = ' '.join(capitalized_words)
    parts = capitalized_sentence.split("'")
    for i in range(len(parts)):
        parts[i] = parts[i][0].capitalize() + parts[i][1:]
    return "'".join(parts)

# Configura o logger para escrever logs em um arquivo com nível INFO
logging.basicConfig(filename=f'{log_dir}/{dia}-{hora2}-{track_log}-04GetLinkAndScrap.log', 
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
    # Se não encontrar, adicione a nova linha
    new_stadium = Stadium(track)
    session.add(new_stadium)
    session.commit()  # Confirma a transação para obter o ID
    stadium_id = new_stadium.id
    print(f"Novo estádio adicionado com ID: {stadium_id}")

driver1 = driver2 = webdriver.Chrome(service=service, options=chrome_options)

#rp_url = 'https://greyhoundbet.racingpost.com/#result-meeting-result/race_id=2053454&track_id=34&r_date=2024-05-07&r_time=10:47'
driver1.get(rp_url)
print(rp_url)
driver1.implicitly_wait(5)
logging.info(f'Racingpost Link: {rp_url}')
src1 = driver1.find_element(By.XPATH, "//div[@class='level level-3 list']").get_attribute('outerHTML')
driver1.quit()

# Cria um objeto BeautifulSoup
soup = BeautifulSoup(src1, 'html.parser')
anchor_elements = soup.find_all('a')
for element in anchor_elements:
    href=element.get('href')
    print(href)

race_info = str(soup.find('span', class_='button'))
match_race = re.search(r'^.+Race (\d).+Going: (.+)<\/span>$', race_info)
raceNum = match_race.group(1) if match_race else None
rpGoing = match_race.group(2) if match_race else None

containner_elements = soup.find_all('div', class_='container')
for element in containner_elements:
    dog_color = str(element.find('span', class_='dog-color').text.strip())
    sire_dam = str(element.find('span', class_='dog-sire-dam').text.strip())
    born = str(element.find('span', class_='dog-date-of-birth').text.strip())
    comment = str(element.find('p', class_='comment').text.strip())
    match_dog_color = re.search(r'^(.+)$', dog_color)
    match_sire_dam = re.search(r'^(.+)-(.+)$', sire_dam)
    match_born = re.search(r'^(.+)$', born)
    match_comment = re.search(r'^\(\d+.\d+\)\s+([\w\s,]+)$', comment)
    dog_color = match_dog_color.group(1) if match_dog_color else None
    sire = match_sire_dam.group(1) if match_sire_dam.group(1) else None
    dam = match_sire_dam.group(2) if match_sire_dam.group(2) else None
    born = match_born.group(1) if match_born else None
    comment = match_comment.group(1) if match_comment else None

    # Adicionar o resultado como uma nova linha no DataFrame
    new_row = pd.DataFrame([{
        'dog_color': dog_color,
        'sire': sire,
        'dam': dam,
        'born': born,
        'comment': comment
    }])
    
    race_result2 = pd.concat([race_result2, new_row], ignore_index=True)

print(race_result2)

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
else:
    print('Dados já estão na tabela!')

#tf_url = 'https://www.timeform.com/greyhound-racing/results/crayford/1307/2018-12-01/632080'

driver2 = webdriver.Chrome(service=service, options=chrome_options)
driver2.get(tf_url)
print(tf_url)
driver2.implicitly_wait(3)
logging.info(f'Timeform Link: {tf_url}')

# Obtém o conteúdo HTML da página
src2 = driver2.find_element(By.XPATH, "//section[@class='mb-bfw-result mb-bfw']").get_attribute('outerHTML')
driver2.quit()

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
#race['comments'] = comments

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
        #tf_id=links_banco['tf_id'].iloc[0],
        #rp_id=links_banco['rp_id'].iloc[0],
        stadium_id=stadium_id
    ).one()
    # Se encontrar, retorne o id da linha existente
    race_id = existing_race.id
    print(f"Dados já estão na tabela! ID: {race_id}")
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
        #tf_id=links_banco['tf_id'].iloc[0],
        #rp_id=links_banco['rp_id'].iloc[0],
        stadium_id=stadium_id
    )
    session.add(new_race)
    session.commit()  # Confirma a transação para obter o ID
    race_id = new_race.id
    print(f"Novo dado adicionado com ID: {race_id}")

#print(race)

# Pega dados da tag Table
tbody = str(section.find('tbody', class_='rrb'))

anchor_elements = section.find('tbody', class_='rrb').find_all('a')

for element in anchor_elements:
    href=element.get('href')
    print(href)

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

    # Adicionar o resultado como uma nova linha no DataFrame
    new_row = pd.DataFrame([{
        'dia': dia,
        'hora': hora,
        'track': track,
        'position': match[2],
        'btn': match[3],
        'trap': match[4],
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
        'trainer': match[15],
        'bsp': match[16]
    }])

    race_result = pd.concat([race_result, new_row], ignore_index=True)


#    race_result.loc[0, 'dia'] = dia
#    race_result['hora'] = hora
#    race_result['track'] = track
#    race_result['position'] = match[2]
#    race_result['btn'] = match[3]
#    race_result['trap'] = match[4]
#    race_result['tfDogId'] = match[6]
#    race_result['dogName'] = capitalize_words(match[7])
#    race_result['dogAge'] = match[8]
#    race_result['dogSex'] = match[9]
#    race_result['bend'] = match[10]
#    race_result['comments'] = match[11]
#    race_result['startPrice'] = match[12]
#    race_result['tfRate'] = match[13]
#    if re.search(r'^\d+\.\d+\s\(\d+\.\d+\)$', match[14]):
#        run_time = re.match(r'^(\d+\.\d+)\s\((\d+\.\d+)\)$', match[14])
#        race_result['runTime'] = run_time.group(1) if run_time else match[14]
#        race_result['sectional'] = run_time.group(2) if run_time else None
#    race_result['trainer'] = capitalize_words(match[15])
#    race_result['bsp'] = match[16]

    # Adicionar o resultado como um dicionário à lista
#    race_result2.append({
#        'dog_color': dog_color,
#        'sire': sire,
#        'dam': dam,
#        'born': born,
#        'comment': comment
#    })

print(race_result)

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
else:
    print('Dados já estão na tabela!')

# Confirma a transação
session.commit()
# Fecha a sessão
session.close()
import re, logging, time, platform
import pandas as pd
from db import connect
from bs4 import BeautifulSoup
from datetime import datetime
from selenium import webdriver
from sqlalchemy.sql import text 
from selenium.webdriver.common.by import By
from sqlalchemy import create_engine, exists
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from sqlalchemy.orm import sessionmaker, declarative_base
from tables import Base, engine, PageSource

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
sql_query = text('SELECT id, dia, hora, track, timeform_id, timeform_url, racingpost_id, racingpost_url FROM linkstoscam WHERE tf_scanned = FALSE and rp_scanned = FALSE ORDER BY dia ASC LIMIT 1')

# Executar a query e converter o resultado em DataFrame
links_banco = pd.read_sql_query(sql_query, engine)

race = pd.DataFrame()
race_result = pd.DataFrame()

dia = links_banco['dia'].iloc[0]
race.loc[0, 'dia'] = dia
hora = str(links_banco['hora'].iloc[0])[:5]
hora2 = hora.replace(':', '_')
race['hora'] = hora
track_log = str(links_banco['track'].iloc[0]).replace(' ', '_').lower()
track = links_banco['track'].iloc[0]
race['track'] = track
race['rp_id'] = links_banco['racingpost_id'].iloc[0]
rp_url = links_banco['racingpost_url'].iloc[0]
race['tf_id'] = links_banco['timeform_id'].iloc[0]
tf_url = links_banco['timeform_url'].iloc[0]

# Atualizar campos tf_scanned e rp_scanned para TRUE
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

rp_lista = []
tf_lista = []
start_time = time.time()

driver1 = driver2 = webdriver.Chrome(service=service, options=chrome_options)
driver1.get(rp_url)
print(rp_url)
driver1.implicitly_wait(5)
logging.info(f'Racingpost Link: {rp_url}')
src1 = driver1.find_element(By.XPATH, "//div[@class='scrollContent']").get_attribute('outerHTML')
pattern1 = re.compile(r'(#result-meeting-result/race_id=\d+&amp;track_id=\d+&amp;r_date=[\d-]+&amp;r_time=[\d:]+)')
driver1.quit()
links1 = pattern1.findall(src1)
#print(src1)

# Verifica se a linha já existe no banco de dados
exists_query = session.query(exists().where(
    (PageSource.dia == links_banco['dia'].iloc[0]) &
    (PageSource.url == rp_url) &
    (PageSource.site == 'rp') &
    (PageSource.scanned_level == 'obter_corrida') &
    (PageSource.html_source == src1)
)).scalar()

if not exists_query:
    link = PageSource(
        dia=links_banco['dia'].iloc[0],
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

# Cria um objeto BeautifulSoup
soup = BeautifulSoup(src2, 'html.parser')

# Encontra a seção com a classe específica
section = soup.find('section', class_='mb-bfw-result mb-bfw')
head_section = str(section.find('div', class_='rph-race-details w-content rp-content rp-setting-race-details'))

match_grade = re.search(r'<span title=".*">Grade: <\/span>\n<b title=".*">\(([A-Z0-9\s-]+)\)<\/b>', head_section)
match_distance = re.search(r'<span title=".*">Distance: <\/span>\n<b title=".*">([0-9]{2,}[a-z])<\/b>', head_section)
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

race['grade'] = grade
race['distance'] = distance
race['raceType'] = raceType
race['tfGoing'] = tfGoing
race['going'] = going
race['prizes'] = prizes
race['prize'] = prize
race['forecast'] = forecast
race['tricast'] = tricast

# Encontra a seção do rodapé
footer_section = section.find('section', class_='w-seo-content w-container')
p_elements = footer_section.findAll('p')
footer = ''

for p in p_elements:
    footer += p.get_text() + ' '

footer = footer.replace('\n', '')
footer = re.sub(r'\s{2,}', ' ', footer)
footer = footer.strip()
race['comments'] = footer

# Pega dados da tag Table
tbody = str(section.find('tbody', class_='rrb'))

# Sua expressão regular
regex = r'((^.+span>(\d)<.+$\n^.+">(.+)<\/td>$\n^.+$\n^.+$\n^.+-(\d) hover-opacity" href="(.+\/(\d+))".+$\n^\s+(.+)$\n^.+$\n^.+$\n^.+">(\d)*(\w)*<\/td>$\n^.+">([\d\w\s-]*)<\/span><\/td>$\n^.+$\n^\s+(.*)$\n^.+$\n^.+$\n^.+$\n^.*$\n^.+$\n^.+$\n^<td.+">(.*)<\/span><\/td>$\n^<td.+">(.*)<\/span>$\n^.+$\n^.+$\n^.+$\n^<td.*">(.*)<\/span>$\n^.+$\n^<td.*">(.*)<\/span>$\n^.+$\n^<td.*">(.*)<\/span><\/td>$))'

# Procurando padrões na string de texto
matches = re.findall(regex, tbody, re.MULTILINE)

# Iterando sobre os resultados e imprimindo os grupos capturados
for match in matches:
    race_result.loc[0, 'dia'] = dia
    race_result['hora'] = hora
    race_result['track'] = track
    race_result['position'] = match[2]
    race_result['btn'] = match[3]
    race_result['trap'] = match[4]
    race_result['tfDogId'] = match[6]
    race_result['dogName'] = capitalize_words(match[7])
    race_result['dogAge'] = match[8]
    race_result['dogSex'] = match[9]
    race_result['bend'] = match[10]
    race_result['comments'] = match[11]
    race_result['startPrice'] = match[12]
    race_result['tfRate'] = match[13]
    if re.search(r'^\d+\.\d+\s\(\d+\.\d+\)$', match[14]):
        run_time = re.match(r'^(\d+\.\d+)\s\((\d+\.\d+)\)$', match[14])
        race_result['runTime'] = run_time.group(1) if run_time else match[14]
        race_result['sectional'] = run_time.group(2) if run_time else None
    race_result['trainer'] = capitalize_words(match[15])
    race_result['bsp'] = match[16]

print(race_result)

# Remove o conteúdo correspondente à expressão regular#tbody = re.sub(regex, '', tbody)
#tbody = re.sub(regex, '', tbody)

# Exibe o texto resultante
#print(tbody)

# Verifica se a linha já existe no banco de dados
exists_query = session.query(exists().where(
    (PageSource.dia == links_banco['dia'].iloc[0]) &
    (PageSource.url == tf_url) &
    (PageSource.site == 'tf') &
    (PageSource.scanned_level == 'obter_corrida') &
    (PageSource.html_source == src2)
)).scalar()

if not exists_query:
    link = PageSource(
        dia=links_banco['dia'].iloc[0],
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
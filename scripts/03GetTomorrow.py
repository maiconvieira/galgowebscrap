from db import connect
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from sqlalchemy import create_engine, exists, text, func
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from sqlalchemy.orm import sessionmaker, declarative_base
from tables import Base, engine, LastDate, RaceToScam, RaceToScamSemPar, PageSource
import re, logging, sys, time, platform
import pandas as pd

# Verifica o sistema operacional
log_dir, driver_path = '', ''
if platform.system() == 'Windows':
    log_dir = 'D:/Projetos/galgowebscrap/logs'
    driver_path = 'C:/Users/maico/.wdm/drivers/chromedriver/win64/127.0.6533.72/chromedriver.exe'
elif platform.system() == 'Linux':
    log_dir = '/home/maicon/galgowebscrap/logs'
    driver_path = '/home/maicon/.wdm/drivers/chromedriver/linux64/127.0.6533.72/chromedriver'
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

today = datetime.now().date()
tomorrow = today + timedelta(days=1)

# Configura o logger para escrever logs em um arquivo com nível INFO
logging.basicConfig(filename=f'{log_dir}/{today}-Tomorrow.log', 
                    format='%(asctime)s %(message)s', 
                    filemode='w',
                    level=logging.INFO,
                    encoding='utf-8')
logging.info(f'Dia escaneado: {today}')
print(f'Dia escaneado: {today}')

# Inicializando DataFrames vazios
#df_rp = pd.DataFrame(columns=['track', 'hora', 'distance', 'rp_race', 'rp_grade', 'rp_id', 'rp_url'])
#df_tf = pd.DataFrame(columns=['track', 'hora', 'distance', 'tf_race', 'tf_grade', 'tf_id', 'tf_url'])
#df_rp = pd.DataFrame(columns=['track', 'hora', 'distance', 'race', 'grade', 'rp_id', 'rp_url'])
#df_tf = pd.DataFrame(columns=['track', 'hora', 'distance', 'race', 'grade', 'tf_id', 'tf_url'])
#rp_lista = []
#tf_lista = []

#driver1 = webdriver.Chrome(service=service, options=chrome_options)
#rp_href = f'https://greyhoundbet.racingpost.com/#meeting-list/view=time'
#rp_href = f'https://greyhoundbet.racingpost.com/#meeting-list/view=time&r_date={tomorrow}'

#driver1.get(rp_href)
#driver1.implicitly_wait(5)
#src1 = driver1.find_element(By.XPATH, "//ul[@class='appList raceList raceListTime']").get_attribute('outerHTML')
#rp_soup = BeautifulSoup(src1, 'html.parser')
#li_elements = rp_soup.find_all('li')

#for li in li_elements:
#    a_element = li.find('a')
#    href_value = a_element.get('href')
#    data_eventlabel_value = a_element.get('data-eventlabel')
#    split_data = data_eventlabel_value.split(',')
#    track = capitalize_words(split_data[0].strip())
#    hora = split_data[1].strip()
#    i_element = li.find('i')
#    h5_element = li.find('h5')
#    h5_text = h5_element.get_text(strip=True) if h5_element else None
#    h5_digits = re.findall(r'\d+', h5_text)
#    race = ' '.join(h5_digits) if h5_digits else None
#    em_element = li.find('em')
#    em_text = em_element.get_text(strip=True) if em_element else None
#    grade_match = re.search(r'Grade: \((.*?)\)', em_text)
#    distance_match = re.search(r'Dis:(\d+)m', em_text)
#    grade = grade_match.group(1) if grade_match else None
#    distance = distance_match.group(1) if distance_match else None
#    match = re.search(r'race_id=(\d+)', href_value)
#    race_id = match.group(1) if match else None
    
    # Adicionando uma nova linha ao DataFrame
#    new_row = pd.DataFrame({
#        'track': [track],
#        'hora': [hora],
#        'distance': [distance],
#    #    'race': [race],
#    #    'grade': [grade],
#        'rp_race': [race],
#        'rp_grade': [grade],
#        'rp_id': [race_id],
#        'rp_url': [f"https://greyhoundbet.racingpost.com/{href_value}"]
#    })
#    df_rp = pd.concat([df_rp, new_row], ignore_index=True)
#driver1.quit()

#driver2 = webdriver.Chrome(service=service, options=chrome_options)
#tf_href = f'https://www.timeform.com/greyhound-racing/racecards'
#driver2.get(tf_href)
#driver2.implicitly_wait(3)
#driver2.find_element(By.XPATH, '/html/body/main/section/section[3]/h1/button[2]').click()
#time.sleep(1)
##driver2.find_element(By.XPATH, '/html/body/main/section/section[3]/div[1]/div[1]/button[2]').click()
##time.sleep(1)
#src2 = driver2.find_element(By.XPATH, "//div[@class='wfr-bytime-content wfr-content']").get_attribute('outerHTML')
#tf_soup = BeautifulSoup(src2, 'html.parser')
#li_elements = tf_soup.find_all('li')
#for li in li_elements:
#    a_element = li.find('a')
#    href_value = a_element.get('href')
#    hora = a_element.get_text(strip=True) if a_element else None
#    b_element = li.find('b')
#    track = capitalize_words(b_element.get_text(strip=True)) if b_element else None
#    div_element = li.find('div')
#    content_text = ''.join(filter(lambda x: isinstance(x, str), div_element.contents))
#    content_text = content_text.strip()
#    split_data = content_text.split(' ')
#    race_digits = re.findall(r'\d+', split_data[0])
#    race = ' '.join(race_digits) if race_digits else None
#    dist_digits = re.findall(r'\d+', split_data[1])
#    distance = ' '.join(dist_digits) if dist_digits else None
#    grade = split_data[3]
#    parts = href_value.split('/')
#    race_id = parts[-1]

    # Adicionando uma nova linha ao DataFrame
#    new_row = pd.DataFrame({
#        'track': [track],
#        'hora': [hora],
#        'distance': [distance],
#    #    'race': [race],
#    #    'grade': [grade],
#        'tf_race': [race],
#        'tf_grade': [grade],
#        'tf_id': [race_id],
#        'tf_url': [f"https://www.timeform.com{href_value}"]
#    })
#    df_tf = pd.concat([df_tf, new_row], ignore_index=True)
#driver2.quit()

#df_merged = pd.merge(df_tf, df_rp, on=['track', 'hora', 'distance'], how='outer')
#df_merged = df_merged.sort_values(by=['hora'])
#unique_tracks = df_merged['track'].unique()

#df_filtered = df_merged[df_merged['tf_url'].notna() & df_merged['rp_url'].notna()]
#for index, row in df_filtered.iterrows():
#    pass

driver3 = webdriver.Chrome(service=service, options=chrome_options)
#driver3.get(row['rp_url'])
driver3.get('https://greyhoundbet.racingpost.com/#card/race_id=2070195&r_date=2024-07-28&tab=card')
#driver3.get('https://greyhoundbet.racingpost.com/#card/race_id=2070928&r_date=2024-07-28&tab=card')
driver3.implicitly_wait(5)
src3 = driver3.find_element(By.XPATH, "//div[@class='webAppPage card']").get_attribute('outerHTML')
rp_soup = BeautifulSoup(src3, 'html.parser')
driver3.quit()

driver4 = webdriver.Chrome(service=service, options=chrome_options)    
#driver4.get(row['tf_url'])
driver4.get('https://www.timeform.com/greyhound-racing/racecards/towcester/2052/2024-07-28/1211053')
#driver4.get('https://www.timeform.com/greyhound-racing/racecards/yarmouth/1152/2024-07-28/1211882')
driver4.implicitly_wait(5)
src4 = driver4.find_element(By.XPATH, "//section[@class='mb-bfw-racecard mb-bfw']").get_attribute('outerHTML')
tf_soup = BeautifulSoup(src4, 'html.parser')
driver4.quit()

div_track = rp_soup.find('div', class_='pageHeader')
rp_track = str(rp_soup.find('h2').get_text()).strip()
tf_track = str(tf_soup.find('div', class_='rph-meeting-details-track hover-opacity').get_text()).strip()

rp_post_pick = str(rp_soup.find('p', class_='p2').get_text()).strip()
numbers_string = rp_post_pick.replace("POST PICK: ", "")
rp_tricast = numbers_string.split('-')
div_element = tf_soup.find('div', class_='rpf-verdict-tri')
img_elements = div_element.find_all('img', class_='rpf-verdict-selection-trap')
tf_tricast = [img.get('alt') for img in img_elements]

rp_race_details = str(rp_soup.find('p', class_='p1').get_text()).strip()
match = re.search(r'Race (\d+)', rp_race_details)
rp_race = match.group(1) if match else None
tf_race_details = str(tf_soup.find('div', class_='rph-race-details w-content rp-content rp-setting-race-details').get_text()).strip()
match = re.search(r'\(R(\d+)\)', tf_race_details)
tf_race = match.group(1) if match else None

rp_grade_split = rp_race_details.split(' - ')
rp_grade_split = rp_grade_split[0].replace('\xa0', ' ')
rp_grade_split = rp_grade_split.split(' ')
#match = re.search(r'Race\s+\d+\s+([A-Za-z0-9]+)\s+-', rp_race_details)
#rp_grade = match.group(1) if match else None
rp_grade = rp_grade_split[-1]
match = re.search(r'Grade:\s*\((.*?)\)', tf_race_details)
tf_grade = match.group(1) if match else None

match = re.search(r'-\s+(\d+)m', rp_race_details)
rp_distance = match.group(1) if match else None
match = re.search(r'Distance:\s*(\d+)m', tf_race_details)
tf_distance = match.group(1) if match else None

print()
print('Track:')
print(f'RP: {rp_track}')
print(f'TF: {tf_track}')
print()
print('Tricast:')
print(f'RP: {rp_tricast}')
print(f'TF: {tf_tricast}')
print()
print('Race:')
print(f'RP: {rp_race}')
print(f'TF: {tf_race}')
print()
print('Grade:')
print(f'RP: {rp_grade}')
print(f'TF: {tf_grade}')
print()
print('Distance:')
print(f'RP: {rp_distance}')
print(f'TF: {tf_distance}')
print('-------------------')
print()

rp_race_card = rp_soup.find('div', class_='level level-5 card')
rp_runner_name = rp_race_card.find_all('div', class_='runner')
rp_runner_comment = rp_race_card.find_all('p', class_='comment')
rp_runner_tbody = rp_race_card.find_all('tbody')

tf_race_card = tf_soup.find('section', class_='rp-body rp-container rpb-container')
tf_runner_details1 = tf_race_card.find_all('tr', class_='rpb-entry-details rpb-entry-details-1')
tf_runner_details2 = tf_race_card.find_all('tr', class_='rpb-entry-details rpb-entry-details-2')
tf_runner_entry_details = tf_race_card.find_all('tr', class_='rpb-hint-details rpb-content rp-setting-entry-details')

if len(rp_runner_name) == len(rp_runner_comment) == len(rp_runner_tbody) == len(tf_runner_details1) == len(tf_runner_details2) == len(tf_runner_entry_details):
    for detail1, detail2, detail3, detail4, detail5, detail6 in zip(rp_runner_name, rp_runner_comment, rp_runner_tbody, tf_runner_details1, tf_runner_details2, tf_runner_entry_details):
        runner_content = str(detail1.prettify()) + str(detail2.prettify()) + str(detail3.prettify()) + str(detail4.prettify()) + str(detail5.prettify()) + str(detail6.prettify())

        rp_race_dog_name_res = re.search(r'<strong>\s*([^<]*?)\s*(?:<|$)', runner_content, re.DOTALL)
        if rp_race_dog_name_res:
            rp_race_dog_name = rp_race_dog_name_res.group(1).strip()
            rp_race_dog_name = capitalize_words(rp_race_dog_name)
        tf_race_dog_name_res = re.search(r'<td>\s*<a[^>]*>\s*(.*?)\s*</a>', runner_content, re.DOTALL)
        if tf_race_dog_name_res:
            tf_race_dog_name = tf_race_dog_name_res.group(1).strip()
            tf_race_dog_name = capitalize_words(tf_race_dog_name)

        rp_race_comment_res = re.search(r'<p\s+class="comment">\s*(.*?)\s*</p>', runner_content, re.DOTALL)
        if rp_race_dog_name_res:
            rp_race_comment = rp_race_comment_res.group(1).strip()
        tf_race_comment_res = re.search(r'<td>\s*<a[^>]*>\s*(.*?)\s*</a>', runner_content, re.DOTALL)
        if tf_race_dog_name_res:
            tf_race_comment = tf_race_comment_res.group(1).strip()
        #rp_race_dog_name = str(runner_content.find('div', class_='runner').get_text()).strip()
        #tf_race_dog_name = str(runner_content.find('a', class_='rpb-greyhound rpb-greyhound-1 hover-opacity').get_text()).strip()

        #print()
        #print('Nome:')
        #print(f'RP: {rp_race_dog_name}')
        #print(f'TF: {tf_race_dog_name}')
        #print()
        #print('Comment:')
        #print(f'RP: {rp_race_comment}')
        #print(f'TF: {tf_race_comment}')
        #print()
        print(runner_content)
        #print()

#nan_tf_url = df_merged['tf_url'].isna().sum()
#nan_rp_url = df_merged['rp_url'].isna().sum()
#print(f'df_rp: {df_rp.count().max()} df_tf: {df_tf.count().max()}')
#print(f"df_merged: {df_merged.count().max()} tf_url: {nan_tf_url} tf_url: {nan_rp_url}")

#df_tf = df_merged[df_merged['_merge'] == 'left_only'].drop(['_merge', 'rp_id', 'rp_url'], axis=1)
#df_tf = df_tf.reset_index(drop=True)
#df_rp = df_merged[df_merged['_merge'] == 'right_only'].drop(['_merge', 'tf_id', 'tf_url'], axis=1)
#df_rp = df_rp.reset_index(drop=True)
#df_merged = df_merged.loc[df_merged['_merge'] == 'both']
#df_merged = df_merged.drop('_merge', axis=1)
#df_merged = df_merged.reset_index(drop=True)

#df_merged = df_merged.drop_duplicates(subset=['dia', 'hora', 'track', 'tf_id', 'tf_url', 'rp_id', 'rp_url'])

# Mostar o nome das Pistas do site Timeform
#for track_value in df_timeform['track'].unique():
#    print(track_value)

# Mostrar o link das corridas que o estadio estiver como NaN
#rp_nan = df_rp.loc[df_rp['track'].isna(), ['rp_url']]
#for url in rp_nan['rp_url']:
#    print(url)

#if not df_merged.empty:
#    ignored_count = 0
#    for index, row in df_merged.iterrows():
#        exists_query = session.query(exists().where(
#            (RaceToScam.dia == row['dia']) &
#            (RaceToScam.hora == row['hora']) &
#            (RaceToScam.track == row['track']) &
#            (RaceToScam.tf_id == row['tf_id']) &
#            (RaceToScam.tf_url == row['tf_url']) &
#            (RaceToScam.rp_id == row['rp_id']) &
#            (RaceToScam.rp_url == row['rp_url'])
#        )).scalar()
#        if not exists_query:
#            link = RaceToScam(
#                dia=row['dia'],
#                hora=row['hora'],
#                track=row['track'],
#                tf_id=row['tf_id'],
#                tf_url=row['tf_url'],
#                tf_scanned=False,
#                rp_id=row['rp_id'],
#                rp_url=row['rp_url'],
#                rp_scanned=False
#            )
#            session.add(link)
#            session.commit()
#        else:
#            ignored_count += 1
#    if ignored_count > 0:
#        logging.info(f'Número de link combinados, que serão ignorados: {ignored_count}')
#        print(f'Número de link combinados, que serão ignorados: {ignored_count}')
#else:
#    logging.info('O DataFrame df_merged está vazio. Não há dados para inserir.')
#    print('O DataFrame df_merged está vazio. Não há dados para inserir.')
#print('')

# Confirma a transação e fecha conexão.
#session.commit()
#session.close()

print()

logging.info(f'Data escaneada: {today}')
print(f'Data escaneada: {today}')
logging.info('Finalizado!')
print('Finalizado!')

end_time = time.time()
execution_time = end_time - start_time
logging.info(f'Tempo de execução: {execution_time} segundos')
print(f'Tempo de execução: {execution_time} segundos')

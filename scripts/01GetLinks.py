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

#sql = text("""
#    INSERT INTO lastdate (dia, scanned)
#    SELECT dates.date, false
#    FROM generate_series('1997-01-01'::date, CURRENT_DATE, '1 day'::interval) AS dates(date)
#    WHERE NOT EXISTS (
#        SELECT 1 FROM lastdate WHERE dia = dates.date
#    )
#    ON CONFLICT (dia) DO NOTHING;
#""")

#session.execute(sql)
#session.commit()

def capitalize_words(sentence):
    words = sentence.split()
    capitalized_words = [word.capitalize() for word in words]
    capitalized_sentence = ' '.join(capitalized_words)
    parts = capitalized_sentence.split("'")
    for i in range(len(parts)):
        parts[i] = parts[i][0].capitalize() + parts[i][1:]
    return "'".join(parts)

def extract_links_html(html_source):
    soup = BeautifulSoup(html_source, 'html.parser')
    links = set()
    for a_tag in soup.find_all('a', href=True):
        links.add(a_tag['href'])
    return list(links)

def extract_links_html2(html_source):
    soup = BeautifulSoup(html_source, 'html.parser')
    links = set()
    for a_tag in soup.find_all('a', class_='waf-header hover-opacity', href=True):
        links.add(a_tag['href'])
    return list(links)

today = datetime.now().date()
if not session.query(session.query(LastDate).filter(LastDate.dia == today).exists()).scalar():
    new_date = LastDate(dia=today, scanned=False)
    session.add(new_date)
    session.commit()

yesterday = today - timedelta(days=1)
if not session.query(session.query(LastDate).filter(LastDate.dia == yesterday).exists()).scalar():
    new_date = LastDate(dia=yesterday, scanned=False)
    session.add(new_date)
    session.commit()

tomorrow = today + timedelta(days=1)
if not session.query(session.query(LastDate).filter(LastDate.dia == tomorrow).exists()).scalar():
    new_date = LastDate(dia=tomorrow, scanned=False)
    session.add(new_date)
    session.commit()

scanned_date = session.query(func.min(LastDate.dia)).filter(LastDate.scanned == False).scalar()
if not scanned_date or scanned_date > today:
    logging.info('Todos os dias na tabela foram escaneados!')
    print('Todos os dias na tabela foram escaneados!')
    sys.exit('Encerrado por não possuir dia para escanear!')
else:
    session.query(LastDate).filter(LastDate.dia == scanned_date).update({LastDate.scanned: True})
    session.commit()

# Configura o logger para escrever logs em um arquivo com nível INFO
logging.basicConfig(filename=f'{log_dir}/{scanned_date}-GetLinks.log', 
                    format='%(asctime)s %(message)s', 
                    filemode='w',
                    level=logging.INFO,
                    encoding='utf-8')
logging.info(f'Dia escaneado: {scanned_date}')
print(f'Dia escaneado: {scanned_date}')

rp_lista = []
tf_lista = []

driver1 = webdriver.Chrome(service=service, options=chrome_options)
rp_href = f'https://greyhoundbet.racingpost.com/#results-list/r_date={scanned_date}'
driver1.get(rp_href)
driver1.implicitly_wait(5)
src1 = driver1.find_element(By.XPATH, "//div[@class='scrollContent']").get_attribute('outerHTML')
pattern1 = re.compile(r'(#result-meeting-result/race_id=\d+&amp;track_id=\d+&amp;r_date=[\d-]+&amp;r_time=[\d:]+)')
links1 = pattern1.findall(src1)
logging.info(f'Racingpost Link: {rp_href}')
print(f'Racingpost Link: {rp_href}')

rp_vazio = tf_vazio = False

if not links1:
    logging.info(f'Não localizou nenhum link no site Racingpost.')
    rp_vazio = True
else:
    for link1 in links1:
        match = re.match(r'#result-meeting-result\/race_id=(\d+)&amp;track_id=(\d+)&amp;r_date=(\d{4}-\d{2}-\d{2})&amp;r_time=(\d{2}:\d{2})', link1)
        if match:
            rp_id, track, dia, hora = match.groups()
            link1 = link1.replace('&amp;', '&')
            rp_url = 'https://greyhoundbet.racingpost.com/' + link1
            rp_lista.append([dia, hora, track, rp_id, rp_url])
        else:
            logging.info(f' URL: {rp_url} não corresponde ao padrão esperado.')
            print(f' URL: {rp_url} não corresponde ao padrão esperado.')
    df_rp = pd.DataFrame(rp_lista, columns=['dia', 'hora', 'track', 'rp_id', 'rp_url'])
    df_rp = df_rp.drop_duplicates(subset=['dia', 'hora', 'track', 'rp_id', 'rp_url'])
    df_rp['track'] = df_rp['track'].map(estadio)
        
    # Extrair links do HTML
    links = extract_links_html(src1)

    # Iterar sobre cada link e salvar no banco de dados
    for link_href in links:
        exists_query = session.query(exists().where(
            (PageSource.dia == scanned_date) &
            (PageSource.url == rp_href) &
            (PageSource.site == 'rp') &
            (PageSource.scanned_level == 'obter_links') &
            (PageSource.html_source == link_href)
        )).scalar()
        
        if not exists_query and re.match(r'^#result-meeting-result/race_id=\d+&track_id=\d+&r_date=\d+-\d+-\d+&r_time=\d+:\d+', link_href):
            link = PageSource(
                dia=scanned_date,
                url=rp_href,
                site='rp',
                scanned_level='obter_links',
                html_source=link_href
            )
            session.add(link)

    # Confirmar as alterações no banco de dados
    session.commit()
    session.close()
driver1.quit()

driver2 = webdriver.Chrome(service=service, options=chrome_options)
tf_href = f'https://www.timeform.com/greyhound-racing/results/{scanned_date}'
driver2.get(tf_href)

target_string = 'For data, please visit https://www.globalsportsapi.com/'
max_retries = 5
retry_count = 0

while retry_count < max_retries:
    body_text = driver2.find_element(By.TAG_NAME, 'body').text
    if body_text == target_string:
        print(f'String encontrada, aguardando 5 segundos... (Tentativa {retry_count+1}/{max_retries})')
        time.sleep(5)
        retry_count += 1
        driver2.refresh()
    else:
        print('String não encontrada, continuando com o script...')
        driver2.implicitly_wait(3)
        src2 = driver2.find_element(By.XPATH, "//section[@class='w-archive-full']").get_attribute('outerHTML')
        pattern2 = re.compile(r'(/results/[\w-]+/\d+/[\d-]+/\d+)')
        links2 = pattern2.findall(src2)
        break
logging.info(f'Timeform Link: {tf_href}')
print(f'Timeform Link: {tf_href}')

if not links2:
    logging.info(f'Não localizou nenhum link no site Timeform.')
    tf_vazio = True
else:
    for link2 in links2:
        match = re.match(r'\/results\/(.+)\/(\d+)\/(\d{4}-\d{2}-\d{2})\/(\d+)', link2)
        if match:
            track, hora, dia, tf_id = match.groups()
            track = capitalize_words(track)
            # Tratar o formato do tempo
            if len(hora) == 3:
                hora = "0" + hora[0] + ":" + hora[1:]
            else:
                hora = hora[:2] + ":" + hora[2:]
            tf_url = 'https://www.timeform.com/greyhound-racing' + link2
            tf_lista.append([dia, hora, track, tf_id, tf_url])
        else:
            logging.info(f'URL: {tf_url} não corresponde ao padrão esperado.')
            print(f'URL: {tf_url} não corresponde ao padrão esperado.')
    df_tf = pd.DataFrame(tf_lista, columns=['dia', 'hora', 'track', 'tf_id', 'tf_url'])
    df_tf = df_tf.drop_duplicates(subset=['dia', 'hora', 'track', 'tf_id', 'tf_url'])
        
    # Extrair links do HTML
    links = extract_links_html2(src2)

    # Iterar sobre cada link e salvar no banco de dados
    for link_href in links:
        exists_query = session.query(exists().where(
            (PageSource.dia == scanned_date) &
            (PageSource.url == tf_href) &
            (PageSource.site == 'tf') &
            (PageSource.scanned_level == 'obter_links') &
            (PageSource.html_source == link_href)
        )).scalar()
        
        if not exists_query and re.match(r'^/greyhound-racing/results/\w+/\d+/\d+-\d+-\d+/\d+', link_href):
            link = PageSource(
                dia=scanned_date,
                url=tf_href,
                site='tf',
                scanned_level='obter_links',
                html_source=link_href
            )
            session.add(link)

    # Confirmar as alterações no banco de dados
    session.commit()
    session.close()
driver2.quit()

print('')

if rp_vazio == True and tf_vazio == True:
    logging.info('Não há links que sejam compativeis com a regex nos dois sites.')
    print('Não há links que sejam compativeis com a regex nos dois sites.')
    session.commit()
    session.close()
    sys.exit('Encerrado por não possuir dados para inserir no banco!')

if rp_vazio == False and tf_vazio == False:
    df_merged = pd.merge(df_tf, df_rp, on=['dia', 'hora', 'track'], how='outer', indicator=True)

    #for var in df_merged['track'].unique():
    #    filtrado = df_merged[df_merged['track'] == var].sort_values(by=['dia', 'hora'])
    #    print(filtrado)

    df_tf = df_merged[df_merged['_merge'] == 'left_only'].drop(['_merge', 'rp_id', 'rp_url'], axis=1)
    df_tf = df_tf.reset_index(drop=True)
    df_rp = df_merged[df_merged['_merge'] == 'right_only'].drop(['_merge', 'tf_id', 'tf_url'], axis=1)
    df_rp = df_rp.reset_index(drop=True)
    df_merged = df_merged.loc[df_merged['_merge'] == 'both']
    df_merged = df_merged.drop('_merge', axis=1)
    df_merged = df_merged.reset_index(drop=True)

    df_merged = df_merged.drop_duplicates(subset=['dia', 'hora', 'track', 'tf_id', 'tf_url', 'rp_id', 'rp_url'])

    # Mostar o nome das Pistas do site Timeform
    #for track_value in df_timeform['track'].unique():
    #    print(track_value)

    # Mostrar o link das corridas que o estadio estiver como NaN
    rp_nan = df_rp.loc[df_rp['track'].isna(), ['rp_url']]
    for url in rp_nan['rp_url']:
        print(url)

    if not df_merged.empty:
        ignored_count = 0
        for index, row in df_merged.iterrows():
            exists_query = session.query(exists().where(
                (RaceToScam.dia == row['dia']) &
                (RaceToScam.hora == row['hora']) &
                (RaceToScam.track == row['track']) &
                (RaceToScam.tf_id == row['tf_id']) &
                (RaceToScam.tf_url == row['tf_url']) &
                (RaceToScam.rp_id == row['rp_id']) &
                (RaceToScam.rp_url == row['rp_url'])
            )).scalar()
            if not exists_query:
                link = RaceToScam(
                    dia=row['dia'],
                    hora=row['hora'],
                    track=row['track'],
                    tf_id=row['tf_id'],
                    tf_url=row['tf_url'],
                    tf_scanned=False,
                    rp_id=row['rp_id'],
                    rp_url=row['rp_url'],
                    rp_scanned=False
                )
                session.add(link)
                session.commit()
            else:
                ignored_count += 1
        if ignored_count > 0:
            logging.info(f'Número de link combinados, que serão ignorados: {ignored_count}')
            print(f'Número de link combinados, que serão ignorados: {ignored_count}')
    else:
        logging.info('O DataFrame df_merged está vazio. Não há dados para inserir.')
        print('O DataFrame df_merged está vazio. Não há dados para inserir.')
    print('')

if rp_vazio == False:
    logging.info('Há links compativeis com a regex do site RacingPost.')
    print('Há links compativeis com a regex do site RacingPost.')
    if not df_rp.empty:
        ignored_count = 0
        for index, row in df_rp.iterrows():
            exists_query = session.query(exists().where(
                (RaceToScamSemPar.dia == row['dia']) &
                (RaceToScamSemPar.hora == row['hora']) &
                (RaceToScamSemPar.track == row['track']) &
                (RaceToScamSemPar.site_id == row['rp_id']) &
                (RaceToScamSemPar.site_url == row['rp_url'])
            )).scalar()
            if not exists_query:
                link = RaceToScamSemPar(
                    dia=row['dia'],
                    hora=row['hora'],
                    track=row['track'],
                    site='rp',
                    site_id=row['rp_id'],
                    site_url=row['rp_url'],
                    scanned=False
                )
                session.add(link)
                session.commit()
            else:
                ignored_count += 1
        if ignored_count > 0:
            logging.info(f'Número de link do site Racingpost, que serão ignorados: {ignored_count}')
            print(f'Número de link do site Racingpost, que serão ignorados: {ignored_count}')
    else:
        logging.info('O DataFrame racingpost está vazio. Não há dados para inserir.')
        print('O DataFrame racingpost está vazio. Não há dados para inserir.')
    print('')

if tf_vazio == False:
    logging.info('Há links compativeis com a regex do site TimeForm.')
    print('Há links compativeis com a regex do site TimeForm.')
    if not df_tf.empty:
        ignored_count = 0
        for index, row in df_tf.iterrows():
            exists_query = session.query(exists().where(
                (RaceToScamSemPar.dia == row['dia']) &
                (RaceToScamSemPar.hora == row['hora']) &
                (RaceToScamSemPar.track == row['track']) &
                (RaceToScamSemPar.site_id == row['tf_id']) &
                (RaceToScamSemPar.site_url == row['tf_url'])
            )).scalar()
            if not exists_query:
                link = RaceToScamSemPar(
                    dia=row['dia'],
                    hora=row['hora'],
                    track=row['track'],
                    site='tf',
                    site_id=row['tf_id'],
                    site_url=row['tf_url'],
                    scanned=False
                )
                session.add(link)
                session.commit()
            else:
                ignored_count += 1
        if ignored_count > 0:
            logging.info(f'Número de link do site Timeform, que serão ignorados: {ignored_count}')
            print(f'Número de link do site Timeform, que serão ignorados: {ignored_count}')
    else:
        logging.info('O DataFrame timeform está vazio. Não há dados para inserir.')
        print('O DataFrame timeform está vazio. Não há dados para inserir.')
    print('')

# Confirma a transação e fecha conexão.
session.commit()
session.close()

logging.info(f'Data escaneada: {scanned_date}')
print(f'Data escaneada: {scanned_date}')
logging.info('Finalizado!')
print('Finalizado!')

end_time = time.time()
execution_time = end_time - start_time
logging.info(f'Tempo de execução: {execution_time} segundos')
print(f'Tempo de execução: {execution_time} segundos')

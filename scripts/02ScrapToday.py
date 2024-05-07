import re, logging, time, sys, os, platform
import pandas as pd
from db import connect
from selenium import webdriver
from datetime import datetime
from sqlalchemy.exc import IntegrityError
from selenium.webdriver.common.by import By
from sqlalchemy import create_engine, exists, update
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from sqlalchemy.orm import sessionmaker, declarative_base
from tables import Base, engine, LastDate, LinksToScam, LinksToScamSemPar, PageSource

# Verifica o sistema operacional
if platform.system() == 'Windows':
    log_dir = '../logs'
elif platform.system() == 'Linux':
    log_dir = '/home/maicon/galgowebscrap/logs'
else:
    print('Sistema operacional não reconhecido')

# Cria as tabelas
Base.metadata.create_all(engine)

options = Options()
options.add_argument('--headless')
options.add_argument('log-level=3')
options.add_argument('--disable-dev-shm-usage')

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

def get_today(session):
    try:
        # Verifica se a data de hoje já existe na tabela LastDate
        today = datetime.now().date()
        existing_date = session.query(LastDate).filter(LastDate.dia == today).first()
        if existing_date:
            return today  # Retorna a data de hoje se ela já existir na tabela

        # Se a data de hoje não existir, insere-a na tabela LastDate com scanned=True
        new_date = LastDate(dia=today, scanned=True)
        session.add(new_date)
        session.commit()
        return today  # Retorna a data de hoje após a inserção bem-sucedida
    except Exception as e:
        print(f"Erro ao inserir data: {e}")
        session.rollback()

def capitalize_words(sentence):
    words = sentence.split()
    capitalized_words = [word.capitalize() for word in words]
    return ' '.join(capitalized_words)

racing_date = get_today(session)

# Configura o logger para escrever logs em um arquivo com nível INFO
logging.basicConfig(filename=f'{log_dir}/{racing_date}_02ScrapToday.log', 
                    format='%(asctime)s %(message)s', 
                    filemode='w',
                    level=logging.INFO,
                    encoding='utf-8')

logging.info(f'Racing_date: {racing_date}')

rp_lista = []
tf_lista = []
source_lista = []

start_time = time.time()
driver1 = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
rp_url = f'https://greyhoundbet.racingpost.com/#results-list/r_date={racing_date}'
driver1.get(rp_url)
driver1.implicitly_wait(5)
logging.info(f'Racingpost Link: {rp_url}')
logging.info(f'')

src1 = driver1.find_element(By.XPATH, "//div[@class='scrollContent']").get_attribute('outerHTML')
pattern1 = re.compile(r'(#result-meeting-result/race_id=\d+&amp;track_id=\d+&amp;r_date=[\d-]+&amp;r_time=[\d:]+)')
links1 = pattern1.findall(src1)

rp_vazio = tf_vazio = False

if not links1:
    logging.info(f'Não localizou nenhum link no site Racingpost.')
    rp_vazio = True
else:
    for link1 in links1:
        match = re.match(r'#result-meeting-result\/race_id=(\d+)&amp;track_id=(\d+)&amp;r_date=(\d{4}-\d{2}-\d{2})&amp;r_time=(\d{2}:\d{2})', link1)
        if match:
            racingpost_id, track, dia, hora = match.groups()
            link1 = link1.replace('&amp;', '&')
            racingpost_url = 'https://greyhoundbet.racingpost.com/' + link1
            rp_lista.append([dia, hora, track, racingpost_id, racingpost_url])
        else:
            logging.info(f' URL: {racingpost_url} não corresponde ao padrão esperado.')
    driver1.quit()
    df_racingpost = pd.DataFrame(rp_lista, columns=['dia', 'hora', 'track', 'racingpost_id', 'racingpost_url'])
    df_racingpost = df_racingpost.drop_duplicates(subset=['dia', 'hora', 'track', 'racingpost_id', 'racingpost_url'])
    df_racingpost['track'] = df_racingpost['track'].map(estadio)

    # Verifica se a linha já existe no banco de dados
    exists_query = session.query(exists().where(
        (PageSource.dia == dia) &
        (PageSource.url == rp_url) &
        (PageSource.site == 'rp') &
        (PageSource.scanned_level == 'obter_links') &
        (PageSource.html_source == src1)
    )).scalar()

    if not exists_query:
        link = PageSource(
            dia=dia,
            url=rp_url,
            site='rp',
            scanned_level='obter_links',
            html_source=src1
        )
        session.add(link)

driver2 = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
tf_url = f'https://www.timeform.com/greyhound-racing/results/{racing_date}'
driver2.get(tf_url)
driver2.implicitly_wait(3)
logging.info(f'Timeform Link: {tf_url}')
logging.info(f'')

src2 = driver2.find_element(By.XPATH, "//section[@class='w-archive-full']").get_attribute('outerHTML')
pattern2 = re.compile(r'(/results/[\w-]+/\d+/[\d-]+/\d+)')
links2 = pattern2.findall(src2)

if not links2:
    logging.info(f'Não localizou nenhum link no site Timeform.')
    tf_vazio = True
else:
    for link2 in links2:
        match = re.match(r'\/results\/(.+)\/(\d+)\/(\d{4}-\d{2}-\d{2})\/(\d+)', link2)
        if match:
            track, hora, dia, timeform_id = match.groups()
            track = capitalize_words(track)
            # Tratar o formato do tempo
            if len(hora) == 3:
                hora = "0" + hora[0] + ":" + hora[1:]
            else:
                hora = hora[:2] + ":" + hora[2:]
            timeform_url = 'https://www.timeform.com' + link2
            tf_lista.append([dia, hora, track, timeform_id, timeform_url])
        else:
            logging.info(f'URL: {timeform_url} não corresponde ao padrão esperado.')
    driver2.quit()
    df_timeform = pd.DataFrame(tf_lista, columns=['dia', 'hora', 'track', 'timeform_id', 'timeform_url'])
    df_timeform = df_timeform.drop_duplicates(subset=['dia', 'hora', 'track', 'timeform_id', 'timeform_url'])

    # Verifica se a linha já existe no banco de dados
    exists_query = session.query(exists().where(
        (PageSource.dia == dia) &
        (PageSource.url == tf_url) &
        (PageSource.site == 'tf') &
        (PageSource.scanned_level == 'obter_links') &
        (PageSource.html_source == src2)
    )).scalar()

    if not exists_query:
        link = PageSource(
            dia=dia,
            url=tf_url,
            site='tf',
            scanned_level='obter_links',
            html_source=src2
        )
        session.add(link)

if rp_vazio == True and tf_vazio == True:
    pass

if rp_vazio == True and tf_vazio == False:
    if not df_timeform.empty:
        # Itera sobre as linhas do DataFrame e insere na tabela
        ignored_count = 0
        for index, row in df_timeform.iterrows():
            # Verifica se a linha já existe no banco de dados
            exists_query = session.query(exists().where(
                (LinksToScamSemPar.dia == row['dia']) &
                (LinksToScamSemPar.hora == row['hora']) &
                (LinksToScamSemPar.track == row['track']) &
                (LinksToScamSemPar.site_id == row['timeform_id']) &
                (LinksToScamSemPar.site_url == row['timeform_url'])
            )).scalar()

            if not exists_query:
                link = LinksToScamSemPar(
                    dia=row['dia'],
                    hora=row['hora'],
                    track=row['track'],
                    site='tf',
                    site_id=row['timeform_id'],
                    site_url=row['timeform_url'],
                    scanned=False
                )
                session.add(link)
            else:
                ignored_count += 1
        if ignored_count > 0:
            logging.info(f'Número de link do site Timeform, que serão ignorados: {ignored_count}')
    else:
        logging.info('O DataFrame timeform está vazio. Não há dados para inserir.')

if rp_vazio == False and tf_vazio == True:
    if not df_racingpost.empty:
        # Itera sobre as linhas do DataFrame e insere na tabela
        ignored_count = 0
        for index, row in df_racingpost.iterrows():
            # Verifica se a linha já existe no banco de dados
            exists_query = session.query(exists().where(
                (LinksToScamSemPar.dia == row['dia']) &
                (LinksToScamSemPar.hora == row['hora']) &
                (LinksToScamSemPar.track == row['track']) &
                (LinksToScamSemPar.site_id == row['racingpost_id']) &
                (LinksToScamSemPar.site_url == row['racingpost_url'])
            )).scalar()

            if not exists_query:
                link = LinksToScamSemPar(
                    dia=row['dia'],
                    hora=row['hora'],
                    track=row['track'],
                    site='rp',
                    site_id=row['racingpost_id'],
                    site_url=row['racingpost_url'],
                    scanned=False
                )
                session.add(link)
            else:
                ignored_count += 1
        if ignored_count > 0:
            logging.info(f'Número de link do site Racingpost, que serão ignorados: {ignored_count}')
    else:
        logging.info('O DataFrame racingpost está vazio. Não há dados para inserir.')
else:
    # Realizar a mesclagem com indicador
    df_merged = pd.merge(df_timeform, df_racingpost, on=['dia', 'hora', 'track'], how='outer', indicator=True)

    # Filtrar as linhas que estão apenas em df_timeform
    timeform = df_merged[df_merged['_merge'] == 'left_only'].drop(['_merge', 'racingpost_id', 'racingpost_url'], axis=1)
    timeform = timeform.reset_index(drop=True)

    # Filtrar as linhas que estão apenas em df_racingpost
    racingpost = df_merged[df_merged['_merge'] == 'right_only'].drop(['_merge', 'timeform_id', 'timeform_url'], axis=1)
    racingpost = racingpost.reset_index(drop=True)

    # Filtrar as linhas onde '_merge' é igual a 'both'
    df_merged = df_merged.loc[df_merged['_merge'] == 'both']

    # Remover a coluna '_merge'
    df_merged = df_merged.drop('_merge', axis=1)
    df_merged = df_merged.reset_index(drop=True)

    # Remove registros duplicados com base nas colunas 'date', 'time', 'track', 'timeform_id' e 'timeform_url'
    df_merged = df_merged.drop_duplicates(subset=['dia', 'hora', 'track', 'timeform_id', 'timeform_url', 'racingpost_id', 'racingpost_url'])

    # Mostar o nome das Pistas do site Timeform
    #for track_value in df_timeform['track'].unique():
    #    print(track_value)

    # Mostrar o link das corridas que o estadio estiver como NaN
    rp_nan = racingpost.loc[racingpost['track'].isna(), ['racingpost_url']]
    for url in rp_nan['racingpost_url']:
        print(url)

    if not df_merged.empty:
        # Itera sobre as linhas do DataFrame e insere na tabela
        ignored_count = 0
        for index, row in df_merged.iterrows():
            # Verifica se a linha já existe no banco de dados
            exists_query = session.query(exists().where(
                (LinksToScam.dia == row['dia']) &
                (LinksToScam.hora == row['hora']) &
                (LinksToScam.track == row['track']) &
                (LinksToScam.timeform_id == row['timeform_id']) &
                (LinksToScam.timeform_url == row['timeform_url']) &
                (LinksToScam.racingpost_id == row['racingpost_id']) &
                (LinksToScam.racingpost_url == row['racingpost_url'])
            )).scalar()

            if not exists_query:
                link = LinksToScam(
                    dia=row['dia'],
                    hora=row['hora'],
                    track=row['track'],
                    timeform_id=row['timeform_id'],
                    timeform_url=row['timeform_url'],
                    tf_scanned=False,
                    racingpost_id=row['racingpost_id'],
                    racingpost_url=row['racingpost_url'],
                    rp_scanned=False
                )
                session.add(link)
            else:
                ignored_count += 1
        if ignored_count > 0:
            logging.info(f'Número de link combinados, que serão ignorados: {ignored_count}')
    else:
        logging.info('O DataFrame df_merged está vazio. Não há dados para inserir.')

    if not racingpost.empty:
        # Itera sobre as linhas do DataFrame e insere na tabela
        ignored_count = 0
        for index, row in racingpost.iterrows():
            # Verifica se a linha já existe no banco de dados
            exists_query = session.query(exists().where(
                (LinksToScamSemPar.dia == row['dia']) &
                (LinksToScamSemPar.hora == row['hora']) &
                (LinksToScamSemPar.track == row['track']) &
                (LinksToScamSemPar.site_id == row['racingpost_id']) &
                (LinksToScamSemPar.site_url == row['racingpost_url'])
            )).scalar()

            if not exists_query:
                link = LinksToScamSemPar(
                    dia=row['dia'],
                    hora=row['hora'],
                    track=row['track'],
                    site='rp',
                    site_id=row['racingpost_id'],
                    site_url=row['racingpost_url'],
                    scanned=False
                )
                session.add(link)
            else:
                ignored_count += 1

        if ignored_count > 0:
            logging.info(f'Número de link do site Racingpost, que serão ignorados: {ignored_count}')
    else:
        logging.info('O DataFrame racingpost está vazio. Não há dados para inserir.')

    if not timeform.empty:
        # Itera sobre as linhas do DataFrame e insere na tabela
        ignored_count = 0
        for index, row in timeform.iterrows():
            # Verifica se a linha já existe no banco de dados
            exists_query = session.query(exists().where(
                (LinksToScamSemPar.dia == row['dia']) &
                (LinksToScamSemPar.hora == row['hora']) &
                (LinksToScamSemPar.track == row['track']) &
                (LinksToScamSemPar.site_id == row['timeform_id']) &
                (LinksToScamSemPar.site_url == row['timeform_url'])
            )).scalar()

            if not exists_query:
                link = LinksToScamSemPar(
                    dia=row['dia'],
                    hora=row['hora'],
                    track=row['track'],
                    site='tf',
                    site_id=row['timeform_id'],
                    site_url=row['timeform_url'],
                    scanned=False
                )
                session.add(link)
            else:
                ignored_count += 1
        if ignored_count > 0:
            logging.info(f'Número de link do site Timeform, que serão ignorados: {ignored_count}')
    else:
        logging.info('O DataFrame timeform está vazio. Não há dados para inserir.')

# Confirma a transação
session.commit()
# Fecha a sessão
session.close()

logging.info(f'Data escaneada: {racing_date}')

logging.info('Finalizado!')
end_time = time.time()
execution_time = end_time - start_time
logging.info(f'Tempo de execução: {execution_time} segundos')
logging.info('')
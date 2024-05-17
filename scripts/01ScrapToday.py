import re, logging, time, platform
import pandas as pd
from db import connect
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from sqlalchemy import create_engine, exists
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from sqlalchemy.orm import sessionmaker, declarative_base
from tables import Base, engine, LastDate, RaceToScam, RaceToScamSemPar, PageSource

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
    capitalized_sentence = ' '.join(capitalized_words)
    parts = capitalized_sentence.split("'")
    for i in range(len(parts)):
        parts[i] = parts[i][0].capitalize() + parts[i][1:]
    return "'".join(parts)

racing_date = get_today(session)

# Configura o logger para escrever logs em um arquivo com nível INFO
logging.basicConfig(filename=f'{log_dir}/{racing_date}-01ScrapToday.log', 
                    format='%(asctime)s %(message)s', 
                    filemode='w',
                    level=logging.INFO,
                    encoding='utf-8')
logging.info(f'Dia escaneado: {racing_date}')
print(f'Dia escaneado: {racing_date}')

rp_lista = []
tf_lista = []

start_time = time.time()

driver1 = driver2 = webdriver.Chrome(service=service, options=chrome_options)
rp_href = f'https://greyhoundbet.racingpost.com/#results-list/r_date={racing_date}'
driver1.get(rp_href)
driver1.implicitly_wait(5)
logging.info(f'Racingpost Link: {rp_href}')
src1 = driver1.find_element(By.XPATH, "//div[@class='scrollContent']").get_attribute('outerHTML')
pattern1 = re.compile(r'(#result-meeting-result/race_id=\d+&amp;track_id=\d+&amp;r_date=[\d-]+&amp;r_time=[\d:]+)')
links1 = pattern1.findall(src1)

tf_href = f'https://www.timeform.com/greyhound-racing/results/{racing_date}'
driver2.get(tf_href)
driver2.implicitly_wait(3)
logging.info(f'Timeform Link: {tf_href}')

src2 = driver2.find_element(By.XPATH, "//section[@class='w-archive-full']").get_attribute('outerHTML')
pattern2 = re.compile(r'(/results/[\w-]+/\d+/[\d-]+/\d+)')
links2 = pattern2.findall(src2)

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
    driver1.quit()
    df_rp = pd.DataFrame(rp_lista, columns=['dia', 'hora', 'track', 'rp_id', 'rp_url'])
    df_rp = df_rp.drop_duplicates(subset=['dia', 'hora', 'track', 'rp_id', 'rp_url'])
    df_rp['track'] = df_rp['track'].map(estadio)

    # Verifica se a linha já existe no banco de dados
    exists_query = session.query(exists().where(
        (PageSource.dia == dia) &
        (PageSource.url == rp_href) &
        (PageSource.site == 'rp') &
        (PageSource.scanned_level == 'obter_links') &
        (PageSource.html_source == src1)
    )).scalar()

    if not exists_query:
        link = PageSource(
            dia=dia,
            url=rp_href,
            site='rp',
            scanned_level='obter_links',
            html_source=src1
        )
        session.add(link)

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
    driver2.quit()
    df_tf = pd.DataFrame(tf_lista, columns=['dia', 'hora', 'track', 'tf_id', 'tf_url'])
    df_tf = df_tf.drop_duplicates(subset=['dia', 'hora', 'track', 'tf_id', 'tf_url'])

    # Verifica se a linha já existe no banco de dados
    exists_query = session.query(exists().where(
        (PageSource.dia == dia) &
        (PageSource.url == tf_href) &
        (PageSource.site == 'tf') &
        (PageSource.scanned_level == 'obter_links') &
        (PageSource.html_source == src2)
    )).scalar()

    if not exists_query:
        link = PageSource(
            dia=dia,
            url=tf_href,
            site='tf',
            scanned_level='obter_links',
            html_source=src2
        )
        session.add(link)

if rp_vazio == True and tf_vazio == True:
    logging.info('Não há links que sejam compativeis com a regex nos dois sites.')
elif rp_vazio == True and tf_vazio == False:
    logging.info('Há links compativeis com a regex do site TimeForm.')
    if not df_tf.empty:
        # Itera sobre as linhas do DataFrame e insere na tabela
        ignored_count = 0
        for index, row in df_tf.iterrows():
            # Verifica se a linha já existe no banco de dados
            exists_query = session.query(exists().where(
                (RaceToScamSemPar.dia == row['dia']) &
                (RaceToScamSemPar.hora == row['hora']) &
                (RaceToScamSemPar.track == row['track']) &
                (RaceToScamSemPar.site_id == row['timeform_id']) &
                (RaceToScamSemPar.site_url == row['timeform_url'])
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
            else:
                ignored_count += 1
        if ignored_count > 0:
            logging.info(f'Número de link do site Timeform, que serão ignorados: {ignored_count}')
    else:
        logging.info('O DataFrame timeform está vazio. Não há dados para inserir.')
elif rp_vazio == False and tf_vazio == True:
    logging.info('Há links compativeis com a regex do site RacingPost.')
    if not df_rp.empty:
        # Itera sobre as linhas do DataFrame e insere na tabela
        ignored_count = 0
        for index, row in df_rp.iterrows():
            # Verifica se a linha já existe no banco de dados
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
            else:
                ignored_count += 1
        if ignored_count > 0:
            logging.info(f'Número de link do site Racingpost, que serão ignorados: {ignored_count}')
    else:
        logging.info('O DataFrame racingpost está vazio. Não há dados para inserir.')
elif rp_vazio == False and tf_vazio == False:
    logging.info('Há links compativeis com a regex nos dois sites.')
    # Realizar a mesclagem com indicador
    df_merged = pd.merge(df_tf, df_rp, on=['dia', 'hora', 'track'], how='outer', indicator=True)

    # Filtrar as linhas que estão apenas em df_timeform
    tf = df_merged[df_merged['_merge'] == 'left_only'].drop(['_merge', 'rp_id', 'rp_url'], axis=1)
    tf = tf.reset_index(drop=True)

    # Filtrar as linhas que estão apenas em df_racingpost
    rp = df_merged[df_merged['_merge'] == 'right_only'].drop(['_merge', 'tf_id', 'tf_url'], axis=1)
    rp = rp.reset_index(drop=True)

    # Filtrar as linhas onde '_merge' é igual a 'both'
    df_merged = df_merged.loc[df_merged['_merge'] == 'both']

    # Remover a coluna '_merge'
    df_merged = df_merged.drop('_merge', axis=1)
    df_merged = df_merged.reset_index(drop=True)

    # Remove registros duplicados com base nas colunas 'date', 'time', 'track', 'timeform_id' e 'timeform_url'
    df_merged = df_merged.drop_duplicates(subset=['dia', 'hora', 'track', 'tf_id', 'tf_url', 'rp_id', 'rp_url'])

    # Mostar o nome das Pistas do site Timeform
    #for track_value in df_timeform['track'].unique():
    #    print(track_value)

    # Mostrar o link das corridas que o estadio estiver como NaN
    rp_nan = rp.loc[rp['track'].isna(), ['rp_url']]
    for url in rp_nan['rp_url']:
        print(url)

    if not df_merged.empty:
        # Itera sobre as linhas do DataFrame e insere na tabela
        ignored_count = 0
        for index, row in df_merged.iterrows():
            # Verifica se a linha já existe no banco de dados
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
            else:
                ignored_count += 1
        if ignored_count > 0:
            logging.info(f'Número de link combinados, que serão ignorados: {ignored_count}')
    else:
        logging.info('O DataFrame df_merged está vazio. Não há dados para inserir.')

    if not rp.empty:
        # Itera sobre as linhas do DataFrame e insere na tabela
        ignored_count = 0
        for index, row in rp.iterrows():
            # Verifica se a linha já existe no banco de dados
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
            else:
                ignored_count += 1

        if ignored_count > 0:
            logging.info(f'Número de link do site Racingpost, que serão ignorados: {ignored_count}')
    else:
        logging.info('O DataFrame racingpost está vazio. Não há dados para inserir.')

    if not tf.empty:
        # Itera sobre as linhas do DataFrame e insere na tabela
        ignored_count = 0
        for index, row in tf.iterrows():
            # Verifica se a linha já existe no banco de dados
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
            else:
                ignored_count += 1
        if ignored_count > 0:
            logging.info(f'Número de link do site Timeform, que serão ignorados: {ignored_count}')
    else:
        logging.info('O DataFrame timeform está vazio. Não há dados para inserir.')
else:
    logging.info('Verificar erro!!!')
    # Seleciona a data mais antiga onde scanned é false
    scanned_date = session.query(LastDate).filter(LastDate.dia == racing_date).first()
    if scanned_date:
        scanned_date.scanned = False
        session.commit()

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
from db import connect
from sqlalchemy import create_engine, and_
from sqlalchemy.orm import sessionmaker, declarative_base
from tables import Base, engine, LastDate, RaceToScam, RaceToScamSemPar, PageSource
from datetime import datetime
import pandas as pd
import re


# Criar a conexão com o banco de dados usando SQLAlchemy
engine = create_engine('postgresql+psycopg2://', creator=connect)
Base = declarative_base()

Session = sessionmaker(bind=engine)
session = Session()

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

def capitalize_words(sentence):
    words = sentence.split()
    capitalized_words = [word.capitalize() for word in words]
    capitalized_sentence = ' '.join(capitalized_words)
    parts = capitalized_sentence.split("'")
    for i in range(len(parts)):
        parts[i] = parts[i][0].capitalize() + parts[i][1:]
    return "'".join(parts)

# Obter registros de PageSource onde scanned_level é 'obter_links' e site é 'rp' ou 'tf'
page_sources = session.query(PageSource).filter(
    PageSource.scanned_level == 'obter_links',
    PageSource.site.in_(['rp', 'tf'])
).all()

# Criar DataFrames
data = [(ps.dia, ps.site, ps.html_source) for ps in page_sources]
df = pd.DataFrame(data, columns=['dia', 'site', 'html_source'])

# Separar em df_rp e df_tf
df_rp = df[df['site'] == 'rp']
df_tf = df[df['site'] == 'tf']

print(df_rp)
print(df_tf)

# Funções para extrair informações das URLs
def extract_rp_track_id(url):
    match = re.search(r'track_id=(\d+)', url)
    if match:
        return match.group(1)
    return None

def extract_tf_track_name(url):
    match = re.search(r'/greyhound-racing/results/([^/]+)/', url)
    if match:
        return match.group(1)
    return None

def extract_rp_time(url):
    match = re.search(r'r_time=(\d{2}:\d{2})', url)
    if match:
        return match.group(1)
    return None

def extract_tf_time(url):
    match = re.search(r'\/greyhound-racing\/results\/.+\/(\d+)\/.*', url)
    if match:
        time_str = match.group(1)
        if len(time_str) == 4:
            return time_str[:2] + ':' + time_str[2:]
        elif len(time_str) == 3:
            return time_str[0] + ':' + time_str[1:]
    return None

def extract_rp_race_id(url):
    match = re.search(r'race_id=(\d+)', url)
    if match:
        return match.group(1)
    return None

def extract_tf_race_id(url):
    match = re.search(r'/greyhound-racing/results/.*/(\d+)', url)
    if match:
        return match.group(1)
    return None

#df_rp = df_rp.copy()
#df_tf = df_tf.copy()

df_rp.loc[:, 'track'] = df_rp['html_source'].apply(extract_rp_track_id)
df_rp.loc[:, 'hora'] = df_rp['html_source'].apply(extract_rp_time)
df_rp.loc[:, 'rp_id'] = df_rp['html_source'].apply(extract_rp_race_id)

df_tf.loc[:, 'track'] = df_tf['html_source'].apply(extract_tf_track_name)
df_tf.loc[:, 'hora'] = df_tf['html_source'].apply(extract_tf_time)
df_tf.loc[:, 'tf_id'] = df_tf['html_source'].apply(extract_tf_race_id)

#df_rp = df_rp.drop('site', axis=1)
#df_tf = df_tf.drop('site', axis=1)

df_rp = df_rp.rename(columns={'html_source': 'rp_url'})
df_rp = df_tf.rename(columns={'html_source': 'tf_url'})

#df_rp['track'] = df_rp['track'].map(estadio)
df_tf['track'] = df_tf['track'].apply(capitalize_words)


#df_merged = pd.merge(df_tf, df_rp, on=['dia', 'hora', 'track'], how='outer', indicator=True)
#df_tf = df_merged[df_merged['_merge'] == 'left_only'].drop(['_merge', 'rp_id', 'rp_url'], axis=1)
#df_tf = df_tf.reset_index(drop=True)
#df_rp = df_merged[df_merged['_merge'] == 'right_only'].drop(['_merge', 'tf_id', 'tf_url'], axis=1)
#df_rp = df_rp.reset_index(drop=True)
#print(df_merged)


#
#
#
#df_merged = df_merged.loc[df_merged['_merge'] == 'both']
#df_merged = df_merged.drop('_merge', axis=1)
#df_merged = df_merged.reset_index(drop=True)

#df_merged = df_merged.drop_duplicates(subset=['dia', 'hora', 'track', 'tf_id', 'tf_url', 'rp_id', 'rp_url'])

session.close()

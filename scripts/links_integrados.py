import pandas as pd
import sqlalchemy
from datetime import date
from sqlalchemy import create_engine, text
from db import connect

estadio = {
    '1'   : 'crayford',
    '4'   : 'monmore',
    '5'   : 'hove',
    '6'   : 'newcastle',
    '7'   : 'oxford',
    '9'   : 'wimbledon',
    '11'  : 'romford',
    '13'  : 'henlow',
    '16'  : 'yarmouth',
    '17'  : 'hall-green',
    '18'  : 'belle-vue',
    '25'  : 'peterborough',
    '33'  : 'nottingham',
    '34'  : 'sheffield',
    '35'  : 'poole',
    '38'  : 'shawfield',
    '39'  : 'swindon',
    '61'  : 'sunderland',
    '62'  : 'perry-barr',
    '63'  : 'suffolk-downs',
    '66'  : 'doncaster',
    '69'  : 'harlow',
    '70'  : 'central-park',
    '73'  : 'valley',
    '76'  : 'kinsley',
    '83'  : 'coventry',
    '86'  : 'pelaw-grange',
    '98'  : 'towcester'
}

# Obter a data atual
data_atual = date.today()

def capitalize_words(sentence):
    words = sentence.replace('-', ' ')
    words = words.split()
    capitalized_words = [word.capitalize() for word in words]
    return ' '.join(capitalized_words)

timeform = """
    SELECT DISTINCT
	    split_part(url, '/', 8) AS date,
        CASE
            WHEN length(split_part(url, '/', 7)) = 3 THEN
                CONCAT(
                    '0', 
                    SUBSTRING(split_part(url, '/', 7) FROM 1 FOR 1), 
                    ':', 
                    SUBSTRING(split_part(url, '/', 7) FROM 2 FOR 2)
                )
            WHEN length(split_part(url, '/', 7)) = 4 THEN
                CONCAT(
                    SUBSTRING(split_part(url, '/', 7) FROM 1 FOR 2),
                    ':',
                    SUBSTRING(split_part(url, '/', 7) FROM 3 FOR 2)
                )
            ELSE
                NULL -- Lidar com outros casos, se necessário
        END AS time,
        split_part(url, '/', 6) AS track, 
        split_part(url, '/', 9) AS timeform_id, 
        url as timeform_url
    FROM
        linkstoscam
    WHERE
        website = 'timeform'
"""

racingpost = """
    SELECT DISTINCT
        substring(split_part(url, '=', 4) FROM '[0-9]+-[0-9]+-[0-9]+') AS date,
        substring(split_part(url, '=', 5) FROM '[0-9]+:[0-9]+') AS time,
        substring(split_part(url, '=', 3) FROM '[0-9]+') AS track,
        substring(split_part(url, '=', 2) FROM '[0-9]+') AS racingpost_id,
        url as racingpost_url
    FROM
        linkstoscam
    WHERE
        website = 'racingpost' and ( 
        CAST(substring(split_part(url, '=', 3) FROM '[0-9]+') AS INTEGER) = 1 OR
        CAST(substring(split_part(url, '=', 3) FROM '[0-9]+') AS INTEGER) = 4 OR
        CAST(substring(split_part(url, '=', 3) FROM '[0-9]+') AS INTEGER) = 5 OR
        CAST(substring(split_part(url, '=', 3) FROM '[0-9]+') AS INTEGER) = 6 OR
        CAST(substring(split_part(url, '=', 3) FROM '[0-9]+') AS INTEGER) = 7 OR
        CAST(substring(split_part(url, '=', 3) FROM '[0-9]+') AS INTEGER) = 9 OR
        CAST(substring(split_part(url, '=', 3) FROM '[0-9]+') AS INTEGER) = 11 OR
        CAST(substring(split_part(url, '=', 3) FROM '[0-9]+') AS INTEGER) = 13 OR
        CAST(substring(split_part(url, '=', 3) FROM '[0-9]+') AS INTEGER) = 16 OR
        CAST(substring(split_part(url, '=', 3) FROM '[0-9]+') AS INTEGER) = 17 OR
        CAST(substring(split_part(url, '=', 3) FROM '[0-9]+') AS INTEGER) = 18 OR
        CAST(substring(split_part(url, '=', 3) FROM '[0-9]+') AS INTEGER) = 25 OR
        CAST(substring(split_part(url, '=', 3) FROM '[0-9]+') AS INTEGER) = 33 OR
        CAST(substring(split_part(url, '=', 3) FROM '[0-9]+') AS INTEGER) = 34 OR
        CAST(substring(split_part(url, '=', 3) FROM '[0-9]+') AS INTEGER) = 35 OR
        CAST(substring(split_part(url, '=', 3) FROM '[0-9]+') AS INTEGER) = 38 OR
        CAST(substring(split_part(url, '=', 3) FROM '[0-9]+') AS INTEGER) = 39 OR
        CAST(substring(split_part(url, '=', 3) FROM '[0-9]+') AS INTEGER) = 61 OR
        CAST(substring(split_part(url, '=', 3) FROM '[0-9]+') AS INTEGER) = 62 OR
        CAST(substring(split_part(url, '=', 3) FROM '[0-9]+') AS INTEGER) = 63 OR
        CAST(substring(split_part(url, '=', 3) FROM '[0-9]+') AS INTEGER) = 66 OR
        CAST(substring(split_part(url, '=', 3) FROM '[0-9]+') AS INTEGER) = 69 OR
        CAST(substring(split_part(url, '=', 3) FROM '[0-9]+') AS INTEGER) = 70 OR
        CAST(substring(split_part(url, '=', 3) FROM '[0-9]+') AS INTEGER) = 73 OR
        CAST(substring(split_part(url, '=', 3) FROM '[0-9]+') AS INTEGER) = 76 OR
        CAST(substring(split_part(url, '=', 3) FROM '[0-9]+') AS INTEGER) = 83 OR
        CAST(substring(split_part(url, '=', 3) FROM '[0-9]+') AS INTEGER) = 86 OR
        CAST(substring(split_part(url, '=', 3) FROM '[0-9]+') AS INTEGER) = 98 )
"""

# Criar a conexão com o banco de dados usando SQLAlchemy
engine = create_engine('postgresql+psycopg2://', creator=connect)

# Executar a consulta e criar o DataFrame para a tabela 'timeform'
df_timeform = pd.read_sql_query(timeform, engine)

# Remove registros duplicados com base nas colunas 'date', 'time', 'track', 'timeform_id' e 'timeform_url'
df_timeform = df_timeform.drop_duplicates(subset=['date', 'time', 'track', 'timeform_id', 'timeform_url'])

# Executar a consulta e criar o DataFrame para a tabela 'racingpost'
df_racingpost = pd.read_sql_query(racingpost, engine)

# Mapear os valores da coluna 'tr' para os valores de 'track' usando o dicionário estadio   
df_racingpost['track'] = df_racingpost['track'].map(estadio)

# Remove registros duplicados com base nas colunas 'date', 'time', 'track', 'timeform_id' e 'timeform_url'
df_racingpost = df_racingpost.drop_duplicates(subset=['date', 'time', 'track', 'racingpost_id', 'racingpost_url'])

# Realizar a mesclagem com indicador
df_merged = pd.merge(df_timeform, df_racingpost, on=['date', 'time', 'track'], how='outer', indicator=True)

# Filtrar as linhas que estão apenas em df_timeform
timeform = df_merged[df_merged['_merge'] == 'left_only'].drop(['_merge', 'racingpost_id', 'racingpost_url'], axis=1)
timeform = df_timeform.reset_index(drop=True)

# Filtrar as linhas que estão apenas em df_racingpost
racingpost = df_merged[df_merged['_merge'] == 'right_only'].drop(['_merge', 'timeform_id', 'timeform_url'], axis=1)
racingpost = df_racingpost.reset_index(drop=True)

# Filtrar as linhas onde '_merge' é igual a 'both'
df_merged = df_merged.loc[df_merged['_merge'] == 'both']

# Remover a coluna '_merge'
df_merged = df_merged.drop('_merge', axis=1)
df_merged = df_merged.reset_index(drop=True)

# Adicionar uma coluna 'scanned' com o valor False
df_merged['scanned'] = False

# Remove registros duplicados com base nas colunas 'date', 'time', 'track', 'timeform_id' e 'timeform_url'
df_merged = df_merged.drop_duplicates(subset=['date', 'time', 'track', 'timeform_id', 'timeform_url', 'racingpost_id', 'racingpost_url', 'scanned'])

# Cria a tabela 'linkstoscam2'
df_merged.to_sql('linkstoscam2', con=engine, if_exists='replace', index=False, 
        method='multi', chunksize=1000, 
        dtype={
            'date': sqlalchemy.types.Date,
            'time': sqlalchemy.types.Time,
            'track': sqlalchemy.types.String,
            'timeform_id': sqlalchemy.types.Integer,
            'timeform_url': sqlalchemy.types.String,
            'racingpost_id': sqlalchemy.types.Integer,
            'racingpost_url': sqlalchemy.types.String,
            'scanned': sqlalchemy.types.Boolean
        })

# Cria a tabela 'df_timeform'
timeform.to_sql('df_timeform', con=engine, if_exists='replace', index=False, 
        method='multi', chunksize=1000, 
        dtype={
            'date': sqlalchemy.types.Date,
            'time': sqlalchemy.types.Time,
            'track': sqlalchemy.types.String,
            'timeform_id': sqlalchemy.types.Integer,
            'timeform_url': sqlalchemy.types.String,
            'scanned': sqlalchemy.types.Boolean
        })

# Cria a tabela 'df_racingpost'
racingpost.to_sql('df_racingpost', con=engine, if_exists='replace', index=False, 
        method='multi', chunksize=1000, 
        dtype={
            'date': sqlalchemy.types.Date,
            'time': sqlalchemy.types.Time,
            'track': sqlalchemy.types.String,
            'racingpost_id': sqlalchemy.types.Integer,
            'racingpost_url': sqlalchemy.types.String,
            'scanned': sqlalchemy.types.Boolean
        })

set_racingpost_id = """
    update race 
    set racingpost_id = linkstoscam2.racingpost_id
    from linkstoscam2 
    where race.racingpost_id is null 
    and race.timeform_id = linkstoscam2.timeform_id
"""

set_racingpost_scanned = """
    update linkstoscam set scanned = true
    from linkstoscam2 where linkstoscam.scanned = false 
    and linkstoscam2.racingpost_url = linkstoscam.url
"""

with engine.connect() as con:
    con.execute(text(set_racingpost_id))
    con.execute(text(set_racingpost_scanned))

# Fechar a conexão (não é necessário com o uso do pandas.read_sql_query)
engine.dispose()
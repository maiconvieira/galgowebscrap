from sqlalchemy import create_engine, text
from db import connect
import pandas as pd
import re
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.automap import automap_base

# Criar a conexão com o banco de dados usando SQLAlchemy
engine = create_engine('postgresql+psycopg2://', creator=connect)

# Criar a sessão
Session = sessionmaker(bind=engine)
session = Session()

# Consulta SQL para atualizar a tabela
update_query = """UPDATE page_source SET dia = TO_DATE(SUBSTRING(url FROM '\d{4}-\d{2}-\d{2}'), 'YYYY-MM-DD')"""

# Executar a atualização
with engine.connect() as conn:
    conn.execute(text(update_query))

# Consulta SQL
sql_query = text("SELECT dia, url, site, html_source FROM page_source")

# Executar a query e converter o resultado em DataFrame
df = pd.read_sql_query(sql_query, engine)

# Filtrar os dados de interesse para anos finais
cond_rp = (df['site'] == 'rp')

# Aplicar a expressão regular em cada linha do DataFrame
df.loc[cond_rp, 'html_source'] = df.loc[cond_rp, 'html_source'].apply(lambda x: re.findall(r'\s{48}<a href="(#result-meeting-result/race_id=\d+&amp;track_id=\d+&amp;r_date=\d+-\d+-\d+&amp;r_time=\d+:\d+)">\d+:\d+</a>', x))

# Filtrar os dados de interesse para anos finais
cond_tf = (df['site'] == 'tf')

# Aplicar a expressão regular em cada linha do DataFrame
df.loc[cond_tf, 'html_source'] = df.loc[cond_tf, 'html_source'].apply(lambda x: re.findall(r'<a class="[a-z-\s]*" href="(/greyhound-racing/results/[\w-]*/\d+/\d+-\d+-\d+/\d+)">\d+:\d+</a>', x))

# Atualizar o banco de dados
for index, row in df.iterrows():
    session.execute(
        text("UPDATE page_source SET html_source=:html_source WHERE dia=:dia AND url=:url"),
        {"html_source": row['html_source'], "dia": row['dia'], "url": row['url']}
    )

# Confirmar as alterações
session.commit()
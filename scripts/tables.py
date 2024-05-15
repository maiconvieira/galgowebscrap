from sqlalchemy import create_engine, Column, Integer, String, Boolean, ForeignKey, UniqueConstraint, Date, Time, Text, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session, relationship
from sqlalchemy.sql import func
from psycopg2 import extras
from db import connect

Base = declarative_base()

# Define as classes de modelo

class PageSource(Base):
    __tablename__ = 'page_source'

    id = Column(Integer, primary_key=True)
    dia = Column(Date)
    url = Column(String)
    site = Column(String)
    scanned_level = Column(String)
    html_source = Column(Text)

class LastDate(Base):
    __tablename__ = 'lastdate'

    id = Column(Integer, primary_key=True)
    dia = Column(Date, unique=True)
    scanned = Column(Boolean, default=False)

class LinksToScam(Base):
    __tablename__ = 'linkstoscam'

    id = Column(Integer, primary_key=True)
    dia = Column(Date)
    hora = Column(Time)
    track = Column(String)
    tf_id = Column(Integer)
    tf_url = Column(String)
    tf_scanned = Column(Boolean)
    rp_id = Column(Integer)
    rp_url = Column(String)
    rp_scanned = Column(Boolean)

class LinksToScamSemPar(Base):
    __tablename__ = 'linkstoscam_sem_par'

    id = Column(Integer, primary_key=True)
    dia = Column(Date)
    hora = Column(Time)
    track = Column(String)
    site = Column(String)
    site_id = Column(Integer)
    site_url = Column(String)
    scanned = Column(Boolean)

class GreyhoundLinksToScam(Base):
    __tablename__ = 'greyhoundlinkstoscam'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    tf_id = Column(Integer, unique=True)
    rp_id = Column(Integer, unique=True)
    url = Column(String, nullable=False, unique=True)
    website = Column(String)
    scanned = Column(Boolean)

class Stadium(Base):
    __tablename__ = 'stadium'

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    url = Column(String)
    address = Column(String)
    email = Column(String)
    location = Column(String)

class Trainer(Base):
    __tablename__ = 'trainer'

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)

class Greyhound(Base):
    __tablename__ = 'greyhound'

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    born_date = Column(Date)
    genre = Column(String)
    colour = Column(String)
    dam = Column(String)
    sire = Column(String)
    owner = Column(String)
    tf_id = Column(Integer)
    rp_id = Column(Integer)

class TrainerGreyhound(Base):
    __tablename__ = 'trainer_greyhound'

    trainer_id = Column(Integer, ForeignKey('trainer.id'), primary_key=True)
    greyhound_id = Column(Integer, ForeignKey('greyhound.id'), primary_key=True)

    trainer = relationship('Trainer')
    greyhound = relationship('Greyhound') 

class Race(Base):
    __tablename__ = 'race'

    id = Column(Integer, primary_key=True)
    dia = Column(Date)
    hora = Column(Time)
    race_num = Column(Integer)
    grade = Column(String)
    distance = Column(Integer)
    race_type = Column(String)
    tf_going = Column(String)
    rp_going = Column(String)
    going = Column(String)
    prizes = Column(String)
    prize = Column(String)
    forecast = Column(String)
    tricast = Column(String)
    tf_id = Column(Integer)
    rp_id = Column(Integer)
    race_comment = Column(String)
    race_comment_ptbr = Column(String)
    stadium_id = Column(Integer, ForeignKey('stadium.id'), nullable=False)
    stadium = relationship('Stadium')

    __table_args__ = (
        UniqueConstraint('dia', 'hora', 'race_num', 'grade', 'distance', 'race_type', 'tf_going', 'rp_going', 'going', 'prizes', 'prize', 'forecast', 'tricast', 'tf_id', 'rp_id'),
    )

class RaceResult(Base):
    __tablename__ = 'race_result'

    id = Column(Integer, primary_key=True)
    position = Column(Integer)
    bnt = Column(String)
    trap = Column(Integer)
    run_time = Column(String)
    sectional = Column(String)
    bend = Column(String)
    remarks_acronym = Column(String)
    remarks = Column(String)
    isp = Column(String)
    bsp = Column(String)
    tfr = Column(String)
    greyhound_weight = Column(String)
    greyhound_id = Column(Integer, ForeignKey('greyhound.id'), nullable=False)
    race_id = Column(Integer, ForeignKey('race.id'), nullable=False)
    greyhound = relationship('Greyhound')
    race = relationship('Race')

def insert_dates(engine):
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        # Insere as datas de 2013-01-01 até hoje na tabela lastdate
        sql = text("""
            INSERT INTO lastdate (dia, scanned)
            SELECT dates.date, false
            FROM generate_series('2013-01-01'::date, CURRENT_DATE, '1 day'::interval) AS dates(date)
            WHERE NOT EXISTS (
                SELECT 1 FROM lastdate WHERE dia = dates.date
            );
        """)
        session.execute(sql)
        session.commit()
    except Exception as e:
        print(f"Erro ao inserir datas: {e}")
        session.rollback()
    finally:
        session.close()

# Configura a conexão com o banco de dados
engine = create_engine('postgresql+psycopg2://', creator=connect)

# Cria as tabelas
Base.metadata.create_all(engine)

# Insere as datas na tabela lastdate
insert_dates(engine)

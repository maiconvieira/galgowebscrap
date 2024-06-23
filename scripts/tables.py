from sqlalchemy import create_engine, Column, Integer, String, Boolean, ForeignKey, UniqueConstraint, Date, Time, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session, relationship
from sqlalchemy.sql import func
from psycopg2 import extras
from db import connect

Base = declarative_base()

# Define as classes de modelo
class LastDate(Base):
    __tablename__ = 'lastdate'

    id = Column(Integer, primary_key=True)
    dia = Column(Date, unique=True)
    scanned = Column(Boolean, default=False)

class PageSource(Base):
    __tablename__ = 'page_source'

    id = Column(Integer, primary_key=True)
    dia = Column(Date)
    url = Column(String)
    site = Column(String)
    scanned_level = Column(String)
    html_source = Column(Text)

class RaceToScam(Base):
    __tablename__ = 'racetoscam'

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

class RaceToScamSemPar(Base):
    __tablename__ = 'racetoscam_sem_par'

    id = Column(Integer, primary_key=True)
    dia = Column(Date)
    hora = Column(Time)
    track = Column(String)
    site = Column(String)
    site_id = Column(Integer)
    site_url = Column(String)
    scanned = Column(Boolean)

class Stadium(Base):
    __tablename__ = 'stadium'

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    url = Column(String)
    address = Column(String)
    email = Column(String)
    location = Column(String)

    def __init__(self, name, url=None, address=None, email=None, location=None):
        self.name = name
        self.url = url
        self.address = address
        self.email = email
        self.location = location

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
    total_porc = Column(String)
    tf_id = Column(Integer)
    rp_id = Column(Integer)
    race_comment = Column(String)
    stadium_id = Column(Integer, ForeignKey('stadium.id'), nullable=False)
    stadium = relationship('Stadium')

    race_results = relationship('RaceResult', back_populates='race')

    __table_args__ = (
        UniqueConstraint('dia', 'hora', 'race_num', 'grade', 'distance', 'race_type', 'tf_going', 'rp_going', 'going', 'prizes', 'prize', 'forecast', 'tricast', 'tf_id', 'rp_id', 'race_comment', 'stadium_id'),
    )

class DogToScam(Base):
    __tablename__ = 'dogtoscam'

    id = Column(Integer, primary_key=True)
    dogName = Column(String)
    tfDogId = Column(Integer)
    tf_url = Column(String)
    tf_scanned = Column(Boolean)
    rpDogId = Column(Integer)
    rp_url = Column(String)
    rp_scanned = Column(Boolean)

    __table_args__ = (
        UniqueConstraint('dogName', 'tfDogId', 'tf_url', 'rpDogId', 'rp_url'),
    )

class DogToScamSemPar(Base):
    __tablename__ = 'dogtoscam_sem_par'

    id = Column(Integer, primary_key=True)
    dogName = Column(String)
    site = Column(String)
    dog_id = Column(Integer)
    dog_url = Column(String)
    dog_scanned = Column(Boolean)

class Trainer(Base):
    __tablename__ = 'trainer'

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)

    def __init__(self, name):
        self.name = name

class Greyhound(Base):
    __tablename__ = 'greyhound'

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    born_date = Column(String)
    genre = Column(String)
    colour = Column(String)
    dam = Column(String)
    sire = Column(String)
    owner = Column(String)
    tf_id = Column(Integer)
    rp_id = Column(Integer)

    race_results = relationship('RaceResult', back_populates='greyhound')

    __table_args__ = (
        UniqueConstraint('name', 'born_date', 'genre', 'colour', 'dam', 'sire', 'owner', 'tf_id', 'rp_id'),
    )

class TrainerGreyhound(Base):
    __tablename__ = 'trainer_greyhound'

    trainer_id = Column(Integer, ForeignKey('trainer.id'), primary_key=True)
    greyhound_id = Column(Integer, ForeignKey('greyhound.id'), primary_key=True)

    trainer = relationship('Trainer')
    greyhound = relationship('Greyhound') 

class RaceResult(Base):
    __tablename__ = 'race_result'

    id = Column(Integer, primary_key=True)
    position = Column(Integer)
    btn = Column(String)
    trap = Column(Integer)
    run_time = Column(String)
    sectional = Column(String)
    bend = Column(String)
    remarks_acronym = Column(String)
    remarks = Column(String)
    isp = Column(String)
    bsp = Column(String)
    tfr = Column(String)
    greyhound_age = Column(Integer)
    greyhound_weight = Column(String)
    greyhound_id = Column(Integer, ForeignKey('greyhound.id'))
    race_id = Column(Integer, ForeignKey('race.id'))

    __table_args__ = (
        UniqueConstraint('greyhound_id', 'race_id'),
    )

    greyhound = relationship('Greyhound', back_populates='race_results')
    race = relationship('Race', back_populates='race_results')

# Configura a conexão com o banco de dados
engine = create_engine('postgresql+psycopg2://', creator=connect)

# Cria as tabelas
Base.metadata.create_all(engine)

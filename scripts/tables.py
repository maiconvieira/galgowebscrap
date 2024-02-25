from db import connect
import psycopg2
from psycopg2 import sql

tables = {
    'lastscannedday': """
        CREATE TABLE IF NOT EXISTS lastscannedday (
            id SERIAL PRIMARY KEY,
            timeform_scannedday VARCHAR(10),
            racingpost_scannedday VARCHAR(10)
        )
    """,
    'linkstoscam': """
        CREATE TABLE IF NOT EXISTS linkstoscam (
            id SERIAL PRIMARY KEY,
            url VARCHAR NOT NULL UNIQUE,
            website VARCHAR(25),
            scanned BOOLEAN
        )
    """,
    'stadium': """
        CREATE TABLE IF NOT EXISTS stadium (
            id SERIAL PRIMARY KEY,
            name VARCHAR(25) NOT NULL UNIQUE,
            url VARCHAR,
            address VARCHAR(100),
            email VARCHAR,
            location VARCHAR
        )
    """,
    'trainer': """
        CREATE TABLE IF NOT EXISTS trainer (
            id SERIAL PRIMARY KEY,
            name VARCHAR(25) NOT NULL UNIQUE
        )
    """,
    'greyhound': """
        CREATE TABLE IF NOT EXISTS greyhound (
            id SERIAL PRIMARY KEY,
            name VARCHAR(60) NOT NULL,
            born_date VARCHAR(12),
            sex VARCHAR(10),
            colour VARCHAR(15),
            dam VARCHAR(20),
            sire VARCHAR(20),
            owner VARCHAR,
            id_timeform INT,
            id_racingpost INT,
            id_trainer INT NOT NULL,
            CONSTRAINT fk_trainer FOREIGN KEY (id_trainer) REFERENCES trainer (id)
        )
    """,
    'race': """
        CREATE TABLE IF NOT EXISTS race (
            id SERIAL PRIMARY KEY,
            date_race VARCHAR(10),
            time_race VARCHAR(5),
            grade VARCHAR(5),
            distance INT,
            racing_type VARCHAR,
            tf_going VARCHAR,
            going VARCHAR,
            prize_total VARCHAR,
            forecast VARCHAR,
            tricast VARCHAR,
            id_timeform INT,
            id_racingpost INT,
            race_comment VARCHAR,
            race_comment_ptbr VARCHAR,
            id_stadium INT NOT NULL,
            CONSTRAINT fk_stadium FOREIGN KEY (id_stadium) REFERENCES stadium (id)
        )
    """,
    'race_result': """
        CREATE TABLE IF NOT EXISTS race_result (
            id SERIAL PRIMARY KEY,
            position VARCHAR(3),
            bnt VARCHAR(5),
            trap INT,
            id_greyhound INT NOT NULL,
            run_time VARCHAR,
            sectional VARCHAR,
            bend VARCHAR,
            remarks_acronym VARCHAR,
            remarks VARCHAR,
            start_price VARCHAR,
            betfair_price VARCHAR,
            tf_rating VARCHAR,
            id_race INT NOT NULL,
            id_trainer INT NOT NULL,
            CONSTRAINT fk_greyhound FOREIGN KEY (id_greyhound) REFERENCES greyhound (id),
            CONSTRAINT fk_race FOREIGN KEY (id_race) REFERENCES race (id),
            CONSTRAINT fk_trainer_race_result FOREIGN KEY (id_trainer) REFERENCES trainer (id)
        )
    """,
    'race_result_trainer': """
        CREATE TABLE IF NOT EXISTS race_result_trainer (
            id_race_result INT NOT NULL,
            id_trainer INT NOT NULL,
            PRIMARY KEY (id_race_result, id_trainer),
            CONSTRAINT fk_race_result FOREIGN KEY (id_race_result) REFERENCES race_result (id),
            CONSTRAINT fk_trainer FOREIGN KEY (id_trainer) REFERENCES trainer (id)
        )
    """,
    'race_result_greyhound': """
        CREATE TABLE IF NOT EXISTS race_result_greyhound (
            id_race_result INT NOT NULL,
            id_greyhound INT NOT NULL,
            PRIMARY KEY (id_race_result, id_greyhound),
            CONSTRAINT fk_race_result FOREIGN KEY (id_race_result) REFERENCES race_result (id),
            CONSTRAINT fk_greyhound FOREIGN KEY (id_greyhound) REFERENCES greyhound (id)
        )
    """,
    'greyhoundlinkstoscam': """
        CREATE TABLE IF NOT EXISTS greyhoundlinkstoscam (
            id SERIAL PRIMARY KEY,
            name VARCHAR,
            id_timeform int UNIQUE,
            id_racingpost int UNIQUE,
            url VARCHAR NOT NULL UNIQUE,
            website VARCHAR(25),
            scanned BOOLEAN
        )
    """
}

def table_exists(table_name):
    with connect() as conn:
        with conn.cursor() as cursor:
            exists_query = sql.SQL("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = %s)")
            cursor.execute(exists_query, (table_name,))
            return cursor.fetchone()[0]

with connect() as conn:
    with conn.cursor() as cursor:
        for table_name, create_table_query in tables.items():
            if not table_exists(table_name):
                cursor.execute(create_table_query)
                conn.commit()
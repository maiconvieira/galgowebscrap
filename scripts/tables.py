from db import connect
import psycopg2
from psycopg2 import sql

tables = {
    'lastscannedday': """
        CREATE TABLE IF NOT EXISTS lastscannedday (
            id SERIAL PRIMARY KEY,
            timeform_scannedday VARCHAR,
            racingpost_scannedday VARCHAR
        )
    """,
    'linkstoscam': """
        CREATE TABLE IF NOT EXISTS linkstoscam (
            id SERIAL PRIMARY KEY,
            url VARCHAR NOT NULL UNIQUE,
            website VARCHAR,
            scanned BOOLEAN
        )
    """,
    'greyhoundlinkstoscam': """
        CREATE TABLE IF NOT EXISTS greyhoundlinkstoscam (
            id SERIAL PRIMARY KEY,
            name VARCHAR,
            timeform_id int UNIQUE,
            racingpost_id int UNIQUE,
            url VARCHAR NOT NULL UNIQUE,
            website VARCHAR,
            scanned BOOLEAN
        )
    """,
    'stadium': """
        CREATE TABLE IF NOT EXISTS stadium (
            id SERIAL PRIMARY KEY,
            name VARCHAR NOT NULL UNIQUE,
            url VARCHAR,
            address VARCHAR,
            email VARCHAR,
            location VARCHAR
        )
    """,
    'trainer': """
        CREATE TABLE IF NOT EXISTS trainer (
            id SERIAL PRIMARY KEY,
            name VARCHAR NOT NULL UNIQUE
        )
    """,
    'greyhound': """
        CREATE TABLE IF NOT EXISTS greyhound (
            id SERIAL PRIMARY KEY,
            name VARCHAR NOT NULL,
            born_date VARCHAR,
            genre VARCHAR,
            colour VARCHAR,
            dam VARCHAR,
            sire VARCHAR,
            owner VARCHAR,
            timeform_id INT,
            racingpost_id INT
        )
    """,
    'trainer_greyhound': """
        CREATE TABLE IF NOT EXISTS trainer_greyhound (
            trainer_id INT NOT NULL,
            greyhound_id INT NOT NULL,
            PRIMARY KEY (trainer_id, greyhound_id),
            CONSTRAINT fk_trainer FOREIGN KEY (trainer_id) REFERENCES trainer (id),
            CONSTRAINT fk_greyhound FOREIGN KEY (greyhound_id) REFERENCES greyhound (id)
        )
    """,
    'race': """
        CREATE TABLE IF NOT EXISTS race (
            id SERIAL PRIMARY KEY,
            race_date VARCHAR,
            race_time VARCHAR,
            grade VARCHAR,
            distance INT,
            race_type VARCHAR,
            tf_going VARCHAR,
            going VARCHAR,
            prize VARCHAR,
            forecast VARCHAR,
            tricast VARCHAR,
            timeform_id INT,
            racingpost_id INT,
            race_comment VARCHAR,
            race_comment_ptbr VARCHAR,
            stadium_id INT NOT NULL,
            CONSTRAINT fk_stadium FOREIGN KEY (stadium_id) REFERENCES stadium (id),
            CONSTRAINT unique_race_data UNIQUE (
                race_date,
                race_time,
                grade,
                distance,
                race_type,
                tf_going,
                going,
                prize,
                forecast,
                tricast,
                timeform_id
            )
        )
    """,
    'race_result': """
        CREATE TABLE IF NOT EXISTS race_result (
            id SERIAL PRIMARY KEY,
            position INT,
            bnt VARCHAR,
            trap INT,
            run_time VARCHAR,
            sectional VARCHAR,
            bend VARCHAR,
            remarks_acronym VARCHAR,
            remarks VARCHAR,
            isp VARCHAR,
            bsp VARCHAR,
            tfr VARCHAR,
            greyhound_weight  VARCHAR,
            greyhound_id INT NOT NULL,
            race_id INT NOT NULL,
            CONSTRAINT fk_greyhound FOREIGN KEY (greyhound_id) REFERENCES greyhound (id),
            CONSTRAINT fk_race FOREIGN KEY (race_id) REFERENCES race (id)
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
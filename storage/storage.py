import json
import logging
from logging import INFO
import pandas as pd
from pathlib import Path
import pymysql.cursors
import sys
import typer


logging.basicConfig(format='[%(levelname)-5s][%(asctime)s][%(module)s:%(lineno)04d] : %(message)s',
                    level=INFO,
                    stream=sys.stderr)
logger: logging.Logger = logging

storage = typer.Typer()

parent_dir = Path().resolve()
file = open('./storage/config.json', 'r')
config = json.loads(file.read())

# MySQL Configurations
local_db = config['database']['local']
user = local_db['user']
password = local_db['password']
db_name = local_db['db_name']


def insert_occurrences(row, cursor: pymysql.cursors.DictCursor) -> None:
    """
    Insert pd.DataFrame row values into MySQL occurrences table

    Args:
        row: pd.DataFrame
            row to read
        cursor: PyMySQL DictCursor object
            executes SQL statement
    Returns:
        None
    """
    
    # If eventDate is not in the format of `YYYY`
    if len(str(row.eventDate)) != 4:
        sql = """INSERT INTO `occurrences` 
                    (`id`, `eventDate`, `latitude`, `longitude`, `individualCount`)
                    VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY
                    UPDATE eventDate=VALUES(eventDate), latitude=VALUES(latitude), longitude=VALUES(longitude), individualCount=VALUES(individualCount)"""
        cursor.execute(sql, (row.occurrenceID, row.eventDate, row.decimalLatitude, row.decimalLongitude, row.individualCount))

    # If in `YYYY` format, default to -00-00 for `mm-dd` values
    else:
        eventDate = row.eventDate
        eventDate = eventDate + "-00-00"
        sql = """INSERT INTO `occurrences` 
                    (`id`, `eventDate`, `latitude`, `longitude`, `individualCount`)
                    VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY
                    UPDATE eventDate=VALUES(eventDate), latitude=VALUES(latitude), longitude=VALUES(longitude), individualCount=VALUES(individualCount)"""
        cursor.execute(sql, (row.occurrenceID, eventDate, row.decimalLatitude, row.decimalLongitude, row.individualCount))


def insert_species(row, cursor: pymysql.cursors.DictCursor) -> None:
    """
    Insert pd.DataFrame row values into MySQL occurrences table

    Args:
        row: pd.DataFrame
            row to read
        cursor: PyMySQL DictCursor object
            executes SQL statement
    Returns:
        None
    """
    species_dict = {'Balaenoptera musculus': 'Blue Whale', 'Physeter macrocephalus': 'Sperm Whale'}
    vernacularName = species_dict[row.species]
    sql = """INSERT INTO `species` 
                    (`id`, `speciesName`, `vernacularName`)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY
                    UPDATE speciesName=VALUES(speciesName), vernacularName=VALUES(vernacularName)"""
    cursor.execute(sql, (row.speciesid, row.species, vernacularName))


@storage.command('to_mysql')
def to_mysql(whale: str, filename: str) -> None:
    """
    Insert pd.DataFrame rows into MySQL tables

    Args:
        whale: str
            specific directory that the whale data is stored in
        file: str
            csv file to read into a DataFrame
    Returns:
        None
    """
    df = pd.read_csv(f'{parent_dir}/data/{whale}/{filename}')
    try:
        logger.info('Inserting rows.')
        conn = pymysql.connect(host='localhost',
                            user=user,
                            password=password,
                            database=db_name,
                            cursorclass=pymysql.cursors.DictCursor)
                            
        with conn.cursor() as cursor:
            for row in df.itertuples(index=False):
                insert_occurrences(row, cursor)
                insert_species(row, cursor)

            conn.commit()
    except pymysql.Error as e:
        logger.info(e)
        conn.rollback()

    conn.close()
    logger.info('Inserts completed.')


if __name__ == '__main__':
    storage()
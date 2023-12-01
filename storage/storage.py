import json
import logging
from logging import INFO
import pandas as pd
from pathlib import Path
import pymysql.cursors
import sys
import typer


logging.basicConfig(format='[%(levelname)-5s][%(asctime)s][%(module)s:%(lineno)04d] : %(message)s', level=INFO, stream=sys.stderr)
logger = logging.getLogger(__name__)

storage = typer.Typer()

root_dir = Path().cwd()
file = open(f'{root_dir}/config.json', 'r')
config = json.loads(file.read())

# MySQL Configurations
local_db = config['database']['local']
user = local_db['user']
password = local_db['password']
db_name = local_db['db_name']

# Whales Dictionary
whales = config['whales']


def insert_occurrences(row, waterBodyId, cursor: pymysql.cursors.DictCursor) -> None:
    """
    Insert pd.DataFrame row values into MySQL occurrences table

    Args:
        row: pd.DataFrame
            row to read
        waterBodyId: int
            foreign key value returned from stored procedure
        cursor: PyMySQL DictCursor object
            executes SQL statement
    Returns:
        None
    """
    # If eventDate is not in the format of `YYYY`
    # if len(str(row.eventDate)) != 4:
    sql = """INSERT INTO `occurrences` 
                (`id`, `eventDate`, `waterBodyId`, `latitude`, `longitude`, `speciesId`, `individualCount`,
                `start_year`, `start_month`, `start_day`, `end_year`, `end_month`, `end_day`, `date_is_valid`)
                VALUES 
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY
                UPDATE 
                eventDate=VALUES(eventDate), latitude=VALUES(latitude), longitude=VALUES(longitude), individualCount=VALUES(individualCount), 
                start_year=VALUES(start_year), start_month=VALUES(start_month), start_day=VALUES(start_day), end_year=VALUES(end_year), end_month=VALUES(end_month),
                end_day=VALUES(end_day), date_is_valid=VALUES(date_is_valid)"""
    cursor.execute(sql, (
        row.occurrenceID, row.eventDate, waterBodyId, row.decimalLatitude, row.decimalLongitude, row.speciesid, row.individualCount, 
        row.start_year, row.start_month, row.start_day, row.end_year, row.end_month, row.end_day, row.date_is_valid
        ))


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
    # reverse whales dict to get vernacular name value
    species_dict = {v['scientificname']: k.replace('_', ' ').title() for k, v in whales.items()}
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
    df = pd.read_csv(f'{root_dir}/data/{whale}/{filename}')
    df['waterBody'] = df['waterBody'].apply(lambda x: None if pd.isna(x) else x)
    try:
        logger.info('Inserting rows.')
        conn = pymysql.connect(host='localhost',
                            user=user,
                            password=password,
                            database=db_name,
                            cursorclass=pymysql.cursors.DictCursor)
                            
        with conn.cursor() as cursor:
            for row in df.itertuples(index=False):
                cursor.callproc('insert_or_update_location', (row.waterBody,))
                result = cursor.fetchone()
                waterBodyId = result['wb_id'] if result else None
                insert_species(row, cursor)
                insert_occurrences(row, waterBodyId, cursor)

            conn.commit()

        conn.close()
        logger.info('Inserts completed.')
    except pymysql.Error as e:
        logger.info(e)
        conn.rollback()



if __name__ == '__main__':
    storage()
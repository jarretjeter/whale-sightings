import json
import logging
from logging import INFO
import pandas as pd
from pathlib import Path
import pymysql.cursors
import sys
import typer
from typing import Union
from typing_extensions import Self

logging.basicConfig(
    format='[%(levelname)-5s][%(asctime)s][%(module)s:%(lineno)04d] : %(message)s', level=INFO, stream=sys.stderr
)
logger = logging.getLogger(__name__)

ROOT_DIR = Path().cwd()
with open(f"{ROOT_DIR}/config.json", 'r') as file:
    config = json.load(file)

# MySQL Configurations
local_db = config['database']['local']
host = local_db['host']
user = local_db['user']
password = local_db['password']
db_name = local_db['db_name']

# Whales Dictionary
whales = config['whales']


class MySQLClient:

    def __init__(self) -> None:
        logger.info('Creating MySQL connection..')
        self.conn = pymysql.connect(
            host=host,
            user=user,
            password=password,
            database=db_name,
            cursorclass=pymysql.cursors.DictCursor
        )
        self.cursor = self.conn.cursor()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.conn.close()
        logger.info('Connection closed.')

    def close(self, commit=True) -> None:
        if commit:
            self.commit()
        self.conn.close()
        logger.info("Connection closed.")

    def execute(self, sql, args: object=None) -> None:
        self.cursor.execute(sql, args)

    def query(self, query, args: object=None) -> Union[dict, None]:
        self.cursor.execute(query, args)
        print(self.cursor.fetchone())

    def commit(self) -> None:
        self.conn.commit()

    def insert_occurrences(self, row, waterBodyId) -> None:
        """
        Insert pd.DataFrame row values into MySQL occurrences table

        Args:
            row: pd.DataFrame
                row to read
            waterBodyId: int
                foreign key value returned from stored procedure
        Returns:
            None
        """
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
        
        self.execute(sql, (
            row.occurrenceID, row.eventDate, waterBodyId, row.decimalLatitude, row.decimalLongitude, row.speciesid, row.individualCount, 
            row.start_year, row.start_month, row.start_day, row.end_year, row.end_month, row.end_day, row.date_is_valid
            ))

    def insert_species(self, row) -> None:
        """
        Insert pd.DataFrame row values into MySQL occurrences table

        Args:
            row: pd.DataFrame
                row to read
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
        self.cursor.execute(sql, (row.speciesid, row.species, vernacularName))

    def insert_or_update_location(self, row) -> Union[int, None]:
        self.cursor.callproc('insert_or_update_location', (row.waterBody,))
        result = self.cursor.fetchone()
        waterBodyId = result['wb_id'] if result else None
        return waterBodyId

    def to_mysql(self, df: pd.DataFrame) -> None:
        """
        Insert pd.DataFrame rows into MySQL tables

        Args:
            df: pd.DataFrame
                data to send to MySQL database
        Returns:
            None
        """
        df['waterBody'] = df['waterBody'].apply(lambda x: None if pd.isna(x) else x)

        try:
            logger.info('Inserting rows.')
                                
            for row in df.itertuples(index=False):
                waterBodyId = self.insert_or_update_location(row)
                self.insert_species(row)
                self.insert_occurrences(row, waterBodyId)

                self.conn.commit()

            logger.info('Inserts completed.')
        except pymysql.Error as e:
            logger.info(e)
            self.conn.rollback()


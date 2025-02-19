import os
import sys
from typing import Union

from dotenv import load_dotenv
import pandas as pd
import sqlalchemy as db
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.exc import OperationalError, NoSuchTableError

from logging_setup import setup_logging
from whales import whale_names


logger = setup_logging()

load_dotenv()
db_host = os.getenv('MYSQL_HOST')
db_user = os.getenv('MYSQL_USER')
db_pass = os.getenv('MYSQL_PASSWORD')
db_name = os.getenv('MYSQL_DATABASE')


class MySQLClient:

    def __init__(self) -> None:
        self.create_engine()
        self.load_tables()

    def create_engine(self) -> None:
        try:
            logger.info(f"Creating engine for database: {db_name} - host: {db_host}")
            self.engine = db.create_engine(f"mysql+pymysql://{db_user}:{db_pass}@{db_host}/{db_name}")
            with self.engine.connect() as conn:  # test database connection
                pass
        except OperationalError as e:
            logger.error(f"Failed to create SQLAlchemy Engine (wrong credentials?): {e}")
            sys.exit(1)
        
    def load_tables(self) -> None:
        try:
            metadata = db.MetaData()
            self.species_table = db.Table('species', metadata, autoload_with=self.engine)
            self.locations_table = db.Table('locations', metadata, autoload_with=self.engine)
            self.occurrences_table = db.Table('occurrences', metadata, autoload_with=self.engine)
            logger.info("Database tables loaded successfully.")
        except NoSuchTableError as e:
            logger.error(f"A table was not found: {e}")
            sys.exit(1)

    def insert_occurrences(self, conn, row, water_body_id) -> None:
        """
        Insert DataFrame row values into occurrences table

        Args:
            conn: sqlalchemy.engine.Connection
            row: pd.DataFrame
                row to read
            waterBodyId: int
                foreign key value returned from stored procedure
        Returns:
            None
        """
        query = insert(self.occurrences_table).values(
            id=row.occurrenceID, eventDate=row.eventDate, waterBodyId=water_body_id, latitude=row.decimalLatitude, 
            longitude=row.decimalLongitude, speciesId=row.speciesid, individualCount=row.individualCount, 
            start_year=row.start_year, start_month=row.start_month, start_day=row.start_day, 
            end_year=row.end_year, end_month=row.end_month, end_day=row.end_day, date_is_valid=row.date_is_valid
            )
        
        on_duplicate_query = query.on_duplicate_key_update(
            eventDate=row.eventDate, latitude=row.decimalLatitude, longitude=row.decimalLongitude, 
            individualCount=row.individualCount, start_year=row.start_year, start_month=row.start_month, 
            start_day=row.start_day, end_year=row.end_year, end_month=row.end_month, end_day=row.end_day, 
            date_is_valid=row.date_is_valid
            )
        
        conn.execute(on_duplicate_query)

    def insert_species(self, conn, row) -> None:
        """
        Insert DataFrame row values into occurrences table

        Args:
            conn: sqlalchemy.engine.Connection
            row: pd.DataFrame
                row to read
        Returns:
            None
        """
        # reverse whales dict to get vernacular name value
        species_dict = {v['scientificname']: k.replace('_', ' ').title() for k, v in whale_names.items()}
        vernacularName = species_dict[row.species]

        query = insert(self.species_table).values(
            id=row.speciesid, speciesName=row.species, vernacularName=vernacularName
        )

        on_duplicate_query = query.on_duplicate_key_update(
            id=row.speciesid, speciesName=row.species, vernacularName=vernacularName
        )

        conn.execute(on_duplicate_query)

    def insert_or_update_location(self, conn, row) -> Union[int, None]:
        """
        Call stored procedure on locations table which either inserts or updates a row 
        """
        exe = conn.execute(db.text('CALL insert_or_update_location(:wb_name)'), {'wb_name': row.waterBody})
        result = exe.fetchone()
        water_body_id = result[0] if result else None
        return water_body_id

    def to_mysql(self, data: Union[pd.DataFrame, str]) -> None:
        """
        Insert DataFrame rows into database tables. Only 1 of the 2 parameters
        can be used.

        Args:
            data: pd.DataFrame or str
                dataframe or csv filename to read from
        Returns:
            None
        """
        df = None
        if isinstance(data, str):
            try:
                df = pd.read_csv(data)
            except FileNotFoundError as e:
                logger.error(f'Failed to read csv filepath - {e}')
                sys.exit(1)
        elif isinstance(data, pd.DataFrame):
            df = data

        df['waterBody'] = df['waterBody'].apply(lambda x: None if pd.isna(x) else x)

        try:
            logger.info('Opening database connection and inserting rows..')
            with self.engine.begin() as conn:          
                for row in df.itertuples(index=False):
                    water_body_id = self.insert_or_update_location(conn, row)
                    self.insert_species(conn, row)
                    self.insert_occurrences(conn, row, water_body_id)
            logger.info('Inserts completed, connection closed.')
        except OperationalError as e:
            logger.error(f'Error during SQL inserts: {e}')

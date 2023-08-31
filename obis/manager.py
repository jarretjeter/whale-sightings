import datetime
import geopandas
import json
import logging
from logging import INFO
from .obis_class import Obis
import pandas as pd
from pathlib import Path
from shapely.geometry import Point
import sys

logging.basicConfig(format='[%(asctime)s][%(module)s:%(lineno)04d] : %(message)s', level=INFO, stream=sys.stderr)
logger: logging.Logger = logging

whales = Obis.whales
data_dir = Obis.data_dir

class WhaleDataManager():
    """Pandas and GeoPandas functionalities for handling whale data.
    """
    key_list = [
    'occurrenceID', 'verbatimEventDate', 'eventDate', 'eventTime',
    'decimalLatitude', 'decimalLongitude', 'coordinatePrecision', 'coordinateUncertaintyInMeters',  
    'locality', 'waterBody', 'bathymetry', 'sst', 'sss', 'shoredistance', 'taxonRemarks', 'individualCount', 'vernacularName', 
    'order', 'orderid', 'family', 'familyid', 
    'genus', 'genusid','species', 'speciesid',
    'rightsHolder', 'ownerInstitutionCode', 'recordedBy','associatedMedia', 'basisOfRecord', 'occurrenceRemarks', 'bibliographicCitation'
    ]

    def __init__(self, whale: str, start_date: str, end_date: str) -> None:
        """
        Args:
            whale: str
                Name for file paths and column values
            start_date: str
                Part of file path to search
            end_date: str
                Part of file path to search"""
        if whale in whales:
            self.whale = whale
        else:
            raise ValueError(f'{whale} not in whales dictionary. {Obis.whales.keys()}')
        self.start = start_date
        self.end = end_date


    def filter_keys(self) -> list:
        """
        Remove irrelevant keys from returned response for later processing
        
        Returns:
            list[dict]"""
        whale = self.whale
        start = self.start
        end = self.end
        key_list = self.key_list
        
        response_file = open(f'{data_dir}/{whale}/{start}--{end}.json')
        response = json.loads(response_file.read())
        response_list = response['results']
        filtered_response = [{k:v for k, v in d.items() if k in key_list} for d in response_list if isinstance(d, dict)]
        return filtered_response
    

    def fill_in(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Fill in NaN values for specific columns

        Returns:
            `pd.DataFrame`"""
        # replace null occurrence ids with -1, -2, -3....
        nan_indices = df[df['occurrenceID'].isnull()].index
        for i, index in enumerate(nan_indices, start=1):
            df.loc[index, 'occurrenceId'] = -i

        whale = self.whale
        whale = whale.replace('_', ' ')
        df['vernacularName'] = df['vernacularName'].fillna(whale)
        # If missing count, report at least 1 whale sighted
        df['individualCount'] = df['individualCount'].fillna(1)
        return df
    

    def convert_dates(self, date_str: str) -> pd.DataFrame:
        """Process and make eventDate column more consistent
        
        Args:
            date_str: str
                eventDate string
        Returns:
            `datetime.date`"""
        # split string if it contains multiple datetimes
        try:
            if '/' in date_str:
                date_list = str.split(date_str, '/')
                # only interested in initial date sighted
                date = date_list[0]
                # if date_str is in YYYY format, return just the year without Pandas adding a month and day by default
                date = pd.to_datetime(date).year if len(date) == 4 else pd.to_datetime(date).date()
            else:
                date = pd.to_datetime(date_str).year if len(date_str) == 4 else pd.to_datetime(date_str).date()
            return date
        except ValueError:
            return f'Invalid date: {date_str}'


    def get_status(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Determine if a whale was spotted alive or dead/hunted and 
        create a bool column based on determination

        Returns:
            `pd.DataFrame`"""
        df['alive'] = df['waterBody'].notna()
        return df


    def create_dataframe(self) -> pd.DataFrame:
        """
        Create a `pandas.DataFrame` from whale data
        
        Returns:
            `pd.DataFrame`"""
        filtered_response = self.filter_keys()
        key_list = self.key_list
        output_dir = Path(f'{data_dir}//{self.whale}')
        output_dir.mkdir(parents=True, exist_ok=True)
        
        df = pd.json_normalize(filtered_response)
        df = df.reindex(columns=key_list)
        self.fill_in(df)
        df['eventDate'] = df['eventDate'].apply(self.convert_dates)
        self.get_status(df)
        filename = f'{output_dir}/{self.start}--{self.end}.csv'
        logger.info(f'Saving dataframe to {filename}')
        df.to_csv(filename, index=False)
        return df
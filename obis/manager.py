import datetime
import geopandas as gpd
import json
import logging
from logging import INFO
from .obis_class import Obis
import pandas as pd
from pathlib import Path
import sys

logging.basicConfig(format='[%(asctime)s][%(module)s:%(lineno)04d] : %(message)s', level=INFO, stream=sys.stderr)
logger = logging.getLogger(__name__)


def load_oceans() -> gpd.GeoDataFrame:
    """
    Load a shape file containing ocean geographic data into a GeoDataFrame
    
    Returns:
        geopandas.GeoDataFrame
    """
    gdf = gpd.read_file('data/Global_Oceans_and_Seas_version_1/goas_v01.shp')
    return gdf


class WhaleDataManager():
    """Pandas and GeoPandas functionalities for handling whale data.
    """
    key_list = [
    'occurrenceID', 'verbatimEventDate', 'eventDate', 'eventTime', 'decimalLatitude', 'decimalLongitude', 'coordinatePrecision', 'coordinateUncertaintyInMeters',  
    'locality', 'waterBody', 'bathymetry', 'sst', 'sss', 'shoredistance', 'taxonRemarks', 'individualCount', 'vernacularName', 'order', 'orderid', 'family', 'familyid', 
    'genus', 'genusid','species', 'speciesid','rightsHolder', 'ownerInstitutionCode', 'recordedBy','associatedMedia', 'basisOfRecord', 'occurrenceRemarks', 'bibliographicCitation'
    ]
    data_dir = './data'
    whales = {'blue_whale': {'scientific_name': 'Balaenoptera musculus'}, 'sperm_whale': {'scientific_name': 'Physeter macrocephalus'}}

    def __init__(self, whale: str, start_date: str, end_date: str) -> None:
        """
        Args:
            whale: str
                Name for file paths and column values
            start_date: str
                Part of file path to search
            end_date: str
                Part of file path to search
        """
        if whale in self.whales:
            self.whale = whale
        else:
            raise ValueError(f'{whale} not in whales dictionary. {Obis.whales.keys()}')
        self.start = start_date
        self.end = end_date


    def filter_keys(self) -> list:
        """
        Remove irrelevant keys from returned response for later processing
        
        Returns:
            list[dict]
        """
        whale = self.whale
        start = self.start
        end = self.end
        key_list = self.key_list
        
        response_file = open(f'{self.data_dir}/{whale}/{start}--{end}.json')
        response = json.loads(response_file.read())
        response_list = response['results']
        filtered_response = [{k:v for k, v in d.items() if k in key_list} for d in response_list if isinstance(d, dict)]
        return filtered_response
    

    def fill_in(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Fill in NaN values for specific columns

        Args: 
            df: pd.DataFrame
                DataFrame object to operate on
        Returns:
            `pd.DataFrame`
        """
        # replace null occurrence ids with -1, -2, -3....
        nan_indices = df[df['occurrenceID'].isnull()].index
        for i, index in enumerate(nan_indices, start=1):
            df.loc[index, 'occurrenceID'] = -i

        whale = self.whale
        whale = whale.replace('_', ' ').title()
        df['vernacularName'] = df['vernacularName'].fillna(whale)
        # If missing count, report at least 1 whale sighted
        df['individualCount'] = df['individualCount'].fillna(1)
        return df
    

    def convert_dates(self, date_str: str) -> datetime.date:
        """Process and make eventDate column more consistent
        
        Args:
            date_str: str
                eventDate string
        Returns:
            `datetime.date`
        """
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

        Args: 
            df: pd.DataFrame
                DataFrame object to operate on
        Returns:
            `pd.DataFrame`
        """
        df['alive'] = df['waterBody'].notna()
        return df


    def get_ocean(self, df: pd.DataFrame) -> pd.DataFrame:
        """Check for longitude/latitude point intersections 
        from the OceanData.ocean geodataframe to get consistent ocean locations

        Args: 
            df: pd.DataFrame
                DataFrame object to operate on
        Returns:
            `pd.DataFrame`
        """
        ocean_gdf = load_oceans()
        points_df = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df['decimalLongitude'], df['decimalLatitude']), crs='EPSG:4326')
        joined_df = gpd.sjoin(points_df, ocean_gdf, how='left', predicate='intersects')
        df['waterBody'] = joined_df['name']
        return df


    def create_dataframe(self) -> pd.DataFrame:
        """
        Create a `pandas.DataFrame` of whale sighting data read from a json file
        
        Returns:
            `pd.DataFrame`
        """
        filtered_response = self.filter_keys()
        key_list = self.key_list
        output_dir = Path(f'{self.data_dir}/{self.whale}')
        output_dir.mkdir(parents=True, exist_ok=True)
        
        df = pd.json_normalize(filtered_response)
        df = df.reindex(columns=key_list)
        # Rows with matching event dates, latitude, and longitude are likely the same event
        df = df.drop_duplicates(subset=['eventDate', 'decimalLatitude', 'decimalLongitude'], keep='first')
        self.fill_in(df)
        df['eventDate'] = df['eventDate'].apply(self.convert_dates)
        self.get_ocean(df)
        filename = f'{output_dir}/{self.start}--{self.end}.csv'
        logger.info(f'Saving dataframe to {filename}')
        df.to_csv(filename, index=False)
    
        return df
    
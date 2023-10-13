import datetime
from dateutil.parser import parse
import geopandas as gpd
import json
import logging
from logging import INFO
import pandas as pd
from pathlib import Path
import re
import sys
from typing import Optional

logging.basicConfig(format='[%(asctime)s][%(module)s:%(lineno)04d] : %(message)s', level=INFO, stream=sys.stderr)
logger = logging.getLogger(__name__)

root_dir = Path().cwd()
file = open(f"{root_dir}/config.json", 'r')
config = json.loads(file.read())
whales = config['whales']


def load_oceans() -> gpd.GeoDataFrame:
    """
    Load a shape file containing ocean geographic data into a GeoDataFrame
    
    Returns:
        geopandas.GeoDataFrame
    """
    logger.info('Loading ocean shapefile..')
    gdf = gpd.read_file('data/Global_Oceans_and_Seas_version_1/goas_v01.shp')
    return gdf


class WhaleDataManager():
    """Pandas and GeoPandas functionalities for handling whale data 
    obtained from OBIS (https://obis.org/).
    """
    key_list = [
    'occurrenceID', 'verbatimEventDate', 'eventDate', 'eventTime', 'decimalLatitude', 'decimalLongitude', 'coordinatePrecision', 'coordinateUncertaintyInMeters',  
    'locality', 'waterBody', 'bathymetry', 'sst', 'sss', 'shoredistance', 'taxonRemarks', 'individualCount', 'vernacularName', 'order', 'orderid', 'family', 'familyid', 
    'genus', 'genusid','species', 'speciesid','rightsHolder', 'ownerInstitutionCode', 'recordedBy','associatedMedia', 'basisOfRecord', 'occurrenceRemarks', 'bibliographicCitation'
    ]
    data_dir = './data'


    def __init__(self, whale: str, startdate: Optional[str]=None, enddate: Optional[str]=None) -> None:
        """
        Args:
            whale: str
                Name for file paths and column values
            startdate, enddate: str
                Used for filepath searching and csv writing.
                If no specific arguments are supplied, a function call will get these values
        """
        if whale in whales:
            self.whale = whale
        else:
            raise ValueError(f'{whale} not in whales dictionary. {whales.keys()}')
        self.start = startdate
        self.end = enddate


    def match_files(self) -> list:
        """
        Get json files that match the class instance's start and end date attributes

        Returns:
            list[Path]
        """
        whale_dir = Path(f'{self.data_dir}/{self.whale}')
        files = list(whale_dir.glob('*.json'))
        if not files:
            raise FileNotFoundError(f'No files found in: {whale_dir}')
        matched = []

        if self.start and self.end:
            start_year = parse(self.start).year
            end_year = parse(self.end).year

            for file in files:
                match = re.search(r'(\d{4})-\d{2}-\d{2}\--(\d{4})-\d{2}-\d{2}', file.name)
                if match:
                    file_start_year = int(match.group(1))
                    file_end_year = int(match.group(2))

                    if start_year <= file_start_year <= end_year and start_year <= file_end_year <= end_year:
                        matched.append(file)
            if matched:
                return matched
            else:
                raise FileNotFoundError('No matching files found')


        elif self.start and not self.end:
            start_year = parse(self.start).year

            for file in files:
                match = re.search(r'(\d{4})-\d{2}-\d{2}\--\d{4}-\d{2}-\d{2}', file.name)
                if match:
                    file_start_year = int(match.group(1))

                    if start_year <= file_start_year:
                        matched.append(file)
            if matched:
                return matched
            else:
                raise FileNotFoundError('No matching files found')
        

        elif not self.start and self.end:
            end_year = parse(self.end).year

            for file in files:
                match = re.search(r'\d{4}-\d{2}-\d{2}\--(\d{4})-\d{2}-\d{2}', file.name)
                if match:
                    file_end_year = int(match.group(1))

                    if file_end_year <= end_year:
                        matched.append(file)
            if matched:
                return matched
            else:
                raise FileNotFoundError('No matching files found')
        
        else:
            return files


    def filter_keys(self) -> list:
        """
        Remove irrelevant keys from returned json response for later processing
        
        Returns:
            list[list[dict]]
        """
        key_list = self.key_list
        filtered_response = []
        files = self.match_files()
        
        response_files = [open(file, 'r') for file in files]
        for file in response_files:
            r = json.loads(file.read())
            results = r['results']
            results = [{k:v for k, v in d.items() if k in key_list} for d in results if isinstance(d, dict)]
            filtered_response.append(results)
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
        df['individualCount'] = pd.to_numeric(df['individualCount'], errors='coerce')
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
            logger.info(f'Invalid date: {date_str}')


    def get_ocean(self, df: pd.DataFrame) -> pd.DataFrame:
        """Check for longitude/latitude point intersections 
        between an ocean GeoDataFrame and whale GeoDataFrame to get consistent ocean locations

        Args: 
            df: pd.DataFrame
                DataFrame object to operate on
        Returns:
            `pd.DataFrame`
        """
        ocean_gdf = load_oceans()
        # Generate whale geodataframe with geometry point column using longitude(x),latitude(y) values
        points_df = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df['decimalLongitude'], df['decimalLatitude']), crs='EPSG:4326')
        # Create joined_df from spatial join intersections between points and polygons
        joined_df = gpd.sjoin(points_df, ocean_gdf, how='left', predicate='intersects')
        # Update waterBody names
        df['waterBody'] = joined_df['name']
        return df
    

    def get_earliest_and_latest(self, dates: pd.Series) -> None:
        """
        Acquire the earliest and latest dates in a `pandas.Series` and save to classes'
        start and end attributes if they are None.

        Args:
            dates: pd.Series
                contains date-like values
        Returns:
            None
        """
        dates_copy = dates.copy(deep=True)

        if not self.start and not self.end:
            for i, d in dates_copy.items():
                if isinstance(d, int) or isinstance(d, str):
                    dates_copy[i] = pd.to_datetime(d).date()

            self.start = min(dates_copy)
            self.end = max(dates_copy)

        elif self.start and not self.end:
            for i, d in dates_copy.items():
                if isinstance(d, int) or isinstance(d, str):
                    dates_copy[i] = pd.to_datetime(d).date()

            self.end = max(dates_copy)
        
        elif self.end and not self.start:
            for i, d in dates_copy.items():
                if isinstance(d, int) or isinstance(d, str):
                    dates_copy[i] = pd.to_datetime(d).date()

            self.start = min(dates_copy)


    def create_dataframe(self) -> pd.DataFrame:
        """
        Create a `pandas.DataFrame` of whale sighting data read from a json file
        and save to a csv file
        
        Returns:
            `pd.DataFrame`
        """
        filtered_response = self.filter_keys()
        key_list = self.key_list
        output_dir = Path(f'{self.data_dir}/{self.whale}')
        output_dir.mkdir(parents=True, exist_ok=True)
        
        df = pd.concat(pd.json_normalize(r) for r in filtered_response)
        df = df.reindex(columns=key_list)
        # Rows with matching event dates, latitude, and longitude are likely the same event
        df = df.drop_duplicates(subset=['eventDate', 'decimalLatitude', 'decimalLongitude'], keep='first')
        df = self.fill_in(df)
        df['eventDate'] = df['eventDate'].apply(self.convert_dates)
        df = self.get_ocean(df)
        self.get_earliest_and_latest(df['eventDate'])
        self.filename = f'{self.start}--{self.end}.csv'
        logger.info(f'Saving dataframe to {output_dir}/{self.filename}')
        df.to_csv(f"{output_dir}/{self.filename}", index=False)
        return df
    
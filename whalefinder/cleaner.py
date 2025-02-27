from calendar import monthrange
import datetime
import json
from pathlib import Path
import re
import sys
from typing import Optional

import geopandas as gpd
import pandas as pd

from logging_setup import setup_logging
from whales import whale_names


logger = setup_logging()


def load_oceans() -> gpd.GeoDataFrame:
    """
    Load a shape file containing ocean geographic data into a GeoDataFrame
    
    Returns:
        geopandas.GeoDataFrame
    """
    logger.info('Loading ocean shapefile..')
    gdf = gpd.read_file('data/GOaS_v1_20211214/goas_v01.shp')
    return gdf


class WhaleDataCleaner:
    """Pandas and GeoPandas functionalities for handling whale data 
    obtained from OBIS (https://obis.org/).
    """
    data_dir = './data'

    def __init__(self, valid_data: dict, error_data: dict, context) -> None:
        """
        Args:
            whale: str
                Name for file paths and column values
            valid_data, error_data: dict
                Data to process. If errors pass checks, they'll be processed with valid data
            startdate, enddate: str
                YYYY-MM-DD format. Used for csv writing.
                If no arguments are supplied, a function call will get values
        """
        if context.whale and context.whale in whale_names:
            self.whale = context.whale
        else:
            raise ValueError(f'{context.whale} not in whale_names dictionary. {whale_names.keys()}')
        self.startdate = context.startdate
        self.enddate = context.enddate
        self.valid_data = valid_data
        self.error_data = error_data

    def fill_in(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Fill in NaN values for occurrenceID and vernacularName columns

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
        return df

    def split_dates(self, date_str: str) -> tuple:
        """
        Split the different formats that the eventDate field comes in into
        start_year, start_month, start_day and end_year, end_month, end_day.
        """
        reg_text_formats = [
        r'^[A-Za-z]+ \d{4}$', # January 2000
        r'^\d{4} [A-Za-z]+$', # 2000 January
        r'^\d{1,2} [A-Za-z]+$', # 07 January
        r'^[A-Za-z]+ \d{1,2}$' # January 07
            ]

        # formats for abbreviated and non-abbreviated months
        parsed_formats = [
            '%b %Y',
            '%Y %b',
            '%d %b',
            '%b %d',
            '%B %Y',
            '%Y %B',
            '%d %B',
            '%B %d'
            ]

        # remove any potential commas and leading/trailing whitespace
        date_str = date_str.replace(',', '').lstrip(' ').rstrip(' ')

        # PARSING DATES WITH LETTER CHARACTERS
        for r_fmt, p_fmt in zip((reg_text_formats) * 2, parsed_formats):
            if re.match(r_fmt, date_str):
                try:
                    date = datetime.strptime(date_str, p_fmt).date()
                    # if no date value was present in the date_str format
                    if '%d' not in p_fmt: 
                        end_day = monthrange(date.year, date.month)[1]
                        return date.year, date.month, date.day, date.year, date.month, end_day
                    else:
                        return (date.year, date.month, date.day) * 2
                except ValueError as e:
                    pass

        try:
            # PARSING DATES WITH NO LETTER CHARACTERS
            # ex: '1972-07-10/1972-07-14'
            if '/' in date_str and '-' in date_str:
                start_date, end_date = date_str.split('/')
                # Remove any potential timezone strings
                time_format = re.compile(r'T.*')
                start_date = re.sub(time_format, '', start_date)
                end_date = re.sub(time_format, '', end_date)
                start_year, start_month, start_day = start_date.split('-')
                end_year, end_month, end_day = end_date.split('-')
                return tuple(map(int, (start_year, start_month, start_day, end_year, end_month, end_day)))
            
            # ex: '1952/1955'
            elif '/' in date_str and '-' not in date_str:
                start_year, end_year = date_str.split('/')
                return int(start_year), 1, 1, int(end_year), 12, 31
            
            # ex: 1952-1955
            elif '-' in date_str and '/' not in date_str:
                date_parts = date_str.split('-')
                if len(date_parts) == 2:
                    year, month = map(int, (date_parts))
                    # check if month is an actual month and not a year, e.g. '2003-05'
                    if month > 0 and month <= 12:
                        end_day = monthrange(year, month)[1]
                        return year, month, 1, year, month, end_day
                    else:
                        # if the string was '1920-1950', month value is actually a year
                        end_year = month
                        return year, 1, 1, end_year, 12, 31
                
                if len(date_parts) == 3:
                    year, month, day = date_str.split('-')
                    return tuple(map(int, (year, month, day))) * 2
            # When the value is just a year
            else:
                return int(date_str), 1, 1, int(date_str), 12, 31
            
        except ValueError:
            logger.warning(f"Failed to process incorrect date format: {date_str}")
            return tuple([0]) * 6

    def is_valid_date(self, date_str: str) -> bool:
        """
        Returns bool value to be passed to DataFrame column
        """
        format = r'^\d{4}-\d{2}-\d{2}$'
        if re.match(format, date_str):
            return True
        else:
            return False

    def get_start_and_end(self, df: pd.DataFrame) -> None:
        """
        If start and end attributes are none, acquire them from dates in dataframe

        Args:
            df: pd.DataFrame
        Returns:
            None
        """
        valid_dates = df[df['date_is_valid'] == True]

        if not self.startdate and not self.enddate:
            self.startdate = min(valid_dates['eventDate'])
            self.enddate = max(valid_dates['eventDate'])

        elif self.startdate and not self.enddate:
            self.enddate = max(valid_dates['eventDate'])
        
        elif self.enddate and not self.startdate:
            self.startdate = min(valid_dates['eventDate'])

        else:
            pass

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
        logger.info("Performing geodata operations..")
        points_df = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df['decimalLongitude'], df['decimalLatitude']), crs='EPSG:4326')
        # Create joined_df from spatial join intersections between points and polygons
        joined_df = gpd.sjoin(points_df, ocean_gdf, how='left', predicate='intersects')
        # Update waterBody names
        df['waterBody'] = joined_df['name']
        return df
    
    def build_error_dataframe(self) -> pd.DataFrame:
        """
        Build dataframe from data that failed validation for processing attempts

        Returns:
            multi-index `pd.DataFrame`
        """
        error_data = self.error_data
        if error_data['errors']:
            dataframes = []

            for i, error in enumerate(error_data['errors']):
                details = error['details']
                input = error['data']

                for j, detail in enumerate(details):
                # Flatten the detail dictionary
                    flat_detail = {f"detail_{k}": v for k, v in detail.items()}
                    # unpack values into single dict
                    record = {**flat_detail, **input}
                    df = pd.DataFrame.from_records([record])

                    # 'i' is the error index, 'j' is the detail index
                    df.index = pd.MultiIndex.from_tuples([(i, j)], names=['error', 'detail'])  
                    dataframes.append(df)

            error_df = pd.concat(dataframes)
            return error_df
        else:
            logger.info(f"No errors present")
            error_df = pd.DataFrame({'': []})
            return error_df 
        
    # TODO Is this still necessary?
    def error_df_to_json(self, error_df: pd.DataFrame) -> None:
        """
        Convert failed error processing attempts back to a dictionary and save to json

        Args:
            error_df: pd.DataFrame
        Returns:
            None
        """
        error_dict = {}
        data_dir = self.data_dir
        whale = self.whale
        output_dir = Path(f"{data_dir}/{whale}/errors")
        output_dir.mkdir(parents=True, exist_ok=True)

        for (error_index, detail_index), row in error_df.iterrows():
            # access df column and row's column value
            details = {col.replace('detail_', ''): row[col] for col in error_df.columns if 'detail_' in col}
            data = {col: row[col] for col in error_df.columns if 'detail_' not in col}

            if 'detail_loc' in details:
                details['loc'] = details['detail_loc'].split(',')

            if error_index not in error_dict:
                error_dict[error_index] = {'details': [], 'data': data}

            # append details to the correct error dictionary
            error_dict[error_index]['details'].append(details)

        error_dict = {'errors': [error for index, error in error_dict.items()]}

        # replace NaN values with null
        nan_regex_pattern = re.compile(r'\bnan\b', re.IGNORECASE)
        error_dict = json.dumps(error_dict, ensure_ascii=False)
        error_dict = re.sub(nan_regex_pattern, 'null', error_dict)
        error_dict = json.loads(error_dict)

        #TODO write code to attempt reprocessing?
        with open(f'{output_dir}/error_data.json', 'w') as file:
            print(f"Saving errors to {file.name}")
            json.dump(error_dict, file, ensure_ascii=False, indent=4)
            
    def process_error_data(self, error_df: pd.DataFrame) -> pd.DataFrame:
        """
        Handle error data. Errors successfully processed will continue through the pipeline.
        Errors not processed are removed from pipeline and saved to json.

        Args:
            error_df: pd.DataFrame
        Returns:
            pd.DataFrame
        """
        if not error_df.empty:
            num_errors = len(error_df)
            error_df[['start_year', 'start_month', 'start_day', 'end_year', 'end_month', 'end_day']] = error_df['eventDate'].apply(lambda x: pd.Series(self.split_dates(x)))
            error_df['processed'] = (error_df[['start_year', 'start_month', 'start_day', 'end_year', 'end_month', 'end_day']] != 0).all(axis=1)
            
            # Differentiate rows that were successfully processed
            processed_df = error_df[error_df['processed']].copy()
            processed_df.reset_index(drop=True, inplace=True)
            processed_df.drop(columns=['detail_type', 'detail_loc', 'detail_msg', 'processed'], inplace=True)
            processed_df.drop_duplicates(inplace=True)

            # save errors that failed to process
            error_df = error_df[error_df['processed'] == False]
            remaining_errors = len(error_df)
            logger.info(f"{num_errors - remaining_errors}/{num_errors} errors processed")
            if not error_df.empty:
                self.error_df_to_json(error_df)

            return processed_df
        
        else:
            return error_df
        
    def process_valid_data(self) -> pd.DataFrame:
        """
        Perform minor transformations on valid data

        Returns:
            pd.DataFrame
        """
        valid_data = self.valid_data
        if valid_data['validated']:
            df = pd.DataFrame([{k:v for k,v in item.items()} for item in valid_data['validated']])
            df[['start_year', 'start_month', 'start_day', 'end_year', 'end_month', 'end_day']] = df['eventDate'].apply(lambda x: pd.Series(self.split_dates(x)))
            return df
        else:
            df = pd.DataFrame({'': []})
            return df

    def merge_data(self) -> pd.DataFrame:
        """
        Concatenate dataframes from processed valid data and processed errors

        Returns:
            pd.DataFrame
        """
        valid_df = self.process_valid_data()
        error_df = self.build_error_dataframe()
        processed_errors_df = self.process_error_data(error_df)
        
        if not valid_df.empty and not processed_errors_df.empty:
            merged_df = pd.concat([valid_df, processed_errors_df], ignore_index=True)
            merged_df['date_is_valid'] = merged_df['eventDate'].apply(self.is_valid_date)
            num_duplicates = merged_df[merged_df.duplicated(subset=['eventDate', 'decimalLatitude', 'decimalLongitude'])]
            merged_df = merged_df.drop_duplicates(subset=['eventDate', 'decimalLatitude', 'decimalLongitude'], keep='first')
            logger.info(f"{len(num_duplicates)} duplicate records removed")
            merged_df = self.fill_in(merged_df)
            merged_df = self.get_ocean(merged_df)
            return merged_df
        
        elif not valid_df.empty and processed_errors_df.empty:
            valid_df['date_is_valid'] = valid_df['eventDate'].apply(self.is_valid_date)
            num_duplicates = valid_df[valid_df.duplicated(subset=['eventDate', 'decimalLatitude', 'decimalLongitude'])]
            valid_df = valid_df.drop_duplicates(subset=['eventDate', 'decimalLatitude', 'decimalLongitude'], keep='first')
            logger.info(f"{len(num_duplicates)} duplicate rows removed")
            valid_df = self.fill_in(valid_df)
            valid_df = self.get_ocean(valid_df)
            return valid_df
        
        elif not processed_errors_df.empty and valid_df.empty:
            processed_errors_df['date_is_valid'] = processed_errors_df['eventDate'].apply(self.is_valid_date)
            num_duplicates = processed_errors_df[processed_errors_df.duplicated(subset=['eventDate', 'decimalLatitude', 'decimalLongitude'])]
            processed_errors_df = processed_errors_df.drop_duplicates(subset=['eventDate', 'decimalLatitude', 'decimalLongitude'], keep='first')
            logger.info(f"{len(num_duplicates)} duplicate rows removed")
            processed_errors_df = self.fill_in(processed_errors_df)
            processed_errors_df = self.get_ocean(processed_errors_df)
            return processed_errors_df
        
        else:
            logger.info("No data to read.")
            sys.exit()

    def process_and_save(self) -> pd.DataFrame:
        """
        Start transformation processes and save to csv file
        
        Returns:
            `pd.DataFrame`
        """
        output_dir = Path(f'{self.data_dir}/{self.whale}')
        output_dir.mkdir(parents=True, exist_ok=True)

        merged_df = self.merge_data()
        self.get_start_and_end(merged_df)
        # TODO do I still need to save csv files?
        self.filename = f"{output_dir}/{self.startdate}--{self.enddate}.csv"
        logger.info(f'Saving dataframe to {self.filename}')
        merged_df.to_csv(f"{self.filename}", index=False)
        return merged_df
        
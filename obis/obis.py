import datetime
import json
import logging
from logging import INFO
import pandas as pd
from pathlib import Path
import requests
import sys
import typer
from typing import Optional

logging.basicConfig(format='[%(asctime)s][%(module)s:%(lineno)04d] : %(message)s', level=INFO, stream=sys.stderr)
logger: logging.Logger = logging

data_dir = './data'
obis_app = typer.Typer(no_args_is_help=True)


def filter_keys(response: requests.Response) -> tuple:
    """
    Remove irrelevant keys from returned response
    
    Args:
        response: `requests.Response`
    Returns:
        tuple[list[dict], list]
    """
    # Relevant keys to keep
    key_list = [
    'occurrenceID', 'verbatimEventDate', 'eventDate', 'eventTime', 'year', 'month', 'day',
    'decimalLatitude', 'decimalLongitude', 'coordinatePrecision', 'coordinateUncertaintyInMeters',  
    'locality', 'waterBody', 'bathymetry', 'sst', 'sss', 'shoredistance', 'taxonRemarks',
    'individualCount', 'vernacularName', 'specificEpithet', 'scientificName', 
    'scientificNameID', 'order', 'orderid', 'family', 'familyid', 'genus', 'genusid',
    'species', 'speciesid', 'rightsHolder', 'ownerInstitutionCode', 'recordedBy','associatedMedia', 
    'basisOfRecord', 'occurrenceRemarks', 'bibliographicCitation'
    ]
    response = response.json()
    response_list = response['results']
    filtered_response_list = [{k:v for k, v in d.items() if k in key_list} for d in response_list if isinstance(d, dict)]
    return filtered_response_list, key_list


def output_json(response: requests.Response, whale: str, start_date: str, end_date: str):
    """
    Save a `requests.Response` to a json file

    Args:
        response: `requests.Response`
            response object to read
        whale: str
            final subdirectory name
        start_date: str
            part of filename
        end_date: str
            part of filename
    Returns:
        None
    """
    output_dir = Path(f'{data_dir}/{whale}')

    output_dir.mkdir(parents=True, exist_ok=True)
    with open(f'{output_dir}/{start_date}--{end_date}.json', 'w') as file:
        logger.info(f"Saving response to json file: '{file.name}'")
        json.dump(response.json(), file, ensure_ascii=False, indent=4)


def fill_ids(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fill in NaN values for occurrenceID column

    Args:
        df: pd.DataFrame
            DataFrame to operate on
    Returns:
        pd.DataFrame
    """
    nan_indices = df[df['occurrenceID'].isnull()].index
    for i, index in enumerate(nan_indices, start=1):
        df.loc[index, 'occurrenceID'] = -i
    return df


def convert_dates(date_str: str) -> datetime.date:
    """
    convert eventDate column strings to datetime.date

    Args:
        date_str: str
            a string in the format of a date
    Returns:
        `datetime.date`
    """
    # split string if it contains multiple datetimes
    try:
        if '/' in date_str:
            date_list = str.split(date_str, '/')
            # only interested in the initial date sighted for now
            date = date_list[0]
            # if date_str is in YYYY format, return just the year without adding month and day by default
            date = pd.to_datetime(date).year if len(date) == 4 else pd.to_datetime(date).date()
        else:
            date = pd.to_datetime(date_str).year if len(date_str) == 4 else pd.to_datetime(date_str).date()
        return date
    except ValueError:
        return f"Invalid date: {date_str}"


def create_dataframe(response: requests.Response, whale: str, start_date: str, end_date: str):
    """
    Create a `pandas.DataFrame` from Obis API response

    Args:
        response: `requests.Response`
            response object to read
        whale: str
            final subdirectory name
        start_date: str
            part of filename
        end_date: str
            part of filename
    Returns:
        `pandas.DataFrame`
    """
    filtered = filter_keys(response)
    response_list = filtered[0]
    key_list = filtered[1]
    output_dir = Path(f'{data_dir}/{whale}')

    output_dir.mkdir(parents=True, exist_ok=True)
    df = pd.json_normalize(response_list)
    df = df.reindex(columns=key_list)
    fill_ids(df)
    # Rows with duplicate event dates, latitude, and longitude are likely the same event
    df['eventDate'] = df['eventDate'].apply(convert_dates)
    df = df.drop_duplicates(subset=['eventDate', 'decimalLatitude', 'decimalLongitude'], keep='first')
    logger.info(f"Saving dataframe to csv: '{output_dir}/{start_date}--{end_date}.csv'")
    df.to_csv(f'{output_dir}/{start_date}--{end_date}.csv', index=False)
    return df


def merge_data(whale: str) -> pd.DataFrame:
    """
    Create and save a merged dataframe from related csv files
    
    Args:
        whale: str
            whale species folder to filter by
    Returns:
        pd.DataFrame
    """
    output_dir = Path(f'{data_dir}/{whale}')
    
    csv_list = [file for file in output_dir.glob('*.csv')]
    df = pd.concat([pd.read_csv(csv) for csv in csv_list], ignore_index=True)


@obis_app.command('obis_request')
def obis_request(whale: str, start_date: str, end_date: str, json: bool=True, dataframe: bool=True) -> requests.Response:
    """
    Send a get request to the obis api (https://api.obis.org/v3) to find information of encounters with a specified whale species.

    Args:
        whale: str
            Whale to filter by
        start_date: str
            Date to start search in the format of `YYYY-MM-DD`
        end_date: str
            Date to end search in the format of `YYYY-MM-DD`
        json: bool, default True
            Save response to a json file
        dataframe: bool, default True
            Save response to `pandas.DataFrame`
    Returns:
        `requests.Response`
    """
    whale_list = {'blue_whale': {'scientific_name': 'Balaenoptera musculus'}, 'sperm_whale': {'scientific_name': 'Physeter macrocephalus'}}
    assert whale in whale_list, f'Enter a whale from whale_list: {whale_list}'
    
    api = 'https://api.obis.org/v3'
    try:
        scientific_name = whale_list[whale]['scientific_name']
        print(scientific_name)
        url = f'{api}/occurrence?scientificname={scientific_name}&startdate={start_date}&enddate={end_date}'
        url = url.replace(' ', '%20')
        r = requests.get(url)
        print(r.status_code)
        if json:
            output_json(response=r, whale=whale, start_date=start_date, end_date=end_date)
        if dataframe:
            create_dataframe(response=r, whale=whale, start_date=start_date, end_date=end_date)
        return r
    except requests.exceptions.RequestException as e:
            logger.info(e)




if __name__ == '__main__':
    obis_app()
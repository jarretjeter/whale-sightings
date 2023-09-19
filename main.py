import datetime as dt
from whalefinder import obis, manager
from storage import *



def main():
    now = str(dt.datetime.now().date())
    obis_api = obis.ObisAPI('blue_whale', '1758-01-01', now)
    obis_api.api_requests()
    wdm = manager.WhaleDataManager('blue_whale', '1758-01-01', now)
    wdm.create_dataframe()
    to_mysql(wdm.whale, wdm.filename)


if __name__ == '__main__':
    print('Running pipeline')
    main()
    print('Pipeline finished')
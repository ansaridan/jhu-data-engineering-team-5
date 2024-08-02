from urllib.request import urlopen
import pandas as pd
import json

URL_STATION_INFO = "https://gbfs.lyft.com/gbfs/2.3/bkn/en/station_information.json"

def get_station_info_data():
    with urlopen(URL_STATION_INFO) as url:
        data_station_status = json.load(url)
    
    columns = ["station_id", "short_name", "name", "lat", "lon", "region_id", "capacity", "rental_uris"]
    station_data = {column:[] for column in columns}
    for station in data_station_status["data"]["stations"]:
        for column in columns:
            if column in station.keys():
                station_data[column].append(station[column])
            else:
                station_data[column].append(None)
    
    df_station_status = pd.DataFrame(station_data)
    df_station_status["rental_uris"] = df_station_status["rental_uris"].astype(str)
    return df_station_status
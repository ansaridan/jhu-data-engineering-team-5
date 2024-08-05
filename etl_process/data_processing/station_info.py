from urllib.request import urlopen
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
from shapely import wkt
from pathlib import Path
import json

URL_STATION_INFO = "https://gbfs.lyft.com/gbfs/2.3/bkn/en/station_information.json"
df_zip = pd.read_csv(Path(__file__).parents[1] / 'raw_data' / 'nyc_zip_boundaries.csv')

def map_lat_lon_data_to_zip(df_lat_lon: pd.DataFrame, df_zip: pd.DataFrame) -> pd.Series:
    # Load MODZCTA data from CSV
    modzcta_df = df_zip.copy()
    modzcta_df['geometry'] = modzcta_df['the_geom'].apply(wkt.loads)
    modzcta_gdf = gpd.GeoDataFrame(modzcta_df, geometry='geometry')
    modzcta_gdf.set_crs(epsg=4326, inplace=True)  # Ensure it has the correct CRS
    
    # Load station data and create GeoDataFrame
    geometry = [Point(xy) for xy in zip(df_lat_lon['lon'], df_lat_lon['lat'])]
    geo_df = gpd.GeoDataFrame(df_lat_lon, geometry=geometry)
    geo_df.set_crs(epsg=4326, inplace=True)
    
    # Perform the spatial join
    joined_gdf = gpd.sjoin(geo_df, modzcta_gdf, how='left', predicate='within')
    
    # Select relevant columns
    result_df = joined_gdf[['lat', 'lon', 'MODZCTA']]

    return result_df['MODZCTA']

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
    df_station_status["zip_code"] = \
        pd.to_numeric(map_lat_lon_data_to_zip(df_station_status, df_zip), errors='coerce') \
        .apply(lambda x: f"{int(x):05d}" if pd.notna(x) and x != 'inf' and x == x else None)
    
    return df_station_status

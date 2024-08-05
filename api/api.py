from flask import Flask, request, jsonify
import sqlite3
import pandas as pd
import requests
from bs4 import BeautifulSoup
import psycopg2
from datetime import datetime
import json
import psycopg2
import psycopg2.extras

app = Flask(__name__)

# In order for the results of our different methods to be accessed throughout, we need to initalize them outside their respective functions
# I am starting them out as dictionaries.
filt_precip = {}
filt_station = {}
filt_general = {}
filt_pressure = {}
filt_sky = {}
filt_wind = {}



# Define a GET endpoint to get all the station ids from the station id table
@app.route('/get_station_ids', methods=['GET'])
def get_station_ids():

    # Connect to database
    conn = psycopg2.connect(
        dbname='new_db', 
        user='awesome_user', 
        password='awesome_password', 
        host='postgres', 
        port='5432'
    )
    ## Create a cursor object to interface with psql
    cur = conn.cursor()

    # execute a select * query to retrieve the data from the wiki_data table in the wiki schema 
    cur.execute("SELECT distinct station_id FROM citibike_project.station_info")

    # Set the results to the variable response so it can be retrieved in the response json
    response = cur.fetchall()

    return jsonify(response)


# Define a GET endpoint to get the results of our wiki_data table from our wiki schema
@app.route('/read_precip_data', methods=['GET'])
def read_precip_data():

    # Connect to database
    conn = psycopg2.connect(
        dbname='new_db', 
        user='awesome_user', 
        password='awesome_password', 
        host='postgres', 
        port='5432'
    )
    ## Create a cursor object to interface with psql
    cur = conn.cursor()

    # execute a select * query to retrieve the data from the wiki_data table in the wiki schema 
    cur.execute("SELECT * FROM citibike_project.weather_precip")
    weather_hold = cur.fetchall()
    weather_precip = pd.DataFrame(weather_hold)
    weather_precip.columns = ['valid', 'date', 'time', 'hour', 'one_hour_precip_amount', 'wxcode1', 'wxcode2', 'ice_accretion_1hr', 
                             'ice_accretion_3hr', 'ice_accretion_6hr', 'snowdepth']
    
    data = request.json

    try:
        start_date = data.get('start_date')
        if start_date != "":
            # reformat the start date to be a date
            start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
            # filter the dataframes based on the start_date value
            weather_precip = weather_precip.loc[(weather_precip['date'] >= start_date)]
    except Exception:
        pass
        
    try:
        end_date = data.get('end_date')
        if end_date != "":
            # reformat the end date to be a date
            end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
            # filter the dataframes based on the start_date value
            weather_precip = weather_precip.loc[(weather_precip['date'] <= end_date)]
    except Exception:
        pass
        
    try:
        start_time = data.get('start_time')
        if start_time != "":
            # filter the dataframes based on the start_date value
            weather_precip['hour'] = weather_precip['hour'].astype(int)
            weather_precip = weather_precip.loc[(weather_precip['hour'] >= int(start_time))]
    except Exception:
        pass

    try:
        end_time = data.get('end_time')
        if end_time != "":
            # filter the dataframes based on the start_date value
            weather_precip = weather_precip.loc[(weather_precip['hour'] <= int(end_time))]
    except Exception:
        pass

    # make date column a string so it can be read in json format
    weather_precip['date'] = weather_precip['date'].astype(str)
    weather_precip['time'] = weather_precip['time'].astype(str)
    weather_precip['valid'] = weather_precip['valid'].astype(str)

    # save the table off as a dictionary so it can be uploaded to the database.
    filt_precip = weather_precip.to_dict(orient='records')
    #data_id = len(filt_precip) + 1
    #filt_precip = weather_precip_dict

    #return jsonify({"Response:": "Your data has been filtered!"})
    return jsonify(filt_precip)

    # Define a GET endpoint to get the results of our wiki_data table from our wiki schema
@app.route('/read_general_data', methods=['GET'])
def read_general_data():

    # Connect to database
    conn = psycopg2.connect(
        dbname='new_db', 
        user='awesome_user', 
        password='awesome_password', 
        host='postgres', 
        port='5432'
    )
    ## Create a cursor object to interface with psql
    cur = conn.cursor()

    # execute a select * query to retrieve the data from the wiki_data table in the wiki schema 
    cur.execute("SELECT * FROM citibike_project.weather_general")
    weather_hold = cur.fetchall()
    weather_general = pd.DataFrame(weather_hold)
    weather_general.columns = ['valid', 'date', 'time', 'hour', 'temperature_f', 'dewpoint_f', 'relative_humidity', 'real_feel_f', 
                              'visibility']
    
    data = request.json

    try:
        start_date = data.get('start_date')
        if start_date != "":
            # reformat the start date to be a date
            start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
            # filter the dataframes based on the start_date value
            weather_general = weather_general.loc[(weather_general['date'] >= start_date)]
    except Exception:
        pass
        
    try:
        end_date = data.get('end_date')
        if end_date != "":
            # reformat the end date to be a date
            end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
            # filter the dataframes based on the start_date value
            weather_general = weather_general.loc[(weather_general['date'] <= end_date)]
    except Exception:
        pass
        
    try:
        start_time = data.get('start_time')
        if start_time != "":
            # filter the dataframes based on the start_date value
            weather_general['hour'] = weather_general['hour'].astype(int)
            weather_general = weather_general.loc[(weather_general['hour'] >= int(start_time))]
    except Exception:
        pass

    try:
        end_time = data.get('end_time')
        if end_time != "":
            # filter the dataframes based on the start_date value
            weather_general = weather_general.loc[(weather_general['hour'] <= int(end_time))]
    except Exception:
        pass

    # make date column a string so it can be read in json format
    weather_general['date'] = weather_general['date'].astype(str)
    weather_general['valid'] = weather_general['valid'].astype(str)
    weather_general['time'] = weather_general['time'].astype(str)
    #weather_general['valid'] = datetime.strptime(str(weather_general['valid']), '%Y-%m-%d %H:%M:%S') 

    # save the table off as a dictionary so it can be uploaded to the database.
    filt_general = weather_general.to_dict(orient='records')
    #data_id = len(filt_general) + 1
    #filt_general[data_id] = weather_general_dict

    #return jsonify({"Response:": "Your data has been filtered!"})
    return jsonify(filt_general)

    # Define a GET endpoint to get the results of our wiki_data table from our wiki schema
@app.route('/read_pressure_data', methods=['GET'])
def read_pressure_data():

    # Connect to database
    conn = psycopg2.connect(
        dbname='new_db', 
        user='awesome_user', 
        password='awesome_password', 
        host='postgres', 
        port='5432')

    ## Create a cursor object to interface with psql
    cur = conn.cursor()
    
    # execute a select * query to retrieve the data from the wiki_data table in the wiki schema 
    cur.execute("SELECT * FROM citibike_project.weather_pressure")
    weather_hold = cur.fetchall()
    weather_pressure = pd.DataFrame(weather_hold)
    weather_pressure.columns = ['valid', 'date', 'time', 'hour', 'altimeter', 'sea_level_pressure']
    
    data = request.json

    try:
        start_date = data.get('start_date')
        if start_date != "":
            # reformat the start date to be a date
            start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
            # filter the dataframes based on the start_date value
            weather_pressure = weather_pressure.loc[(weather_pressure['date'] >= start_date)]
    except Exception:
        pass
        
    try:
        end_date = data.get('end_date')
        if end_date != "":
            # reformat the end date to be a date
            end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
            # filter the dataframes based on the start_date value
            weather_pressure = weather_pressure.loc[(weather_pressure['date'] <= end_date)]
    except Exception:
        pass
        
    try:
        start_time = data.get('start_time')
        if start_time != "":
            # filter the dataframes based on the start_date value
            weather_pressure['hour'] = weather_pressure['hour'].astype(int)
            weather_pressure = weather_pressure.loc[(weather_pressure['hour'] >= int(start_time))]
    except Exception:
        pass

    try:
        end_time = data.get('end_time')
        if end_time != "":
            # filter the dataframes based on the start_date value
            weather_pressure = weather_pressure.loc[(weather_pressure['hour'] <= int(end_time))]
    except Exception:
        pass

    # make date column a string so it can be read in json format
    weather_pressure['date'] = weather_pressure['date'].astype(str)
    weather_pressure['valid'] = weather_pressure['valid'].astype(str)
    weather_pressure['time'] = weather_pressure['time'].astype(str)

    # save the table off as a dictionary so it can be uploaded to the database.
    filt_pressure = weather_pressure.to_dict(orient='records')
    #data_id = len(filt_pressure) + 1
    #filt_pressure[data_id] = weather_pressure_dict


    #return jsonify({"Response:": "Your data has been filtered!"})
    return jsonify(filt_pressure)

    # Define a GET endpoint to get the results of our wiki_data table from our wiki schema
@app.route('/read_sky_data', methods=['GET'])
def read_sky_data():

    # Connect to database
    conn = psycopg2.connect(
        dbname='new_db', 
        user='awesome_user', 
        password='awesome_password', 
        host='postgres', 
        port='5432')

    ## Create a cursor object to interface with psql
    cur = conn.cursor()
        
    # execute a select * query to retrieve the data from the wiki_data table in the wiki schema 
    cur.execute("SELECT * FROM citibike_project.weather_sky_coverage")
    weather_hold = cur.fetchall()
    weather_sky = pd.DataFrame(weather_hold)
    weather_sky.columns = ['valid', 'date', 'time', 'hour', 'sky_coverage']
    
    data = request.json

    try:
        start_date = data.get('start_date')
        if start_date != "":
            # reformat the start date to be a date
            start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
            # filter the dataframes based on the start_date value
            weather_sky = weather_sky.loc[(weather_sky['date'] >= start_date)]
    except Exception:
        pass
        
    try:
        end_date = data.get('end_date')
        if end_date != "":
            # reformat the end date to be a date
            end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
            # filter the dataframes based on the start_date value
            weather_sky = weather_sky.loc[(weather_sky['date'] <= end_date)]
    except Exception:
        pass
        
    try:
        start_time = data.get('start_time')
        if start_time != "":
            # filter the dataframes based on the start_date value
            weather_sky['hour'] = weather_sky['hour'].astype(int)
            weather_sky = weather_sky.loc[(weather_sky['hour'] >= int(start_time))]
    except Exception:
        pass

    try:
        end_time = data.get('end_time')
        if end_time != "":
            # filter the dataframes based on the start_date value
            weather_sky = weather_sky.loc[(weather_sky['hour'] <= int(end_time))]
    except Exception:
        pass

    # make date column a string so it can be read in json format
    weather_sky['date'] = weather_sky['date'].astype(str)
    weather_sky['valid'] = weather_sky['valid'].astype(str)
    weather_sky['time'] = weather_sky['time'].astype(str)

    # save the table off as a dictionary so it can be uploaded to the database.
    filt_sky = weather_sky.to_dict(orient='records')
    #data_id = len(filt_sky) + 1
    #filt_sky[data_id] = weather_sky_dict


    #return jsonify({"Response:": "Your data has been filtered!"})
    return jsonify(filt_sky)

    # Define a GET endpoint to get the results of our wiki_data table from our wiki schema
@app.route('/read_wind_data', methods=['GET'])
def read_wind_data():

    # Connect to database
    conn = psycopg2.connect(
        dbname='new_db', 
        user='awesome_user', 
        password='awesome_password', 
        host='postgres', 
        port='5432')

    ## Create a cursor object to interface with psql
    cur = conn.cursor()
        
    # execute a select * query to retrieve the data from the wiki_data table in the wiki schema 
    cur.execute("SELECT * FROM citibike_project.weather_wind")
    weather_hold = cur.fetchall()
    weather_wind = pd.DataFrame(weather_hold)
    weather_wind.columns = ['valid', 'date', 'time', 'hour', 'direction', 'windspeed_mph', 'windspeed_knots', 'max_windspeed_mph', 
                            'max_windspeed_knots', 'max_windspeed_time']
    
    data = request.json

    try:
        start_date = data.get('start_date')
        if start_date != "":
            # reformat the start date to be a date
            start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
            # filter the dataframes based on the start_date value
            weather_wind = weather_wind.loc[(weather_wind['date'] >= start_date)]
    except Exception:
        pass
        
    try:
        end_date = data.get('end_date')
        if end_date != "":
            # reformat the end date to be a date
            end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
            # filter the dataframes based on the start_date value
            weather_wind = weather_wind.loc[(weather_wind['date'] <= end_date)]
    except Exception:
        pass
        
    try:
        start_time = data.get('start_time')
        if start_time != "":
            # filter the dataframes based on the start_date value
            weather_wind['hour'] = weather_wind['hour'].astype(int)
            weather_wind = weather_wind.loc[(weather_wind['hour'] >= int(start_time))]
    except Exception:
        pass

    try:
        end_time = data.get('end_time')
        if end_time != "":
            # filter the dataframes based on the start_date value
            weather_wind = weather_wind.loc[(weather_wind['hour'] <= int(end_time))]
    except Exception:
        pass

    # make date column a string so it can be read in json format
    weather_wind['date'] = weather_wind['date'].astype(str)
    weather_wind['valid'] = weather_wind['valid'].astype(str)
    weather_wind['time'] = weather_wind['time'].astype(str)
    weather_wind['max_windspeed_time'] = weather_wind['time'].astype(str)

    # save the table off as a dictionary so it can be uploaded to the database.
    filt_wind = weather_wind.to_dict(orient='records')
    #data_id = len(filt_wind) + 1
    #filt_wind[data_id] = weather_wind_dict

    #return jsonify({"Response:": "Your data has been filtered!"})
    return jsonify(filt_wind)

    # Define a GET endpoint to get the results of our wiki_data table from our wiki schema
@app.route('/read_station_data', methods=['GET'])
def read_station_data():

    # Connect to database
    conn = psycopg2.connect(
        dbname='new_db', 
        user='awesome_user', 
        password='awesome_password', 
        host='postgres', 
        port='5432')

    ## Create a cursor object to interface with psql
    cur = conn.cursor()
        
    # Do the same for the station ids
    # execute a select * query to retrieve the data from the wiki_data table in the wiki schema 
    cur.execute("SELECT distinct station_id FROM citibike_project.station_info")
    station_hold = cur.fetchall()
    stations = pd.DataFrame(station_hold)
    stations.columns = ['station_id']

    try:
        station_id = data.get('station_id')
        if station_id != "":
            # split the start index for the station_id
            x = station_id.split("-")
            start_idx = x[0]
            end_idx = x[1]

            # filter the dataframes based on the start_idx and end_idx values
            stations = stations[int(start_idx):int(end_idx)]
            
    except Exception:
        pass

    # Do the same for the station ids
    # execute a select * query to retrieve the data from the wiki_data table in the wiki schema 
    cur.execute("SELECT * FROM citibike_project.station_info")
    station_hold = cur.fetchall()
    station_df = pd.DataFrame(station_hold)
    station_df.columns = ['short_name', 'station_id', 'name', 'lat', 'lon', 'region_id', 'capacity', 'rental_uris']

    station_df_filt = station_df[station_df['station_id'].isin(stations['station_id'])]
            

    # save the table off as a dictionary so it can be uploaded to the database.
    filt_station = station_df_filt.to_dict(orient='records')
    #data_id = len(filt_station) + 1
    #filt_station[data_id] = stations_dict

    
    #return jsonify({"Response:": "Your data has been filtered!"})
    return jsonify(filt_station)

if __name__ == '__main__':
    port = 8001
    app.run(debug=True, port=port)

from flask import Flask, request, jsonify
import pandas as pd
import psycopg2
from datetime import datetime
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


def get_db_connection_and_cursor():
    """Initializes a Postgres DB connection and cursor."""
    conn = psycopg2.connect(
        dbname="new_db",
        user="awesome_user",
        password="awesome_password",
        host="postgres",
        port="5432",
    )
    cur = conn.cursor()
    return conn, cur


def parse_params(data):
    start_date, end_date, start_time, end_time = None, None, None, None
    # parse state date
    try:
        start_date = data.get("start_date")
        start_date = (
            datetime.strptime(start_date, "%Y-%m-%d").date()
            if start_date != ""
            else None
        )
    except Exception:
        start_date = None

    # parse end date
    try:
        end_date = data.get("end_date")
        end_date = (
            datetime.strptime(end_date, "%Y-%m-%d").date() if end_date != "" else None
        )
    except Exception:
        end_date = None

    try:
        start_time = data.get("start_time")
        start_time = int(start_time) if start_time != "" else None
    except Exception:
        start_time = None

    try:
        end_time = data.get("end_time")
        end_time = int(end_time) if end_time != "" else None
    except Exception:
        end_time = None

    return {
        "start_date": start_date,
        "end_date": end_date,
        "start_time": start_time,
        "end_time": end_time,
    }


def filter_by_params(df, params):
    df_filtered = df.copy()
    if params["start_date"]:
        df_filtered = df_filtered.loc[(df_filtered["date"] >= params["start_date"])]
    if params["end_date"]:
        df_filtered = df_filtered.loc[(df_filtered["date"] <= params["end_date"])]
    if params["start_time"]:
        df_filtered = df_filtered.loc[
            (df_filtered["hour"].astype(int) >= params["start_time"])
        ]
    if params["end_time"]:
        df_filtered = df_filtered.loc[
            (df_filtered["hour"].astype(int) <= params["end_time"])
        ]
    return df_filtered


@app.route("/get_station_ids", methods=["GET"])
def get_station_ids():
    """GET endpoint to return all Citibike station ids."""
    conn, cur = get_db_connection_and_cursor()

    # execute a select * query to retrieve the data from the wiki_data table in the wiki schema
    cur.execute("SELECT DISTINCT station_id FROM citibike_project.station_info")

    # Set the results to the variable response so it can be retrieved in the response json
    response = cur.fetchall()

    return jsonify(response)


# Define a GET endpoint to get the results of our wiki_data table from our wiki schema
@app.route("/read_station_data", methods=["GET"])
def read_station_data():

    conn, cur = get_db_connection_and_cursor()

    # Do the same for the station ids
    # execute a select * query to retrieve the data from the wiki_data table in the wiki schema
    try:
        data = request.json
        station_id = data["station_id"]
        query = f"""
            SELECT * FROM citibike_project.station_info WHERE station_id = '{station_id}'
        """
        stations = pd.read_sql_query(query, conn).to_dict()
    except:
        return {"status": [f"{station_id} not found"]}

    return jsonify(stations)


def get_weather_data(table_name, request):
    conn, cur = get_db_connection_and_cursor()

    # execute a select * query to retrieve the data
    query = f"""SELECT * FROM citibike_project.{table_name}"""
    weather_precip = pd.read_sql_query(query, conn)

    data = request.json
    params = parse_params(data)
    print(params)
    weather_data = filter_by_params(weather_precip, params)

    # make date column a string so it can be read in json format
    for str_col in ["date", "time", "valid"]:
        weather_data[str_col] = weather_data[str_col].astype(str)

    return jsonify(weather_data.to_dict())


@app.route("/read_precip_data", methods=["GET"])
def read_precip_data():
    """GET endpoint to get precipitation weather data."""
    return get_weather_data("weather_precip", request)


# Define a GET endpoint to get the results of our wiki_data table from our wiki schema
@app.route("/read_general_data", methods=["GET"])
def read_general_data():
    return get_weather_data("weather_general", request)

    # Define a GET endpoint to get the results of our wiki_data table from our wiki schema


@app.route("/read_pressure_data", methods=["GET"])
def read_pressure_data():
    return get_weather_data("weather_pressure", request)

    # Define a GET endpoint to get the results of our wiki_data table from our wiki schema


@app.route("/read_sky_data", methods=["GET"])
def read_sky_data():
    return get_weather_data("weather_sky_coverage", request)

    # Define a GET endpoint to get the results of our wiki_data table from our wiki schema


@app.route("/read_wind_data", methods=["GET"])
def read_wind_data():
    return get_weather_data("weather_wind", request)


if __name__ == "__main__":
    port = 8001
    app.run(debug=True, port=port)

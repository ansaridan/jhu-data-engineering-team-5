from flask import Flask, request, jsonify
import sqlite3
import pandas as pd
import requests
from bs4 import BeautifulSoup
import psycopg2

app = Flask(__name__)

# In order for the results of our different methods to be accessed throughout, we need to initalize them outside their respective functions
# I am starting them out as dictionaries.
post_data = {}
del_data = {}
put_data = {}

# Define a GET endpoint to get the results of our wiki_data table from our wiki schema
@app.route('/read_wx_precip_data', methods=['GET'])
def read_data():

    ## Connect to Database
    conn = psycopg2.connect(
        host="localhost", # "localhost"
        port=5432,
        database="new_db",
        user="awesome_user",
        password="awesome_password")
    ## Create a cursor object to interface with psql
    cur = conn.cursor()

    # execute a select * query to retrieve the data from the wiki_data table in the wiki schema 
    cur.execute("SELECT * FROM final_project.weather_precip;")

    # Set the results to the variable response so it can be retrieved in the response json
    response = cur.fetchall()

    return jsonify(response)

if __name__ == '__main__':
    port = 8001
    app.run(debug=True, port=port)

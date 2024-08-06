# Final Project: Citibike Data
Data Engineering Practices & Principles, EN.685.652.8VL.SU24

Hannah Haas, Jeremy Hirschler, Isabel Perry, Dan Ansari

---

# Project Description
This repository contains all of the code to:
- **Coallate Data**: coallates data from the Citibike rideshare program in NYC, alongside weather and demographic data
- **Build a Database**: creates a Postgres database of the collected data
- **Run an automated report**: starts an Airflow server which generates+stores an HTML report
- **Deploys an API**: runs an API which allows the user to query Citibike station information

The project has been configured to separate the data coallaction step from the above steps so that:
- The below steps can be followed to build the database from included CSV files of processed data. This allows for more seamless running of the report automation and API
- Scripts to generate each dataset are included separately and have instructions for each respective script to be run

---

# Environment Set Up

## Docker Image Creation
The below requires [Docker Desktop](https://www.docker.com/products/docker-desktop/) be installed first.

1. Start your Docker application

- **Linux**: `sudo systemctl start docker`
- **Mac/PC**: Start the Docker Desktop application

2. Clone this repo to a directory on your machine

```
git clone https://github.com/ansaridan/jhu-data-engineering-team-5.git
```

3. In a terminal, navigate into the cloned repo directory

```
cd jhu-data-engineering-team-5
```

5.  Create the Docker image from the repo's specifications + start the containers

```
docker-compose up --build
```

5.  Build the database from within the docker container
After confirming that the containers are running (especially the Postgres container), run the ETL script from a CLI in the `jhu-data-enginerring-team-5_jupyter_1` container:
```
python3 etl_process/etl.py
```
This file should output logs along the way, but will take 5-10 minutes to run.

---

# Using the data outputs
After ensuring that the docker containers are running, and the ETL script has run successfully, you can interact with our data environment as follows. We have configured our `docker-compose` to start each of the Jupyter Notebooks, Airflow Webserver+Scheduler and API apps, but it's worth checking that they are all up and running.

## Running automated reporting in Airflow
- You can access the Airflow webserver at `http://localhost:8080/home` with username/password = admin@example.com/admin
- Inside Airflow, you can run the report by turning on the `run_report_dag`
- The resulting interactive HTML reports will appear in `reporting/reports` and can be viewed in Chrome

## Running/Using the Flask API
- The Flask API can be found at `http://localhost:8001` and has a few key functinoailities:
  - `/get_station_ids` = TKTK Hannah to Document
  - `/read_precip_data` = TKTK Hannah to Document
  - `/read_general_data` = TKTK Hannah to Document

## Running Jupyter for Development
- You can use Jupyter for development at `localhost:8080` with the token provided in the container logs

---

# Deliverables Summary

The project requirements are:
**[1] Create at least five separate tables based on the data schema**
- The tables in our project are outlined in the `etl_process/schemas` folder

**[2] Create an entity-relationship diagram (ERD) of your schema in PDF format, including any
keys or relationships**
- The ERD diagram can be found in at `etl_process/erd`

**[3] Create an automation and an API to export or consume an aggregated view or interesting report of the data.**
- See the above section `Running automated reporting in Airflow` and you can find the outputs in `reporting/reports`

**[4] Jupyter Notebook(s) or Python script(s) containing the data transformation
code.**

The following scripts can be run after the environment set up to generate our processed CSV files:
- script 1
- script 2
- script 2

**[4] Have SQL script containing the table creation and data loading commands.**
- The `etl_process.etl.py` script comingles Python and SQL commands around our schema YAML files and processed CSVs to set up the database

**[5] Have automation code and a web API implementation in Python**
- See above notes on the Flask API 

**[6] Include required Docker files**
- see the repo's `Dockerfile` and `docker-compose.yml`

**[7] Include documentation file in PDF format (that can just be a save of this markdown file)**
- see the PDF version of this file under `documentation.pdf`

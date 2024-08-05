# Final Project: Citibike Data
Data Engineering Practices & Principles, EN.685.652.8VL.SU24

Hannah Haas, Jeremy Hirschler, Isabel Perry, Dan Ansari

---

## Project Description
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

The below forces a full image recreation for consistency but can be truncated at `up` for subsequent runs.
```
docker-compose up --build --force-recreate --no-cache
```

5.  Build the database from within the docker container
After confirming that the containers are running (especially the Postgres container), run the ETL script from a CLI in a container:
```
python3 etl_process/etl.py
```

---

# Using the data outputs
After ensuring that the docker containers are running, and the ETL script has run successfully, you can interact with our data environment as follows:

## Running/Using the Report Airflow
- Follow the link in the running image and use the username/password in the Docker file

## Running/Using the Flask API
- 

## Running Jupyter for Development
- Follow the link in the running image and put in the provided token

---

## Deliverables & Deadlines

The project requirements are:
- Create at least five separate tables based on the data schema.
- Create an entity-relationship diagram (ERD) of your schema in PDF format, including any
keys or relationships. You can use any tool to create the ERD, such as Draw.io or Lucidchart.
- Create an automation and an API to export or consume an aggregated view or interesting report of the data. 
- Jupyter Notebook(s) or Python script(s) containing the data transformation
code.
- Have SQL script containing the table creation and data loading commands.
- Have automation code and a web API implementation in Python
- Include required Docker files
- Include documentation file in PDF format (that can just be a save of this markdown file)

**Wednesday, July 31st**
- All to have python functions committed to the repo to handle their respective
  - **DAN**: function to parse Citibike API data into table
  - **ISABEL**: function to download and parse monthly citibike ride data
    - See example [here](https://github.com/dfansari/235labs-flask/blob/master/labsAppFramework/citibike/etl.ipynb) if you want an idea of how to do that programmatically in python (notebook cell 24)
  - **HANNAH**: function to download and parse weather data
  - **JEREMY**: function to download and parse Census and IRS data

Note that if it is not possible to have the task run end-to-end in python, we'll need to commit the data to this repo and add a COPY function into the docker container creation -- so it's preferable if we can handle that in python.

**DAN**: Create ERD in online platform that we can all contribute to
**ALL**: Settle on report design and implementation

**Monday, August 4th**
- **ALL** Add respective tables to ERD
- **SOMEONE CLAIM** Set up 'interesting report'

**Tuesday, August 6th**
- **DAN** turn this in

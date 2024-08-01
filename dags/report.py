# # docker exec -it jhu_docker_airflow-jupyter_1 airflow webserver
# # docker exec -it jhu_docker_airflow-jupyter_1 airflow schedu
# # MODULE 7 - 100 points total

# import os
# import json
# from datetime import datetime
# from typing import Optional, Any
# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
# from airflow.operators.dummy_operator import DummyOperator
# from airflow.utils.trigger_rule import TriggerRule
# from datetime import datetime, timedelta


# import psycopg2
# import psycopg2.extras
# import psycopg2.extensions as psql_ext
# from psycopg2 import sql
# import plotly.graph_objs as go

# # define the default arguments
# default_args = {
#     'owner': 'data_engineer',
#     'start_date': datetime(2024, 4, 12),
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# # define the DAG
# with DAG('run_report_dag', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:

#     def run_report():
#         # PSQL db connection using psycopg2
#         conn = psycopg2.connect(
#             dbname='new_db', 
#             user='awesome_user', 
#             password='awesome_password', 
#             host='postgres', 
#             port='5432'
#         )
        
#         df_stations = pd.read_sql_query(f"SELECT * FROM {PROJECT_SCHEMA}.station_info", conn)
        
#         df_weather_precip = pd.read_sql_query(f"SELECT * FROM {PROJECT_SCHEMA}.weather_precip", conn)
#         df_weather_precip["one_hour_precip_amount"] = pd.to_numeric(df_weather_precip['one_hour_precip_amount'], errors='coerce')
#         df_weather_precip_daily = df_weather_precip.groupby(by="date")[["one_hour_precip_amount"]].sum()
        
#         # Create simple plots
#         bar1 = go.Bar(x=df_weather_precip_daily.index, y=df_weather_precip_daily.one_hour_precip_amount)
#         layout1 = go.Layout()
#         fig1 = go.Figure([bar1], layout1)
        
#         # Create another simple plot
#         # Create a table
#         fig2 = go.Figure(data=[go.Table(
#             header=dict(
#                 values=list(df_largest_stations.to_dict().keys()),
#                 fill_color='paleturquoise',
#                 align='left'
#             ),
#             cells=dict(
#                 values=df_largest_stations.values.T,
#                 fill_color='lavender',
#                 align='left'
#             ))
#         ])
        
#         # Convert figures to HTML strings
#         fig1_html = fig1.to_html(full_html=False, include_plotlyjs='cdn')
#         fig2_html = fig2.to_html(full_html=False, include_plotlyjs='cdn')
        
#         # Create the HTML template
#         template = """
#         <html>
        
#         df_largest_stations = df_stations.sort_values(by="capacity", ascending=False).iloc[:50]
#         <head>
#             <title>Plotly Report</title>
#             <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
#         </head>
#         <body>
#             <h1>CitiBike Report</h1>
#             <div id='divPlotly1'>
#                 <h2>Daily Precipitation</h2>
#                 {fig1_html}
#             </div>
#             <div id='divPlotly2'>
#                 <h2>Largest Stations</h2>
#                 {fig2_html}
#             </div>
#         </body>
#         </html>
#         """
        
#         # Write the HTML report to a file
#         with open('report.html', 'w') as f:
#             f.write(template.format(fig1_html=fig1_html, fig2_html=fig2_html))

#     report_task = PythonOperator(task_id="run_report", python_callable=run_report)
    
    
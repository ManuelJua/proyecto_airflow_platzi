from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.sensors.filesystem import FileSensor

default_args={"dependens_on_past":True}

def _generate_platzi_data(**kwargs):
    import pandas as pd
    data = pd.DataFrame({"student": ["Maria Cruz", "Daniel Crema",
    "Elon Musk", "Karol Castrejon", "Freddy Vega"],
    "timestamp": [kwargs['logical_date'],
    kwargs['logical_date'], kwargs['logical_date'], kwargs['logical_date'],
    kwargs['logical_date']]})
    data.to_csv(f"/tmp/platzi_data_{kwargs['ds_nodash']}.csv",header=True)


with DAG(dag_id="11-Proyecto",
         description="Proyecto final curso airflow de platzi",
         schedule_interval="@daily",
         start_date=datetime(2022,5,30),
         end_date=datetime(2023,10,1),
        #  default_args=default_args,
         max_active_runs=1) as dag:
    
    t_nasa_conf=BashOperator(task_id="NASA_confirmation",
        bash_command="sleep 2 && echo 'OK' > /tmp/response_{{ds}}.txt")

    # t_waiting=ExternalTaskSensor(task_id="waiting_nasa_conf",
    #                             external_task_id="NASA_confimation",
    #                             external_dag_id="11-Proyecto",
    #                             poke_interval=5)

    t_space_x=BashOperator(task_id="space_x_api_call",
            bash_command="curl -o /tmp/history.json -L 'https://api.spacexdata.com/v4/history'")
    
    t_platzi_sat=PythonOperator(task_id='sat_response',
                                python_callable=_generate_platzi_data)
    
    t_file_sensor=FileSensor(task_id="waiting_file",
                  filepath="/tmp/platzi_data_{{ds_nodash}}.csv")
    
    t_visualize=BashOperator(task_id="visualize_csv",
                             bash_command="ls /tmp && head /tmp/platzi_data_{{ds_nodash}}.csv")
    t_send_message=BashOperator(task_id="notify_teams",
                                bash_command="echo '{{ds}}: Los datos a#han sido cargados con exito'")

    t_nasa_conf  >> t_space_x >> t_platzi_sat  >> t_file_sensor >> t_visualize >> t_send_message
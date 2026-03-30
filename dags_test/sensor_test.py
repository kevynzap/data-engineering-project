from airflow.sensors.filesystem import FileSensor
from airflow.decorators import dag, task
from datetime import datetime 

@dag(
    dag_id = "sensor_test",
    start_date = datetime(2026, 1, 1),
    schedule = None,
    catchup = False,
    tags = ["agg_produtos","agg_tempo", "agg_usuarios", "analytics", "gold"]
)

def main():

    wait_for_file = FileSensor(
        task_id="wait_for_aisles_file",
        filepath="input_data/aisles/*.csv", 
        poke_interval=30,
        timeout=60 * 5,
        mode="reschedule", #"poke" -> trava worker
        fs_conn_id="fs_default"
    )

    @task(task_id="teste")
    def test():
        print("final do teste")

    a = test()
    wait_for_file >> a

main()
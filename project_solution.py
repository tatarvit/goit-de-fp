from prefect import flow, task
import subprocess

@task(name="Landing to Bronze")
def landing_to_bronze():
    subprocess.run(["spark-submit", "landing_to_bronze.py"], check=True)

@task(name="Bronze to Silver")
def bronze_to_silver():
    subprocess.run(["spark-submit", "bronze_to_silver.py"], check=True)

@task(name="Silver to Gold")
def silver_to_gold():
    subprocess.run(["spark-submit", "silver_to_gold.py"], check=True)

@flow(name="Batch Data Lake Project")
def batch_data_lake_project():
    landing_to_bronze()
    bronze_to_silver()
    silver_to_gold()

if __name__ == "__main__":
    batch_data_lake_project()
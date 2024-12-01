
import requests as r
from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator


# Function to fetch weather data
def fetch_weather_data(city, api_key):
    lat, lon = 25.7617, 80.1918  # Coordinates (not used here)
    response = r.get(f"https://api.openweathermap.org/data/2.5/weather?q={city}&APPID={api_key}")
    weather = response.json()
    if response.status_code == 200:
        main = weather.get("weather", [{}])[0].get("main", "Unknown")
        description = weather.get("weather", [{}])[0].get("description", "No details")
        temp = weather.get("main", {}).get("temp", "No temperature info")
        return f"Weather in {city}: {main} ({description}) with temperature {temp}K"
    else:
        return f"Failed to fetch weather data for {city}: {weather.get('message', 'Unknown error')}"

# Email content generation
def generate_email_content(**kwargs):
    city = kwargs['city']
    api_key = kwargs['api_key']
    weather_summary = fetch_weather_data(city, api_key)
    return weather_summary
def my_function():
    print("Hello from Airflow!")


# DAG Definition
API_KEY = "89e6d377a181af6a107dd26f8176742d"
CITY = "London"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id='weather_email_dag',
    description="A DAG to fetch weather data and email it",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 12, 1),
    catchup=False,
    tags=["example"],
) as dag:

    # Fetch weather data task
    fetch_weather_task = PythonOperator(
        task_id="fetch_weather_data",
        python_callable=generate_email_content,
        op_kwargs={"city": CITY, "api_key": API_KEY},
    )

    # Email task
    email_task = EmailOperator(
        task_id="send_weather_email",
        to="dha@gmail.com",
        subject=f"Weather Update for {CITY}",
        html_content="{{ ti.xcom_pull(task_ids='fetch_weather_data') }}",
    )

    fetch_weather_task >> email_task
    task = PythonOperator(
    task_id='run_my_function',
    python_callable=my_function
    )
    #this creates a file and contents to file
    bash_task=BashOperator(
        task_id='simple_bash_script',
        bash_command="echo 'Hey this is bash script ;)  '>  /Users/dharmatejasamudrala/airflow/dags/bash.txt "
    )


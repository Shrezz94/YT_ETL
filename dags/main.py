from airflow import DAG
from datetime import datetime,timedelta
import pendulum
from api.video_stats import get_channel_playlist_id, get_video_id, extract_video_data, save_to_json

local_tz = pendulum.timezone("America/Vancouver")
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 1, tzinfo=local_tz),
    'email_on_failure': False,
    'email_on_retry': False,
    'email': ['user@example.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
        dag_id ='youtube_video_stats',
        default_args=default_args,
        description='A DAG to fetch and save YouTube video stats',
        schedule_interval='* 21 * * *',  # Daily at 9 PM
        catchup=False
    ) as dag:
      
        #Define tasks
        
        playlist_ids = get_channel_playlist_id()
        video_id_list = get_video_id(playlist_ids)
        video_data = extract_video_data(video_id_list)
        save_to_json_task =save_to_json(video_data)
        
        # Set task dependencies
        playlist_ids >> video_id_list >> video_data >> save_to_json_task
        
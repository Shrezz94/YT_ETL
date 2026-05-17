import dotenv
import requests
import json 
import os
from dotenv import load_dotenv
from pathlib import Path

# Load .env from the same directory as this script
env_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=env_path)
API_KEY = os.getenv("API_KEY")
print(f"API_KEY: {API_KEY}")


CHANNEL_ID = "MrBeast"
url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_ID}&key={API_KEY}"


def get_channel_playlist_id():
    try:
        response = requests.get(url)
        response.raise_for_status()  # Check for HTTP errors
        data = response.json()

        channel_items = data["items"][0]
        channel_playlistId = channel_items["contentDetails"]["relatedPlaylists"]["uploads"]
        return channel_playlistId
    except requests.exceptions.RequestException as e:
        raise e
    
if __name__ == "__main__":
    get_channel_playlist_id()
  
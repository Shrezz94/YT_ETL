from datetime import date

import dotenv
import requests
import json 
import os
from dotenv import load_dotenv
from pathlib import Path
from airflow.decorators import task
from airflow.models import Variable


API_KEY = Variable.get("API_KEY")

# Headers to mimic Postman
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}

CHANNEL_ID = Variable.get("CHANNEL_HANDLE")
url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_ID}&key={API_KEY}"

@task
def get_channel_playlist_id():
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()  # Check for HTTP errors
        data = response.json()

        channel_items = data["items"][0]
        channel_playlistId = channel_items["contentDetails"]["relatedPlaylists"]["uploads"]
        return channel_playlistId
    except requests.exceptions.RequestException as e:
        raise e

@task  
def get_video_id(playlist_id):
    maxresults = 50
    videoids = []
    nextpagetoken = None
    base_url = f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={maxresults}&playlistId={playlist_id}&key={API_KEY}"
    try:
        while True:
            url=base_url
            if nextpagetoken:
                url += f"&pageToken={nextpagetoken}"
            response = requests.get(url, headers=HEADERS)
            response.raise_for_status()  # Check for HTTP errors
            data = response.json()
            for item in data.get("items", []):
                videoids.append(item["contentDetails"]["videoId"])
            nextpagetoken = data.get("nextPageToken")
            print(f"Fetched {len(videoids)} video IDs so far...")
            if not nextpagetoken:
                break
        return videoids
    except requests.exceptions.RequestException as e:
        raise e

@task           
def extract_video_data(video_id_list):
    video_data = []
    for idx, video_id in enumerate(video_id_list, 1):
        video_url = f"https://youtube.googleapis.com/youtube/v3/videos?part=snippet,statistics&id={video_id}&key={API_KEY}"
        try:
            response = requests.get(video_url, headers=HEADERS)
            response.raise_for_status()  # Check for HTTP errors
            data = response.json()
            for item in data.get("items", []):
                video_id = item.get("id")
                snippet = item.get("snippet", {})
                content_details = item.get("contentDetails", {})
                statistics = item.get("statistics", {})
                video_info = {
                    "video_id": video_id,
                    "title": snippet.get("title"),
                    "published_at": snippet.get("publishedAt"),
                    "duration": content_details.get("duration"),
                    "view_count": statistics.get("viewCount",None),
                    "like_count": statistics.get("likeCount",None),
                    "comment_count": statistics.get("commentCount",None)
                }
                video_data.append(video_info)
                #print(f"Extracted data for video {idx}/{len(video_id_list)}: {video_info['title']}")
            else:
                print(f"No data found for video ID: {video_id}")
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for video ID {video_id}: {e}")
    return video_data         

@task
def save_to_json(data):
    #Path(__file__).parent / "data"  # Ensure data directory exists
   
    path =  Path(__file__).parent / "data" / f"YT_video_data_{date.today()}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
    print("Data saved to video_data.json")
    
    
if __name__ == "__main__":
    playlist_ids=get_channel_playlist_id()
    video_id_list= get_video_id(playlist_ids)
    video_data= extract_video_data(video_id_list)
    save_to_json(video_data)
    
    
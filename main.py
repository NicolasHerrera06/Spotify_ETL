#import sqlalchemy 
#from sqlalchemy import sessionmaker
import pandas as pd 
import requests 
import json 
from datetime import datetime 
import datetime 
import sqlite3

DATABASE_LOCATION = "sqlite: ///my_played_trancks.sqlite"
USER_ID = "Napo_06"
TOKEN = "BQDByv2E1Lsf83U1qK_TYuI6dljPicFMdbeuMw3U23Peka2icqLE-8ean2xKVZRGbv8DozbTplEZF28_xn-xeaHRgPFJ1fgHrufizO6UxnezR3njqfQO_VFZRj8mqt3NukMZpgZkUp61O6j1QJ3yWi5_votbecyBEYn9qaDZEqMYbsUISOKOWxuO-sspUXWyOEA"

if __name__ == "__main__":

    headers  = {
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : "Bearer {token}".format(token=TOKEN)
    }

today = datetime.datetime.now()
yesterday = today - datetime.timedelta(days=1)
yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday_unix_timestamp), headers = headers)

data = r.json()

print(data)

song_names = []
artist_names = []
played_at_list = []
timestamps = []

for song in data["items"]:
    song_names.append(song["track"]["name"])
    artist_names.append(song["track"]["album"]["artists"][0]["name"])
    played_at_list.append(song["played_at"])
    timestamps.append(song["played_at"[0:10]])

song_dict = {
    "song_name" : song_names,
    "artist_names": artist_names,
    "played_at_list": played_at_list,
    "timestamps": timestamps
}

song_df = pd.DataFrame(song_dict, columns = ["song_name","artist_names","played_at_list","timestamps"])

print(song_df)
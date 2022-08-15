#import sqlalchemy 
#from sqlalchemy import sessionmaker
import pandas as pd 
import requests 
import json 
from datetime import datetime 
import datetime 
import sqlite3

DATABASE_LOCATION = "sqlite: ///my_played_trancks.sqlite"
USER_ID = ""
TOKEN = ""

# Genera el token en: https://developer.spotify.com/console/get-recently-played/
# Nota: Necesitas una cuenta de Spotify (puede ser una cuenta gratuita)
# Generate your token here: https://developer.spotify.com/console/get-recently-played/
# Note: You need a Spotify account (can ve easily created for free) 

def check_if_valid_data(df: pd.DataFrame) -> bool:
    # Verifica si el dataframe está vacío
    #Check if dataframe is empty
    if df.empty:
        print("No hay canciones descargadas. Termina la ejecución")
        return False
    # Verifica la Primary Key
    # Primary Key Check
    if pd.Series(df["played_at"]).is_unique:
        pass
    else:
        raise Exception("Hay duplicados por violación en la verificacion de la Primary Key")

    #Verificar nulos
    #Check for nulls 
    if df.isnull().values.any():
        raise Exception("Se encontraron valores nulos")
    
    # Check that all timestamps are of yesterday's date 
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
    
    timestamps = df["timestamp"].tolist()
    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp, "%Y-%m-%d") != yesterday:
            raise Exception("At least one od the returned songs does not come from within the las 24 houres")

    return True 



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
    "played_at": played_at_list,
    "timestamp": timestamps
}

song_df = pd.DataFrame(song_dict, columns = ["song_name","artist_names","played_at","timestamp"])

# Validación
# Validate 
if check_if_valid_data(song_df):
    print("Validación de datos, procesando carga")

#print(song_df)
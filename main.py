import sqlalchemy 
import pandas as pd 
import requests 
import json 
from datetime import datetime 
import datetime 
import sqlite3

DATABASE_LOCATION = "sqlite:///my_played_tracks.sqlite"
USER_ID = "Napo_06"
TOKEN = "BQCRjSS36w8GJqd4zQWblwWRGC2weEGapIqPzGu0RGJys-fQJrhlUNa_YiuPE03xusB7eds8l3HE2UfRLU6JOda5BdESXgI--RrUv6114W-vvVD2LcwGhTEglnLZzz98-UGGVVRSxgVDNFiELXZNwnxoO20qrQ8D0a4FpUs3uqK-GdnD2WKAutj35wNmAgTh-e0"

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
    #yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    #yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
    
    #timestamps = df["timestamp"].tolist()
    #for timestamp in timestamps:
    #    if datetime.datetime.strptime(timestamp, "%Y-%m-%d") != yesterday:
    #        raise Exception("At least one od the returned songs does not come from within the las 24 houres")

    #return True 



if __name__ == "__main__":

    # Parte de extraccion del proceso ETL
    # Extract part of the ETL process

    headers  = {
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : "Bearer {token}".format(token=TOKEN)
    }
# Convierte la hora a formate Unix in milisegundos
# Convert time to Unix timestamp in miliseconds 
today = datetime.datetime.now()
yesterday = today - datetime.timedelta(days=60)
yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday_unix_timestamp), headers = headers)

data = r.json()

#print(data)

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

# Carga
# Load 
engine =  sqlalchemy.create_engine(DATABASE_LOCATION)
conn = sqlite3.connect("my_played_tracks.sqlite")
cursor = conn.cursor()

sql_query = """
CREATE TABLE IF NOT EXISTS my_played_tracks(
    song_name VARCHAR(200),
    artist_name VARCHAR(200),
    played_at VARCHAR(200), 
    timestamp VARCHAR(200),
    CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
)
"""

cursor.execute(sql_query)
print("Creación de tablas exitosa")

try:
    song_df.to_sql("my_played_tracks", engine, index=False, if_exists='append')
except:
    print("Los datos ya existen en la tabla")

conn.close()
print("Se cierra la conexión")
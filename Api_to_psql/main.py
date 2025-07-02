import requests
import os
import json
from dotenv import load_dotenv
from db import get_db_connection


load_dotenv()

API_KEY = os.getenv("WEATHER_API_KEY")
LOCATION = os.getenv("LOCATION")
BASE_URL = "http://api.weatherapi.com/v1/current.json"

def fetch_weather():
    url = f"{BASE_URL}?key={API_KEY}&q={LOCATION}&aqi=yes"  
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()

       
        print(" Weather data fetched from API:")
        print(json.dumps(data, indent=4))

        return data
    else:
        print(f" Error fetching data: {response.status_code}, {response.text}")
        return None

def insert_weather_data(data):
    conn = get_db_connection()
    cursor = conn.cursor()

    city = data["location"]["name"]
    country = data["location"]["country"]
    temp_c = data["current"]["temp_c"]
    condition = data["current"]["condition"]["text"]
    wind_kph = data["current"]["wind_kph"]
    humidity = data["current"]["humidity"]

  
    air_quality = json.dumps(data["current"].get("air_quality", {}))

    insert_query = """
        INSERT INTO weather_data (
            city, country, temperature_c, condition,
            wind_kph, humidity, air_quality
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    cursor.execute(insert_query, (
        city, country, temp_c, condition,
        wind_kph, humidity, air_quality
    ))

    conn.commit()
    cursor.close()
    conn.close()
    print("Data inserted successfully into PostgreSQL.")

def main():
    weather = fetch_weather()
    if weather:
        insert_weather_data(weather)

if __name__ == "__main__":
    main()

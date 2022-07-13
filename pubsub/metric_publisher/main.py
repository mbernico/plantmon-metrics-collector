from typing import Dict
import base64
import json
import os
import logging
import requests

from flask import Flask, request, jsonify
from google.cloud import bigquery

WEATHER_URL = 'https://api.openweathermap.org/data/2.5/weather'
LAT = '41.886260'
LON = '-87.776760'

client = bigquery.Client()

logger = logging.getLogger()
logger.setLevel(os.getenv('LOG_LEVEL', 'DEBUG'))

app = Flask(__name__)

def write_row_to_bq(device_data: Dict, weather_data: Dict) -> int:
  """Writes data to bq.
  
  Returns: -1 for errors, else 0.
  """
  row = [
    {
    'device_id': device_data['device_id'], 
    'moisture_pct': device_data['moisture_pct'], 
    'moisture_value': device_data['moisture_value'],
    'temperature': weather_data['temperature'],
    'humidity': weather_data['humidity'],
    'timestamp': device_data['publish_time'],
    'pressure': weather_data['pressure'],
    'wind_speed': weather_data['wind_speed'],
    'weather_description': weather_data['weather_description']
    }
  ]

  errors = client.insert_rows_json(os.getenv('TABLE_ID'), row)
  if errors == []:
    logging.debug("New rows have been added.")
    return -1
  else:
    logging.debug("Encountered errors while inserting rows: {}".format(errors))
    return 0

def get_weather():
  """Gets current weather"""
  weather_api_key = os.getenv('WEATHER_API_KEY')
  url = f'{WEATHER_URL}?lat={LAT}&lon={LON}&units=metric&appid={weather_api_key}'
  r = requests.get(url)
  data = r.json()
  return {
    'temperature': float(data['main']['temp']),
    'humidity': float(data['main']['humidity']),
    'pressure': float(data['main']['pressure']),
    'wind_speed': float(data['wind']['speed']),
    'weather_description': str(data['weather'][0]['description'])
  }


@app.route('/', methods=['POST'])
def index():
  envelope = request.get_json()
  try:
    pubsub_message = envelope['message']
  except TypeError:
    msg = 'Message not contained in envelope.'
    logging.ERROR(msg)
    return f'Bad Request: {msg}', 500
  except KeyError:
    msg = 'Key error in envelope.'
    logging.ERROR(msg)
    return f'Bad Request: {msg}', 500

  # Convert data to a JSON string, then a dict.
  data = base64.b64decode(pubsub_message['data']).decode('utf-8').strip()
  try:
    data = json.loads(data)
  except json.JSONDecodeError:
    logging.warn('Empty JSON, not a weather message?')
    # ignore and move on.
    return jsonify(success=True)

  device_data = {
      'device_id': str(pubsub_message['attributes']['deviceId']),
      'publish_time': str(pubsub_message['publishTime']),
      'moisture_pct': float(data['MoisturePct']),
      'moisture_value': float(data['MoistureVal'])
  }
  weather_data = get_weather()
  errors = write_row_to_bq(device_data, weather_data)

  if errors:
    return jsonify(success=False)
  else:
    return 'BQ Write Failure', 500

if __name__ == '__main__':
    PORT = int(os.getenv('PORT', 8080))

    # This is used when running locally. Gunicorn is used to run the
    # application on Cloud Run. See entrypoint in Dockerfile.
    app.run(host='127.0.0.1', port=PORT, debug=True)
from typing import Dict
import base64
import json
import os
import logging

from flask import Flask, request, jsonify
from google.cloud import bigquery

TABLE_ID = 'fresh-replica-355617.plantmon.telemetry'

client = bigquery.Client()

logger = logging.getLogger()
logger.setLevel(os.getenv('LOG_LEVEL', 'DEBUG'))

app = Flask(__name__)

def write_row_to_bq(
    device_id: str,
    data: Dict,
    temperature: float, 
    humidity: float,
    publish_time: str,
    pressure: float,
    wind_speed: float,
    weather_description: str) -> int: 
  """Writes data to bq.
  
  Returns: -1 for errors, else 0.
  """
  row = [{
    'device_id': device_id, 
    'moisture_pct': float(data['MoisturePct']), 
    'moisture_value': float(data['MoistureVal']), 
    'temperature': temperature, 
    'humidity': humidity,
    'timestamp': publish_time,
    'pressure': pressure,
    'wind_speed': wind_speed,
    'weather_description': weather_description}]

  errors = client.insert_rows_json(TABLE_ID, row)  # Make an API request.
  if errors == []:
    logging.debug("New rows have been added.")
    return -1
  else:
   logging.debug("Encountered errors while inserting rows: {}".format(errors))
   return 0

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

  device_id = pubsub_message['attributes']['deviceId']
  logging.debug(f'DeviceId: {device_id}')

  publish_time = pubsub_message['publishTime']
  logging.debug(f'Publish Time: {publish_time}')

  # Data will be a JSON string.
  data = base64.b64decode(pubsub_message["data"]).decode("utf-8").strip()
  logging.debug(f'Data: {data}')
  data = json.loads(data)

  temperature = -1
  humidity = -1
  pressure = -1
  wind_speed = -1
  weather_description = ""

  errors = write_row_to_bq(device_id, data, temperature, humidity,
      publish_time, pressure, wind_speed, weather_description)

  if errors:
    return jsonify(success=False)
  else:
    return 'BQ Write Failure', 500

if __name__ == '__main__':
    PORT = int(os.getenv('PORT', 8080))

    # This is used when running locally. Gunicorn is used to run the
    # application on Cloud Run. See entrypoint in Dockerfile.
    app.run(host='127.0.0.1', port=PORT, debug=True)
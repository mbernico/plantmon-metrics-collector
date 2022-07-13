import base64
import os
import logging

from flask import Flask, request

logger = logging.getLogger()
logger.setLevel(os.getenv('LOG_LEVEL', 'DEBUG'))

app = Flask(__name__)

@app.route('/', methods=['POST'])
def index():

  envelope = request.get_json()
  try:
    pubsub_message = envelope['message']
  except TypeError:
    msg = 'Message not contained in envelope.'
    logging.ERROR(msg)
    return f'Bad Request: {msg}', 400
  except KeyError:
    msg = 'Key error in envelope.'
    logging.ERROR(msg)
    return f'Bad Request: {msg}', 400

  device_id = pubsub_message['attributes']['deviceId']
  logging.debug(f'DeviceId: {device_id}')

  publish_time = pubsub_message['publishTime']
  logging.debug(f'Publish Time: {publish_time}')

  data = base64.b64decode(pubsub_message["data"]).decode("utf-8").strip()
  logging.debug(f'Data: {data}')

if __name__ == '__main__':
    PORT = int(os.getenv('PORT', 8080))

    # This is used when running locally. Gunicorn is used to run the
    # application on Cloud Run. See entrypoint in Dockerfile.
    app.run(host='127.0.0.1', port=PORT, debug=True)
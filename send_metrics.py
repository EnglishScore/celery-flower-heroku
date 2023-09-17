import time
import requests
import base64
import socket
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

FLOWER_BASE_URL = f'https://{os.getenv("HEROKU_APP_DEFAULT_DOMAIN_NAME")}/api'
FLOWER_BASIC_AUTH = os.getenv("FLOWER_BASIC_AUTH")
if FLOWER_BASIC_AUTH:
    FLOWER_BASIC_AUTH = base64.b64encode(FLOWER_BASIC_AUTH.encode()).decode("utf-8")
else:
    logger.error("FLOWER_BASIC_AUTH not set in environment variables")
APP_ENV = os.getenv("APP_ENV")
GRAPHITE_ID = os.getenv("GRAPHITE_ID", "")
WAIT_TIME_IN_SECONDS = int(os.getenv("WAIT_TIME_IN_SECONDS", 60))

def send_msg(message):
    logger.info(f"sending message:\n{message}")
    try:
        with socket.socket() as sock:
            sock.connect(("b03e3804.carbon.hostedgraphite.com", 2003))
            sock.sendall(message.encode())
    except Exception as e:
        logger.error(f"Error sending message to graphite: {e}")

def get_broker_queue_lengths():
    """
    See [Flower API docs](https://flower.readthedocs.io/en/latest/api.html) for flower response details

    Returns:
        (dict): eg. {'celery@worker1': 100}
    """

    headers = {
        "Authorization": f"Basic {FLOWER_BASIC_AUTH}"
    }
    
    try:
        response = requests.get(f"{FLOWER_BASE_URL}/queues/length", headers=headers)
        response.raise_for_status()

        active_queues = response.json().get("active_queues", [])
        return {queue["name"]: queue["messages"] for queue in active_queues}
    
    except requests.RequestException as e:
        logger.error(f"Error fetching broker queue lengths: {e}")
        return None

def send_celery_broker_metrics_to_graphite():
    queue_lengths = get_broker_queue_lengths()
    if not queue_lengths:
        logger.warning("No broker metrics received.")
        return

    data = [f"{GRAPHITE_ID}.{APP_ENV}.broker_queues.{name}.length {length}" 
            for name, length in queue_lengths.items()]

    send_msg("\n".join(data) + "\n")

if __name__ == "__main__":
    while True:
        send_celery_broker_metrics_to_graphite()
        time.sleep(WAIT_TIME_IN_SECONDS)

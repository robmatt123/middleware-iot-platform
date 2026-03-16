BROKER_HOST = "localhost"
BROKER_PORT = 1883
KEEPALIVE = 60

RAW_TOPICS = [
    ("greenfield/raw/+/+", 1),
]

PROCESSED_TOPIC_TEMPLATE = "greenfield/processed/{greenhouse}/{metric}"
ALERT_TOPIC_TEMPLATE = "greenfield/alerts/{greenhouse}"
STATUS_TOPIC = "greenfield/system/middleware/status"

TEMPERATURE_ALERT_THRESHOLD = 28.0
HUMIDITY_ALERT_MIN = 35.0
CO2_ALERT_THRESHOLD = 900.0

VALID_METRICS = {"temperature", "humidity", "co2", "light"}
VALID_UNITS = {"C", "%", "ppm", "lux"}

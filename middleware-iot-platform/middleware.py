import json
import logging
import signal
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import paho.mqtt.client as mqtt

from config import (
    ALERT_TOPIC_TEMPLATE,
    BROKER_HOST,
    BROKER_PORT,
    CO2_ALERT_THRESHOLD,
    HUMIDITY_ALERT_MIN,
    KEEPALIVE,
    PROCESSED_TOPIC_TEMPLATE,
    RAW_TOPICS,
    STATUS_TOPIC,
    TEMPERATURE_ALERT_THRESHOLD,
    VALID_METRICS,
    VALID_UNITS,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

running = True


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_topic(topic: str) -> Optional[Tuple[str, str]]:
    parts = topic.split("/")
    if len(parts) != 4:
        return None

    if parts[0] != "greenfield" or parts[1] != "raw":
        return None

    greenhouse = parts[2]
    metric = parts[3]

    return greenhouse, metric


def validate_payload(data: Dict[str, Any], greenhouse: str, metric: str) -> Tuple[bool, Optional[str]]:
    required_fields = {"sensor_id", "timestamp", "type", "value", "unit"}

    missing = required_fields - data.keys()
    if missing:
        return False, f"Missing fields: {sorted(missing)}"

    if not isinstance(data["sensor_id"], str) or not data["sensor_id"].strip():
        return False, "Invalid sensor_id"

    if data["type"] != metric:
        return False, f"Payload type '{data['type']}' does not match topic metric '{metric}'"

    if metric not in VALID_METRICS:
        return False, f"Unsupported metric '{metric}'"

    if data["unit"] not in VALID_UNITS:
        return False, f"Unsupported unit '{data['unit']}'"

    try:
        float(data["value"])
    except (ValueError, TypeError):
        return False, "Value must be numeric"

    return True, None


def normalize_payload(data: Dict[str, Any], greenhouse: str, metric: str) -> Dict[str, Any]:
    value = float(data["value"])

    normalized = {
        "message_id": f"{greenhouse}-{metric}-{int(datetime.now(timezone.utc).timestamp() * 1000)}",
        "source_topic": f"greenfield/raw/{greenhouse}/{metric}",
        "greenhouse": greenhouse,
        "sensor_id": data["sensor_id"],
        "timestamp": data["timestamp"],
        "processed_at": utc_now_iso(),
        "metric": metric,
        "value": value,
        "unit": data["unit"],
        "quality": "valid",
    }

    return normalized


def build_alert_if_needed(normalized: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    metric = normalized["metric"]
    value = normalized["value"]
    greenhouse = normalized["greenhouse"]

    if metric == "temperature" and value > TEMPERATURE_ALERT_THRESHOLD:
        return {
            "alert_type": "high_temperature",
            "severity": "warning",
            "greenhouse": greenhouse,
            "metric": metric,
            "value": value,
            "threshold": TEMPERATURE_ALERT_THRESHOLD,
            "timestamp": utc_now_iso(),
            "message": f"Temperature exceeded threshold in {greenhouse}.",
        }

    if metric == "humidity" and value < HUMIDITY_ALERT_MIN:
        return {
            "alert_type": "low_humidity",
            "severity": "warning",
            "greenhouse": greenhouse,
            "metric": metric,
            "value": value,
            "threshold": HUMIDITY_ALERT_MIN,
            "timestamp": utc_now_iso(),
            "message": f"Humidity dropped below threshold in {greenhouse}.",
        }

    if metric == "co2" and value > CO2_ALERT_THRESHOLD:
        return {
            "alert_type": "high_co2",
            "severity": "critical",
            "greenhouse": greenhouse,
            "metric": metric,
            "value": value,
            "threshold": CO2_ALERT_THRESHOLD,
            "timestamp": utc_now_iso(),
            "message": f"CO2 concentration exceeded threshold in {greenhouse}.",
        }

    return None


def publish_json(client: mqtt.Client, topic: str, payload: Dict[str, Any], qos: int = 1, retain: bool = False) -> None:
    result = client.publish(topic, json.dumps(payload), qos=qos, retain=retain)
    if result.rc != mqtt.MQTT_ERR_SUCCESS:
        logging.error("Failed publishing to topic %s", topic)


def publish_status(client: mqtt.Client, status: str) -> None:
    payload = {
        "service": "middleware-iot-platform",
        "status": status,
        "timestamp": utc_now_iso(),
    }
    publish_json(client, STATUS_TOPIC, payload, qos=1, retain=True)


def on_connect(client: mqtt.Client, userdata: Any, flags: Dict[str, Any], rc: int) -> None:
    if rc == 0:
        logging.info("Connected to MQTT broker at %s:%s", BROKER_HOST, BROKER_PORT)

        for topic, qos in RAW_TOPICS:
            client.subscribe(topic, qos=qos)
            logging.info("Subscribed to %s (QoS=%s)", topic, qos)

        publish_status(client, "online")
    else:
        logging.error("Connection failed with result code %s", rc)


def on_disconnect(client: mqtt.Client, userdata: Any, rc: int) -> None:
    if rc != 0:
        logging.warning("Unexpected disconnection from broker (rc=%s)", rc)
    else:
        logging.info("Disconnected from broker")


def on_message(client: mqtt.Client, userdata: Any, msg: mqtt.MQTTMessage) -> None:
    logging.info("Message received on topic %s", msg.topic)

    parsed = parse_topic(msg.topic)
    if not parsed:
        logging.warning("Invalid topic format: %s", msg.topic)
        return

    greenhouse, metric = parsed

    try:
        raw_data = json.loads(msg.payload.decode("utf-8"))
    except json.JSONDecodeError:
        logging.error("Invalid JSON payload on topic %s", msg.topic)
        return

    is_valid, error = validate_payload(raw_data, greenhouse, metric)
    if not is_valid:
        logging.error("Payload validation failed: %s", error)
        return

    normalized = normalize_payload(raw_data, greenhouse, metric)

    processed_topic = PROCESSED_TOPIC_TEMPLATE.format(
        greenhouse=greenhouse,
        metric=metric
    )
    publish_json(client, processed_topic, normalized, qos=1)
    logging.info("Published normalized message to %s", processed_topic)

    alert_payload = build_alert_if_needed(normalized)
    if alert_payload:
        alert_topic = ALERT_TOPIC_TEMPLATE.format(greenhouse=greenhouse)
        publish_json(client, alert_topic, alert_payload, qos=1)
        logging.warning("Published alert to %s: %s", alert_topic, alert_payload["alert_type"])


def handle_shutdown(signum: int, frame: Any) -> None:
    global running
    logging.info("Shutdown signal received")
    running = False


def main() -> None:
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    client = mqtt.Client(client_id="middleware-iot-platform")
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message

    client.will_set(
        STATUS_TOPIC,
        payload=json.dumps({
            "service": "middleware-iot-platform",
            "status": "offline",
            "timestamp": utc_now_iso(),
        }),
        qos=1,
        retain=True
    )

    client.connect(BROKER_HOST, BROKER_PORT, KEEPALIVE)
    client.loop_start()

    logging.info("Middleware service started")

    try:
        while running:
            signal.pause()
    except AttributeError:
        import time
        while running:
            time.sleep(1)
    finally:
        publish_status(client, "offline")
        client.loop_stop()
        client.disconnect()
        logging.info("Middleware service stopped")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        logging.exception("Fatal middleware error: %s", exc)
        sys.exit(1)

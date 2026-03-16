# Middleware IoT Platform

Application-level middleware for smart agriculture based on MQTT.

## Features

- subscribes to raw sensor topics
- validates JSON payloads
- normalizes messages
- republishes processed data
- generates alerts on threshold violations
- publishes service status

## Topics

### Raw
`greenfield/raw/<greenhouse>/<metric>`

### Processed
`greenfield/processed/<greenhouse>/<metric>`

### Alerts
`greenfield/alerts/<greenhouse>`

### Service status
`greenfield/system/middleware/status`

## Run

```bash
pip install -r requirements.txt
python middleware.py
```

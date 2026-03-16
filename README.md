# middleware-iot-platform
Middleware MQTT platform for agricultural IoT integration.
# Middleware IoT Platform

This repository describes the MQTT middleware used to connect IoT sensors and applications in a smart agriculture environment.

## Architecture

The middleware uses:

- MQTT protocol
- Eclipse Mosquitto broker
- Publish/Subscribe communication model

## Components

Producer → IoT sensors  
Broker → Mosquitto  
Consumer → Monitoring application

## Topics

greenfield/serra01/temperature  
greenfield/serra01/humidity  
commands/serra01/cooling

## Requirements

Install Mosquitto:

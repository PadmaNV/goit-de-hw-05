from kafka import KafkaConsumer, KafkaProducer
from configs import kafka_config
import json

# Налаштування Kafka Consumer
consumer = KafkaConsumer(
    "VN_building_sensors",  # Топік для читання даних
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda v: v.decode('utf-8'),
    auto_offset_reset='earliest',  # Зчитування повідомлень з початку
    enable_auto_commit=True,       # Автоматичне підтвердження зчитаних повідомлень
    group_id='data_processor_group'
)


# Налаштування Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')
)



# Топіки для сповіщень
temperature_alert_topic = "VN_temperature_alerts"
humidity_alert_topic = "VN_humidity_alerts"

print("Subscribed to topic 'VN_building_sensors'")

# Обробка повідомлень
try:
    for message in consumer:
        sensor_data = message.value
        sensor_id = sensor_data.get("sensor_id")
        temperature = sensor_data.get("temperature")
        humidity = sensor_data.get("humidity")
        timestamp = sensor_data.get("timestamp")

        # Генерація сповіщення для температури
        if temperature > 40:
            alert = {
                "sensor_id": sensor_id,
                "timestamp": timestamp,
                "temperature": temperature,
                "message": "Спека неможлива, температура більше 40°C"
            }
            producer.send(temperature_alert_topic, key=sensor_id, value=alert)
            print(f"Temperature alert sent: {alert}")

        # Генерація сповіщення для вологості
        if humidity > 80 or humidity < 20:
            alert = {
                "sensor_id": sensor_id,
                "timestamp": timestamp,
                "humidity": humidity,
                "message": "Чи то вогко чи сухо, вологості більше 80% або менше 20%"
            }
            producer.send(humidity_alert_topic, key=sensor_id, value=alert)
            print(f"Humidity alert sent: {alert}")
except KeyboardInterrupt:
    print("\nProcessing stopped by user.")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    consumer.close()
    producer.close()

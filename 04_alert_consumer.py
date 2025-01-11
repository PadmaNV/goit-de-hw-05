from kafka import KafkaConsumer
from configs import kafka_config
import json

# Налаштування Kafka Consumer
consumer = KafkaConsumer(
    "VN_temperature_alerts",  # Топік для сповіщень температури
    "VN_humidity_alerts",    # Топік для сповіщень вологості
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',  # Читати повідомлення з початку
    enable_auto_commit=True,       # Автоматичне підтвердження зчитаних повідомлень
    group_id='alert_consumer_group'
)

print("Subscribed to topics: 'VN_temperature_alerts', 'VN_humidity_alerts'")

# Обробка сповіщень
try:
    for message in consumer:
        alert_data = message.value
        topic = message.topic
        print(f"Received alert from topic '{topic}': {alert_data}")
except KeyboardInterrupt:
    print("\nAlert processing stopped by user.")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    consumer.close()

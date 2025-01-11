from kafka import KafkaProducer
from configs import kafka_config
import json
import time
import random
import uuid

# Створення Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')  # Серіалізація ключа
)

# Ідентифікатор для топіку
topic_name = "VN_building_sensors"

# Генерація постійного ID датчика для цього запуску
sensor_id = str(uuid.uuid4())[:10]

print(f"Simulating data for sensor ID: {sensor_id}")

# Імітація надсилання даних
try:
    while True:
        # Генерація випадкових даних
        data = {
            "sensor_id": sensor_id,
            "timestamp": time.time(),  # Час отримання даних
            "temperature": round(random.uniform(25, 45), 2),  # Температура (від 25 до 45)
            "humidity": round(random.uniform(15, 85), 2)  # Вологість (від 15 до 85)
        }
        # Використання ID датчика як ключа
        key = sensor_id
        # Відправлення даних у топік
        producer.send(topic_name, key=key, value=data)
        producer.flush()
        print(f"Data sent with value: {data}")
        time.sleep(2)  # Відправлення даних кожні 2 секунд
except KeyboardInterrupt:
    print("\nSimulation stopped by user.")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    producer.close()



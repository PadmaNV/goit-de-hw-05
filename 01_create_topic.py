from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config

# Створення клієнта Kafka
admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)

# Ідентифікатор для топіків
identifier = "VN"

# Визначення трьох нових топіків
topics = [
    NewTopic(name=f"{identifier}_building_sensors", num_partitions=2, replication_factor=1),
    NewTopic(name=f"{identifier}_temperature_alerts", num_partitions=1, replication_factor=1),
    NewTopic(name=f"{identifier}_humidity_alerts", num_partitions=1, replication_factor=1)
]


# Створення нового топіку якщо він не існує
try:
    existing_topics = admin_client.list_topics()
    topics_to_create = [
        topic for topic in topics if topic.name not in existing_topics
    ]
    if topics_to_create:
        admin_client.create_topics(new_topics=topics_to_create, validate_only=False)
        print(f"Topics {[topic.name for topic in topics_to_create]} created successfully.")
    else:
        print("All topics already exist.")
except Exception as e:
    print(f"An error occurred: {e}")


all_topics = admin_client.list_topics()
my_topics = [topic for topic in all_topics if topic.startswith(identifier)]
print("My topics:", my_topics)



# Закриття зв'язку з клієнтом
admin_client.close()


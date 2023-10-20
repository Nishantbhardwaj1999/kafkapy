import csv
import threading
from confluent_kafka import Producer, Consumer, KafkaError

producer_config = {
    'bootstrap.servers': 'localhost:9092',  # Kafka broker address
    'client.id': 'kafka-producer'
}
def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}] - {msg.offset()}')

# Kafka consumer configuration
consumer_config = {
    'bootstrap.servers': 'localhost:9092',  # Kafka broker address
    'group.id': 'kafka-group',
    'auto.offset.reset': 'earliest'  # Start reading the topic from the beginning if no offset is stored
}

# Kafka topic to which you want to send and receive messages
kafka_topic = 'first-topic'

# Function to read data from CSV file and send it to Kafka
def kafka_producer(csv_file_path, bootstrap_servers, kafka_topic):
    # Create a Kafka producer instance
    producer = Producer(producer_config)

    # Read data from CSV file and send messages to Kafka
    with open(csv_file_path, 'r') as csvfile:
        csvreader = csv.DictReader(csvfile)
        for row in csvreader:
            # Assuming 'message_column' is the column in your CSV containing the message data
            message = row['orderid']
            # Send the message to Kafka topic
            producer.produce(kafka_topic, message.encode('utf-8'), callback=delivery_report)
            # Wait for any outstanding messages to be delivered and delivery reports received
            producer.poll(0)
    producer.flush()
# Function to consume messages from Kafka topic
def kafka_consumer(consumer_config, kafka_topic):
    # Create a Kafka consumer instance
    consumer = Consumer(consumer_config)

    # Subscribe to the Kafka topic
    consumer.subscribe([kafka_topic])

    # Poll for messages
    while True:
        msg = consumer.poll(1.0)  # Poll for messages, waiting at most 1 second for new messages
        
        if msg is None:
            continue
        if msg.error():
            # Handle errors
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event - not an error
                continue
            else:
                print(msg.error())
                break

        # Process the received message
        print('Received message: {}'.format(msg.value().decode('utf-8')))

    # Close the Kafka consumer
    consumer.close()

# Example usage of the producer and consumer functions
csv_file_path = 'output\order_data.csv'

# Start producer and consumer threads
producer_thread = threading.Thread(target=kafka_producer, args=(csv_file_path, producer_config['bootstrap.servers'], kafka_topic))
consumer_thread = threading.Thread(target=kafka_consumer, args=(consumer_config, kafka_topic))

producer_thread.start()
consumer_thread.start()

# Wait for threads to finish (you can handle this differently based on your application's requirements)
producer_thread.join()
consumer_thread.join()

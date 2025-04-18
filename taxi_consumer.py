from kafka import KafkaConsumer, KafkaProducer
import json

def process_data(record):
    try:
        pickup_location = record.get('PULocationID', None)
        trip_distance = float(record.get('trip_distance', 0))
        fare_amount = float(record.get('fare_amount', 0))
        return {
            'pickup_location': pickup_location,
            'average_trip_distance': trip_distance,
            'average_fare': fare_amount
        }
    except Exception as e:
        print(f"Error processing record: {e}")
        return None

def consume_and_process(input_topic, output_topic, bootstrap_servers):
    consumer = KafkaConsumer(
        input_topic,
        bootstrap_servers=bootstrap_servers,
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='earliest',  # Ensure it starts reading from the beginning
        enable_auto_commit=True
    )
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    print("Consumer started...")
    for message in consumer:
        print(f"Consumed: {message.value}")  # Debug log
        processed_record = process_data(message.value)
        if processed_record:
            producer.send(output_topic, value=processed_record)
            print(f"Produced to output topic: {processed_record} \n\n")  # Debug log
    
    consumer.close()
    producer.close()

if __name__ == "__main__":
    consume_and_process(
        input_topic="nyc_taxi_data",
        output_topic="nyc_taxi_aggregates",
        bootstrap_servers=["localhost:9092"]
    )


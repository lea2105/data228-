from kafka import KafkaProducer
import pandas as pd
import json

def serialize_record(record):
    """
    Convert non-serializable types to serializable ones.
    """
    for key, value in record.items():
        if isinstance(value, pd.Timestamp):  # Convert Timestamps to strings
            record[key] = value.isoformat()
        elif pd.isna(value):  # Handle NaN values
            record[key] = None
    return record

def produce_data(parquet_file, topic_name, bootstrap_servers):
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    df = pd.read_parquet(parquet_file)
    print("Loaded data from Parquet file.")
    for _, row in df.iterrows():
        message = serialize_record(row.to_dict())
        producer.send(topic_name, value=message)
        print(f"Produced: {message} \n\n" )  # Debug log
    producer.close()
    print("Finished producing messages.")

if __name__ == "__main__":
    produce_data(
        parquet_file="yellow_tripdata_2025-01.parquet", 
        topic_name="nyc_taxi_data",
        bootstrap_servers=["localhost:9092"]
    )


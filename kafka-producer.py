
import datetime
import threading
from decimal import *
from time import sleep
from uuid import uuid4,UUID
import time

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
import pandas as pd

def delivery_report(err,msg):
    if err is not None:
        print("Delivery failed for User Record {}:{}".format(msg.key(),err))
        return
    print('User Record {} successfully produced to {} [{}] at offset {}'.format(msg.key,msg.topic,msg.partition,msg.offset()))
    print("=====================================")
    

kafka_config={
    'bootstrap.servers':'pkc-p11xm.us-east-1.aws.confluent.cloud:9092',
    'sasl.mechanisms':'PLAIN',
    'security.protocol':'SASL_SSL',
    'sasl.username':'BAHWWFQ5WR2T7WEA',
    'sasl.password':'cfltQm2GwMOODIbfEb5Hbp3co0owBqZduSboiqWCglsOHq5eY5JawpuqRaY8EnfQ'
}

schema_registry_client=SchemaRegistryClient({
    'url':'https://psrc-4m23250.us-east-1.aws.confluent.cloud',
    'basic.auth.user.info':'{}:{}'.format('JBRHGIYPU5ZOM77G','cflt4TWt4PpKBEGt/8BBXDRyCuHY8/vg0PNhcUA4wuwurkHtyzGXnyw76F/d+2DQ')
})

subject_name='retail-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str
print("Schema Registry")
print(schema_str)

key_serializer=StringSerializer('utf-8')
value_serializer=AvroSerializer(schema_registry_client,schema_str)

producer=SerializingProducer({
    'bootstrap.servers':kafka_config['bootstrap.servers'],
    'security.protocol':kafka_config['security.protocol'],
    'sasl.mechanisms':kafka_config['sasl.mechanisms'],
    'sasl.username':kafka_config['sasl.username'],
    'sasl.password':kafka_config['sasl.password'],
    'key.serializer':key_serializer,
    'value.serializer':value_serializer
})

df=pd.read_csv('retail_data.csv')
df=df.fillna(df)
print(df.head(5))
print()
print("df iter rows",df.iterrows())
print()
for index,row in df.iterrows():
    data_value=row.to_dict()
    
   
    producer.produce(
        topic='retail',
        key=str(index),
        value=data_value,
        on_delivery=delivery_report
    )
    producer.flush()
    time.sleep(2)
    

print("All data succesfully published to Kafka")


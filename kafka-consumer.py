import threading
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

kafka_config={
    'bootstrap.servers':'pkc-p11xm.us-east-1.aws.confluent.cloud:9092',
    'sasl.mechanisms':'PLAIN',
    'security.protocol':'SASL_SSL',
    'sasl.username':'BAHWWFQ5WR2T7WEA',
    'sasl.password':'cfltQm2GwMOODIbfEb5Hbp3co0owBqZduSboiqWCglsOHq5eY5JawpuqRaY8EnfQ',
    'group.id':'group2',
    'auto.offset.reset':'earliest'
}

schema_registry_client=SchemaRegistryClient({
    'url':'https://psrc-4m23250.us-east-1.aws.confluent.cloud',
    'basic.auth.user.info':'{}:{}'.format('JBRHGIYPU5ZOM77G','cflt4TWt4PpKBEGt/8BBXDRyCuHY8/vg0PNhcUA4wuwurkHtyzGXnyw76F/d+2DQ')
})

subject_name='retail- value'
schema_str=schema_registry_client.get_latest_version(subject_name).schema.schema_str

key_deserializer=StringDeserializer('utf-8')
value_deserializer=AvroDeserializer(schema_registry_client,schema_str)
consumer=DeserializingConsumer({
    'bootstrap.servers':kafka_config['bootstrap.servers'],
    'sasl.mechanisms':kafka_config['sasl.mechanisms'],
    'security.protocol':kafka_config['security.protocol'],
    'sasl.username':kafka_config['sasl.username'],
    'sasl.password':kafka_config['sasl.password'],
    'key.deserializer':key_deserializer,
    'value.deserializer':value_deserializer,
    'group.id':kafka_config['group.id'],
    'auto.offset.reset':kafka_config['auto.offset.reset']

})


consumer.subscribe(['retail'])
try:
    while True:
        msg=consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Consumer error : {} ".format(msg.error()))
            continue
        print("Succesfully consumed Record with Key {} and Value {}".format(msg.key(),msg.value()))
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
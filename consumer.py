import logging
from confluent_kafka import Consumer, KafkaError

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'pageview-group1',
    'auto.offset.reset': 'earliest',
    'debug': 'broker,topic,msg'  # Enable all debug logging
}

consumer = Consumer(conf)
consumer.subscribe(['pageview'])

logger.info("Starting Kafka consumer...")
logger.info(f"Configuration: {conf}")

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            logger.debug("No message received")
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                logger.info('Reached end of partition')
            else:
                logger.error(f'Error: {msg.error()}')
        else:
            logger.info(f"""
                topic     => {msg.topic()}
                partition => {msg.partition()}
                offset    => {msg.offset()}
                key       => {msg.key().decode('utf-8') if msg.key() else None}
                value     => {msg.value().decode('utf-8')}
            """)

except KeyboardInterrupt:
    logger.info("Keyboard interrupt received. Shutting down...")

finally:
    consumer.close()
    logger.info("Consumer closed.")
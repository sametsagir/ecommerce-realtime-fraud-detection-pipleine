import time
import random
import uuid
import logging
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
TOPIC_NAME = "orders"
SCHEMA_PATH = "../schemas/order_schema.avsc"


class ECommerceTrafficSimulator:
    """
    Simulates user purchase traffic on an e-commerce platform and injects 
    synthetic transaction anomalies (price manipulation, credit card velocity attacks).
    """
    def __init__(self, bootstrap_servers, schema_registry_url, topic):
        self.topic = topic
        
        # Initialize Schema Registry client
        self.schema_registry_client = SchemaRegistryClient({'url': schema_registry_url})
        
        # Load Avro schema definition
        with open(SCHEMA_PATH, "r") as f:
            self.schema_str = f.read()
            
        # Configure serializers for Kafka key-value formats
        self.key_serializer = StringSerializer('utf_8')
        self.value_serializer = AvroSerializer(
            self.schema_registry_client, 
            self.schema_str
        )
        
        # Configure Kafka Producer properties
        producer_conf = {
            'bootstrap.servers': bootstrap_servers,
            'acks': 'all',  # Guarantee message delivery to all replicas
            'retries': 5,
            'max.in.flight.requests.per.connection': 1  # Preserve message ordering during retries
        }
        self.producer = Producer(producer_conf)
        
        self.payment_methods = ["credit_card", "debit_card", "bank_transfer", "mobile_payment"]
        self.product_prices = {
            "PROD-100": 1500.0,
            "PROD-200": 450.0,
            "PROD-300": 8500.0,
            "PROD-400": 120.0,
            "PROD-500": 25000.0
        }

    def generate_normal_order(self) -> dict:
        """Generates a standard customer transaction payload."""
        prod_id = random.choice(list(self.product_prices.keys()))
        price = self.product_prices[prod_id]
        
        order = {
            "order_id": str(uuid.uuid4()),
            "user_id": f"USER-{random.randint(1000, 9999)}",
            "product_id": prod_id,
            "price": price,
            "quantity": random.choice([1, 1, 1, 2]),
            "timestamp": int(time.time() * 1000),  # Epoch milliseconds
            "ip_address": f"192.168.1.{random.randint(2, 254)}",
            "payment_method": random.choice(self.payment_methods)
        }
        return order

    def generate_zero_price_anomaly(self) -> dict:
        """Simulates an application bug resulting in zero price values."""
        prod_id = random.choice(list(self.product_prices.keys()))
        order = self.generate_normal_order()
        order["price"] = 0.0
        order["product_id"] = prod_id
        logger.warning(f"Generated Anomaly (ZERO_PRICE): Order ID: {order['order_id']}")
        return order

    def generate_high_amount_anomaly(self) -> dict:
        """Simulates transaction value spike (possible stolen credit card scenario)."""
        order = self.generate_normal_order()
        random_price = round(random.uniform(50000.0, 150000.0), 2)
        order["price"] = random_price
        logger.warning(f"Generated Anomaly (HIGH_AMOUNT): Price: {random_price} TL, Order ID: {order['order_id']}")
        return order

    def generate_velocity_anomaly_burst(self) -> list:
        """
        Simulates a velocity attack where a script attempts multiple purchase checkouts 
        under a single user account and IP within less than a second.
        """
        user_id = f"SCAMMER-{random.randint(100, 999)}"
        ip_addr = f"10.0.0.{random.randint(2, 254)}"
        orders = []
        
        logger.warning(f"Generating Anomaly (VELOCITY_ATTACK): User: {user_id}")
        for _ in range(8):
            prod_id = random.choice(list(self.product_prices.keys()))
            orders.append({
                "order_id": str(uuid.uuid4()),
                "user_id": user_id,
                "product_id": prod_id,
                "price": self.product_prices[prod_id],
                "quantity": 1,
                "timestamp": int(time.time() * 1000),
                "ip_address": ip_addr,
                "payment_method": "credit_card"
            })
            time.sleep(0.05)  # Pause to verify time-window aggregations in Spark
            
        return orders

    def delivery_report(self, err, msg):
        """Callback to log delivery status confirmed by Kafka broker."""
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

    def run(self):
        """Orchestrates continuous traffic simulation based on probability weight distribution."""
        logger.info("Starting e-commerce traffic simulator. Press Ctrl+C to stop.")
        
        try:
            while True:
                rand_val = random.random()
                
                if rand_val < 0.900:
                    # 90.0% probability: Normal transactional traffic
                    order = self.generate_normal_order()
                    self.send_to_kafka(order)
                    time.sleep(random.uniform(0.2, 0.5))
                    
                elif rand_val < 0.940:
                    # 4.0% probability: Zero price anomaly (system bug)
                    order = self.generate_zero_price_anomaly()
                    self.send_to_kafka(order)
                    time.sleep(random.uniform(0.5, 1.5))
                    
                elif rand_val < 0.970:
                    # 3.0% probability: Stolen card / transaction price spike
                    order = self.generate_high_amount_anomaly()
                    self.send_to_kafka(order)
                    time.sleep(random.uniform(0.5, 1.5))
                    
                else:
                    # 3.0% probability: Velocity bot checkout spike
                    orders = self.generate_velocity_anomaly_burst()
                    for order in orders:
                        self.send_to_kafka(order)
                    time.sleep(random.uniform(1.0, 3.0))
                    
        except KeyboardInterrupt:
            logger.info("Simulator terminated by user.")
        finally:
            logger.info("Flushing pending messages in memory queue...")
            self.producer.flush()

    def send_to_kafka(self, order: dict):
        """Serializes dict record to Avro binary format and writes to Kafka."""
        try:
            self.producer.produce(
                topic=self.topic,
                key=self.key_serializer(order["order_id"]),
                value=self.value_serializer(
                    order, 
                    SerializationContext(self.topic, MessageField.VALUE)
                ),
                callback=self.delivery_report
            )
            self.producer.poll(0)
        except Exception as e:
            logger.error(f"Failed to produce record to Kafka: {str(e)}")


if __name__ == "__main__":
    simulator = ECommerceTrafficSimulator(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        schema_registry_url=SCHEMA_REGISTRY_URL,
        topic=TOPIC_NAME
    )
    simulator.run()

"""
Tarea 3 - Big Data (202016911)
Productor de Kafka - simula la llegada en tiempo real de nuevos pedidos
de la tienda de comercio electronico (mismo catalogo del dataset Online Retail).
Adaptado del ejemplo de sensores del Anexo 3 (kafka_producer.py) al escenario
de "pedidos de e-commerce llegando en tiempo real".
Ejecutar en la VM (Hadoop+Spark+Kafka ya instalados, ver Anexo 3):
    python3 kafka_pedido_producer.py
"""
import time
import json
import random
from kafka import KafkaProducer
PRODUCTS = [
    ('84077', 'WORLD WAR 2 GLIDERS ASSTD DESIGNS', 0.29),
    ('85099B', 'JUMBO BAG RED RETROSPOT', 2.08),
    ('84879', 'ASSORTED COLOUR BIRD ORNAMENT', 1.69),
    ('22197', 'POPCORN HOLDER', 0.85),
    ('21212', 'PACK OF 72 RETROSPOT CAKE CASES', 0.55),
    ('85123A', 'WHITE HANGING HEART T-LIGHT HOLDER', 2.95),
    ('23084', 'RABBIT NIGHT LIGHT', 2.08),
    ('22492', 'MINI PAINT SET VINTAGE', 0.65),
    ('22616', 'PACK OF 12 LONDON TISSUES', 0.29),
    ('21977', 'PACK OF 60 PINK PAISLEY CAKE CASES', 0.55),
    ('22178', 'VICTORIAN GLASS HANGING T-LIGHT', 1.25),
    ('15036', 'ASSORTED COLOURS SILK FAN', 0.83),
    ('17003', 'BROCADE RING PURSE', 0.29),
    ('21915', 'RED HARMONICA IN BOX', 1.25),
    ('22386', 'JUMBO BAG PINK POLKADOT', 2.08),
]
COUNTRIES = ['United Kingdom', 'Germany', 'France', 'EIRE', 'Spain',
             'Netherlands', 'Belgium', 'Switzerland']
_invoice_counter = 90000
def generate_order():
    """Genera un pedido simulado con la misma estructura del dataset real."""
    global _invoice_counter
    _invoice_counter += 1
    stock_code, description, unit_price = random.choice(PRODUCTS)
    quantity = random.randint(1, 20)
    return {
        "InvoiceNo": str(_invoice_counter),
        "StockCode": stock_code,
        "Description": description,
        "Quantity": quantity,
        "UnitPrice": unit_price,
        "Country": random.choice(COUNTRIES),
        "timestamp": int(time.time()),
    }
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8'),
)
TOPIC = "pedidos_ecommerce"
while True:
    pedido = generate_order()
    producer.send(TOPIC, value=pedido)
    print(f"Sent: {pedido}")
    time.sleep(1)

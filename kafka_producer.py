import json
import random
from datetime import datetime, timedelta
from time import sleep
from faker import Faker
from kafka import KafkaProducer

# Initialisation
fake = Faker()
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',  # Nom du service Docker
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Fonction pour générer une transaction
def generate_transaction(i, start_time):
    timestamp = start_time + timedelta(seconds=i)
    return {
        "user_id": f"u{random.randint(10, 9999)}",
        "transaction_id": f"t-{i:07}",
        "amount": round(random.uniform(1.0, 50000.0), 2),
        "currency": random.choice(["EUR", "USD", "GBP", "DH", "CNY"]),
        "timestamp": timestamp.isoformat(),
        "location": fake.city(),
        "method": random.choice(["credit_card", "weero", "paypal", "crypto"]),
        "merchant_id": fake.company()
    }

def generate_carousel_transactions(start_time, user_id, base_i):
    """ Génère 5 petites transactions <30€ chez 5 marchands différents pour un même user """
    transactions = []
    for j in range(5):
        timestamp = start_time + timedelta(seconds=j * 10)  # toutes les 10 secondes
        transaction = {
            "user_id": user_id,
            "transaction_id": f"carousel-{base_i + j}",
            "amount": round(random.uniform(5.0, 29.99), 2),  # < 30
            "currency": "EUR",
            "timestamp": timestamp.isoformat(),
            "location": fake.city(),
            "method": random.choice(["credit_card", "paypal"]),
            "merchant_id": f"merchant_{j}"  # 5 marchands différents
        }
        transactions.append(transaction)
    return transactions


# Simulation de transactions en continu
def produce_transactions(rate_per_second=10, total_transactions=100_000):
    ref_time = datetime(2025, 6, 1)
    i = 0
    
    print(f"Démarrage du producer - {rate_per_second} transactions/sec")
    
    while i < total_transactions:
        for _ in range(rate_per_second):
            transaction = generate_transaction(i, ref_time)
            producer.send('transactions', value=transaction)
            print(f"Sent: {transaction['transaction_id']} - {transaction['amount']} {transaction['currency']}")
            i += 1
            if i >= total_transactions:
                break
        sleep(1)
    
        while i < total_transactions:
            for _ in range(rate_per_second):
                # Une fois toutes les 1000 transactions, on injecte une fraude carrousel
                if i % 1000 == 0:
                    user = f"u999"
                    fraud_txs = generate_carousel_transactions(ref_time + timedelta(seconds=i), user, i)
                    for tx in fraud_txs:
                        producer.send('transactions', value=tx)
                        print(f"Sent: {tx['transaction_id']} - {tx['amount']} {tx['currency']} {tx['merchant_id']}")
                        i += 1
                else:
                    transaction = generate_transaction(i, ref_time)
                    producer.send('transactions', value=transaction)
                    print(f"Sent: {transaction['transaction_id']} - {transaction['amount']} {transaction['currency']}")
                    i += 1
                if i >= total_transactions:
                    break
            sleep(1)

    producer.flush()
    print(f"Terminé ! {total_transactions} transactions envoyées.")

# Exécution
if __name__ == "__main__":
    produce_transactions(rate_per_second=50, total_transactions=100_000)
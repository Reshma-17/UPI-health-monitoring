import json
import time
import random
import redis
from datetime import datetime, timedelta
from kafka import KafkaProducer
from dataclasses import dataclass, asdict
from typing import List, Dict
import uuid

@dataclass
class Transaction:
    txn_id: str
    user_id: str
    amount: float
    merchant: str
    status: str
    payment_method: str
    timestamp: str
    location: str
    device_id: str
    ip_address: str
    merchant_category: str
    risk_score: float = 0.0
    is_anomaly: bool = False

class AnomalyPatternGenerator:
    """Generate various anomaly patterns for testing detection systems"""
    
    def __init__(self):
        self.normal_merchants = ["Amazon", "Flipkart", "Swiggy", "Zomato", "Uber", "PhonePe", "PayTM", "Myntra", "BigBasket", "BookMyShow"]
        self.suspicious_merchants = ["CashAdvanceNow", "InstantLoan247", "CryptoExchange", "BettingPlus", "GamingWins"]
        self.normal_locations = ["Mumbai", "Delhi", "Bangalore", "Chennai", "Pune", "Hyderabad", "Kolkata", "Ahmedabad"]
        self.suspicious_locations = ["Unknown", "VPN_Location", "TOR_Exit", "Blacklisted_Country"]
        
    def generate_normal_transaction(self) -> Transaction:
        """Generate normal transaction"""
        return Transaction(
            txn_id=f"TXN_{random.randint(100000, 999999)}",
            user_id=f"USER_{random.randint(1000, 9999)}",
            amount=round(random.uniform(10.0, 5000.0), 2),
            merchant=random.choice(self.normal_merchants),
            status=random.choice(["success", "failed", "pending"]),
            payment_method=random.choice(["UPI", "Card", "NetBanking", "Wallet"]),
            timestamp=datetime.now().isoformat(),
            location=random.choice(self.normal_locations),
            device_id=f"device_{random.randint(1000, 9999)}",
            ip_address=f"192.168.{random.randint(1,255)}.{random.randint(1,255)}",
            merchant_category=random.choice(["Food", "Shopping", "Transport", "Entertainment", "Utilities"]),
            risk_score=random.uniform(0.1, 0.3)
        )
    
    def generate_velocity_anomaly(self) -> List[Transaction]:
        """Generate rapid-fire transactions (velocity anomaly)"""
        user_id = f"USER_{random.randint(1000, 9999)}"
        transactions = []
        
        for i in range(random.randint(15, 25)):  # Suspicious rapid transactions
            txn = Transaction(
                txn_id=f"VEL_{random.randint(100000, 999999)}",
                user_id=user_id,
                amount=round(random.uniform(500.0, 2000.0), 2),
                merchant=random.choice(self.normal_merchants),
                status="success",
                payment_method="UPI",
                timestamp=(datetime.now() + timedelta(seconds=i*2)).isoformat(),
                location=random.choice(self.normal_locations),
                device_id=f"device_{random.randint(1000, 9999)}",
                ip_address=f"192.168.{random.randint(1,255)}.{random.randint(1,255)}",
                merchant_category="Shopping",
                risk_score=random.uniform(0.7, 0.9),
                is_anomaly=True
            )
            transactions.append(txn)
        
        return transactions
    
    def generate_amount_anomaly(self) -> Transaction:
        """Generate unusually high amount transaction"""
        return Transaction(
            txn_id=f"AMT_{random.randint(100000, 999999)}",
            user_id=f"USER_{random.randint(1000, 9999)}",
            amount=round(random.uniform(50000.0, 200000.0), 2),  # Suspiciously high amount
            merchant=random.choice(self.normal_merchants),
            status="success",
            payment_method="Card",
            timestamp=datetime.now().isoformat(),
            location=random.choice(self.normal_locations),
            device_id=f"device_{random.randint(1000, 9999)}",
            ip_address=f"10.0.{random.randint(1,255)}.{random.randint(1,255)}",
            merchant_category="Shopping",
            risk_score=random.uniform(0.8, 0.95),
            is_anomaly=True
        )
    
    def generate_location_anomaly(self) -> Transaction:
        """Generate transaction from suspicious location"""
        return Transaction(
            txn_id=f"LOC_{random.randint(100000, 999999)}",
            user_id=f"USER_{random.randint(1000, 9999)}",
            amount=round(random.uniform(1000.0, 10000.0), 2),
            merchant=random.choice(self.suspicious_merchants),
            status="success",
            payment_method="Card",
            timestamp=datetime.now().isoformat(),
            location=random.choice(self.suspicious_locations),
            device_id=f"device_{random.randint(1000, 9999)}",
            ip_address=f"127.0.0.{random.randint(1,255)}",  # Suspicious IP
            merchant_category="High_Risk",
            risk_score=random.uniform(0.85, 0.99),
            is_anomaly=True
        )
    
    def generate_time_anomaly(self) -> Transaction:
        """Generate transaction at unusual time"""
        # Generate transaction at 3 AM (unusual time)
        unusual_time = datetime.now().replace(hour=3, minute=random.randint(0, 59))
        
        return Transaction(
            txn_id=f"TIME_{random.randint(100000, 999999)}",
            user_id=f"USER_{random.randint(1000, 9999)}",
            amount=round(random.uniform(5000.0, 25000.0), 2),
            merchant=random.choice(self.normal_merchants),
            status="success",
            payment_method="NetBanking",
            timestamp=unusual_time.isoformat(),
            location=random.choice(self.normal_locations),
            device_id=f"device_{random.randint(1000, 9999)}",
            ip_address=f"192.168.{random.randint(1,255)}.{random.randint(1,255)}",
            merchant_category="Shopping",
            risk_score=random.uniform(0.6, 0.8),
            is_anomaly=True
        )

class EnhancedTransactionProducer:
    def __init__(self):
        self.kafka_producer = None
        self.redis_client = None
        self.anomaly_generator = AnomalyPatternGenerator()
        self.connect_services()
        
    def connect_services(self):
        """Connect to Kafka and Redis"""
        # Kafka connection with retry logic
        max_retries = 30
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                self.kafka_producer = KafkaProducer(
                    bootstrap_servers=['kafka:29092'],
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    acks='all',
                    retries=5
                )
                print("✅ Connected to Kafka successfully!")
                break
            except Exception as e:
                retry_count += 1
                print(f"⏳ Waiting for Kafka... (attempt {retry_count}/{max_retries})")
                time.sleep(5)
        
        # Redis connection
        try:
            self.redis_client = redis.Redis(host='redis', port=6379, decode_responses=True)
            self.redis_client.ping()
            print("✅ Connected to Redis successfully!")
        except Exception as e:
            print(f"❌ Failed to connect to Redis: {e}")
            
    def update_metrics(self, transaction: Transaction):
        """Update real-time metrics in Redis"""
        if not self.redis_client:
            return
            
        try:
            # Update transaction count
            self.redis_client.incr("txn:total_count")
            
            # Update status counts
            self.redis_client.incr(f"txn:status:{transaction.status}")
            
            # Update merchant counts
            self.redis_client.incr(f"txn:merchant:{transaction.merchant}")
            
            # Update amount metrics
            self.redis_client.lpush("txn:amounts", transaction.amount)
            self.redis_client.ltrim("txn:amounts", 0, 999)  # Keep last 1000 amounts
            
            # Update anomaly metrics
            if transaction.is_anomaly:
                self.redis_client.incr("txn:anomaly_count")
                self.redis_client.lpush("txn:anomalies", json.dumps(asdict(transaction)))
                self.redis_client.ltrim("txn:anomalies", 0, 99)  # Keep last 100 anomalies
                
        except Exception as e:
            print(f"❌ Error updating metrics: {e}")
    
    def send_transaction(self, transaction: Transaction):
        """Send transaction to Kafka"""
        try:
            # Send to transactions topic
            future = self.kafka_producer.send('transactions', value=asdict(transaction))
            future.get(timeout=10)
            
            # Send anomalies to separate topic for immediate processing
            if transaction.is_anomaly:
                future = self.kafka_producer.send('anomalies', value=asdict(transaction))
                future.get(timeout=10)
            
            # Update metrics
            self.update_metrics(transaction)
            
            return True
        except Exception as e:
            print(f"❌ Error sending transaction: {e}")
            return False
    
    def generate_transaction_burst(self):
        """Generate different types of transactions"""
        
        # 70% normal transactions
        if random.random() < 0.7:
            return [self.anomaly_generator.generate_normal_transaction()]
        
        # 10% velocity anomalies
        elif random.random() < 0.8:
            return self.anomaly_generator.generate_velocity_anomaly()
        
        # 10% amount anomalies
        elif random.random() < 0.9:
            return [self.anomaly_generator.generate_amount_anomaly()]
        
        # 5% location anomalies
        elif random.random() < 0.95:
            return [self.anomaly_generator.generate_location_anomaly()]
        
        # 5% time anomalies
        else:
            return [self.anomaly_generator.generate_time_anomaly()]
    
    def run(self):
        """Main production loop"""
        print("🚀 Starting Enhanced UPI Transaction Producer...")
        print("📊 Generating normal transactions and anomaly patterns...")
        print("=" * 60)
        
        count = 0
        try:
            while True:
                # Generate transaction burst
                transactions = self.generate_transaction_burst()
                
                # Send each transaction
                for txn in transactions:
                    if self.send_transaction(txn):
                        count += 1
                        status_emoji = "🔴" if txn.is_anomaly else "🟢"
                        print(f"{status_emoji} [{count}] {txn.txn_id} | ${txn.amount} | {txn.status} | {txn.merchant} | Risk: {txn.risk_score:.2f}")
                
                # Variable delay for realistic patterns
                time.sleep(random.uniform(0.5, 3.0))
                
        except KeyboardInterrupt:
            print("\n⚠️ Shutting down producer...")
        except Exception as e:
            print(f"❌ Producer error: {e}")
        finally:
            if self.kafka_producer:
                self.kafka_producer.close()
            print("✅ Producer closed")

if __name__ == "__main__":
    producer = EnhancedTransactionProducer()
    producer.run()
import pathway as pw
import json
import redis
from typing import Dict, Any

class SimplePathwayProcessor:
    """Minimal Pathway implementation for fraud detection"""
    
    def __init__(self):
        self.redis_client = redis.Redis(host='redis', port=6379, decode_responses=True)
        
    def run(self):
        print("Starting Simple Pathway Processor...")
        
        # Read from Kafka
        transactions = pw.io.kafka.read(
            rdkafka_settings={
                "bootstrap.servers": "kafka:29092",
                "group.id": "pathway-processor",
                "auto.offset.reset": "latest"
            },
            topic="transactions",
            format="json",
            autocommit_duration_ms=1000,
        )
        
        # Simple processing: extract basic fields
        processed = transactions.select(
            txn_id=pw.this.data["txn_id"],
            user_id=pw.this.data["user_id"],
            amount=pw.this.data["amount"].as_float(),
            merchant=pw.this.data["merchant"],
            timestamp=pw.this.data["timestamp"]
        )
        
        # Filter high amount transactions
        high_amount = processed.filter(pw.this.amount > 10000)
        
        # Simple anomaly alert format
        alerts = high_amount.select(
            anomaly_id=pw.this.txn_id + "_PATHWAY",
            transaction_id=pw.this.txn_id,
            user_id=pw.this.user_id,
            amount=pw.this.amount,
            merchant=pw.this.merchant,
            anomaly_type=pw.apply(lambda: "HIGH_AMOUNT", pw.this),
            severity=pw.apply(lambda: "HIGH", pw.this),
            confidence_score=pw.apply(lambda: 0.8, pw.this),
            detected_at=pw.now(),
            description=pw.apply(lambda row: f"High amount transaction: ${row['amount']}", pw.this)
        )
        
        # Output to Kafka
        pw.io.kafka.write(
            alerts,
            rdkafka_settings={
                "bootstrap.servers": "kafka:29092",
            },
            topic="pathway_alerts",
            format="json"
        )
        
        # Update metrics
        processed.output_to(self.update_metrics)
        
        print("Pathway processing pipeline started")
        
        # Run Pathway
        pw.run()
    
    def update_metrics(self, row):
        """Update Redis metrics"""
        try:
            self.redis_client.incr("pathway:transactions_processed")
            if row["amount"] > 10000:
                self.redis_client.incr("pathway:high_amount_detected")
                print(f"Pathway detected high amount: {row['txn_id']} - ${row['amount']}")
        except Exception as e:
            print(f"Error updating metrics: {e}")

if __name__ == "__main__":
    processor = SimplePathwayProcessor()
    processor.run()
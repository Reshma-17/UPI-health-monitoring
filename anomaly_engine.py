import json
import time
import redis
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from kafka import KafkaConsumer, KafkaProducer
from collections import defaultdict, deque
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Tuple
import logging
import threading
import requests
import chromadb
from chromadb.utils import embedding_functions
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from sentence_transformers import SentenceTransformer

@dataclass
class Anomaly:
    anomaly_id: str
    transaction_id: str
    anomaly_type: str
    severity: str  # LOW, MEDIUM, HIGH, CRITICAL
    confidence_score: float
    description: str
    detected_at: str
    user_id: str
    amount: float
    merchant: str
    risk_factors: List[str]
    recommended_actions: List[str]
    llm_explanation: Optional[str] = None

class ThresholdDetector:
    """Implements threshold-based anomaly detection"""
    
    def __init__(self):
        self.thresholds = {
            'max_amount': 50000.0,
            'max_velocity': 10,  # transactions per minute
            'max_failed_attempts': 5,
            'suspicious_hours': [0, 1, 2, 3, 4, 5],  # 12 AM to 5 AM
            'max_risk_score': 0.8
        }
        
    def detect_amount_anomaly(self, transaction: Dict) -> Optional[Anomaly]:
        """Detect unusually high amount transactions"""
        if transaction['amount'] > self.thresholds['max_amount']:
            return Anomaly(
                anomaly_id=f"AMT_{int(time.time())}",
                transaction_id=transaction['txn_id'],
                anomaly_type="HIGH_AMOUNT",
                severity="HIGH",
                confidence_score=0.9,
                description=f"Transaction amount ${transaction['amount']:.2f} exceeds threshold of ${self.thresholds['max_amount']}",
                detected_at=datetime.now().isoformat(),
                user_id=transaction['user_id'],
                amount=transaction['amount'],
                merchant=transaction['merchant'],
                risk_factors=["Unusually high transaction amount"],
                recommended_actions=["Manual review required", "Verify with customer", "Check merchant legitimacy"]
            )
        return None
    
    def detect_time_anomaly(self, transaction: Dict) -> Optional[Anomaly]:
        """Detect transactions at unusual times"""
        txn_time = datetime.fromisoformat(transaction['timestamp'])
        if txn_time.hour in self.thresholds['suspicious_hours']:
            return Anomaly(
                anomaly_id=f"TIME_{int(time.time())}",
                transaction_id=transaction['txn_id'],
                anomaly_type="UNUSUAL_TIME",
                severity="MEDIUM",
                confidence_score=0.7,
                description=f"Transaction occurred at {txn_time.hour}:00, which is unusual",
                detected_at=datetime.now().isoformat(),
                user_id=transaction['user_id'],
                amount=transaction['amount'],
                merchant=transaction['merchant'],
                risk_factors=["Transaction at unusual hour"],
                recommended_actions=["Monitor user activity", "Check for account compromise"]
            )
        return None

class PatternDetector:
    """Implements pattern-based anomaly detection using ML techniques"""
    
    def __init__(self, redis_client):
        self.redis_client = redis_client
        self.user_profiles = defaultdict(lambda: {
            'transaction_history': deque(maxlen=100),
            'avg_amount': 0.0,
            'std_amount': 0.0,
            'common_merchants': set(),
            'common_locations': set(),
            'last_update': datetime.now()
        })
        
    def update_user_profile(self, transaction: Dict):
        """Update user behavioral profile"""
        user_id = transaction['user_id']
        profile = self.user_profiles[user_id]
        
        # Add transaction to history
        profile['transaction_history'].append(transaction)
        
        # Update statistics
        amounts = [t['amount'] for t in profile['transaction_history']]
        if amounts:
            profile['avg_amount'] = np.mean(amounts)
            profile['std_amount'] = np.std(amounts)
        
        profile['common_merchants'].add(transaction['merchant'])
        profile['common_locations'].add(transaction['location'])
        profile['last_update'] = datetime.now()
        
    def detect_velocity_anomaly(self, user_id: str, current_time: datetime) -> Optional[Anomaly]:
        """Detect rapid-fire transactions (velocity attack)"""
        # Get recent transactions from Redis
        recent_key = f"user:{user_id}:recent_txns"
        recent_txns = self.redis_client.lrange(recent_key, 0, 50)
        
        if not recent_txns:
            return None
            
        # Parse transactions and check velocity
        transactions = []
        for txn_str in recent_txns:
            try:
                txn = json.loads(txn_str)
                txn_time = datetime.fromisoformat(txn['timestamp'])
                if (current_time - txn_time).seconds < 300:  # Last 5 minutes
                    transactions.append(txn)
            except:
                continue
                
        if len(transactions) > 15:  # More than 15 transactions in 5 minutes
            return Anomaly(
                anomaly_id=f"VEL_{int(time.time())}",
                transaction_id=transactions[-1]['txn_id'],
                anomaly_type="HIGH_VELOCITY",
                severity="CRITICAL",
                confidence_score=0.95,
                description=f"User {user_id} made {len(transactions)} transactions in 5 minutes",
                detected_at=current_time.isoformat(),
                user_id=user_id,
                amount=sum(t['amount'] for t in transactions),
                merchant="Multiple",
                risk_factors=["Rapid transaction velocity", "Possible bot activity"],
                recommended_actions=["Immediate account freeze", "Contact customer", "Security review"]
            )
        
        return None
    
    def detect_behavioral_anomaly(self, transaction: Dict) -> Optional[Anomaly]:
        """Detect deviations from normal user behavior"""
        user_id = transaction['user_id']
        profile = self.user_profiles[user_id]
        
        if len(profile['transaction_history']) < 10:
            return None  # Not enough data
            
        # Check for amount deviation
        z_score = abs((transaction['amount'] - profile['avg_amount']) / (profile['std_amount'] + 0.01))
        
        if z_score > 3:  # 3 standard deviations
            return Anomaly(
                anomaly_id=f"BEH_{int(time.time())}",
                transaction_id=transaction['txn_id'],
                anomaly_type="BEHAVIORAL_DEVIATION",
                severity="MEDIUM",
                confidence_score=min(0.9, z_score / 5.0),
                description=f"Amount ${transaction['amount']:.2f} deviates significantly from user's normal pattern (z-score: {z_score:.2f})",
                detected_at=datetime.now().isoformat(),
                user_id=user_id,
                amount=transaction['amount'],
                merchant=transaction['merchant'],
                risk_factors=[f"Amount deviation (z-score: {z_score:.2f})", "Unusual spending pattern"],
                recommended_actions=["Monitor user activity", "Send security alert to user"]
            )
        
        return None

class FreeRAGKnowledgeRetriever:
    """Free RAG system using ChromaDB + Ollama for contextual anomaly explanation"""
    
    def __init__(self, chroma_host="chromadb", chroma_port=8000, ollama_host="ollama", ollama_port=11434):
        self.chroma_client = None
        self.collection = None
        self.ollama_host = ollama_host
        self.ollama_port = ollama_port
        self.embedding_model = None
        self.setup_chroma(chroma_host, chroma_port)
        self.setup_embedding_model()
        
    def setup_chroma(self, host, port):
        """Connect to ChromaDB vector database"""
        try:
            self.chroma_client = chromadb.HttpClient(host=host, port=port)
            
            # Get or create collection
            try:
                self.collection = self.chroma_client.get_collection("fintech_knowledge")
            except:
                # Create collection if it doesn't exist
                self.collection = self.chroma_client.create_collection(
                    name="fintech_knowledge",
                    metadata={"description": "Fintech fraud detection knowledge base"}
                )
                
                # Add some initial knowledge
                self.add_initial_knowledge()
                
            print("✅ Connected to ChromaDB successfully!")
        except Exception as e:
            print(f"❌ Failed to connect to ChromaDB: {e}")
    
    def setup_embedding_model(self):
        """Setup free embedding model"""
        try:
            # Use sentence-transformers for free embeddings
            self.embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
            print("✅ Loaded free embedding model successfully!")
        except Exception as e:
            print(f"❌ Failed to load embedding model: {e}")
    
    def add_initial_knowledge(self):
        """Add initial fraud detection knowledge"""
        knowledge_items = [
            {
                "id": "rbi_guideline_1",
                "text": "RBI mandates multi-factor authentication for transactions above Rs 50,000 to prevent fraud",
                "source": "RBI",
                "category": "Payment Security"
            },
            {
                "id": "velocity_rule_1", 
                "text": "More than 10 transactions per minute indicates potential bot activity and requires immediate investigation",
                "source": "NPCI",
                "category": "Velocity Rules"
            },
            {
                "id": "risk_assessment_1",
                "text": "Transactions between 11 PM to 6 AM carry higher risk scores for fraud detection systems",
                "source": "NPCI",
                "category": "Risk Assessment"
            },
            {
                "id": "fraud_pattern_1",
                "text": "Account takeover attacks typically show rapid succession of small transactions followed by large withdrawals",
                "source": "Security Research",
                "category": "Fraud Patterns"
            },
            {
                "id": "merchant_risk_1",
                "text": "High-risk merchant categories include cryptocurrency exchanges, gambling sites, and cash advance services",
                "source": "Fintech News",
                "category": "Merchant Risk"
            }
        ]
        
        if self.collection and self.embedding_model:
            try:
                texts = [item["text"] for item in knowledge_items]
                embeddings = self.embedding_model.encode(texts).tolist()
                
                self.collection.add(
                    embeddings=embeddings,
                    documents=texts,
                    metadatas=[{"source": item["source"], "category": item["category"]} for item in knowledge_items],
                    ids=[item["id"] for item in knowledge_items]
                )
                print("✅ Added initial knowledge to vector database")
            except Exception as e:
                print(f"❌ Error adding initial knowledge: {e}")
    
    def get_embedding(self, text: str) -> List[float]:
        """Get free embedding for text"""
        try:
            if self.embedding_model:
                return self.embedding_model.encode([text])[0].tolist()
            else:
                return [0.0] * 384  # Default dimension for all-MiniLM-L6-v2
        except Exception as e:
            print(f"❌ Error getting embedding: {e}")
            return [0.0] * 384
    
    def retrieve_context(self, anomaly: Anomaly) -> List[str]:
        """Retrieve relevant knowledge for anomaly"""
        if not self.collection or not self.embedding_model:
            return []
            
        try:
            # Create query based on anomaly
            query = f"{anomaly.anomaly_type} {anomaly.description} fintech fraud detection"
            query_embedding = self.get_embedding(query)
            
            # Search similar contexts
            results = self.collection.query(
                query_embeddings=[query_embedding],
                n_results=3
            )
            
            contexts = []
            if results and results['documents']:
                contexts = results['documents'][0]
            
            return contexts
        except Exception as e:
            print(f"❌ Error retrieving context: {e}")
            return []
    
    def generate_explanation_with_ollama(self, anomaly: Anomaly, contexts: List[str]) -> str:
        """Generate LLM explanation using local Ollama"""
        try:
            context_text = "\n".join(contexts) if contexts else "No specific context available"
            
            prompt = f"""You are a financial fraud analyst. Analyze this anomaly and provide a clear explanation.

ANOMALY DETAILS:
- Type: {anomaly.anomaly_type}
- Severity: {anomaly.severity}
- Description: {anomaly.description}
- Amount: ${anomaly.amount}
- User: {anomaly.user_id}
- Merchant: {anomaly.merchant}

RELEVANT KNOWLEDGE:
{context_text}

Provide a brief explanation (2-3 sentences) covering:
1. Why this is suspicious
2. Immediate actions needed

Keep response under 200 words and professional."""

            # Call Ollama API
            ollama_url = f"http://{self.ollama_host}:{self.ollama_port}/api/generate"
            
            response = requests.post(ollama_url, json={
                "model": "llama2",  # Use llama2 model
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": 0.3,
                    "top_k": 10,
                    "top_p": 0.9
                }
            }, timeout=30)
            
            if response.status_code == 200:
                result = response.json()
                return result.get('response', 'Unable to generate explanation.')
            else:
                return f"Error generating explanation: HTTP {response.status_code}"
                
        except Exception as e:
            print(f"❌ Error generating explanation: {e}")
            return f"This {anomaly.anomaly_type.lower().replace('_', ' ')} anomaly requires immediate attention. The transaction shows suspicious patterns that deviate from normal behavior. Recommended actions: {', '.join(anomaly.recommended_actions[:2])}."

class FreeAnomalyDetectionEngine:
    """Main anomaly detection engine using free tools"""
    
    def __init__(self):
        self.kafka_consumer = None
        self.kafka_producer = None
        self.redis_client = None
        self.db_connection = None
        self.threshold_detector = ThresholdDetector()
        self.pattern_detector = None
        self.rag_retriever = None
        self.setup_connections()
        
    def setup_connections(self):
        """Setup all service connections"""
        # Kafka
        max_retries = 30
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                self.kafka_consumer = KafkaConsumer(
                    'transactions',
                    bootstrap_servers=['kafka:29092'],
                    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                    group_id='anomaly-detector'
                )
                
                self.kafka_producer = KafkaProducer(
                    bootstrap_servers=['kafka:29092'],
                    value_serializer=lambda v: json.dumps(v).encode('utf-8')
                )
                print("✅ Connected to Kafka successfully!")
                break
            except Exception as e:
                retry_count += 1
                print(f"⏳ Waiting for Kafka... (attempt {retry_count}/{max_retries})")
                time.sleep(5)
        
        # Redis
        try:
            self.redis_client = redis.Redis(host='redis', port=6379, decode_responses=True)
            self.redis_client.ping()
            self.pattern_detector = PatternDetector(self.redis_client)
            print("✅ Connected to Redis successfully!")
        except Exception as e:
            print(f"❌ Failed to connect to Redis: {e}")
        
        # PostgreSQL
        try:
            self.db_connection = psycopg2.connect(
                host="postgres",
                database="fintech_monitoring",
                user="fintech",
                password="fintech123"
            )
            self.init_database()
            print("✅ Connected to PostgreSQL successfully!")
        except Exception as e:
            print(f"❌ Failed to connect to PostgreSQL: {e}")
        
        # Free RAG System
        try:
            self.rag_retriever = FreeRAGKnowledgeRetriever()
        except Exception as e:
            print(f"❌ Failed to initialize free RAG system: {e}")
    
    def init_database(self):
        """Initialize database tables"""
        if not self.db_connection:
            return
            
        cursor = self.db_connection.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS anomalies (
                id SERIAL PRIMARY KEY,
                anomaly_id VARCHAR(100) UNIQUE,
                transaction_id VARCHAR(100),
                anomaly_type VARCHAR(50),
                severity VARCHAR(20),
                confidence_score FLOAT,
                description TEXT,
                detected_at TIMESTAMP,
                user_id VARCHAR(50),
                amount DECIMAL(10,2),
                merchant VARCHAR(100),
                risk_factors TEXT[],
                recommended_actions TEXT[],
                llm_explanation TEXT,
                status VARCHAR(20) DEFAULT 'OPEN',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.db_connection.commit()
        cursor.close()
    
    def store_anomaly(self, anomaly: Anomaly):
        """Store anomaly in database"""
        if not self.db_connection:
            return
            
        cursor = self.db_connection.cursor()
        try:
            cursor.execute("""
                INSERT INTO anomalies (
                    anomaly_id, transaction_id, anomaly_type, severity, 
                    confidence_score, description, detected_at, user_id, 
                    amount, merchant, risk_factors, recommended_actions, llm_explanation
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                anomaly.anomaly_id, anomaly.transaction_id, anomaly.anomaly_type,
                anomaly.severity, anomaly.confidence_score, anomaly.description,
                anomaly.detected_at, anomaly.user_id, anomaly.amount,
                anomaly.merchant, anomaly.risk_factors, anomaly.recommended_actions,
                anomaly.llm_explanation
            ))
            self.db_connection.commit()
        except Exception as e:
            print(f"❌ Error storing anomaly: {e}")
            self.db_connection.rollback()
        finally:
            cursor.close()
    
    def process_transaction(self, transaction: Dict):
        """Process a transaction for anomalies"""
        anomalies = []
        
        # Threshold-based detection
        amount_anomaly = self.threshold_detector.detect_amount_anomaly(transaction)
        if amount_anomaly:
            anomalies.append(amount_anomaly)
        
        time_anomaly = self.threshold_detector.detect_time_anomaly(transaction)
        if time_anomaly:
            anomalies.append(time_anomaly)
        
        # Pattern-based detection
        if self.pattern_detector:
            self.pattern_detector.update_user_profile(transaction)
            
            velocity_anomaly = self.pattern_detector.detect_velocity_anomaly(
                transaction['user_id'], 
                datetime.now()
            )
            if velocity_anomaly:
                anomalies.append(velocity_anomaly)
            
            behavioral_anomaly = self.pattern_detector.detect_behavioral_anomaly(transaction)
            if behavioral_anomaly:
                anomalies.append(behavioral_anomaly)
        
        # Process detected anomalies
        for anomaly in anomalies:
            self.enrich_anomaly_with_rag(anomaly)
            self.store_anomaly(anomaly)
            self.send_anomaly_alert(anomaly)
        
        # Store transaction for velocity tracking
        if self.redis_client:
            user_key = f"user:{transaction['user_id']}:recent_txns"
            self.redis_client.lpush(user_key, json.dumps(transaction))
            self.redis_client.ltrim(user_key, 0, 100)
            self.redis_client.expire(user_key, 3600)  # 1 hour TTL
    
    def enrich_anomaly_with_rag(self, anomaly: Anomaly):
        """Enrich anomaly with RAG-based explanation"""
        if self.rag_retriever:
            try:
                contexts = self.rag_retriever.retrieve_context(anomaly)
                explanation = self.rag_retriever.generate_explanation_with_ollama(anomaly, contexts)
                anomaly.llm_explanation = explanation
            except Exception as e:
                print(f"❌ Error enriching anomaly: {e}")
    
    def send_anomaly_alert(self, anomaly: Anomaly):
        """Send anomaly to alert management system"""
        try:
            self.kafka_producer.send('alerts', value=asdict(anomaly))
            print(f"🚨 ANOMALY DETECTED: {anomaly.anomaly_type} - {anomaly.severity} - {anomaly.transaction_id}")
        except Exception as e:
            print(f"❌ Error sending alert: {e}")
    
    def run(self):
        """Main processing loop"""
        print("🔍 Starting Free Anomaly Detection Engine...")
        print("📊 Monitoring transactions for anomalies...")
        print("=" * 60)
        
        try:
            for message in self.kafka_consumer:
                transaction = message.value
                self.process_transaction(transaction)
                
        except KeyboardInterrupt:
            print("\n⚠️ Shutting down anomaly detector...")
        except Exception as e:
            print(f"❌ Error in anomaly detector: {e}")
        finally:
            if self.kafka_consumer:
                self.kafka_consumer.close()
            if self.kafka_producer:
                self.kafka_producer.close()
            print("✅ Free anomaly detector closed")

if __name__ == "__main__":
    detector = FreeAnomalyDetectionEngine()
    detector.run()
import time
import json
import requests
import os
from datetime import datetime, timedelta
from typing import List, Dict
import psycopg2
import chromadb
from sentence_transformers import SentenceTransformer
import threading
import schedule

class KnowledgeSource:
    """Base class for knowledge sources"""
    
    def fetch_content(self) -> List[Dict]:
        raise NotImplementedError

class RBINewsSource(KnowledgeSource):
    """Fetch RBI (Reserve Bank of India) news and circulars"""
    
    def fetch_content(self) -> List[Dict]:
        # Mock RBI content for demo (in production, use actual RSS feeds)
        mock_rbi_content = [
            {
                "text": "RBI issues guidelines on digital payment security requiring multi-factor authentication for transactions above Rs 50,000",
                "source": "RBI",
                "category": "Payment Security",
                "date": datetime.now().isoformat()
            },
            {
                "text": "New fraud detection norms mandate real-time monitoring of unusual transaction patterns and velocity checks",
                "source": "RBI",
                "category": "Fraud Prevention",
                "date": datetime.now().isoformat()
            },
            {
                "text": "KYC compliance requires enhanced due diligence for high-risk transactions and suspicious merchant categories",
                "source": "RBI",
                "category": "Compliance",
                "date": datetime.now().isoformat()
            },
            {
                "text": "Account freezing protocols for suspected fraud cases must be implemented within 60 seconds of detection",
                "source": "RBI",
                "category": "Risk Management",
                "date": datetime.now().isoformat()
            }
        ]
        
        return mock_rbi_content

class NPCIGuidelines(KnowledgeSource):
    """Fetch NPCI (National Payments Corporation of India) guidelines"""
    
    def fetch_content(self) -> List[Dict]:
        mock_npci_content = [
            {
                "text": "UPI transaction limits: Individual transactions capped at Rs 1 lakh, daily limit of Rs 1 lakh per user",
                "source": "NPCI",
                "category": "Transaction Limits",
                "date": datetime.now().isoformat()
            },
            {
                "text": "Suspicious velocity patterns: More than 10 transactions per minute indicates potential bot activity requiring immediate investigation",
                "source": "NPCI",
                "category": "Velocity Rules",
                "date": datetime.now().isoformat()
            },
            {
                "text": "Risk scoring framework: Transactions between 11 PM to 6 AM carry higher risk scores for fraud detection systems",
                "source": "NPCI",
                "category": "Risk Assessment",
                "date": datetime.now().isoformat()
            },
            {
                "text": "Merchant risk categories: Cryptocurrency exchanges, gambling sites, and cash advance services are high-risk",
                "source": "NPCI",
                "category": "Merchant Classification",
                "date": datetime.now().isoformat()
            }
        ]
        
        return mock_npci_content

class FintechNewsSource(KnowledgeSource):
    """Fetch fintech security news"""
    
    def fetch_content(self) -> List[Dict]:
        mock_fintech_news = [
            {
                "text": "Latest fraud techniques: Cybercriminals using AI to generate fake identities for account takeover attacks",
                "source": "Fintech News",
                "category": "Fraud Trends",
                "date": datetime.now().isoformat()
            },
            {
                "text": "Mobile payment security: Biometric authentication reduces fraud by 87% compared to PIN-based systems",
                "source": "Fintech News",
                "category": "Security Technology",
                "date": datetime.now().isoformat()
            },
            {
                "text": "Social engineering attacks on UPI: Fraudsters impersonating bank officials to steal transaction PINs",
                "source": "Fintech News",
                "category": "Social Engineering",
                "date": datetime.now().isoformat()
            },
            {
                "text": "Machine learning models detect card-not-present fraud with 94% accuracy using transaction patterns",
                "source": "Fintech News",
                "category": "ML Research",
                "date": datetime.now().isoformat()
            }
        ]
        
        return mock_fintech_news

class SecurityResearchSource(KnowledgeSource):
    """Fetch latest security research and best practices"""
    
    def fetch_content(self) -> List[Dict]:
        mock_research_content = [
            {
                "text": "Behavioral analysis shows users typically spend less than 5 minutes between transactions, longer gaps may indicate account takeover",
                "source": "Security Research",
                "category": "Behavioral Analysis",
                "date": datetime.now().isoformat()
            },
            {
                "text": "Geographic anomalies: Transactions from new locations without travel history require additional verification steps",
                "source": "Security Research",
                "category": "Location Intelligence",
                "date": datetime.now().isoformat()
            },
            {
                "text": "Device fingerprinting reduces fraud by 73% when combined with transaction pattern analysis",
                "source": "Security Research",
                "category": "Device Security",
                "date": datetime.now().isoformat()
            },
            {
                "text": "Time-based anomalies: Transactions outside normal user activity hours show 3x higher fraud rates",
                "source": "Security Research",
                "category": "Temporal Analysis",
                "date": datetime.now().isoformat()
            }
        ]
        
        return mock_research_content

class FreeKnowledgeIngestionEngine:
    """Free knowledge ingestion engine using ChromaDB + sentence-transformers"""
    
    def __init__(self):
        self.db_connection = None
        self.chroma_client = None
        self.collection = None
        self.embedding_model = None
        self.knowledge_sources = [
            RBINewsSource(),
            NPCIGuidelines(),
            FintechNewsSource(),
            SecurityResearchSource()
        ]
        self.setup_connections()
        self.setup_scheduler()
    
    def setup_connections(self):
        """Setup database and ChromaDB connections"""
        # PostgreSQL connection
        try:
            self.db_connection = psycopg2.connect(
                host="postgres",
                database="fintech_monitoring",
                user="fintech",
                password="fintech123"
            )
            self.init_knowledge_tables()
            print("✅ Connected to PostgreSQL successfully!")
        except Exception as e:
            print(f"❌ Failed to connect to PostgreSQL: {e}")
        
        # ChromaDB connection
        try:
            self.chroma_client = chromadb.HttpClient(host="chromadb", port=8000)
            self.setup_chroma_collection()
            print("✅ Connected to ChromaDB successfully!")
        except Exception as e:
            print(f"❌ Failed to connect to ChromaDB: {e}")
        
        # Load embedding model
        try:
            self.embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
            print("✅ Loaded embedding model successfully!")
        except Exception as e:
            print(f"❌ Failed to load embedding model: {e}")
    
    def init_knowledge_tables(self):
        """Initialize knowledge storage tables"""
        if not self.db_connection:
            return
        
        cursor = self.db_connection.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS knowledge_base (
                id SERIAL PRIMARY KEY,
                content_hash VARCHAR(64) UNIQUE,
                text TEXT NOT NULL,
                source VARCHAR(100),
                category VARCHAR(100),
                embedding_id VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ingestion_logs (
                id SERIAL PRIMARY KEY,
                source VARCHAR(100),
                status VARCHAR(50),
                items_processed INTEGER,
                errors TEXT,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        self.db_connection.commit()
        cursor.close()
    
    def setup_chroma_collection(self):
        """Setup ChromaDB collection for embeddings"""
        collection_name = "fintech_knowledge"
        
        try:
            # Try to get existing collection
            self.collection = self.chroma_client.get_collection(collection_name)
            print(f"✅ Using existing collection: {collection_name}")
        except:
            # Create new collection if it doesn't exist
            try:
                self.collection = self.chroma_client.create_collection(
                    name=collection_name,
                    metadata={"description": "Fintech fraud detection knowledge base"}
                )
                print(f"✅ Created new collection: {collection_name}")
            except Exception as e:
                print(f"❌ Failed to create collection: {e}")
    
    def get_embedding(self, text: str) -> List[float]:
        """Get embedding for text using sentence-transformers"""
        try:
            if self.embedding_model:
                return self.embedding_model.encode([text])[0].tolist()
            else:
                return [0.0] * 384  # Default dimension
        except Exception as e:
            print(f"❌ Error getting embedding: {e}")
            return [0.0] * 384
    
    def store_knowledge_item(self, item: Dict) -> bool:
        """Store knowledge item in database and vector store"""
        try:
            # Generate content hash for deduplication
            import hashlib
            content_hash = hashlib.sha256(item['text'].encode()).hexdigest()
            
            # Check if already exists
            if self.db_connection:
                cursor = self.db_connection.cursor()
                cursor.execute("SELECT id FROM knowledge_base WHERE content_hash = %s", (content_hash,))
                if cursor.fetchone():
                    cursor.close()
                    return False  # Already exists
                cursor.close()
            
            # Get embedding
            embedding = self.get_embedding(item['text'])
            
            # Store in ChromaDB
            if self.collection:
                embedding_id = f"kb_{int(time.time())}_{content_hash[:8]}"
                
                self.collection.add(
                    embeddings=[embedding],
                    documents=[item['text']],
                    metadatas=[{"source": item['source'], "category": item['category']}],
                    ids=[embedding_id]
                )
                
                # Store in PostgreSQL
                if self.db_connection:
                    cursor = self.db_connection.cursor()
                    cursor.execute("""
                        INSERT INTO knowledge_base (content_hash, text, source, category, embedding_id)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (content_hash, item['text'], item['source'], item['category'], embedding_id))
                    
                    self.db_connection.commit()
                    cursor.close()
                
                return True
            
        except Exception as e:
            print(f"❌ Error storing knowledge item: {e}")
            if self.db_connection:
                self.db_connection.rollback()
            return False
    
    def ingest_from_source(self, source: KnowledgeSource) -> Dict:
        """Ingest knowledge from a specific source"""
        source_name = source.__class__.__name__
        print(f"📥 Ingesting from {source_name}...")
        
        try:
            items = source.fetch_content()
            processed_count = 0
            errors = []
            
            for item in items:
                try:
                    if self.store_knowledge_item(item):
                        processed_count += 1
                        print(f"  ✅ Stored: {item['text'][:100]}...")
                except Exception as e:
                    errors.append(str(e))
                    print(f"  ❌ Failed to store item: {e}")
            
            # Log ingestion result
            if self.db_connection:
                cursor = self.db_connection.cursor()
                cursor.execute("""
                    INSERT INTO ingestion_logs (source, status, items_processed, errors)
                    VALUES (%s, %s, %s, %s)
                """, (source_name, "SUCCESS" if not errors else "PARTIAL", processed_count, json.dumps(errors)))
                self.db_connection.commit()
                cursor.close()
            
            return {
                "source": source_name,
                "processed": processed_count,
                "errors": len(errors),
                "status": "SUCCESS" if not errors else "PARTIAL"
            }
            
        except Exception as e:
            print(f"❌ Error ingesting from {source_name}: {e}")
            return {"source": source_name, "processed": 0, "errors": 1, "status": "FAILED"}
    
    def run_full_ingestion(self):
        """Run full ingestion from all sources"""
        print("🚀 Starting Free Knowledge Ingestion...")
        print("=" * 50)
        
        results = []
        for source in self.knowledge_sources:
            result = self.ingest_from_source(source)
            results.append(result)
            time.sleep(2)  # Small delay between sources
        
        print("\n📊 Ingestion Summary:")
        total_processed = sum(r['processed'] for r in results)
        total_errors = sum(r['errors'] for r in results)
        
        for result in results:
            status_emoji = "✅" if result['status'] == "SUCCESS" else "⚠️" if result['status'] == "PARTIAL" else "❌"
            print(f"  {status_emoji} {result['source']}: {result['processed']} items, {result['errors']} errors")
        
        print(f"\n🎯 Total: {total_processed} items processed, {total_errors} errors")
        return results
    
    def setup_scheduler(self):
        """Setup scheduled ingestion"""
        # Schedule ingestion every 2 hours (lighter than original)
        schedule.every(2).hours.do(self.run_full_ingestion)
        
        # Schedule specific sources at different intervals
        schedule.every(30).minutes.do(lambda: self.ingest_from_source(FintechNewsSource()))
        schedule.every(4).hours.do(lambda: self.ingest_from_source(RBINewsSource()))
        schedule.every(6).hours.do(lambda: self.ingest_from_source(NPCIGuidelines()))
        schedule.every(8).hours.do(lambda: self.ingest_from_source(SecurityResearchSource()))
    
    def run_scheduler(self):
        """Run the scheduling loop"""
        print("⏰ Starting Free Knowledge Ingestion Scheduler...")
        
        # Run initial ingestion
        self.run_full_ingestion()
        
        # Start scheduler
        while True:
            try:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
            except KeyboardInterrupt:
                print("\n⚠️ Shutting down scheduler...")
                break
            except Exception as e:
                print(f"❌ Scheduler error: {e}")
                time.sleep(60)

def main():
    """Main function"""
    engine = FreeKnowledgeIngestionEngine()
    
    # Run in scheduler mode
    threading.Thread(target=engine.run_scheduler, daemon=True).start()
    
    # Keep main thread alive
    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        print("👋 Free knowledge ingestion service stopped")

if __name__ == "__main__":
    main()
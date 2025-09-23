from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import redis
import psycopg2
from psycopg2.extras import RealDictCursor
import json
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from pydantic import BaseModel
import os
import uvicorn

app = FastAPI(
    title="🛡️ Fraud Monitoring API",
    description="API for real-time fraud detection and monitoring",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pydantic models
class TransactionQuery(BaseModel):
    user_id: Optional[str] = None
    merchant: Optional[str] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    min_amount: Optional[float] = None
    max_amount: Optional[float] = None

class AnomalyQuery(BaseModel):
    severity: Optional[str] = None
    anomaly_type: Optional[str] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    limit: Optional[int] = 100

class MitigationAction(BaseModel):
    action_type: str  # FREEZE_ACCOUNT, THROTTLE_TRANSACTIONS, BLOCK_MERCHANT
    target_id: str    # user_id, merchant_name, etc.
    duration: Optional[int] = 3600  # seconds
    reason: str

# Database connections
class DatabaseManager:
    def __init__(self):
        self.redis_client = None
        self.db_connection = None
        self.setup_connections()
    
    def setup_connections(self):
        try:
            self.redis_client = redis.Redis(
                host=os.getenv('REDIS_HOST', 'redis'),
                port=int(os.getenv('REDIS_PORT', 6379)),
                decode_responses=True
            )
            self.redis_client.ping()
        except Exception as e:
            print(f"❌ Failed to connect to Redis: {e}")
        
        try:
            self.db_connection = psycopg2.connect(
                host=os.getenv('DB_HOST', 'postgres'),
                database=os.getenv('DB_NAME', 'fintech_monitoring'),
                user=os.getenv('DB_USER', 'fintech'),
                password=os.getenv('DB_PASSWORD', 'fintech123')
            )
        except Exception as e:
            print(f"❌ Failed to connect to PostgreSQL: {e}")

    def get_redis(self):
        return self.redis_client
    
    def get_db(self):
        return self.db_connection

db_manager = DatabaseManager()

def get_redis():
    return db_manager.get_redis()

def get_db():
    return db_manager.get_db()

# API endpoints
@app.get("/")
async def root():
    return {
        "message": "🛡️ Fintech Fraud Monitoring API", 
        "version": "1.0.0",
        "status": "active"
    }

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    redis_status = "connected" if db_manager.redis_client else "disconnected"
    db_status = "connected" if db_manager.db_connection else "disconnected"
    
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "services": {
            "redis": redis_status,
            "postgresql": db_status
        }
    }

@app.get("/metrics/realtime")
async def get_realtime_metrics(redis_client = Depends(get_redis)):
    """Get real-time system metrics"""
    if not redis_client:
        raise HTTPException(status_code=500, detail="Redis connection unavailable")
    
    try:
        metrics = {
            'total_transactions': int(redis_client.get('txn:total_count') or 0),
            'success_count': int(redis_client.get('txn:status:success') or 0),
            'failed_count': int(redis_client.get('txn:status:failed') or 0),
            'pending_count': int(redis_client.get('txn:status:pending') or 0),
            'anomaly_count': int(redis_client.get('txn:anomaly_count') or 0),
            'alerts_total': int(redis_client.get('alerts_total') or 0),
            'alerts_critical': int(redis_client.get('alerts_severity_critical') or 0),
            'alerts_high': int(redis_client.get('alerts_severity_high') or 0),
            'alerts_medium': int(redis_client.get('alerts_severity_medium') or 0),
            'alerts_low': int(redis_client.get('alerts_severity_low') or 0),
        }
        
        # Calculate rates
        total = max(metrics['total_transactions'], 1)
        metrics['success_rate'] = (metrics['success_count'] / total) * 100
        metrics['failure_rate'] = (metrics['failed_count'] / total) * 100
        metrics['anomaly_rate'] = (metrics['anomaly_count'] / total) * 100
        
        return metrics
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching metrics: {str(e)}")

@app.get("/transactions/recent")
async def get_recent_transactions(
    limit: int = 50,
    redis_client = Depends(get_redis)
):
    """Get recent transactions"""
    if not redis_client:
        raise HTTPException(status_code=500, detail="Redis connection unavailable")
    
    try:
        # This would typically come from a transactions stream
        # For demo, we'll return mock data structure
        recent_txns = []
        
        # Get recent transaction amounts and create mock transactions
        amounts = redis_client.lrange('txn:amounts', 0, limit-1)
        
        for i, amount in enumerate(amounts):
            recent_txns.append({
                "txn_id": f"TXN_{1000000 + i}",
                "user_id": f"USER_{1000 + (i % 1000)}",
                "amount": float(amount),
                "status": "success",
                "timestamp": (datetime.now() - timedelta(minutes=i)).isoformat(),
                "merchant": ["Amazon", "Flipkart", "Swiggy", "Zomato"][i % 4]
            })
        
        return {"transactions": recent_txns, "count": len(recent_txns)}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching transactions: {str(e)}")

@app.post("/anomalies/search")
async def search_anomalies(
    query: AnomalyQuery,
    db_connection = Depends(get_db)
):
    """Search anomalies with filters"""
    if not db_connection:
        raise HTTPException(status_code=500, detail="Database connection unavailable")
    
    try:
        cursor = db_connection.cursor(cursor_factory=RealDictCursor)
        
        # Build dynamic query
        sql_query = "SELECT * FROM anomalies WHERE 1=1"
        params = []
        
        if query.severity:
            sql_query += " AND severity = %s"
            params.append(query.severity)
        
        if query.anomaly_type:
            sql_query += " AND anomaly_type = %s"
            params.append(query.anomaly_type)
        
        if query.start_date:
            sql_query += " AND detected_at >= %s"
            params.append(query.start_date)
        
        if query.end_date:
            sql_query += " AND detected_at <= %s"
            params.append(query.end_date)
        
        sql_query += " ORDER BY detected_at DESC"
        
        if query.limit:
            sql_query += f" LIMIT {query.limit}"
        
        cursor.execute(sql_query, params)
        anomalies = cursor.fetchall()
        cursor.close()
        
        return {"anomalies": [dict(anomaly) for anomaly in anomalies], "count": len(anomalies)}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error searching anomalies: {str(e)}")

@app.get("/anomalies/stats")
async def get_anomaly_statistics(
    hours: int = 24,
    db_connection = Depends(get_db)
):
    """Get anomaly statistics for specified time period"""
    if not db_connection:
        raise HTTPException(status_code=500, detail="Database connection unavailable")
    
    try:
        cursor = db_connection.cursor(cursor_factory=RealDictCursor)
        
        # Get anomaly counts by severity
        severity_query = """
        SELECT severity, COUNT(*) as count
        FROM anomalies 
        WHERE detected_at >= NOW() - INTERVAL '%s hours'
        GROUP BY severity
        """
        cursor.execute(severity_query, (hours,))
        severity_stats = cursor.fetchall()
        
        # Get anomaly counts by type
        type_query = """
        SELECT anomaly_type, COUNT(*) as count
        FROM anomalies 
        WHERE detected_at >= NOW() - INTERVAL '%s hours'
        GROUP BY anomaly_type
        """
        cursor.execute(type_query, (hours,))
        type_stats = cursor.fetchall()
        
        # Get hourly distribution
        hourly_query = """
        SELECT DATE_TRUNC('hour', detected_at) as hour, COUNT(*) as count
        FROM anomalies 
        WHERE detected_at >= NOW() - INTERVAL '%s hours'
        GROUP BY hour
        ORDER BY hour
        """
        cursor.execute(hourly_query, (hours,))
        hourly_stats = cursor.fetchall()
        
        cursor.close()
        
        return {
            "time_period_hours": hours,
            "severity_distribution": [dict(row) for row in severity_stats],
            "type_distribution": [dict(row) for row in type_stats],
            "hourly_distribution": [dict(row) for row in hourly_stats]
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching anomaly stats: {str(e)}")

@app.post("/mitigation/execute")
async def execute_mitigation_action(
    action: MitigationAction,
    background_tasks: BackgroundTasks,
    redis_client = Depends(get_redis)
):
    """Execute mitigation action"""
    if not redis_client:
        raise HTTPException(status_code=500, detail="Redis connection unavailable")
    
    try:
        action_id = f"mitigation_{int(datetime.now().timestamp())}"
        
        if action.action_type == "FREEZE_ACCOUNT":
            # Simulate account freeze
            freeze_key = f"frozen_account:{action.target_id}"
            redis_client.setex(freeze_key, action.duration, action.reason)
            
        elif action.action_type == "THROTTLE_TRANSACTIONS":
            # Simulate transaction throttling
            throttle_key = f"throttle:{action.target_id}"
            redis_client.setex(throttle_key, action.duration, "5")  # Max 5 per hour
            
        elif action.action_type == "BLOCK_MERCHANT":
            # Simulate merchant blocking
            block_key = f"blocked_merchant:{action.target_id}"
            redis_client.setex(block_key, action.duration, action.reason)
            
        else:
            raise HTTPException(status_code=400, detail="Invalid action type")
        
        # Log the action
        log_entry = {
            "action_id": action_id,
            "action_type": action.action_type,
            "target_id": action.target_id,
            "duration": action.duration,
            "reason": action.reason,
            "executed_at": datetime.now().isoformat(),
            "status": "SUCCESS"
        }
        
        redis_client.lpush("mitigation_log", json.dumps(log_entry))
        redis_client.ltrim("mitigation_log", 0, 999)  # Keep last 1000 entries
        
        return {
            "message": f"Mitigation action {action.action_type} executed successfully",
            "action_id": action_id,
            "status": "SUCCESS"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error executing mitigation: {str(e)}")

@app.get("/mitigation/status/{target_id}")
async def get_mitigation_status(
    target_id: str,
    redis_client = Depends(get_redis)
):
    """Check mitigation status for a target"""
    if not redis_client:
        raise HTTPException(status_code=500, detail="Redis connection unavailable")
    
    try:
        status = {}
        
        # Check if account is frozen
        freeze_key = f"frozen_account:{target_id}"
        if redis_client.exists(freeze_key):
            ttl = redis_client.ttl(freeze_key)
            status["account_frozen"] = {
                "active": True,
                "reason": redis_client.get(freeze_key),
                "remaining_seconds": ttl
            }
        else:
            status["account_frozen"] = {"active": False}
        
        # Check if transactions are throttled
        throttle_key = f"throttle:{target_id}"
        if redis_client.exists(throttle_key):
            ttl = redis_client.ttl(throttle_key)
            limit = redis_client.get(throttle_key)
            status["transaction_throttle"] = {
                "active": True,
                "limit": limit,
                "remaining_seconds": ttl
            }
        else:
            status["transaction_throttle"] = {"active": False}
        
        # Check if merchant is blocked
        block_key = f"blocked_merchant:{target_id}"
        if redis_client.exists(block_key):
            ttl = redis_client.ttl(block_key)
            status["merchant_blocked"] = {
                "active": True,
                "reason": redis_client.get(block_key),
                "remaining_seconds": ttl
            }
        else:
            status["merchant_blocked"] = {"active": False}
        
        return {
            "target_id": target_id,
            "mitigation_status": status,
            "checked_at": datetime.now().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error checking mitigation status: {str(e)}")

@app.get("/alerts/recent")
async def get_recent_alerts(
    limit: int = 20,
    redis_client = Depends(get_redis)
):
    """Get recent alerts"""
    if not redis_client:
        raise HTTPException(status_code=500, detail="Redis connection unavailable")
    
    try:
        alert_history = redis_client.lrange("alert_history", 0, limit-1)
        alerts = []
        
        for alert_str in alert_history:
            try:
                alert = json.loads(alert_str)
                alerts.append(alert)
            except json.JSONDecodeError:
                continue
        
        return {"alerts": alerts, "count": len(alerts)}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching alerts: {str(e)}")

@app.get("/system/status")
async def get_system_status(
    redis_client = Depends(get_redis),
    db_connection = Depends(get_db)
):
    """Get comprehensive system status"""
    status = {
        "timestamp": datetime.now().isoformat(),
        "services": {},
        "metrics": {},
        "alerts": {}
    }
    
    # Check service health
    status["services"]["redis"] = "healthy" if redis_client else "unhealthy"
    status["services"]["postgresql"] = "healthy" if db_connection else "unhealthy"
    
    # Get key metrics if Redis is available
    if redis_client:
        try:
            status["metrics"] = {
                "total_transactions": int(redis_client.get('txn:total_count') or 0),
                "total_anomalies": int(redis_client.get('txn:anomaly_count') or 0),
                "total_alerts": int(redis_client.get('alerts_total') or 0),
                "critical_alerts": int(redis_client.get('alerts_severity_critical') or 0)
            }
        except Exception as e:
            status["metrics"]["error"] = str(e)
    
    # System health score
    healthy_services = sum(1 for service in status["services"].values() if service == "healthy")
    total_services = len(status["services"])
    status["health_score"] = (healthy_services / total_services) * 100
    
    return status

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )
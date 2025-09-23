import json
import time
import redis
import requests
import smtplib
from datetime import datetime
from kafka import KafkaConsumer
from typing import Dict, List
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
import logging
from dataclasses import dataclass

@dataclass
class AlertChannel:
    name: str
    enabled: bool
    config: Dict

class SlackNotifier:
    """Send alerts to Slack"""
    
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url
        
    def send_alert(self, anomaly: Dict) -> bool:
        """Send anomaly alert to Slack"""
        try:
            severity_colors = {
                "LOW": "#36a64f",      # Green
                "MEDIUM": "#ff9500",   # Orange  
                "HIGH": "#ff0000",     # Red
                "CRITICAL": "#8b0000"  # Dark Red
            }
            
            severity_emojis = {
                "LOW": "🟡",
                "MEDIUM": "🟠", 
                "HIGH": "🔴",
                "CRITICAL": "🚨"
            }
            
            emoji = severity_emojis.get(anomaly['severity'], "⚠️")
            color = severity_colors.get(anomaly['severity'], "#ff9500")
            
            payload = {
                "text": f"{emoji} *FRAUD ALERT* - {anomaly['severity']} Severity",
                "attachments": [
                    {
                        "color": color,
                        "fields": [
                            {"title": "Anomaly Type", "value": anomaly['anomaly_type'], "short": True},
                            {"title": "Confidence", "value": f"{anomaly['confidence_score']:.2%}", "short": True},
                            {"title": "Transaction ID", "value": anomaly['transaction_id'], "short": True},
                            {"title": "User ID", "value": anomaly['user_id'], "short": True},
                            {"title": "Amount", "value": f"${anomaly['amount']:,.2f}", "short": True},
                            {"title": "Merchant", "value": anomaly['merchant'], "short": True},
                            {"title": "Description", "value": anomaly['description'], "short": False}
                        ],
                        "footer": "Fintech Monitoring System",
                        "ts": int(datetime.now().timestamp())
                    }
                ]
            }
            
            # Add LLM explanation if available
            if anomaly.get('llm_explanation'):
                payload["attachments"][0]["fields"].append({
                    "title": "AI Analysis",
                    "value": anomaly['llm_explanation'][:500] + "..." if len(anomaly['llm_explanation']) > 500 else anomaly['llm_explanation'],
                    "short": False
                })
            
            # Add recommended actions
            if anomaly.get('recommended_actions'):
                actions_text = "\n".join([f"• {action}" for action in anomaly['recommended_actions']])
                payload["attachments"][0]["fields"].append({
                    "title": "Recommended Actions",
                    "value": actions_text,
                    "short": False
                })
            
            response = requests.post(self.webhook_url, json=payload, timeout=10)
            response.raise_for_status()
            
            print(f"✅ Slack alert sent for {anomaly['anomaly_id']}")
            return True
            
        except Exception as e:
            print(f"❌ Failed to send Slack alert: {e}")
            return False

class TelegramNotifier:
    """Send alerts to Telegram"""
    
    def __init__(self, bot_token: str, chat_id: str):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.api_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    
    def send_alert(self, anomaly: Dict) -> bool:
        """Send anomaly alert to Telegram"""
        try:
            severity_emojis = {
                "LOW": "🟡",
                "MEDIUM": "🟠", 
                "HIGH": "🔴",
                "CRITICAL": "🚨"
            }
            
            emoji = severity_emojis.get(anomaly['severity'], "⚠️")
            
            message = f"""
{emoji} *FRAUD ALERT - {anomaly['severity']} SEVERITY*

🆔 *Anomaly ID:* `{anomaly['anomaly_id']}`
🔍 *Type:* {anomaly['anomaly_type']}
📊 *Confidence:* {anomaly['confidence_score']:.2%}

💳 *Transaction Details:*
• ID: `{anomaly['transaction_id']}`
• User: `{anomaly['user_id']}`
• Amount: ${anomaly['amount']:,.2f}
• Merchant: {anomaly['merchant']}

📝 *Description:*
{anomaly['description']}
"""

            # Add LLM explanation if available
            if anomaly.get('llm_explanation'):
                message += f"\n🤖 *AI Analysis:*\n{anomaly['llm_explanation'][:800]}"
                if len(anomaly['llm_explanation']) > 800:
                    message += "..."
            
            # Add recommended actions
            if anomaly.get('recommended_actions'):
                message += "\n\n✅ *Recommended Actions:*"
                for action in anomaly['recommended_actions']:
                    message += f"\n• {action}"
            
            message += f"\n\n🕐 *Detected:* {anomaly['detected_at']}"
            
            payload = {
                "chat_id": self.chat_id,
                "text": message,
                "parse_mode": "Markdown"
            }
            
            response = requests.post(self.api_url, json=payload, timeout=10)
            response.raise_for_status()
            
            print(f"✅ Telegram alert sent for {anomaly['anomaly_id']}")
            return True
            
        except Exception as e:
            print(f"❌ Failed to send Telegram alert: {e}")
            return False

class EmailNotifier:
    """Send alerts via email"""
    
    def __init__(self, smtp_config: Dict):
        self.smtp_server = smtp_config.get('server', 'smtp.gmail.com')
        self.smtp_port = smtp_config.get('port', 587)
        self.username = smtp_config.get('username')
        self.password = smtp_config.get('password')
        self.from_email = smtp_config.get('from_email', self.username)
        self.to_emails = smtp_config.get('to_emails', [])
    
    def send_alert(self, anomaly: Dict) -> bool:
        """Send anomaly alert via email"""
        try:
            subject = f"🚨 FRAUD ALERT - {anomaly['severity']} - {anomaly['anomaly_type']}"
            
            html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 40px; }}
        .alert-header {{ background-color: #ff4444; color: white; padding: 20px; text-align: center; }}
        .alert-content {{ padding: 20px; border: 1px solid #ddd; }}
        .detail-table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
        .detail-table th, .detail-table td {{ border: 1px solid #ddd; padding: 12px; text-align: left; }}
        .detail-table th {{ background-color: #f2f2f2; }}
        .actions {{ background-color: #f9f9f9; padding: 15px; margin-top: 20px; }}
        .severity-{anomaly['severity'].lower()} {{ color: {"red" if anomaly['severity'] in ["HIGH", "CRITICAL"] else "orange" if anomaly['severity'] == "MEDIUM" else "green"}; }}
    </style>
</head>
<body>
    <div class="alert-header">
        <h1>🚨 FRAUD ALERT DETECTED</h1>
        <h2 class="severity-{anomaly['severity'].lower()}">{anomaly['severity']} SEVERITY</h2>
    </div>
    
    <div class="alert-content">
        <h3>Transaction Details</h3>
        <table class="detail-table">
            <tr><th>Anomaly ID</th><td>{anomaly['anomaly_id']}</td></tr>
            <tr><th>Anomaly Type</th><td>{anomaly['anomaly_type']}</td></tr>
            <tr><th>Confidence Score</th><td>{anomaly['confidence_score']:.2%}</td></tr>
            <tr><th>Transaction ID</th><td>{anomaly['transaction_id']}</td></tr>
            <tr><th>User ID</th><td>{anomaly['user_id']}</td></tr>
            <tr><th>Amount</th><td>${anomaly['amount']:,.2f}</td></tr>
            <tr><th>Merchant</th><td>{anomaly['merchant']}</td></tr>
            <tr><th>Detected At</th><td>{anomaly['detected_at']}</td></tr>
        </table>
        
        <h3>Description</h3>
        <p>{anomaly['description']}</p>
"""

            if anomaly.get('llm_explanation'):
                html_content += f"""
        <h3>🤖 AI Analysis</h3>
        <p>{anomaly['llm_explanation']}</p>
"""

            if anomaly.get('recommended_actions'):
                actions_html = "".join([f"<li>{action}</li>" for action in anomaly['recommended_actions']])
                html_content += f"""
        <div class="actions">
            <h3>✅ Recommended Actions</h3>
            <ul>{actions_html}</ul>
        </div>
"""

            html_content += """
    </div>
</body>
</html>
"""
            
            msg = MIMEMultipart()
            msg['From'] = self.from_email
            msg['To'] = ', '.join(self.to_emails)
            msg['Subject'] = subject
            msg.attach(MIMEText(html_content, 'html'))
            
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.username, self.password)
                server.send_message(msg)
            
            print(f"✅ Email alert sent for {anomaly['anomaly_id']}")
            return True
            
        except Exception as e:
            print(f"❌ Failed to send email alert: {e}")
            return False

class MitigationSimulator:
    """Simulate automated mitigation actions"""
    
    def __init__(self, redis_client):
        self.redis_client = redis_client
    
    def simulate_account_freeze(self, user_id: str, duration: int = 3600) -> Dict:
        """Simulate freezing user account"""
        try:
            freeze_key = f"frozen_account:{user_id}"
            self.redis_client.setex(freeze_key, duration, "FRAUD_DETECTED")
            
            print(f"🧊 Simulated account freeze for {user_id} (duration: {duration}s)")
            return {
                "action": "ACCOUNT_FREEZE",
                "user_id": user_id,
                "duration": duration,
                "status": "SUCCESS",
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            return {"action": "ACCOUNT_FREEZE", "status": "FAILED", "error": str(e)}
    
    def simulate_transaction_throttling(self, user_id: str, limit: int = 5) -> Dict:
        """Simulate throttling user transactions"""
        try:
            throttle_key = f"throttle:{user_id}"
            self.redis_client.setex(throttle_key, 3600, limit)  # 1 hour throttle
            
            print(f"🐌 Simulated transaction throttling for {user_id} (limit: {limit}/hour)")
            return {
                "action": "TRANSACTION_THROTTLE", 
                "user_id": user_id,
                "limit": limit,
                "status": "SUCCESS",
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            return {"action": "TRANSACTION_THROTTLE", "status": "FAILED", "error": str(e)}
    
    def simulate_merchant_block(self, merchant: str, duration: int = 7200) -> Dict:
        """Simulate blocking transactions to suspicious merchant"""
        try:
            block_key = f"blocked_merchant:{merchant}"
            self.redis_client.setex(block_key, duration, "SUSPICIOUS_ACTIVITY")
            
            print(f"🚫 Simulated merchant block for {merchant} (duration: {duration}s)")
            return {
                "action": "MERCHANT_BLOCK",
                "merchant": merchant,
                "duration": duration,
                "status": "SUCCESS",
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            return {"action": "MERCHANT_BLOCK", "status": "FAILED", "error": str(e)}
    
    def execute_mitigation(self, anomaly: Dict) -> List[Dict]:
        """Execute appropriate mitigation actions based on anomaly"""
        actions = []
        
        severity = anomaly.get('severity', 'MEDIUM')
        anomaly_type = anomaly.get('anomaly_type', '')
        user_id = anomaly.get('user_id')
        merchant = anomaly.get('merchant')
        
        # Critical severity - immediate account freeze
        if severity == 'CRITICAL':
            if user_id:
                actions.append(self.simulate_account_freeze(user_id, 7200))  # 2 hours
        
        # High severity - throttle transactions
        elif severity == 'HIGH':
            if user_id:
                actions.append(self.simulate_transaction_throttling(user_id, 3))  # Max 3 per hour
        
        # High velocity anomalies - immediate throttling
        if anomaly_type == 'HIGH_VELOCITY' and user_id:
            actions.append(self.simulate_transaction_throttling(user_id, 1))  # Max 1 per hour
        
        # Suspicious merchant - block temporarily
        if 'MERCHANT' in anomaly_type or merchant in ['CashAdvanceNow', 'InstantLoan247', 'CryptoExchange']:
            actions.append(self.simulate_merchant_block(merchant, 3600))  # 1 hour
        
        return actions

class AlertManager:
    """Main alert management system"""
    
    def __init__(self):
        self.kafka_consumer = None
        self.redis_client = None
        self.notifiers = {}
        self.mitigation_simulator = None
        self.setup_connections()
        self.setup_notifiers()
    
    def setup_connections(self):
        """Setup Kafka and Redis connections"""
        # Kafka
        max_retries = 30
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                self.kafka_consumer = KafkaConsumer(
                    'alerts',
                    bootstrap_servers=['kafka:29092'],
                    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                    group_id='alert-manager'
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
            self.mitigation_simulator = MitigationSimulator(self.redis_client)
            print("✅ Connected to Redis successfully!")
        except Exception as e:
            print(f"❌ Failed to connect to Redis: {e}")
    
    def setup_notifiers(self):
        """Setup notification channels"""
        # Slack
        slack_webhook = os.getenv('SLACK_WEBHOOK_URL')
        if slack_webhook:
            self.notifiers['slack'] = SlackNotifier(slack_webhook)
            print("✅ Slack notifier configured")
        
        # Telegram  
        telegram_token = os.getenv('TELEGRAM_BOT_TOKEN')
        telegram_chat = os.getenv('TELEGRAM_CHAT_ID')
        if telegram_token and telegram_chat:
            self.notifiers['telegram'] = TelegramNotifier(telegram_token, telegram_chat)
            print("✅ Telegram notifier configured")
        
        # Email (configure if needed)
        email_config = {
            'username': os.getenv('EMAIL_USERNAME'),
            'password': os.getenv('EMAIL_PASSWORD'),
            'to_emails': os.getenv('EMAIL_RECIPIENTS', '').split(',') if os.getenv('EMAIL_RECIPIENTS') else []
        }
        if email_config['username'] and email_config['to_emails']:
            self.notifiers['email'] = EmailNotifier(email_config)
            print("✅ Email notifier configured")
    
    def should_send_alert(self, anomaly: Dict) -> bool:
        """Determine if alert should be sent (rate limiting, deduplication)"""
        anomaly_id = anomaly.get('anomaly_id')
        severity = anomaly.get('severity', 'MEDIUM')
        
        # Check if alert was recently sent for this anomaly
        alert_key = f"alert_sent:{anomaly_id}"
        if self.redis_client and self.redis_client.exists(alert_key):
            print(f"⏭️ Skipping duplicate alert for {anomaly_id}")
            return False
        
        # Rate limiting based on severity
        rate_limits = {
            'LOW': 300,      # 5 minutes
            'MEDIUM': 180,   # 3 minutes  
            'HIGH': 60,      # 1 minute
            'CRITICAL': 0    # No rate limiting
        }
        
        if self.redis_client:
            self.redis_client.setex(alert_key, rate_limits.get(severity, 180), 1)
        
        return True
    
    def process_alert(self, anomaly: Dict):
        """Process incoming anomaly alert"""
        if not self.should_send_alert(anomaly):
            return
        
        anomaly_id = anomaly.get('anomaly_id')
        severity = anomaly.get('severity', 'MEDIUM')
        
        print(f"🚨 Processing alert: {anomaly_id} ({severity})")
        
        # Send notifications
        notification_results = {}
        for channel, notifier in self.notifiers.items():
            try:
                success = notifier.send_alert(anomaly)
                notification_results[channel] = success
            except Exception as e:
                print(f"❌ Error sending {channel} notification: {e}")
                notification_results[channel] = False
        
        # Execute automated mitigation if configured
        mitigation_results = []
        if self.mitigation_simulator and severity in ['HIGH', 'CRITICAL']:
            try:
                mitigation_results = self.mitigation_simulator.execute_mitigation(anomaly)
                print(f"🤖 Executed {len(mitigation_results)} mitigation actions")
            except Exception as e:
                print(f"❌ Error executing mitigation: {e}")
        
        # Store alert processing results in Redis
        if self.redis_client:
            alert_record = {
                'anomaly_id': anomaly_id,
                'severity': severity,
                'processed_at': datetime.now().isoformat(),
                'notifications': notification_results,
                'mitigations': mitigation_results
            }
            
            self.redis_client.lpush('alert_history', json.dumps(alert_record))
            self.redis_client.ltrim('alert_history', 0, 999)  # Keep last 1000 alerts
            
            # Update alert metrics
            self.redis_client.incr('alerts_total')
            self.redis_client.incr(f'alerts_severity_{severity.lower()}')
            
            if any(notification_results.values()):
                self.redis_client.incr('alerts_sent_successfully')
    
    def run(self):
        """Main alert processing loop"""
        print("🚨 Starting Alert Management System...")
        print("📢 Monitoring for anomaly alerts...")
        print("=" * 60)
        
        try:
            for message in self.kafka_consumer:
                anomaly = message.value
                self.process_alert(anomaly)
                
        except KeyboardInterrupt:
            print("\n⚠️ Shutting down alert manager...")
        except Exception as e:
            print(f"❌ Error in alert manager: {e}")
        finally:
            if self.kafka_consumer:
                self.kafka_consumer.close()
            print("✅ Alert manager closed")

if __name__ == "__main__":
    alert_manager = AlertManager()
    alert_manager.run()
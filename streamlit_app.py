import streamlit as st
import redis
import psycopg2
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import json
import time
from datetime import datetime, timedelta
import numpy as np
from typing import Dict, List
import os

# Page configuration
st.set_page_config(
    page_title="🛡️ Fintech Fraud Monitoring",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded"
)

class DashboardConnector:
    """Handle connections to Redis and PostgreSQL"""
    
    def __init__(self):
        self.redis_client = None
        self.db_connection = None
        self.setup_connections()
    
    def setup_connections(self):
        """Setup Redis and PostgreSQL connections"""
        try:
            self.redis_client = redis.Redis(
                host=os.getenv('REDIS_HOST', 'redis'),
                port=int(os.getenv('REDIS_PORT', 6379)),
                decode_responses=True
            )
            self.redis_client.ping()
        except Exception as e:
            st.error(f"❌ Failed to connect to Redis: {e}")
        
        try:
            self.db_connection = psycopg2.connect(
                host=os.getenv('DB_HOST', 'postgres'),
                database=os.getenv('DB_NAME', 'fintech_monitoring'),
                user=os.getenv('DB_USER', 'fintech'),
                password=os.getenv('DB_PASSWORD', 'fintech123')
            )
        except Exception as e:
            st.error(f"❌ Failed to connect to PostgreSQL: {e}")

@st.cache_data(ttl=30)  # Cache for 30 seconds
def get_realtime_metrics(_connector: DashboardConnector) -> Dict:
    """Get real-time metrics from Redis"""
    if not _connector.redis_client:
        return {}
    
    try:
        metrics = {
            'total_transactions': int(_connector.redis_client.get('txn:total_count') or 0),
            'success_count': int(_connector.redis_client.get('txn:status:success') or 0),
            'failed_count': int(_connector.redis_client.get('txn:status:failed') or 0),
            'pending_count': int(_connector.redis_client.get('txn:status:pending') or 0),
            'anomaly_count': int(_connector.redis_client.get('txn:anomaly_count') or 0),
            'alerts_total': int(_connector.redis_client.get('alerts_total') or 0),
            'alerts_critical': int(_connector.redis_client.get('alerts_severity_critical') or 0),
            'alerts_high': int(_connector.redis_client.get('alerts_severity_high') or 0),
            'alerts_medium': int(_connector.redis_client.get('alerts_severity_medium') or 0),
            'alerts_low': int(_connector.redis_client.get('alerts_severity_low') or 0),
        }
        
        # Calculate derived metrics
        metrics['success_rate'] = (metrics['success_count'] / max(metrics['total_transactions'], 1)) * 100
        metrics['anomaly_rate'] = (metrics['anomaly_count'] / max(metrics['total_transactions'], 1)) * 100
        
        # Get recent transaction amounts
        amounts = _connector.redis_client.lrange('txn:amounts', 0, 999)
        if amounts:
            amounts = [float(amt) for amt in amounts]
            metrics['avg_amount'] = np.mean(amounts)
            metrics['total_volume'] = sum(amounts)
        else:
            metrics['avg_amount'] = 0
            metrics['total_volume'] = 0
        
        return metrics
        
    except Exception as e:
        st.error(f"Error fetching metrics: {e}")
        return {}

@st.cache_data(ttl=60)  # Cache for 1 minute
def get_anomaly_data(_connector: DashboardConnector) -> pd.DataFrame:
    """Get anomaly data from PostgreSQL"""
    if not _connector.db_connection:
        return pd.DataFrame()
    
    try:
        query = """
        SELECT anomaly_id, transaction_id, anomaly_type, severity, confidence_score,
               description, detected_at, user_id, amount, merchant, llm_explanation
        FROM anomalies 
        WHERE detected_at >= NOW() - INTERVAL '24 hours'
        ORDER BY detected_at DESC
        LIMIT 100
        """
        
        df = pd.read_sql(query, _connector.db_connection)
        return df
        
    except Exception as e:
        st.error(f"Error fetching anomaly data: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=60)
def get_recent_anomalies(_connector: DashboardConnector) -> List[Dict]:
    """Get recent anomalies from Redis"""
    if not _connector.redis_client:
        return []
    
    try:
        anomaly_list = _connector.redis_client.lrange('txn:anomalies', 0, 9)  # Last 10
        anomalies = []
        
        for anomaly_str in anomaly_list:
            try:
                anomaly = json.loads(anomaly_str)
                anomalies.append(anomaly)
            except:
                continue
        
        return anomalies
        
    except Exception as e:
        st.error(f"Error fetching recent anomalies: {e}")
        return []

def create_metrics_overview(metrics: Dict):
    """Create metrics overview section"""
    st.header("📊 Real-Time Metrics Overview")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Total Transactions",
            value=f"{metrics.get('total_transactions', 0):,}",
            delta=f"+{metrics.get('total_transactions', 0) - metrics.get('prev_total', 0)}" if metrics.get('prev_total') else None
        )
        
        st.metric(
            label="Success Rate",
            value=f"{metrics.get('success_rate', 0):.1f}%",
            delta=f"{metrics.get('success_rate', 0) - 95:.1f}%" if metrics.get('success_rate', 0) < 95 else "Normal"
        )
    
    with col2:
        st.metric(
            label="Total Volume",
            value=f"${metrics.get('total_volume', 0):,.2f}",
            delta="Growing" if metrics.get('total_volume', 0) > 0 else None
        )
        
        st.metric(
            label="Average Amount",
            value=f"${metrics.get('avg_amount', 0):.2f}",
            delta=None
        )
    
    with col3:
        anomaly_rate = metrics.get('anomaly_rate', 0)
        st.metric(
            label="Anomaly Rate",
            value=f"{anomaly_rate:.2f}%",
            delta="🚨 High" if anomaly_rate > 5 else "Normal",
            delta_color="inverse" if anomaly_rate > 5 else "normal"
        )
        
        st.metric(
            label="Total Anomalies",
            value=f"{metrics.get('anomaly_count', 0):,}",
            delta=f"+{metrics.get('anomaly_count', 0)}" if metrics.get('anomaly_count', 0) > 0 else None
        )
    
    with col4:
        st.metric(
            label="Critical Alerts",
            value=f"{metrics.get('alerts_critical', 0)}",
            delta="🚨 URGENT" if metrics.get('alerts_critical', 0) > 0 else "None",
            delta_color="inverse" if metrics.get('alerts_critical', 0) > 0 else "normal"
        )
        
        st.metric(
            label="Total Alerts",
            value=f"{metrics.get('alerts_total', 0)}",
            delta=None
        )

def create_transaction_charts(metrics: Dict):
    """Create transaction analysis charts"""
    st.header("📈 Transaction Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Transaction Status Distribution
        status_data = {
            'Status': ['Success', 'Failed', 'Pending'],
            'Count': [
                metrics.get('success_count', 0),
                metrics.get('failed_count', 0), 
                metrics.get('pending_count', 0)
            ],
            'Color': ['#28a745', '#dc3545', '#ffc107']
        }
        
        fig_status = px.pie(
            values=status_data['Count'],
            names=status_data['Status'],
            title="Transaction Status Distribution",
            color_discrete_sequence=status_data['Color']
        )
        fig_status.update_layout(height=400)
        st.plotly_chart(fig_status, use_container_width=True)
    
    with col2:
        # Alert Severity Distribution  
        alert_data = {
            'Severity': ['Critical', 'High', 'Medium', 'Low'],
            'Count': [
                metrics.get('alerts_critical', 0),
                metrics.get('alerts_high', 0),
                metrics.get('alerts_medium', 0),
                metrics.get('alerts_low', 0)
            ]
        }
        
        fig_alerts = px.bar(
            x=alert_data['Severity'],
            y=alert_data['Count'],
            title="Alert Severity Distribution",
            color=alert_data['Count'],
            color_continuous_scale='Reds'
        )
        fig_alerts.update_layout(height=400, showlegend=False)
        st.plotly_chart(fig_alerts, use_container_width=True)

def create_anomaly_timeline(df_anomalies: pd.DataFrame):
    """Create anomaly detection timeline"""
    if df_anomalies.empty:
        st.warning("No anomaly data available")
        return
    
    st.header("🔍 Anomaly Detection Timeline")
    
    # Process timeline data
    df_timeline = df_anomalies.copy()
    df_timeline['detected_at'] = pd.to_datetime(df_timeline['detected_at'])
    df_timeline['hour'] = df_timeline['detected_at'].dt.floor('H')
    
    # Aggregate by hour and severity
    timeline_agg = df_timeline.groupby(['hour', 'severity']).size().reset_index(name='count')
    
    # Create timeline chart
    fig_timeline = px.line(
        timeline_agg, 
        x='hour', 
        y='count',
        color='severity',
        title='Anomaly Detection Over Time (24 Hours)',
        color_discrete_map={
            'CRITICAL': '#8b0000',
            'HIGH': '#ff0000', 
            'MEDIUM': '#ff9500',
            'LOW': '#36a64f'
        }
    )
    
    fig_timeline.update_layout(height=400)
    st.plotly_chart(fig_timeline, use_container_width=True)
    
    # Anomaly type distribution
    col1, col2 = st.columns(2)
    
    with col1:
        anomaly_types = df_anomalies['anomaly_type'].value_counts()
        fig_types = px.bar(
            x=anomaly_types.values,
            y=anomaly_types.index,
            orientation='h',
            title='Anomaly Types Distribution',
            color=anomaly_types.values,
            color_continuous_scale='Reds'
        )
        fig_types.update_layout(height=400, showlegend=False)
        st.plotly_chart(fig_types, use_container_width=True)
    
    with col2:
        # Confidence score distribution
        fig_confidence = px.histogram(
            df_anomalies,
            x='confidence_score',
            nbins=20,
            title='Confidence Score Distribution',
            color_discrete_sequence=['#ff6b6b']
        )
        fig_confidence.update_layout(height=400)
        st.plotly_chart(fig_confidence, use_container_width=True)

def create_recent_alerts_section(recent_anomalies: List[Dict]):
    """Create recent alerts section"""
    st.header("🚨 Recent Anomaly Alerts")
    
    if not recent_anomalies:
        st.info("No recent anomalies detected")
        return
    
    for i, anomaly in enumerate(recent_anomalies):
        severity = anomaly.get('status', 'MEDIUM')
        severity_colors = {
            'CRITICAL': '#8b0000',
            'HIGH': '#ff0000',
            'MEDIUM': '#ff9500', 
            'LOW': '#36a64f'
        }
        
        severity_emojis = {
            'CRITICAL': '🚨',
            'HIGH': '🔴',
            'MEDIUM': '🟠',
            'LOW': '🟡'
        }
        
        with st.expander(
            f"{severity_emojis.get(severity, '⚠️')} {anomaly.get('txn_id', 'Unknown')} - "
            f"${anomaly.get('amount', 0):.2f} - {anomaly.get('merchant', 'Unknown')}",
            expanded=i < 3  # Expand first 3 alerts
        ):
            col1, col2 = st.columns([2, 1])
            
            with col1:
                st.write(f"**Transaction ID:** {anomaly.get('txn_id', 'N/A')}")
                st.write(f"**User ID:** {anomaly.get('user_id', 'N/A')}")
                st.write(f"**Amount:** ${anomaly.get('amount', 0):.2f}")
                st.write(f"**Merchant:** {anomaly.get('merchant', 'N/A')}")
                st.write(f"**Location:** {anomaly.get('location', 'N/A')}")
                st.write(f"**Status:** {anomaly.get('status', 'N/A')}")
            
            with col2:
                st.write(f"**Risk Score:** {anomaly.get('risk_score', 0):.2f}")
                st.write(f"**Timestamp:** {anomaly.get('timestamp', 'N/A')}")
                st.write(f"**Device:** {anomaly.get('device_id', 'N/A')}")
                st.write(f"**Payment Method:** {anomaly.get('payment_method', 'N/A')}")
                
                if anomaly.get('is_anomaly'):
                    st.error("🚨 ANOMALY DETECTED")
                else:
                    st.success("✅ Normal Transaction")

def create_ai_insights_section(df_anomalies: pd.DataFrame):
    """Create AI insights section"""
    st.header("🤖 AI-Powered Insights")
    
    if df_anomalies.empty:
        st.info("No anomaly data available for AI analysis")
        return
    
    # Show recent LLM explanations
    recent_explanations = df_anomalies[df_anomalies['llm_explanation'].notna()].head(3)
    
    if not recent_explanations.empty:
        st.subheader("💡 Recent AI Explanations")
        
        for _, anomaly in recent_explanations.iterrows():
            with st.expander(f"🔍 {anomaly['anomaly_type']} - {anomaly['transaction_id']}"):
                st.write(f"**Confidence:** {anomaly['confidence_score']:.2%}")
                st.write(f"**Amount:** ${anomaly['amount']:,.2f}")
                st.write(f"**Merchant:** {anomaly['merchant']}")
                
                st.markdown("**AI Analysis:**")
                st.info(anomaly['llm_explanation'])
    
    # AI-generated insights
    st.subheader("📊 Pattern Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Top risk merchants
        if 'merchant' in df_anomalies.columns:
            risk_merchants = df_anomalies.groupby('merchant').agg({
                'confidence_score': 'mean',
                'amount': 'sum'
            }).sort_values('confidence_score', ascending=False).head(5)
            
            st.write("**Highest Risk Merchants:**")
            for merchant, data in risk_merchants.iterrows():
                st.write(f"• {merchant}: {data['confidence_score']:.2%} risk, ${data['amount']:,.2f} volume")
    
    with col2:
        # Time-based patterns
        if 'detected_at' in df_anomalies.columns:
            df_time = df_anomalies.copy()
            df_time['hour'] = pd.to_datetime(df_time['detected_at']).dt.hour
            hourly_anomalies = df_time['hour'].value_counts().sort_index()
            
            st.write("**Peak Anomaly Hours:**")
            for hour, count in hourly_anomalies.head(5).items():
                st.write(f"• {hour:02d}:00 - {count} anomalies")

def main():
    """Main dashboard function"""
    st.title("🛡️ Fintech Fraud Monitoring Dashboard")
    st.markdown("Real-time monitoring and AI-powered fraud detection system")
    
    # Initialize connector
    _connector = DashboardConnector()
    
    # Sidebar controls
    st.sidebar.header("⚙️ Dashboard Controls")
    
    auto_refresh = st.sidebar.checkbox("🔄 Auto Refresh", value=True)
    refresh_interval = st.sidebar.slider("Refresh Interval (seconds)", 5, 60, 30)
    
    if auto_refresh:
        # Auto-refresh placeholder
        placeholder = st.empty()
        
        while auto_refresh:
            with placeholder.container():
                # Get data
                metrics = get_realtime_metrics(_connector)
                df_anomalies = get_anomaly_data(_connector)
                recent_anomalies = get_recent_anomalies(_connector)
                
                # Create dashboard sections
                create_metrics_overview(metrics)
                st.divider()
                
                create_transaction_charts(metrics)
                st.divider()
                
                create_anomaly_timeline(df_anomalies)
                st.divider()
                
                create_recent_alerts_section(recent_anomalies)
                st.divider()
                
                create_ai_insights_section(df_anomalies)
                
                # Status footer
                st.markdown("---")
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.caption(f"🔄 Last updated: {datetime.now().strftime('%H:%M:%S')}")
                with col2:
                    st.caption(f"📊 Total Transactions: {metrics.get('total_transactions', 0):,}")
                with col3:
                    st.caption(f"🚨 Active Anomalies: {metrics.get('anomaly_count', 0)}")
            
            time.sleep(refresh_interval)
    else:
        # Static dashboard
        metrics = get_realtime_metrics(_connector)
        df_anomalies = get_anomaly_data(_connector)
        recent_anomalies = get_recent_anomalies(_connector)
        
        create_metrics_overview(metrics)
        st.divider()
        
        create_transaction_charts(metrics)
        st.divider()
        
        create_anomaly_timeline(df_anomalies)
        st.divider()
        
        create_recent_alerts_section(recent_anomalies)
        st.divider()
        
        create_ai_insights_section(df_anomalies)

if __name__ == "__main__":
    main()
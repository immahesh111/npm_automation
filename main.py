import streamlit as st
import pymongo
from pymongo import MongoClient
import pytz
from datetime import datetime, timedelta
import pandas as pd
import os
from dotenv import load_dotenv
import uuid
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from io import BytesIO

# Load environment variables
load_dotenv()
MONGO_URI = "mongodb+srv://maheshkumar17032k3:kooK8I6bgdRUcUx9@npmticket.knzwnmz.mongodb.net/?retryWrites=true&w=majority&appName=npmticket"

# Page configuration
st.set_page_config(
    page_title="Production Line Dashboard",
    page_icon="🏭",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Add custom CSS
st.markdown("""
<style>
    /* Custom styles for expanders */
    .stExpander {
        border: none !important;
        box-shadow: 0 4px 15px rgba(0, 0, 0, 0.1) !important;
        margin-bottom: 1rem !important;
    }

    .stExpander > div:first-child {
        border-radius: 10px !important;
        padding: 1rem !important;
        background: var(--expander-bg-color, linear-gradient(90deg, #1e3c72 0%, #2a5298 100%)) !important;
        color: white !important;
        font-weight: 600 !important;
        font-size: 16px !important;
    }

    .stExpander > div:first-child:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 25px rgba(0, 0, 0, 0.2);
        transition: all 0.3s ease;
    }

    .stExpander > div:nth-child(2) {
        border-radius: 0 0 10px 10px !important;
        padding: 1.5rem !important;
        background: white !important;
        border: 1px solid #e2e8f0 !important;
    }

    /* Expander styling */
    [data-testid="stExpander"] {
        background: #f8f9fa !important;
        border-radius: 10px !important;
        padding: 0.5rem !important;
        box-shadow: 0 2px 8px rgba(0,0,0,0.1) !important;
    }

    /* Header styling for different states */
    .expander-good [data-testid="stExpanderHeader"] {
        background: linear-gradient(90deg, #48bb78 0%, #38a169 100%) !important;
        color: white !important;
        border-radius: 8px !important;
        padding: 1rem !important;
    }

    .expander-warning [data-testid="stExpanderHeader"] {
        background: linear-gradient(90deg, #ed8936 0%, #dd6b20 100%) !important;
        color: white !important;
        border-radius: 8px !important;
        padding: 1rem !important;
    }

    .expander-critical [data-testid="stExpanderHeader"] {
        background: linear-gradient(90deg, #f56565 0%, #e53e3e 100%) !important;
        color: white !important;
        border-radius: 8px !important;
        padding: 1rem !important;
    }
</style>
""", unsafe_allow_html=True)

# MongoDB connection with improved error handling
@st.cache_resource
def init_connection():
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        db = client["production_db"]
        db.command("ping")
        return db
    except pymongo.errors.ConnectionError as e:
        st.error(f"🚫 Failed to connect to MongoDB: {str(e)}")
        st.stop()

db = init_connection()

# Time zone
ist = pytz.timezone('Asia/Kolkata')

# Enhanced CSS for professional UI
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

* {
    font-family: 'Inter', sans-serif;
}

.stApp {
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    min-height: 100vh;
}

.main-header {
    background: rgba(255, 255, 255, 0.95);
    backdrop-filter: blur(20px);
    padding: 2rem;
    border-radius: 20px;
    margin-bottom: 2rem;
    box-shadow: 0 8px 32px rgba(0, 0, 0, 0.1);
    border: 1px solid rgba(255, 255, 255, 0.2);
    text-align: center;
}

.main-title {
    color: #2d3748;
    font-size: 3rem;
    font-weight: 700;
    margin-bottom: 0.5rem;
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
}

.main-subtitle {
    color: #4a5568;
    font-size: 1.2rem;
    font-weight: 400;
    opacity: 0.8;
}

.metric-card {
    background: rgba(255, 255, 255, 0.95);
    backdrop-filter: blur(20px);
    padding: 1.5rem;
    border-radius: 16px;
    box-shadow: 0 8px 32px rgba(0, 0, 0, 0.1);
    border: 1px solid rgba(255, 255, 255, 0.2);
    text-align: center;
    transition: all 0.3s ease;
    margin-bottom: 1rem;
}

.metric-card:hover {
    transform: translateY(-5px);
    box-shadow: 0 12px 40px rgba(0, 0, 0, 0.15);
}

.metric-value {
    font-size: 2.5rem;
    font-weight: 700;
    margin-bottom: 0.5rem;
}

.metric-label {
    color: #4a5568;
    font-size: 0.9rem;
    font-weight: 500;
    text-transform: uppercase;
    letter-spacing: 0.5px;
}

.metric-good { color: #48bb78; }
.metric-warning { color: #ed8936; }
.metric-danger { color: #f56565; }
.metric-info { color: #4299e1; }

.data-card {
    background: rgba(255, 255, 255, 0.95);
    backdrop-filter: blur(20px);
    padding: 1.5rem;
    border-radius: 16px;
    margin-bottom: 1rem;
    box-shadow: 0 8px 32px rgba(0, 0, 0, 0.1);
    border: 1px solid rgba(255, 255, 255, 0.2);
    transition: all 0.3s ease;
}

.data-card:hover {
    transform: translateY(-2px);
    box-shadow: 0 12px 40px rgba(0, 0, 0, 0.15);
}

.line-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 1rem;
    padding-bottom: 0.5rem;
    border-bottom: 2px solid #e2e8f0;
}

.line-title {
    font-size: 1.5rem;
    font-weight: 600;
    color: #2d3748;
}

.status-badge {
    padding: 0.5rem 1rem;
    border-radius: 50px;
    font-size: 0.8rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.5px;
}

.status-good {
    background: linear-gradient(135deg, #48bb78, #38a169);
    color: white;
}

.status-warning {
    background: linear-gradient(135deg, #ed8936, #dd6b20);
    color: white;
}

.status-danger {
    background: linear-gradient(135deg, #f56565, #e53e3e);
    color: white;
}

.ticket-card {
    background: linear-gradient(135deg, #faf5ff, #f7fafc);
    border: 1px solid #e2e8f0;
    border-radius: 12px;
    padding: 1rem;
    margin: 0.5rem 0;
    transition: all 0.3s ease;
    position: relative;
    overflow: hidden;
}

.ticket-card::before {
    content: '';
    position: absolute;
    left: 0;
    top: 0;
    height: 100%;
    width: 4px;
    background: linear-gradient(135deg, #667eea, #764ba2);
}

.ticket-card:hover {
    transform: translateX(5px);
    box-shadow: 0 8px 25px rgba(0, 0, 0, 0.1);
}

.ticket-open::before {
    background: linear-gradient(135deg, #f56565, #e53e3e);
}

.ticket-closed::before {
    background: linear-gradient(135deg, #48bb78, #38a169);
}

.priority-high {
    background: linear-gradient(135deg, #f56565, #e53e3e);
    color: white;
    padding: 0.25rem 0.75rem;
    border-radius: 20px;
    font-size: 0.75rem;
    font-weight: 600;
}

.priority-medium {
    background: linear-gradient(135deg, #ed8936, #dd6b20);
    color: white;
    padding: 0.25rem 0.75rem;
    border-radius: 20px;
    font-size: 0.75rem;
    font-weight: 600;
}

.priority-low {
    background: linear-gradient(135deg, #4299e1, #3182ce);
    color: white;
    padding: 0.25rem 0.75rem;
    border-radius: 20px;
    font-size: 0.75rem;
    font-weight: 600;
}

.chart-container {
    background: rgba(255, 255, 255, 0.95);
    backdrop-filter: blur(20px);
    padding: 2rem;
    border-radius: 16px;
    margin: 1rem 0;
    box-shadow: 0 8px 32px rgba(0, 0, 0, 0.1);
    border: 1px solid rgba(255, 255, 255, 0.2);
}

.sidebar .sidebar-content {
    background: rgba(255, 255, 255, 0.95);
    backdrop-filter: blur(20px);
}

.stButton > button {
    background: linear-gradient(135deg, #667eea, #764ba2);
    color: white;
    border: none;
    border-radius: 10px;
    padding: 0.75rem 1.5rem;
    font-weight: 600;
    transition: all 0.3s ease;
    box-shadow: 0 4px 15px rgba(0, 0, 0, 0.2);
}

.stButton > button:hover {
    transform: translateY(-2px);
    box-shadow: 0 8px 25px rgba(0, 0, 0, 0.3);
}

.alert-success {
    background: linear-gradient(135deg, #48bb78, #38a169);
    color: white;
    padding: 1rem;
    border-radius: 10px;
    margin: 1rem 0;
    font-weight: 500;
}

.alert-warning {
    background: linear-gradient(135deg, #ed8936, #dd6b20);
    color: white;
    padding: 1rem;
    border-radius: 10px;
    margin: 1rem 0;
    font-weight: 500;
}

.progress-container {
    background: #e2e8f0;
    border-radius: 10px;
    height: 8px;
    overflow: hidden;
    margin: 0.5rem 0;
}

.progress-bar {
    height: 100%;
    background: linear-gradient(135deg, #48bb78, #38a169);
    border-radius: 10px;
    transition: width 0.3s ease;
}

.stats-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 1rem;
    margin: 1rem 0;
}

.filter-section {
    background: rgba(255, 255, 255, 0.95);
    backdrop-filter: blur(20px);
    padding: 1.5rem;
    border-radius: 16px;
    margin-bottom: 1rem;
    box-shadow: 0 8px 32px rgba(0, 0, 0, 0.1);
}

/* Expander styling */
.streamlit-expanderHeader {
    background: linear-gradient(90deg, #1e3c72 0%, #2a5298 100%);
    color: white !important;
    border-radius: 10px !important;
    padding: 1rem !important;
    font-size: 1rem !important;
    font-weight: 600 !important;
    margin-bottom: 0.5rem !important;
    border: none !important;
    box-shadow: 0 4px 15px rgba(0, 0, 0, 0.1);
    transition: all 0.3s ease !important;
}

/* Status-specific expander styles */
.expander-good {
    background: linear-gradient(90deg, #48bb78 0%, #38a169 100%) !important;
}

.expander-warning {
    background: linear-gradient(90deg, #ed8936 0%, #dd6b20 100%) !important;
}

.expander-critical {
    background: linear-gradient(90deg, #f56565 0%, #e53e3e 100%) !important;
}

.streamlit-expanderHeader:hover {
    transform: translateY(-2px);
    box-shadow: 0 8px 25px rgba(0, 0, 0, 0.2);
}

.streamlit-expanderContent {
    background: white;
    border-radius: 10px;
    padding: 1.5rem !important;
    border: 1px solid #e2e8f0;
    margin-top: 0.5rem;
    box-shadow: 0 4px 15px rgba(0, 0, 0, 0.05);
}
</style>
""", unsafe_allow_html=True)

# MongoDB connection with improved error handling
@st.cache_resource
def init_connection():
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        db = client["production_db"]
        db.command("ping")
        return db
    except pymongo.errors.ConnectionError as e:
        st.error(f"🚫 Failed to connect to MongoDB: {str(e)}")
        st.stop()

db = init_connection()

# Time zone
ist = pytz.timezone('Asia/Kolkata')

# Utility functions
def utc_to_ist(utc_dt):
    """Convert UTC datetime to IST"""
    return utc_dt.replace(tzinfo=pytz.utc).astimezone(ist)

def calculate_efficiency(output, target):
    """Calculate efficiency percentage"""
    if target == 0:
        return 0
    return min((output / target) * 100, 100)

def get_status_class(oee, efficiency):
    """Determine status class based on performance"""
    if oee >= 75 and efficiency >= 80:
        return "good"
    elif oee >= 60 and efficiency >= 60:
        return "warning"
    else:
        return "danger"

# Initialize session state
if "auto_refresh" not in st.session_state:
    st.session_state.auto_refresh = False
if "refresh_interval" not in st.session_state:
    st.session_state.refresh_interval = 30

# Header Section
st.markdown("""
<div class="main-header">
    <h1 class="main-title">🏭 Production Line Dashboard</h1>
    <p class="main-subtitle">Real-time monitoring and analytics for production efficiency</p>
</div>
""", unsafe_allow_html=True)

# Sidebar Configuration
with st.sidebar:
    st.markdown("### 🎛️ Dashboard Controls")
    
    # Auto-refresh toggle
    st.session_state.auto_refresh = st.toggle("Auto Refresh", value=st.session_state.auto_refresh)
    if st.session_state.auto_refresh:
        st.session_state.refresh_interval = st.selectbox(
            "Refresh Interval (seconds)", 
            [10, 30, 60, 120], 
            index=1
        )
    
    st.markdown("---")
    
    # Filters
    st.markdown("### 📊 Filters")
    
    # Line selection
    lines = [line["_id"] for line in db.lines.find()] + ["All"]
    selected_line = st.selectbox("🏭 Production Line", lines, index=len(lines)-1)
    
    # Time range
    time_range = st.selectbox(
        "📅 Time Range", 
        ["Last Hour", "Last 4 Hours", "Last 12 Hours", "Last Day", "Last 3 Days", "Last Week", "Custom"],
        index=3
    )
    
    if time_range == "Custom":
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("Start Date")
        with col2:
            end_date = st.date_input("End Date")
        start_time = datetime.combine(start_date, datetime.min.time()).replace(tzinfo=ist)
        end_time = datetime.combine(end_date, datetime.max.time()).replace(tzinfo=ist)
    else:
        end_time = datetime.now(ist).replace(minute=0, second=0, microsecond=0)
        hours_back = {
            "Last Hour": 1, "Last 4 Hours": 4, "Last 12 Hours": 12,
            "Last Day": 24, "Last 3 Days": 72, "Last Week": 168
        }[time_range]
        start_time = end_time - timedelta(hours=hours_back)
    
    # Display options
    st.markdown("### 🎨 Display Options")
    items_per_page = st.selectbox("Items Per Page", [10, 20, 50, 100], index=1)
    show_charts = st.toggle("Show Charts", value=True)
    show_tickets_only = st.toggle("Show Only Lines with Tickets", value=False)
    
    st.markdown("---")
    
    # Last updated
    latest_data = db.hourly_data.find_one(sort=[("timestamp", -1)])
    if latest_data:
        last_updated = utc_to_ist(latest_data['timestamp']).strftime('%Y-%m-%d %H:%M')
        st.markdown(f"**🕒 Last Updated**: {last_updated} IST")

# Search functionality
search_col1, search_col2 = st.columns([3, 1])
with search_col1:
    search_query = st.text_input(
        "🔍 Search", 
        placeholder="Search tickets, descriptions, or line IDs...",
        help="Search across ticket IDs, descriptions, and line identifiers"
    )
with search_col2:
    if st.button("🔄 Refresh", use_container_width=True):
        st.rerun()

# Fetch and process data (shared for both ticket and dashboard views)
query_filter = {"timestamp": {"$gte": start_time.astimezone(pytz.utc), "$lt": end_time.astimezone(pytz.utc)}}
if selected_line != "All":
    query_filter["line_id"] = selected_line

# Get performance data
performance_data = list(db.hourly_data.find(query_filter))
total_items = len(performance_data)
total_pages = (total_items + items_per_page - 1) // items_per_page if total_items > 0 else 1
page = 1  # Default page

# Get summary statistics
total_lines = len(lines) - 1
open_tickets = db.tickets.count_documents({"status": "Open"})
closed_tickets = db.tickets.count_documents({"status": "Closed"})

# Calculate performance metrics
avg_oee = sum(data.get("oee", 0) for data in performance_data) / len(performance_data) if performance_data else 0
lines_with_issues = len(set(data["line_id"] for data in performance_data if data.get("oee", 0) < 60))

# Handle specific ticket view
query_params = st.query_params
ticket_id = query_params.get("ticket_id")

if ticket_id:
    ticket = db.tickets.find_one({"ticket_id": ticket_id})
    if ticket:
        # Ticket detail view
        st.markdown(f"""
        <div class="data-card">
            <div class="line-header">
                <h2>🎫 Ticket Details</h2>
                <span class="status-badge status-{'good' if ticket['status'] == 'Closed' else 'danger'}">
                    {ticket['status']}
                </span>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        col1, col2 = st.columns(2)
        with col1:
            st.markdown(f"**Ticket ID:** {ticket['ticket_id']}")
            st.markdown(f"**Line:** {ticket['line_id']}")
            st.markdown(f"**Issue Type:** {ticket['issue_type']}")
            st.markdown(f"**Priority:** <span class='priority-{ticket['priority'].lower()}'>{ticket['priority']}</span>", unsafe_allow_html=True)
        
        with col2:
            created_time = utc_to_ist(ticket['timestamp']).strftime('%Y-%m-%d %H:%M')
            st.markdown(f"**Created:** {created_time} IST")
            if ticket.get('closed_at'):
                closed_time = utc_to_ist(ticket['closed_at']).strftime('%Y-%m-%d %H:%M')
                st.markdown(f"**Closed:** {closed_time} IST")
        
        st.markdown(f"**Description:** {ticket['description']}")
        
        # Ticket history
        if "history" in ticket and ticket["history"]:
            st.markdown("### 📋 Ticket History")
            for action in reversed(ticket["history"]):
                action_time = utc_to_ist(action['timestamp']).strftime('%Y-%m-%d %H:%M')
                st.markdown(f"- **{action['action']}** at {action_time}: {action.get('reason', 'N/A')}")
        
        # Close ticket form
        if ticket["status"] == "Open":
            with st.form(f"close_ticket_{ticket['_id']}", clear_on_submit=True):
                st.markdown("### ✅ Close Ticket")
                reason = st.text_area("Closure Reason", height=100, placeholder="Provide detailed reason for closure...")
                resolution_type = st.selectbox("Resolution Type", ["Fixed", "Duplicate", "Cannot Reproduce", "Not an Issue", "Other"])
                
                col1, col2 = st.columns(2)
                with col1:
                    if st.form_submit_button("Close Ticket", type="primary", use_container_width=True):
                        if reason.strip():
                            action_id = f"ACTION_{datetime.now(pytz.utc).strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"
                            db.tickets.update_one(
                                {"_id": ticket["_id"]},
                                {
                                    "$set": {
                                        "status": "Closed",
                                        "closed_at": datetime.now(pytz.utc),
                                        "resolution_type": resolution_type
                                    },
                                    "$push": {
                                        "history": {
                                            "action_id": action_id,
                                            "timestamp": datetime.now(pytz.utc),
                                            "action": "Closed",
                                            "reason": reason.strip(),
                                            "resolution_type": resolution_type
                                        }
                                    }
                                }
                            )
                            st.success("✅ Ticket closed successfully!")
                            st.query_params.clear()
                            st.rerun()
                        else:
                            st.error("❌ Reason cannot be empty.")
                
                with col2:
                    if st.form_submit_button("Cancel", use_container_width=True):
                        st.query_params.clear()
                        st.rerun()
    else:
        st.error("❌ Ticket not found.")
        if st.button("← Back to Dashboard"):
            st.query_params.clear()
            st.rerun()

else:
    # Main dashboard view
    
    # Summary Cards
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value metric-info">{total_lines}</div>
            <div class="metric-label">Total Lines</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value metric-danger">{open_tickets}</div>
            <div class="metric-label">Open Tickets</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value metric-good">{closed_tickets}</div>
            <div class="metric-label">Closed Tickets</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value metric-{'good' if avg_oee >= 75 else 'warning' if avg_oee >= 60 else 'danger'}">{avg_oee:.1f}%</div>
        <div class="metric-label">Avg OEE</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col5:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value metric-warning">{lines_with_issues}</div>
            <div class="metric-label">Lines w/ Issues</div>
        </div>
        """, unsafe_allow_html=True)
    
    # Charts Section
    if show_charts and performance_data:
        st.markdown('<div class="chart-container">', unsafe_allow_html=True)
        
        if selected_line != "All":
            # Single line detailed chart
            df_chart = pd.DataFrame([
                {
                    "Timestamp": utc_to_ist(data["timestamp"]),
                    "OEE": data.get("oee", 0),
                    "Output": data.get("output", 0),
                    "Target": data.get("target_uph", 0),
                    "Efficiency": calculate_efficiency(data.get("output", 0), data.get("target_uph", 1))
                }
                for data in sorted(performance_data, key=lambda x: x["timestamp"])
            ])
            
            # Create subplots
            fig = make_subplots(
                rows=2, cols=1,
                subplot_titles=('OEE Trend', 'Output vs Target'),
                vertical_spacing=0.12
            )
            
            # OEE trend
            fig.add_trace(
                go.Scatter(
                    x=df_chart["Timestamp"], 
                    y=df_chart["OEE"],
                    mode='lines+markers',
                    name='OEE %',
                    line=dict(color='#667eea', width=3),
                    marker=dict(size=6)
                ),
                row=1, col=1
            )
            
            # Output vs Target
            fig.add_trace(
                go.Bar(
                    x=df_chart["Timestamp"], 
                    y=df_chart["Output"],
                    name='Actual Output',
                    marker_color='#48bb78',
                    opacity=0.8
                ),
                row=2, col=1
            )
            
            fig.add_trace(
                go.Scatter(
                    x=df_chart["Timestamp"], 
                    y=df_chart["Target"],
                    mode='lines',
                    name='Target',
                    line=dict(color='#f56565', width=2, dash='dash')
                ),
                row=2, col=1
            )
            
            fig.update_layout(
                title=f"Performance Analysis - {selected_line}",
                height=600,
                showlegend=True,
                template='plotly_white'
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
        else:
            # Multi-line overview chart
            line_summary = {}
            for data in performance_data:
                line_id = data["line_id"]
                if line_id not in line_summary:
                    line_summary[line_id] = {"oee": [], "output": [], "target": []}
                line_summary[line_id]["oee"].append(data.get("oee", 0))
                line_summary[line_id]["output"].append(data.get("output", 0))
                line_summary[line_id]["target"].append(data.get("target_uph", 0))
            
            # Average OEE by line
            avg_data = []
            for line_id, metrics in line_summary.items():
                avg_oee = sum(metrics["oee"]) / len(metrics["oee"]) if metrics["oee"] else 0
                avg_output = sum(metrics["output"]) / len(metrics["output"]) if metrics["output"] else 0
                avg_target = sum(metrics["target"]) / len(metrics["target"]) if metrics["target"] else 0
                avg_data.append({
                    "Line": line_id,
                    "Avg OEE": avg_oee,
                    "Avg Output": avg_output,
                    "Avg Target": avg_target,
                    "Efficiency": calculate_efficiency(avg_output, avg_target)
                })
            
            df_summary = pd.DataFrame(avg_data)
            
            col1, col2 = st.columns(2)
            
            with col1:
                fig_oee = px.bar(
                    df_summary, 
                    x="Line", 
                    y="Avg OEE",
                    title="Average OEE by Line",
                    color="Avg OEE",
                    color_continuous_scale="RdYlGn",
                    range_color=[0, 100]
                )
                fig_oee.update_layout(height=400)
                st.plotly_chart(fig_oee, use_container_width=True)
            
            with col2:
                fig_eff = px.scatter(
                    df_summary, 
                    x="Avg Output", 
                    y="Avg Target",
                    size="Efficiency",
                    color="Efficiency",
                    hover_data=["Line"],
                    title="Output vs Target Analysis",
                    color_continuous_scale="RdYlGn"
                )
                fig_eff.update_layout(height=400)
                st.plotly_chart(fig_eff, use_container_width=True)
        
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Data Export
    if performance_data:
        df_export = pd.DataFrame([
            {
                "Line": data["line_id"],
                "Timestamp": utc_to_ist(data["timestamp"]).strftime('%Y-%m-%d %H:%M'),
                "Output": data.get("output", 0),
                "Target": data.get("target_uph", 0),
                "OEE": data.get("oee", 0),
                "Efficiency": calculate_efficiency(data.get("output", 0), data.get("target_uph", 1)),
                "Status": "Achieved" if data.get("oee", 0) >= 60 and calculate_efficiency(data.get("output", 0), data.get("target_uph", 1)) >= 60 else ""
            }
            for data in performance_data
        ])
        
        col1, col2, col3 = st.columns([1, 1, 2])
        with col1:
            csv = df_export.to_csv(index=False)
            st.download_button(
                "📊 Export CSV",
                data=csv,
                file_name=f"production_data_{datetime.now(ist).strftime('%Y%m%d_%H%M')}.csv",
                mime="text/csv",
                use_container_width=True
            )
        
        with col2:
            # Excel export
            buffer = BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                df_export.to_excel(writer, sheet_name='Production Data', index=False)
            
            st.download_button(
                "📈 Export Excel",
                data=buffer.getvalue(),
                file_name=f"production_data_{datetime.now(ist).strftime('%Y%m%d_%H%M')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
    
    # Pagination
    if total_pages > 1:
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            page = st.number_input(
                f"Page (1-{total_pages})", 
                min_value=1, 
                max_value=total_pages, 
                value=1,
                help=f"Showing {total_items} total items"
            )
    
    # Process and display data
    start_idx = (page - 1) * items_per_page
    end_idx = min(start_idx + items_per_page, total_items)
    page_data = performance_data[start_idx:end_idx]
    
    # Group data by line
    line_data = {}
    for data in page_data:
        line_id = data["line_id"]
        if line_id not in line_data:
            line_data[line_id] = []
        line_data[line_id].append(data)
    
    # Filter lines with tickets if requested
    if show_tickets_only:
        lines_with_tickets = set()
        for data in page_data:
            ticket_count = db.tickets.count_documents({"hourly_data_id": data["_id"]})
            if ticket_count > 0:
                lines_with_tickets.add(data["line_id"])
        line_data = {k: v for k, v in line_data.items() if k in lines_with_tickets}
    
    # Display message if no data
    if not line_data:
        st.markdown("""
        <div class="data-card" style="text-align: center; padding: 3rem;">
            <h3>📭 No Data Available</h3>
            <p>No production data found for the selected filters and time range.</p>
            <p>Try adjusting your filters or time range to see more data.</p>
        </div>
        """, unsafe_allow_html=True)
    else:
        # Display data by line
        for line_name, line_hours in sorted(line_data.items()):
            # Calculate line statistics
            line_avg_oee = sum(data.get("oee", 0) for data in line_hours) / len(line_hours)
            line_total_output = sum(data.get("output", 0) for data in line_hours)
            line_total_target = sum(data.get("target_uph", 0) for data in line_hours)
            line_efficiency = calculate_efficiency(line_total_output, line_total_target)
            line_status = get_status_class(line_avg_oee, line_efficiency)
            
            # Line header with statistics
            st.markdown(f"""
            <div class="data-card">
                <div class="line-header">
                    <div>
                        <h2 class="line-title">🏭 {line_name}</h2>
                        <div style="display: flex; gap: 1rem; margin-top: 0.5rem;">
                            <small><strong>Avg OEE:</strong> {line_avg_oee:.1f}%</small>
                            <small><strong>Total Output:</strong> {line_total_output:,.0f}</small>
                            <small><strong>Efficiency:</strong> {line_efficiency:.1f}%</small>
                        </div>
                    </div>
                    <span class="status-badge status-{line_status}">
                        {'🟢 Optimal' if line_status == 'good' else '🟡 Warning' if line_status == 'warning' else '🔴 Critical'}
                    </span>
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            # Individual hour entries
            for data in sorted(line_hours, key=lambda x: x["timestamp"], reverse=True):
                timestamp = utc_to_ist(data["timestamp"])
                output = data.get("output", 0)
                target = data.get("target_uph", 0)
                oee = data.get("oee", 0)
                efficiency = calculate_efficiency(output, target)
                status = get_status_class(oee, efficiency)
                
                # Create expandable hour section
                status_colors = {
                    'good': 'linear-gradient(90deg, #48bb78 0%, #38a169 100%)',
                    'warning': 'linear-gradient(90deg, #ed8936 0%, #dd6b20 100%)',
                    'critical': 'linear-gradient(90deg, #f56565 0%, #e53e3e 100%)'
                }

                st.markdown(f"""
                    <style>
                        [data-testid="stExpander"] {{
                            --expander-bg-color: {status_colors.get(status, status_colors['critical'])} !important;
                        }}
                    </style>
                """, unsafe_allow_html=True)
                
                status_class = {
                    'good': 'expander-good',
                    'warning': 'expander-warning',
                    'critical': 'expander-critical'  # Changed from 'danger' to 'critical' to match get_status_class()
                }.get(status, 'expander-critical')  # Uses 'expander-critical' as default if status doesn't match

                st.markdown(f'<div class="{status_class}">', unsafe_allow_html=True)
                with st.expander(
                    f"Click to View Tickets"
                    f"⏰ {timestamp.strftime('%Y-%m-%d %H:%M')} - "
                    f"{'✅ Targets Met' if status == 'good' else '⚠️ Below Target' if status == 'warning' else '❌ Critical'}"
                ):
                    # Performance metrics
                    col1, col2, col3, col4 = st.columns(4)
                    
                    with col1:
                        st.markdown(f"""
                        <div class="metric-card">
                            <div class="metric-value metric-info">{output:,.0f}</div>
                            <div class="metric-label">Output</div>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    with col2:
                        st.markdown(f"""
                        <div class="metric-card">
                            <div class="metric-value metric-info">{target:,.0f}</div>
                            <div class="metric-label">Target</div>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    with col3:
                        oee_class = "good" if oee >= 75 else "warning" if oee >= 60 else "danger"
                        st.markdown(f"""
                        <div class="metric-card">
                            <div class="metric-value metric-{oee_class}">{oee:.1f}%</div>
                            <div class="metric-label">OEE</div>
                            <div class="progress-container">
                                <div class="progress-bar" style="width: {min(oee, 100)}%"></div>
                            </div>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    with col4:
                        eff_class = "good" if efficiency >= 80 else "warning" if efficiency >= 60 else "danger"
                        st.markdown(f"""
                        <div class="metric-card">
                            <div class="metric-value metric-{eff_class}">{efficiency:.1f}%</div>
                            <div class="metric-label">Efficiency</div>
                            <div class="progress-container">
                                <div class="progress-bar" style="width: {min(efficiency, 100)}%"></div>
                            </div>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    # Additional metrics
                    if data.get("downtime_minutes"):
                        st.markdown(f"**⏱️ Downtime:** {data['downtime_minutes']} minutes")
                    
                    if data.get("quality_rate"):
                        st.markdown(f"**✅ Quality Rate:** {data['quality_rate']:.1f}%")
                    
                    if data.get("availability"):
                        st.markdown(f"**🔄 Availability:** {data['availability']:.1f}%")
                    
                    # Fetch and display tickets
                    ticket_query = {"hourly_data_id": data["_id"]}
                    if search_query:
                        ticket_query["$or"] = [
                            {"ticket_id": {"$regex": search_query, "$options": "i"}},
                            {"description": {"$regex": search_query, "$options": "i"}},
                            {"issue_type": {"$regex": search_query, "$options": "i"}}
                        ]
                    
                    tickets = list(db.tickets.find(ticket_query).sort("timestamp", -1))
                    
                    if tickets:
                        st.markdown(f"### 🎫 Tickets ({len(tickets)})")
                        
                        for ticket in tickets:
                            ticket_class = "ticket-open" if ticket["status"] == "Open" else "ticket-closed"
                            priority_class = f"priority-{ticket['priority'].lower()}"
                            created_time = utc_to_ist(ticket['timestamp']).strftime('%H:%M')
                            
                            # Ticket action buttons
                            col_ticket1, col_ticket2 = st.columns([4, 1])
                            
                            with col_ticket1:
                                st.markdown(f"""
                                <div class="ticket-card {ticket_class}">
                                    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.5rem;">
                                        <strong>🎫 {ticket['issue_type']}</strong>
                                        <span class="{priority_class}">{ticket['priority']}</span>
                                    </div>
                                    <p style="margin: 0.5rem 0; color: #4a5568;">{ticket['description']}</p>
                                    <div style="display: flex; justify-content: space-between; align-items: center; font-size: 0.8rem; color: #6b7280;">
                                        <span>📅 {created_time}</span>
                                        <span>🏷️ {ticket['ticket_id']}</span>
                                        <span>{'🟢 Closed' if ticket['status'] == 'Closed' else '🔴 Open'}</span>
                                    </div>
                                </div>
                                """, unsafe_allow_html=True)
                            
                            with col_ticket2:
                                if st.button("View", key=f"view_{ticket['ticket_id']}", use_container_width=True, help="View ticket details"):
                                    st.query_params["ticket_id"] = ticket['ticket_id']
                                    st.rerun()
                    else:
                        if search_query:
                            st.info(f"🔍 No tickets found matching '{search_query}' for this time period.")
                        else:
                            st.info("✅ No tickets recorded for this time period.")
                    
                    # Quick actions
                    st.markdown("### ⚡ Quick Actions")
                    quick_col1, quick_col2, quick_col3 = st.columns(3)
                    
                    with quick_col1:
                        if st.button(
                            "📝 Create Ticket", 
                            key=f"create_ticket_{data['_id']}", 
                            use_container_width=True
                        ):
                            st.session_state[f"show_ticket_form_{data['_id']}"] = True
                    
                    with quick_col2:
                        if st.button(
                            "📊 Detailed Analytics", 
                            key=f"analytics_{data['_id']}", 
                            use_container_width=True
                        ):
                            st.info("📊 Detailed analytics feature coming soon!")
                    
                    with quick_col3:
                        if st.button(
                            "📋 Export Data", 
                            key=f"export_{data['_id']}", 
                            use_container_width=True
                        ):
                            # Create single hour export
                            single_hour_data = pd.DataFrame([{
                                "Line": data["line_id"],
                                "Timestamp": timestamp.strftime('%Y-%m-%d %H:%M'),
                                "Output": output,
                                "Target": target,
                                "OEE": oee,
                                "Efficiency": efficiency,
                                "Status": "Achieved" if status == 'good' else "Below Target"
                            }])
                            csv_single = single_hour_data.to_csv(index=False)
                            st.download_button(
                                "Download Hour Data",
                                data=csv_single,
                                file_name=f"{line_name}_{timestamp.strftime('%Y%m%d_%H%M')}.csv",
                                mime="text/csv",
                                key=f"download_{data['_id']}"
                            )
                    
                    # Ticket creation form
                    if st.session_state.get(f"show_ticket_form_{data['_id']}", False):
                        with st.form(f"create_ticket_{data['_id']}", clear_on_submit=True):
                            st.markdown("#### 🎫 Create New Ticket")
                            
                            ticket_col1, ticket_col2 = st.columns(2)
                            
                            with ticket_col1:
                                issue_type = st.selectbox(
                                    "Issue Type",
                                    ["Machine Breakdown", "Quality Issue", "Material Shortage", 
                                     "Maintenance Required", "Operator Issue", "Process Deviation", "Other"],
                                    key=f"issue_type_{data['_id']}"
                                )
                                priority = st.selectbox(
                                    "Priority",
                                    ["High", "Medium", "Low"],
                                    key=f"priority_{data['_id']}"
                                )
                            
                            with ticket_col2:
                                operator_name = st.text_input(
                                    "Operator Name",
                                    key=f"operator_{data['_id']}"
                                )
                                estimated_downtime = st.number_input(
                                    "Estimated Downtime (minutes)",
                                    min_value=0,
                                    value=0,
                                    key=f"downtime_{data['_id']}"
                                )
                            
                            description = st.text_area(
                                "Description",
                                placeholder="Provide detailed description of the issue...",
                                height=100,
                                key=f"description_{data['_id']}"
                            )
                            
                            form_col1, form_col2 = st.columns(2)
                            
                            with form_col1:
                                if st.form_submit_button("🎫 Create Ticket", type="primary", use_container_width=True):
                                    if description.strip():
                                        new_ticket_id = f"TKT_{datetime.now(pytz.utc).strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:6].upper()}"
                                        new_ticket = {
                                            "ticket_id": new_ticket_id,
                                            "line_id": data["line_id"],
                                            "hourly_data_id": data["_id"],
                                            "issue_type": issue_type,
                                            "description": description.strip(),
                                            "priority": priority,
                                            "status": "Open",
                                            "timestamp": datetime.now(pytz.utc),
                                            "operator_name": operator_name.strip() if operator_name.strip() else "Unknown",
                                            "estimated_downtime": estimated_downtime,
                                            "history": [{
                                                "action_id": f"CREATE_{datetime.now(pytz.utc).strftime('%Y%m%d_%H%M%S')}",
                                                "timestamp": datetime.now(pytz.utc),
                                                "action": "Created",
                                                "reason": "Initial ticket creation"
                                            }]
                                        }
                                        
                                        db.tickets.insert_one(new_ticket)
                                        st.success(f"✅ Ticket {new_ticket_id} created successfully!")
                                        st.session_state[f"show_ticket_form_{data['_id']}"] = False
                                        st.rerun()
                                    else:
                                        st.error("❌ Description cannot be empty.")
                            
                            with form_col2:
                                if st.form_submit_button("❌ Cancel", use_container_width=True):
                                    st.session_state[f"show_ticket_form_{data['_id']}"] = False
                                    st.rerun()

# Auto-refresh functionality
if st.session_state.auto_refresh:
    import time
    time.sleep(st.session_state.refresh_interval)
    st.rerun()

# Footer
st.markdown("---")
col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("**📊 Dashboard Statistics**")
    st.markdown(f"- Total Records: {total_items:,}" if total_items else "- Total Records: 0")
    st.markdown(f"- Current Page: {page}/{total_pages}" if total_pages > 0 else "- Current Page: 0/0")

with col2:
    st.markdown("**⏰ Time Information**")
    st.markdown(f"- Time Range: {(end_time - start_time).days} days")
    st.markdown(f"- Timezone: Asia/Kolkata (IST)")

with col3:
    st.markdown("**🔄 System Status**")
    st.markdown("- Database: ✅ Connected")
    st.markdown(f"- Auto Refresh: {'✅ On' if st.session_state.auto_refresh else '❌ Off'}")

st.markdown("""
<div style="text-align: center; padding: 2rem; color: #6b7280; font-size: 0.9rem;">
    <p>🏭 Production Line Dashboard | Built with Streamlit & MongoDB</p>
    <p>Last updated: {}</p>
</div>
""".format(datetime.now(ist).strftime('%Y-%m-%d %H:%M:%S IST')), unsafe_allow_html=True)
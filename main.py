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
    page_title="NPM Production Line Dashboard",
    page_icon="🏭",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for enhanced UI/UX
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

* {
    font-family: 'Inter', sans-serif;
    transition: all 0.3s ease;
}

.stApp {
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    min-height: 100vh;
}

.main-header {
    background: rgba(255, 255, 255, 0.95) !important;
    backdrop-filter: blur(20px);
    padding: 2.5rem;
    border-radius: 20px;
    margin: 1rem 0 2rem;
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
    font-size: 1.8rem;
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

.error-table {
    width: 100%;
    border-collapse: collapse;
    margin-top: 1rem;
}

.error-table th, .error-table td {
    padding: 0.75rem;
    text-align: left;
    border: 1px solid #e2e8f0;
    color: #2d3748;
    font-size: 0.9rem;
}

.error-table th {
    background-color: #667eea;
    color: white;
    font-weight: 600;
}

.error-table tr:nth-child(even) {
    background-color: #ffffff;
}

.error-table tr:nth-child(odd) {
    background-color: #edf2f7;
}

.error-table tr:hover {
    background-color: #e2e8f0;
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

.alert-danger {
    background: linear-gradient(135deg, #f56565, #e53e3e);
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
}

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
}

.stExpander > div:nth-child(2) {
    border-radius: 0 0 10px 10px !important;
    padding: 1.5rem !important;
    background: white !important;
    border: 1px solid #e2e8f0 !important;
}

.expander-good { --expander-bg-color: linear-gradient(90deg, #48bb78 0%, #38a169 100%) !important; }
.expander-warning { --expander-bg-color: linear-gradient(90deg, #ed8936 0%, #dd6b20 100%) !important; }
.expander-critical { --expander-bg-color: linear-gradient(90deg, #f56565 0%, #e53e3e 100%) !important; }

/* Dark theme support */
@media (prefers-color-scheme: dark) {
    .stApp {
        background: linear-gradient(135deg, #2d3748 0%, #4a5568 100%);
    }
    .main-header, .metric-card, .data-card, .chart-container, .sidebar .sidebar-content {
        background: rgba(45, 55, 72, 0.95) !important;
        color: #e2e8f0 !important;
        border: 1px solid rgba(255, 255, 255, 0.1);
    }
    .main-title {
        color: #e2e8f0;
        background: linear-gradient(135deg, #63b3ed 0%, #b794f4 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    .main-subtitle, .metric-label, .line-title {
        color: #e2e8f0;
    }
    .progress-container {
        background: #4a5568;
    }
    .ticket-card {
        background: linear-gradient(135deg, #4a5568, #2d3748);
    }
    .data-card {
        border-bottom: 2px solid #4a5568;
    }
    .error-table th, .error-table td {
        color: #e2e8f0;
        border: 1px solid #4a5568;
    }
    .error-table th {
        background-color: #4a5568;
    }
    .error-table tr:nth-child(even) {
        background-color: #2d3748;
    }
    .error-table tr:nth-child(odd) {
        background-color: #4a5568;
    }
    .error-table tr:hover {
        background-color: #718096;
    }
}
</style>
""", unsafe_allow_html=True)

# MongoDB connection
@st.cache_resource
def init_connection():
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        db = client["production_data"]  # Correct database name
        db.command("ping")
        # Verify available collections
        collections = db.list_collection_names()
        #st.info(f"Connected to production_data database. Available collections: {collections}")
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
    try:
        if isinstance(utc_dt, str):
            utc_dt = datetime.strptime(utc_dt, '%Y-%m-%d %H:%M:%S')
        return utc_dt.replace(tzinfo=pytz.utc).astimezone(ist)
    except:
        return utc_dt

def calculate_efficiency(output, target):
    """Calculate efficiency percentage"""
    return min((output / target) * 100, 100) if target > 0 else 0

def get_status_class(performance):
    """Determine status class based on performance"""
    return "good" if performance >= 75 else "warning" if performance >= 60 else "danger"

def display_error_table(errors, title):
    """Display feeder or nozzle errors in a table"""
    if errors and len(errors) > 1:
        df = pd.DataFrame(errors[1:], columns=errors[0])
        st.markdown(f"**{title}**")
        st.markdown(
            df.to_html(classes="error-table", index=False),
            unsafe_allow_html=True
        )
        csv = df.to_csv(index=False)
        st.download_button(
            f"📥 Export {title} CSV",
            data=csv,
            file_name=f"{title}_{datetime.now(ist).strftime('%Y%m%d_%H%M')}.csv",
            mime="text/csv",
            key=f"export_{title}_{uuid.uuid4()}"
        )
    else:
        st.info(f"No {title.lower()} data available.")

# Initialize session state
if "auto_refresh" not in st.session_state:
    st.session_state.auto_refresh = False
if "refresh_interval" not in st.session_state:
    st.session_state.refresh_interval = 30
if "sort_key" not in st.session_state:
    st.session_state.sort_key = "timestamp"
if "sort_order" not in st.session_state:
    st.session_state.sort_order = "desc"

# Header Section
st.markdown("""
<div class="main-header">
    <h1 class="main-title">🏭 NPM Production Line Dashboard</h1>
    <p class="main-subtitle">Real-time monitoring, analytics, and insights for production efficiency</p>
</div>
""", unsafe_allow_html=True)

# Sidebar Configuration
with st.sidebar:
    st.markdown("### 🎛️ Dashboard Controls")
    
    st.session_state.auto_refresh = st.toggle("Auto Refresh", value=st.session_state.auto_refresh)
    if st.session_state.auto_refresh:
        st.session_state.refresh_interval = st.selectbox(
            "Refresh Interval (seconds)", [10, 30, 60, 120], index=1
        )
    
    st.markdown("---")
    st.markdown("### 📊 Filters")
    
    LINES = ['Line_1', 'Line_2', 'Line_3', 'Line_4', 'Line_6', 'Line_7', 'Line_8', 'Line_9', 'Line_11', 'Line_13', 'Line_14', 'Line_16']
    lines = LINES + ["All"]
    selected_line = st.selectbox("🏭 Production Line", lines, index=len(lines)-1)
    
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
        hours_back = {"Last Hour": 1, "Last 4 Hours": 4, "Last 12 Hours": 12, "Last Day": 24, "Last 3 Days": 72, "Last Week": 168}[time_range]
        start_time = end_time - timedelta(hours=hours_back)
    
    st.markdown("### 🎨 Display Options")
    items_per_page = st.selectbox("Items Per Page", [10, 20, 50, 100], index=1)
    show_charts = st.toggle("Show Charts", value=True)
    show_tickets_only = st.toggle("Show Only Lines with Tickets", value=False)
    ticket_priority_filter = st.multiselect("Ticket Priority", ["High", "Medium", "Low"], default=["High", "Medium", "Low"])
    issue_type_filter = st.multiselect(
        "Issue Type", 
        ["Low Performance", "Low Output Ratio", "High Downtime", "Machine Breakdown", "Quality Issue", "Material Shortage", "Maintenance Required", "Operator Issue", "Process Deviation", "Other"],
        default=["Low Performance", "Low Output Ratio", "High Downtime", "Machine Breakdown", "Quality Issue", "Material Shortage", "Maintenance Required", "Operator Issue", "Process Deviation", "Other"]
    )
    
    st.markdown("### 🔍 Sorting")
    sort_key = st.selectbox("Sort By", ["Timestamp", "Performance", "Output", "Downtime"], index=0)
    sort_order = st.selectbox("Sort Order", ["Descending", "Ascending"], index=0)
    st.session_state.sort_key = sort_key.lower()
    st.session_state.sort_order = "desc" if sort_order == "Descending" else "asc"
    
    st.markdown("---")
    latest_data = db[LINES[0]].find_one(sort=[("timestamp", -1)])
    if latest_data:
        last_updated = latest_data['timestamp']
        st.markdown(f"**🕒 Last Updated**: {last_updated} IST")

# Search and Refresh
search_col1, search_col2 = st.columns([3, 1])
with search_col1:
    search_query = st.text_input("🔍 Search", placeholder="Search tickets, descriptions, or line IDs...")
with search_col2:
    if st.button("🔄 Refresh", use_container_width=True):
        st.rerun()

# Debug ticket count
total_tickets = 0

# Fetch data
if selected_line == "All":
    performance_data = []
    for line in LINES:
        collection = db[line]
        query = {"timestamp": {"$gte": start_time.strftime('%Y-%m-%d %H:%M:%S'), "$lt": end_time.strftime('%Y-%m-%d %H:%M:%S')}}
        line_data = list(collection.find(query))
        for data in line_data:
            data["line_name"] = line
        performance_data.extend(line_data)
else:
    collection = db[selected_line]
    query = {"timestamp": {"$gte": start_time.strftime('%Y-%m-%d %H:%M:%S'), "$lt": end_time.strftime('%Y-%m-%d %H:%M:%S')}}
    performance_data = list(collection.find(query))
    for data in performance_data:
        data["line_name"] = selected_line

# Sort data
sort_key_map = {"timestamp": "timestamp", "performance": "performance", "output": "output", "downtime": "down_time"}
sort_field = sort_key_map[st.session_state.sort_key]
performance_data.sort(key=lambda x: x.get(sort_field, 0), reverse=(st.session_state.sort_order == "desc"))

total_items = len(performance_data)
total_pages = (total_items + items_per_page - 1) // items_per_page if total_items > 0 else 1
page = st.number_input(f"Page (1-{total_pages})", min_value=1, max_value=total_pages, value=1, help=f"Showing {total_items} total items") if total_pages > 1 else 1

# Summary statistics
total_lines = len(LINES)
open_tickets = sum(len(doc.get("tickets", [])) for line in LINES for doc in db[line].find({"tickets.status": "Open", "tickets.priority": {"$in": ticket_priority_filter}, "tickets.issue_type": {"$in": issue_type_filter}}))
closed_tickets = sum(len(doc.get("tickets", [])) for line in LINES for doc in db[line].find({"tickets.status": "Closed", "tickets.priority": {"$in": ticket_priority_filter}, "tickets.issue_type": {"$in": issue_type_filter}}))
avg_performance = sum(data.get("performance", 0) for data in performance_data) / len(performance_data) if performance_data else 0
lines_with_issues = len(set(data["line_name"] for data in performance_data if data.get("performance", 0) < 60))

# Alerts for critical issues
high_priority_tickets = sum(len(doc.get("tickets", [])) for line in LINES for doc in db[line].find({"tickets.status": "Open", "tickets.priority": "High"}))
if high_priority_tickets > 0:
    st.markdown(f"""
    <div class="alert-danger">
        ⚠️ {high_priority_tickets} High-Priority Tickets require immediate attention! Check the ticket section below.
    </div>
    """, unsafe_allow_html=True)

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
        <div class="metric-value metric-{'good' if avg_performance >= 75 else 'warning' if avg_performance >= 60 else 'danger'}">{avg_performance:.1f}%</div>
        <div class="metric-label">Avg Performance</div>
    </div>
    """, unsafe_allow_html=True)
with col5:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-value metric-warning">{lines_with_issues}</div>
        <div class="metric-label">Lines w/ Issues</div>
    </div>
    """, unsafe_allow_html=True)

# Ticket Analytics Chart
if show_charts:
    st.markdown('<div class="chart-container">', unsafe_allow_html=True)
    ticket_counts = {
        "Open": open_tickets,
        "Closed": closed_tickets
    }
    df_tickets = pd.DataFrame.from_dict(ticket_counts, orient='index', columns=['Count'])
    fig_tickets = px.pie(df_tickets, values='Count', names=df_tickets.index, title="Ticket Status Distribution", color_discrete_sequence=['#f56565', '#48bb78'])
    fig_tickets.update_layout(height=400, template='plotly_white')
    st.plotly_chart(fig_tickets, use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)

# Performance Charts
if show_charts and performance_data:
    st.markdown('<div class="chart-container">', unsafe_allow_html=True)
    if selected_line != "All":
        df_chart = pd.DataFrame([
            {
                "Timestamp": data["timestamp"],
                "Performance": data.get("performance", 0),
                "Output": data.get("output", 0),
                "Target": data.get("target_uph", 0),
                "Efficiency": calculate_efficiency(data.get("output", 0), data.get("target_uph", 1)),
                "Downtime": data.get("down_time", 0)
            }
            for data in sorted(performance_data, key=lambda x: x["timestamp"])
        ])
        
        fig = make_subplots(
            rows=3, cols=1,
            subplot_titles=('Performance Trend', 'Output vs Target', 'Downtime'),
            vertical_spacing=0.15
        )
        fig.add_trace(go.Scatter(x=df_chart["Timestamp"], y=df_chart["Performance"], mode='lines+markers', name='Performance %', line=dict(color='#667eea', width=3), marker=dict(size=6)), row=1, col=1)
        fig.add_trace(go.Bar(x=df_chart["Timestamp"], y=df_chart["Output"], name='Actual Output', marker_color='#48bb78', opacity=0.8), row=2, col=1)
        fig.add_trace(go.Scatter(x=df_chart["Timestamp"], y=df_chart["Target"], mode='lines', name='Target', line=dict(color='#f56565', width=2, dash='dash')), row=2, col=1)
        fig.add_trace(go.Bar(x=df_chart["Timestamp"], y=df_chart["Downtime"], name='Downtime (min)', marker_color='#ed8936', opacity=0.8), row=3, col=1)
        fig.update_layout(title=f"Performance Analysis - {selected_line}", height=800, showlegend=True, template='plotly_white')
        st.plotly_chart(fig, use_container_width=True)
    else:
        line_summary = {}
        for data in performance_data:
            line_id = data["line_name"]
            if line_id not in line_summary:
                line_summary[line_id] = {"performance": [], "output": [], "target": [], "downtime": []}
            line_summary[line_id]["performance"].append(data.get("performance", 0))
            line_summary[line_id]["output"].append(data.get("output", 0))
            line_summary[line_id]["target"].append(data.get("target_uph", 0))
            line_summary[line_id]["downtime"].append(data.get("down_time", 0))
        
        avg_data = [
            {
                "Line": line_id,
                "Avg Performance": sum(metrics["performance"]) / len(metrics["performance"]) if metrics["performance"] else 0,
                "Avg Output": sum(metrics["output"]) / len(metrics["output"]) if metrics["output"] else 0,
                "Avg Target": sum(metrics["target"]) / len(metrics["target"]) if metrics["target"] else 0,
                "Avg Downtime": sum(metrics["downtime"]) / len(metrics["downtime"]) if metrics["downtime"] else 0,
                "Efficiency": calculate_efficiency(sum(metrics["output"]), sum(metrics["target"]) if sum(metrics["target"]) else 1)
            }
            for line_id, metrics in line_summary.items()
        ]
        df_summary = pd.DataFrame(avg_data)
        
        col1, col2 = st.columns(2)
        with col1:
            fig_performance = px.bar(df_summary, x="Line", y="Avg Performance", title="Average Performance by Line", color="Avg Performance", color_continuous_scale="RdYlGn", range_color=[0, 100])
            fig_performance.update_layout(height=400)
            st.plotly_chart(fig_performance, use_container_width=True)
        with col2:
            fig_eff = px.scatter(df_summary, x="Avg Output", y="Avg Target", size="Efficiency", color="Efficiency", hover_data=["Line"], title="Output vs Target Analysis", color_continuous_scale="RdYlGn")
            fig_eff.update_layout(height=400)
            st.plotly_chart(fig_eff, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

# Data Export
if performance_data:
    df_export = pd.DataFrame([
        {
            "Line": data["line_name"],
            "Timestamp": data["timestamp"],
            "Time Range": data["time_range"],
            "Model": data.get("model", "N/A"),
            "Output": data.get("output", 0),
            "Target": data.get("target_uph", 0),
            "Performance": data.get("performance", 0),
            "Efficiency": calculate_efficiency(data.get("output", 0), data.get("target_uph", 1)),
            "Run Time (min)": data.get("run_time", 0),
            "Downtime (min)": data.get("down_time", 0),
            "Status": "Achieved" if data.get("performance", 0) >= 60 else "Below Target"
        }
        for data in performance_data
    ])
    col1, col2 = st.columns(2)
    with col1:
        csv = df_export.to_csv(index=False)
        st.download_button("📊 Export CSV", data=csv, file_name=f"production_data_{datetime.now(ist).strftime('%Y%m%d_%H%M')}.csv", mime="text/csv", use_container_width=True)
    with col2:
        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            df_export.to_excel(writer, sheet_name='Production Data', index=False)
        st.download_button("📈 Export Excel", data=buffer.getvalue(), file_name=f"production_data_{datetime.now(ist).strftime('%Y%m%d_%H%M')}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)

# Handle ticket view
query_params = st.query_params
ticket_id = query_params.get("ticket_id")

if ticket_id:
    ticket = None
    ticket_line = None
    for line in LINES:
        doc = db[line].find_one({"tickets.ticket_id": ticket_id})
        if doc:
            ticket = next((t for t in doc.get("tickets", []) if t["ticket_id"] == ticket_id), None)
            ticket_line = line
            break
    if ticket:
        st.markdown(f"""
        <div class="data-card">
            <div class="line-header">
                <h2>🎫 Ticket Details - {ticket['ticket_id']}</h2>
                <span class="status-badge status-{'good' if ticket['status'] == 'Closed' else 'danger'}">{ticket['status']}</span>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        col1, col2 = st.columns(2)
        with col1:
            st.markdown(f"**Line:** {ticket['line_name']}")
            st.markdown(f"**Issue Type:** {ticket['issue_type']}")
            st.markdown(f"**Priority:** <span class='priority-{ticket['priority'].lower()}'>{ticket['priority']}</span>", unsafe_allow_html=True)
            st.markdown(f"**Operator:** {ticket.get('operator_name', 'Unknown')}")
        with col2:
            st.markdown(f"**Created:** {ticket['timestamp']}")
            st.markdown(f"**Hour:** {ticket.get('hour', ticket['timestamp'])}")
            if ticket.get('closed_at'):
                st.markdown(f"**Closed:** {ticket['closed_at']}")
            st.markdown(f"**Est. Downtime:** {ticket.get('estimated_downtime', 0)} min")
        
        st.markdown(f"**Description:** {ticket['description']}")
        
        if "history" in ticket and ticket["history"]:
            st.markdown("### 📋 Ticket History")
            for action in reversed(ticket["history"]):
                st.markdown(f"- **{action['action']}** at {action['timestamp']}: {action.get('reason', 'N/A')}")
        
        if ticket.get("root_cause"):
            st.markdown(f"**Root Cause Analysis:** {ticket['root_cause']}")
        
        if ticket["status"] == "Open":
            with st.form(f"close_ticket_{ticket['ticket_id']}", clear_on_submit=True):
                st.markdown("### ✅ Close Ticket")
                root_cause = st.text_area("Root Cause Analysis", height=100, placeholder="Provide detailed root cause analysis for closing the ticket...", key=f"root_cause_{ticket['ticket_id']}")
                resolution_type = st.selectbox("Resolution Type", ["Fixed", "Duplicate", "Cannot Reproduce", "Not an Issue", "Other"], key=f"resolution_{ticket['ticket_id']}")
                
                col1, col2 = st.columns(2)
                with col1:
                    if st.form_submit_button("Close Ticket", type="primary", use_container_width=True):
                        if root_cause.strip():
                            action_id = f"ACTION_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"
                            db[ticket_line].update_one(
                                {"tickets.ticket_id": ticket["ticket_id"]},
                                {
                                    "$set": {
                                        "tickets.$.status": "Closed",
                                        "tickets.$.closed_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                        "tickets.$.resolution_type": resolution_type,
                                        "tickets.$.root_cause": root_cause.strip()
                                    },
                                    "$push": {
                                        "tickets.$.history": {
                                            "action_id": action_id,
                                            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                            "action": "Closed",
                                            "reason": f"Root Cause: {root_cause.strip()}"
                                        }
                                    }
                                }
                            )
                            st.success("✅ Ticket closed successfully!")
                            st.query_params.clear()
                            st.rerun()
                        else:
                            st.error("❌ Root Cause Analysis is mandatory to close the ticket.")
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
    start_idx = (page - 1) * items_per_page
    end_idx = min(start_idx + items_per_page, total_items)
    page_data = performance_data[start_idx:end_idx]
    
    line_data = {}
    for data in page_data:
        line_id = data["line_name"]
        if line_id not in line_data:
            line_data[line_id] = []
        line_data[line_id].append(data)
    
    if show_tickets_only:
        lines_with_tickets = set()
        for line in LINES:
            docs = db[line].find({"tickets": {"$exists": True, "$ne": []}, "tickets.status": "Open", "tickets.priority": {"$in": ticket_priority_filter}, "tickets.issue_type": {"$in": issue_type_filter}})
            for doc in docs:
                lines_with_tickets.add(line)
        line_data = {k: v for k, v in line_data.items() if k in lines_with_tickets}
    
    if not line_data:
        st.markdown("""
        <div class="data-card" style="text-align: center; padding: 3rem;">
            <h3>📭 No Data Available</h3>
            <p>No production data found for the selected filters and time range.</p>
            <p>Try adjusting your filters or time range to see more data.</p>
        </div>
        """, unsafe_allow_html=True)
    else:
        for line_name, line_hours in sorted(line_data.items(), key=lambda x: x[0]):
            line_avg_performance = sum(data.get("performance", 0) for data in line_hours) / len(line_hours)
            line_total_output = sum(data.get("output", 0) for data in line_hours)
            line_total_target = sum(data.get("target_uph", 0) for data in line_hours)
            line_total_downtime = sum(data.get("down_time", 0) for data in line_hours)
            line_efficiency = calculate_efficiency(line_total_output, line_total_target)
            line_status = get_status_class(line_avg_performance)
            
            st.markdown(f"""
            <div class="data-card">
                <div class="line-header">
                    <div>
                        <h2 class="line-title">🏭 {line_name}</h2>
                        <div style="display: flex; gap: 1rem; margin-top: 0.5rem;">
                            <small><strong>Avg Performance:</strong> {line_avg_performance:.1f}%</small>
                            <small><strong>Total Output:</strong> {line_total_output:,.0f}</small>
                            <small><strong>Efficiency:</strong> {line_efficiency:.1f}%</small>
                            <small><strong>Total Downtime:</strong> {line_total_downtime:.1f} min</small>
                        </div>
                    </div>
                    <span class="status-badge status-{line_status}">
                        {'🟢 Optimal' if line_status == 'good' else '🟡 Warning' if line_status == 'warning' else '🔴 Critical'}
                    </span>
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            for data in sorted(line_hours, key=lambda x: x.get(sort_field, 0), reverse=(st.session_state.sort_order == "desc")):
                timestamp = data["timestamp"]
                time_range = data["time_range"]
                output = data.get("output", 0)
                target = data.get("target_uph", 0)
                performance = data.get("performance", 0)
                run_time = data.get("run_time", 0)
                downtime = data.get("down_time", 0)
                efficiency = calculate_efficiency(output, target)
                status = get_status_class(performance)
                
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
                
                with st.expander(f"⏰ {timestamp} | {time_range} - {'✅ Targets Met' if status == 'good' else '⚠️ Below Target' if status == 'warning' else '❌ Critical'}"):
                    col1, col2, col3, col4, col5 = st.columns(5)
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
                        st.markdown(f"""
                        <div class="metric-card">
                            <div class="metric-value metric-{status}">{performance:.1f}%</div>
                            <div class="metric-label">Performance</div>
                            <div class="progress-container"><div class="progress-bar" style="width: {min(performance, 100)}%"></div></div>
                        </div>
                        """, unsafe_allow_html=True)
                    with col4:
                        st.markdown(f"""
                        <div class="metric-card">
                            <div class="metric-value metric-info">{run_time:.1f} min</div>
                            <div class="metric-label">Run Time</div>
                        </div>
                        """, unsafe_allow_html=True)
                    with col5:
                        downtime_class = "danger" if downtime > 1 else "good"
                        st.markdown(f"""
                        <div class="metric-card">
                            <div class="metric-value metric-{downtime_class}">{downtime:.1f} min</div>
                            <div class="metric-label">Downtime</div>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    display_error_table(data.get("feeder_errors", []), f"Top 5 Feeder Errors ({line_name})")
                    display_error_table(data.get("nozzle_errors", []), f"Top 5 Nozzle Errors ({line_name})")
                    
                    ticket_query = {
                        "timestamp": {"$gte": start_time.strftime('%Y-%m-%d %H:%M:%S'), "$lte": end_time.strftime('%Y-%m-%d %H:%M:%S')},
                        "tickets": {"$exists": True, "$ne": []}
                    }
                    tickets = []
                    seen_ticket_ids = set()  # Track unique ticket IDs to prevent duplicates
                    for doc in db[line_name].find(ticket_query):
                        for ticket in doc.get("tickets", []):
                            if (ticket["status"] == "Open" and
                                ticket["priority"] in ticket_priority_filter and
                                ticket["issue_type"] in issue_type_filter and
                                ticket["ticket_id"] not in seen_ticket_ids):
                                if search_query:
                                    if any(search_query.lower() in str(ticket.get(field, "")).lower()
                                        for field in ["ticket_id", "description", "issue_type"]):
                                        tickets.append(ticket)
                                        seen_ticket_ids.add(ticket["ticket_id"])
                                else:
                                    tickets.append(ticket)
                                    seen_ticket_ids.add(ticket["ticket_id"])

                    if tickets:
                        st.markdown(f"### 🎫 Tickets ({len(tickets)})")
                        # Debug: Log ticket IDs to verify uniqueness
                        st.write(f"Debug: Rendering ticket IDs: {[ticket['ticket_id'] for ticket in tickets]}")
                        for idx, ticket in enumerate(tickets):
                        ticket_class = "ticket-open" if ticket["status"] == "Open" else "ticket-closed"
                            priority_class = f"priority-{ticket['priority'].lower()}"
                            col_ticket1, col_ticket2 = st.columns([4, 1])
                            with col_ticket1:
                                st.markdown(f"""
                                <div class="ticket-card {ticket_class}">
                                    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.5rem;">
                                        <strong>🎫 {ticket['issue_type']} ({ticket['line_name']})</strong>
                                        <span class="{priority_class}">{ticket['priority']}</span>
                                    </div>
                                    <p style="margin: 0.5rem 0; color: #4a5568;">{ticket['description']}</p>
                                    <div style="display: flex; justify-content: space-between; align-items: center; font-size: 0.8rem; color: #6b7280;">
                                        <span>📅 {ticket['timestamp']}</span>
                                        <span>⏰ Hour: {ticket.get('hour', ticket['timestamp'])}</span>
                                        <span>🏷️ {ticket['ticket_id']}</span>
                                        <span>{'🟢 Closed' if ticket['status'] == 'Closed' else '🔴 Open'}</span>
                                    </div>
                                </div>
                                """, unsafe_allow_html=True)
                            with col_ticket2:
                                # Create a unique key using ticket_id, line_name, timestamp, and index
                                button_key = f"view_{ticket['ticket_id']}_{ticket['line_name']}_{ticket['timestamp'].replace(':', '-')}_{idx}"
                                if st.button("View", key=button_key, use_container_width=True):
                                    st.query_params["ticket_id"] = ticket['ticket_id']
                                    st.rerun()
                    else:
                        st.info(f"✅ No tickets found for {line_name} in this time range with the current filters.")
                        st.markdown(f"**Debug Info:** Searched for tickets in {line_name} with time range={start_time.strftime('%Y-%m-%d %H:%M:%S')} to {end_time.strftime('%Y-%m-%d %H:%M:%S')}, priority={ticket_priority_filter}, issue_type={issue_type_filter}")
                    
                    st.markdown("### ⚡ Quick Actions")
                    quick_col1, quick_col2, quick_col3 = st.columns(3)
                    with quick_col1:
                        if st.button("📝 Create Ticket", key=f"create_ticket_{data['_id']}", use_container_width=True):
                            st.session_state[f"show_ticket_form_{data['_id']}"] = True
                    with quick_col2:
                        if st.button("📊 Detailed Analytics", key=f"analytics_{data['_id']}", use_container_width=True):
                            st.info("📊 Detailed analytics feature coming soon!")
                    with quick_col3:
                        if st.button("📋 Export Data", key=f"export_{data['_id']}", use_container_width=True):
                            single_hour_data = pd.DataFrame([{
                                "Line": data["line_name"],
                                "Timestamp": timestamp,
                                "Time Range": time_range,
                                "Model": data.get("model", "N/A"),
                                "Output": output,
                                "Target": target,
                                "Performance": performance,
                                "Efficiency": efficiency,
                                "Run Time (min)": run_time,
                                "Downtime (min)": downtime
                            }])
                            csv_single = single_hour_data.to_csv(index=False)
                            st.download_button("Download Hour Data", data=csv_single, file_name=f"{line_name}_{timestamp}.csv", mime="text/csv", key=f"download_{data['_id']}")
                    
                    if st.session_state.get(f"show_ticket_form_{data['_id']}", False):
                        with st.form(f"create_ticket_{data['_id']}", clear_on_submit=True):
                            st.markdown("#### 🎫 Create New Ticket")
                            ticket_col1, ticket_col2 = st.columns(2)
                            with ticket_col1:
                                issue_type = st.selectbox("Issue Type", ["Low Performance", "Low Output Ratio", "High Downtime", "Machine Breakdown", "Quality Issue", "Material Shortage", "Maintenance Required", "Operator Issue", "Process Deviation", "Other"], key=f"issue_type_{data['_id']}")
                                priority = st.selectbox("Priority", ["High", "Medium", "Low"], key=f"priority_{data['_id']}")
                            with ticket_col2:
                                operator_name = st.text_input("Operator Name", key=f"operator_{data['_id']}")
                                estimated_downtime = st.number_input("Estimated Downtime (minutes)", min_value=0, value=0, key=f"downtime_{data['_id']}")
                            description = st.text_area("Description", placeholder="Provide detailed description of the issue...", height=100, key=f"description_{data['_id']}")
                            hour = st.text_input("Hour", value=timestamp, key=f"hour_{data['_id']}")
                            form_col1, form_col2 = st.columns(2)
                            with form_col1:
                                if st.form_submit_button("🎫 Create Ticket", type="primary", use_container_width=True):
                                    if description.strip():
                                        new_ticket_id = f"TKT_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:6].upper()}"
                                        new_ticket = {
                                            "ticket_id": new_ticket_id,
                                            "line_name": data["line_name"],
                                            "performance_data_id": data["_id"],
                                            "issue_type": issue_type,
                                            "description": description.strip(),
                                            "priority": priority,
                                            "status": "Open",
                                            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                            "hour": hour,
                                            "operator_name": operator_name.strip() if operator_name.strip() else "Unknown",
                                            "estimated_downtime": estimated_downtime,
                                            "history": [{
                                                "action_id": f"CREATE_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                                                "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                "action": "Created",
                                                "reason": "Initial ticket creation"
                                            }],
                                            "metrics": {
                                                "downtime": data.get("down_time", 0),
                                                "output": data.get("output", 0),
                                                "target": data.get("target_uph", 0),
                                                "performance": data.get("performance", 0)
                                            }
                                        }
                                        db[data["line_name"]].update_one(
                                            {"_id": data["_id"]},
                                            {"$push": {"tickets": new_ticket}}
                                        )
                                        st.success(f"✅ Ticket {new_ticket_id} created successfully!")
                                        st.session_state[f"show_ticket_form_{data['_id']}"] = False
                                        st.rerun()
                                    else:
                                        st.error("❌ Description cannot be empty.")
                            with form_col2:
                                if st.form_submit_button("❌ Cancel", use_container_width=True):
                                    st.session_state[f"show_ticket_form_{data['_id']}"] = False
                                    st.rerun()

# Auto-refresh
if st.session_state.auto_refresh:
    import time
    time.sleep(st.session_state.refresh_interval)
    st.rerun()

# Footer
st.markdown("---")
col1, col2, col3 = st.columns(3)
with col1:
    st.markdown("**📊 Dashboard Statistics**")
    st.markdown(f"- Total Records: {total_items:,}")
    st.markdown(f"- Current Page: {page}/{total_pages}")
with col2:
    st.markdown("**⏰ Time Information**")
    st.markdown(f"- Time Range: {start_time.strftime('%Y-%m-%d %H:%M')} to {end_time.strftime('%Y-%m-%d %H:%M')} IST")
    st.markdown(f"- Timezone: Asia/Kolkata (IST)")
with col3:
    st.markdown("**🔄 System Status**")
    st.markdown("- Database: ✅ Connected")
    st.markdown(f"- Auto Refresh: {'✅ On' if st.session_state.auto_refresh else '❌ Off'}")

st.markdown("""
<div style="text-align: center; padding: 2rem; color: #6b7280; font-size: 0.9rem;">
    <p>🏭 NPM Production Line Dashboard | Built with Streamlit & MongoDB</p>
    <p>Last updated: {}</p>
</div>
""".format(datetime.now(ist).strftime('%Y-%m-%d %H:%M:%S IST')), unsafe_allow_html=True)
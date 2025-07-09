import pymongo
from pymongo import MongoClient
from datetime import datetime, timedelta
import pytz
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import logging

# MongoDB connection
MONGO_URI = "mongodb+srv://maheshkumar17032k3:kooK8I6bgdRUcUx9@npmticket.knzwnmz.mongodb.net/?retryWrites=true&w=majority&appName=npmticket"
DB_NAME = "production_data"

# Email configuration
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_USERNAME = "engg.datanalytics.padget@dixoninfo.com"
SMTP_PASSWORD = "fewd ihjy vqjh xavy"
FROM_EMAIL = "engg.datanalytics.padget@dixoninfo.com"
TO_EMAILS = [
    "smtmis.padget68@dixoninfo.com",
    "ppc.padget68@dixoninfo.com",
    "karuna.padget@dixoninfo.com",
    "ie.padget68@dixoninfo.com",
    "rahul.padget68@dixoninfo.com",
    "rajeev.padget@dixoninfo.com",
    "praveen.padget68@dixoninfo.com",
    "deepakgaur.padget@dixoninfo.com",
    "deepak.s@dixoninfo.com",
    "sangeetg.padget@dixoninfo.com",
    "raushansingh.padget@dixoninfo.com",
    "pavann.padget@dixoninfo.com",
    "smtkitting.padget68@dixoninfo.com",
    "bharatdash.padget@dixoninfo.com",
    "rajgopalr.padget@dixoninfo.com",
    "harishk.padget@dixoninfo.com",
    "themahiece@gmail.com",
]


# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Line names to query from MongoDB (consistent with npmtomongo.py)
LINES = [
    'Line_1', 'Line_2', 'Line_3', 'Line_4', 'Line_6', 'Line_7', 'Line_8',
    'Line_9', 'Line_11', 'Line_13', 'Line_14', 'Line_16'
]

def send_production_summary_email():
    """Send a professional email with line-wise production data and tickets for the last hour."""
    try:
        # Connect to MongoDB
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')  # Test connection
        db = client[DB_NAME]
        logging.info("Connected to MongoDB successfully")
    except Exception as e:
        logging.error(f"Failed to connect to MongoDB: {str(e)}")
        return

    # Define time range (last hour in Asia/Kolkata timezone)
    now = datetime.now(pytz.timezone('Asia/Kolkata'))
    one_hour_ago = now - timedelta(hours=1)
    time_range = f"{one_hour_ago.strftime('%Y-%m-%d %H:%M:%S')} to {now.strftime('%Y-%m-%d %H:%M:%S')} (IST)"

    # Initialize data structures
    line_data = []
    all_tickets = []

    # Query data for each line
    for line_name in LINES:
        try:
            collection = db[line_name]
            # Query the most recent document within the last hour
            doc = collection.find_one(
                {"timestamp": {"$gte": one_hour_ago.strftime('%Y-%m-%d %H:%M:%S'), "$lte": now.strftime('%Y-%m-%d %H:%M:%S')}},
                sort=[("timestamp", pymongo.DESCENDING)]
            )
            if doc:
                line_data.append({
                    'line_name': line_name,
                    'time_range': doc.get('time_range', 'N/A'),
                    'model': doc.get('model', 'N/A'),
                    'target_uph': doc.get('target_uph', 0),
                    'output': doc.get('output', 0),
                    'performance': doc.get('performance', 0),
                    'run_time': doc.get('run_time', 0),
                    'down_time': doc.get('down_time', 0),
                    'feeder_errors': doc.get('feeder_errors', []),
                    'nozzle_errors': doc.get('nozzle_errors', []),
                    'tickets': doc.get('tickets', [])
                })
                all_tickets.extend(doc.get('tickets', []))
            else:
                logging.warning(f"No data found for {line_name} in the last hour")
                line_data.append({
                    'line_name': line_name,
                    'time_range': 'N/A',
                    'model': 'N/A',
                    'target_uph': 0,
                    'output': 0,
                    'performance': 0,
                    'run_time': 0,
                    'down_time': 0,
                    'feeder_errors': [],
                    'nozzle_errors': [],
                    'tickets': []
                })
        except Exception as e:
            logging.error(f"Error querying data for {line_name}: {str(e)}")
            line_data.append({
                'line_name': line_name,
                'time_range': 'N/A',
                'model': 'N/A',
                'target_uph': 0,
                'output': 0,
                'performance': 0,
                'run_time': 0,
                'down_time': 0,
                'feeder_errors': [],
                'nozzle_errors': [],
                'tickets': []
            })

    # Generate HTML for production summary table
    production_columns = [
        "Line No", "Model", "Target UPH", "Output", "Performance", "Run Time (min)", "Downtime (min)"
    ]
    production_table_html = """
    <table style='border-collapse: collapse; width: 100%; font-family: Arial, sans-serif; margin: 20px 0;'>
        <thead>
            <tr style='background: linear-gradient(90deg, #2b6cb0, #2c5282); color: #ffffff; text-align: left;'>
    """
    for col in production_columns:
        production_table_html += f"<th style='padding: 12px 15px; border: 1px solid #e2e8f0; font-weight: 600;'>{col}</th>"
    production_table_html += "</tr></thead><tbody>"

    for i, data in enumerate(line_data):
        bg_color = "#f7fafc" if i % 2 == 0 else "#ffffff"
        production_table_html += f"<tr style='background-color: {bg_color}; transition: background-color 0.3s;'>"
        production_table_html += f"<td style='padding: 10px 15px; border: 1px solid #e2e8f0;'>{data['line_name']}</td>"
        production_table_html += f"<td style='padding: 10px 15px; border: 1px solid #e2e8f0;'>{data['model']}</td>"
        production_table_html += f"<td style='padding: 10px 15px; border: 1px solid #e2e8f0;'>{data['target_uph']:.2f}</td>"
        production_table_html += f"<td style='padding: 10px 15px; border: 1px solid #e2e8f0;'>{data['output']}</td>"
        performance_style = "color: #e53e3e;" if data['performance'] < 60 else "color: #2b6cb0;"
        production_table_html += f"<td style='padding: 10px 15px; border: 1px solid #e2e8f0; {performance_style}'>{data['performance']:.2f}%</td>"
        production_table_html += f"<td style='padding: 10px 15px; border: 1px solid #e2e8f0;'>{data['run_time']:.2f}</td>"
        downtime_style = "color: #e53e3e;" if data['down_time'] > 1 else "color: #2b6cb0;"
        production_table_html += f"<td style='padding: 10px 15px; border: 1px solid #e2e8f0; {downtime_style}'>{data['down_time']:.2f}</td>"
        production_table_html += "</tr>"
    production_table_html += "</tbody></table>"

    # Generate HTML for tickets table
    ticket_columns = [
        "Line No", "Issue Type", "Description", "Priority", "Ticket Link"
    ]
    ticket_table_html = """
    <table style='border-collapse: collapse; width: 100%; font-family: Arial, sans-serif; margin: 20px 0;'>
        <thead>
            <tr style='background: linear-gradient(90deg, #2b6cb0, #2c5282); color: #ffffff; text-align: left;'>
    """
    for col in ticket_columns:
        ticket_table_html += f"<th style='padding: 12px 15px; border: 1px solid #e2e8f0; font-weight: 600;'>{col}</th>"
    ticket_table_html += "</tr></thead><tbody>"

    for i, ticket in enumerate(all_tickets):
        bg_color = "#f7fafc" if i % 2 == 0 else "#ffffff"
        priority_style = "color: #e53e3e;" if ticket['priority'] == "High" else "color: #2b6cb0;"
        ticket_link = f"https://npm-tickets.streamlit.app/?ticket_id={ticket['ticket_id']}"
        ticket_table_html += f"<tr style='background-color: {bg_color}; transition: background-color 0.3s;'>"
        ticket_table_html += f"<td style='padding: 10px 15px; border: 1px solid #e2e8f0;'>{ticket['line_name']}</td>"
        ticket_table_html += f"<td style='padding: 10px 15px; border: 1px solid #e2e8f0;'>{ticket['issue_type']}</td>"
        ticket_table_html += f"<td style='padding: 10px 15px; border: 1px solid #e2e8f0;'>{ticket['description']}</td>"
        ticket_table_html += f"<td style='padding: 10px 15px; border: 1px solid #e2e8f0; {priority_style}'>{ticket['priority']}</td>"
        ticket_table_html += f"<td style='padding: 10px 15px; border: 1px solid #e2e8f0;'><a href='{ticket_link}' style='color: #2b6cb0; text-decoration: none; font-weight: 500;'>View Ticket</a></td>"
        ticket_table_html += "</tr>"
    ticket_table_html += "</tbody></table>"

    # Generate HTML for feeder and nozzle errors
    feeder_nozzle_html = ""
    for data in line_data:
        feeder_nozzle_html += f"<h3 style='color: #2b6cb0; margin-top: 20px;'>{data['line_name']} - Feeder and Nozzle Errors</h3>"
        
        # Feeder errors table
        feeder_nozzle_html += f"<h4 style='color: #2d3748; margin: 10px 0;'>Top 5 Feeder Errors (Table {data['line_name']})</h4>"
        feeder_nozzle_html += """
        <table style='border-collapse: collapse; width: 100%; font-family: Arial, sans-serif; margin: 10px 0;'>
            <thead>
                <tr style='background: linear-gradient(90deg, #2b6cb0, #2c5282); color: #ffffff; text-align: left;'>
        """
        feeder_columns = data['feeder_errors'][0] if data['feeder_errors'] else []
        for col in feeder_columns:
            feeder_nozzle_html += f"<th style='padding: 12px 15px; border: 1px solid #e2e8f0; font-weight: 600;'>{col}</th>"
        feeder_nozzle_html += "</tr></thead><tbody>"
        for i, row in enumerate(data['feeder_errors'][1:] if data['feeder_errors'] else []):
            bg_color = "#f7fafc" if i % 2 == 0 else "#ffffff"
            feeder_nozzle_html += f"<tr style='background-color: {bg_color};'>"
            for cell in row:
                feeder_nozzle_html += f"<td style='padding: 10px 15px; border: 1px solid #e2e8f0;'>{cell}</td>"
            feeder_nozzle_html += "</tr>"
        feeder_nozzle_html += "</tbody></table>"
        if not data['feeder_errors']:
            feeder_nozzle_html += "<p style='color: #718096; margin: 10px 0;'>No feeder data or 'Error Count' column found.</p>"

        # Nozzle errors table
        feeder_nozzle_html += f"<h4 style='color: #2d3748; margin: 10px 0;'>Top 5 Nozzle Errors (Table {data['line_name']})</h4>"
        feeder_nozzle_html += """
        <table style='border-collapse: collapse; width: 100%; font-family: Arial, sans-serif; margin: 10px 0;'>
            <thead>
                <tr style='background: linear-gradient(90deg, #2b6cb0, #2c5282); color: #ffffff; text-align: left;'>
        """
        nozzle_columns = data['nozzle_errors'][0] if data['nozzle_errors'] else []
        for col in nozzle_columns:
            feeder_nozzle_html += f"<th style='padding: 12px 15px; border: 1px solid #e2e8f0; font-weight: 600;'>{col}</th>"
        feeder_nozzle_html += "</tr></thead><tbody>"
        for i, row in enumerate(data['nozzle_errors'][1:] if data['nozzle_errors'] else []):
            bg_color = "#f7fafc" if i % 2 == 0 else "#ffffff"
            feeder_nozzle_html += f"<tr style='background-color: {bg_color};'>"
            for cell in row:
                feeder_nozzle_html += f"<td style='padding: 10px 15px; border: 1px solid #e2e8f0;'>{cell}</td>"
            feeder_nozzle_html += "</tr>"
        feeder_nozzle_html += "</tbody></table>"
        if not data['nozzle_errors']:
            feeder_nozzle_html += "<p style='color: #718096; margin: 10px 0;'>No nozzle data or 'Error Count' column found.</p>"

    # Compose email body
    body = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <style>
            body {{
                font-family: Arial, sans-serif;
                line-height: 1.6;
                color: #2d3748;
                max-width: 900px;
                margin: 0 auto;
                padding: 20px;
                background-color: #f7fafc;
            }}
            .header {{
                background: linear-gradient(90deg, #2b6cb0, #2c5282);
                color: white;
                padding: 20px;
                border-radius: 8px 8px 0 0;
                text-align: center;
            }}
            .content {{
                background: white;
                padding: 20px;
                border-radius: 0 0 8px 8px;
                box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            }}
            .footer {{
                text-align: center;
                margin-top: 20px;
                color: #718096;
                font-size: 0.9em;
            }}
            h2 {{
                color: #2b6cb0;
                margin-bottom: 15px;
            }}
            h3 {{
                color: #2b6cb0;
                margin: 15px 0 10px;
            }}
            p {{
                margin: 10px 0;
            }}
            a:hover {{
                text-decoration: underline;
            }}
            table tr:hover {{
                background-color: #edf2f7;
            }}
        </style>
    </head>
    <body>
        <div class="header">
            <h2>NPM Production Line Report</h2>
        </div>
        <div class="content">
            <p><strong>Time Range (IST):</strong> {time_range}</p>
            <p>Dear Team,</p>
            <p>This email provides a comprehensive summary of the production performance for the latest cycle, including key metrics, top feeder and nozzle errors, and any generated tickets for issues detected. Please review the details below and take necessary actions via the provided ticket links.</p>
            
            <h3>Production Summary</h3>
            {production_table_html}
            
            <h3>Ticket Summary</h3>
            {ticket_table_html if all_tickets else "<p>No tickets generated for this cycle.</p>"}
            
            {feeder_nozzle_html}
            
            <p>For detailed analysis, please access the <a href='https://npm-tickets.streamlit.app/' style='color: #2b6cb0; text-decoration: none; font-weight: 500;'>Streamlit Dashboard</a>.</p>
            <p>Best regards,<br>Data Analytics Team<br>Padget Electronics</p>
        </div>
        <div class="footer">
            <p>Generated automatically by NPM Monitoring System | {now.strftime('%Y-%m-%d %H:%M:%S')} IST</p>
        </div>
    </body>
    </html>
    """

    # Set up email
    msg = MIMEMultipart()
    msg["From"] = FROM_EMAIL
    msg["To"] = ", ".join(TO_EMAILS)
    msg["Subject"] = f"NPM Production Line Report - {now.strftime('%Y-%m-%d %H:%M')} IST"

    msg.attach(MIMEText(body, "html"))

    # Send email
    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USERNAME, SMTP_PASSWORD)
            server.sendmail(FROM_EMAIL, TO_EMAILS, msg.as_string())
        logging.info("Email sent successfully")
        print("Email sent successfully")
    except Exception as e:
        logging.error(f"Failed to send email: {str(e)}")
        print(f"Failed to send email: {str(e)}")
    finally:
        client.close()
        logging.info("MongoDB connection closed")

if __name__ == "__main__":
    send_production_summary_email()
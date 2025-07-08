import pymongo
from pymongo import MongoClient
from datetime import datetime, timedelta
import pytz
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# MongoDB connection
MONGO_URI = "mongodb+srv://maheshkumar17032k3:kooK8I6bgdRUcUx9@npmticket.knzwnmz.mongodb.net/?retryWrites=true&w=majority&appName=npmticket"
client = MongoClient(MONGO_URI)
db = client["production_db"]

# Email configuration (replace with your actual SMTP details)
SMTP_SERVER = "smtp.gmail.com"  # Example: Gmail SMTP server
SMTP_PORT = 587
SMTP_USERNAME = "engg.datanalytics.padget@dixoninfo.com"  # Your email
SMTP_PASSWORD = "fewd ihjy vqjh xavy"  # Your app-specific password
FROM_EMAIL = "engg.datanalytics.padget@dixoninfo.com"
TO_EMAILS = ["rajgopalr.padget@dixoninfo.com", "harishk.padget@dixoninfo.com","themahiece@gmail.com","jagdeesh.raj@dixoninfo.com"]  # List of recipients

def send_ticket_summary_email():
    """Send a professional email with a summary of tickets generated in the last hour."""
    # Define time range (last hour)
    now = datetime.now(pytz.utc)
    one_hour_ago = now - timedelta(hours=1)
    
    # Query tickets from the last hour
    tickets = list(db.tickets.find({"timestamp": {"$gte": one_hour_ago, "$lt": now}}))
    
    if not tickets:
        print("No tickets to report for this cycle.")
        return
    
    # Define table columns
    columns = [
        "Line No", "OEE", "Output", "Target", "Performance", "Quality", 
        "Downtime", "Description", "Issue Type", "Ticket Link"
    ]
    
    # Generate HTML table with professional styling
    table_html = """
    <table style='border-collapse: collapse; width: 100%; font-family: Arial, sans-serif; margin: 20px 0;'>
        <thead>
            <tr style='background: linear-gradient(90deg, #2b6cb0, #2c5282); color: #ffffff; text-align: left;'>
    """
    for col in columns:
        table_html += f"<th style='padding: 12px 15px; border: 1px solid #e2e8f0; font-weight: 600;'>{col}</th>"
    table_html += "</tr></thead><tbody>"
    
    for i, ticket in enumerate(tickets):
        bg_color = "#f7fafc" if i % 2 == 0 else "#ffffff"  # Alternating row colors
        line_no = ticket.get("line_id", "N/A")
        oee = f"{ticket.get('oee', 0):.2f}%"
        output = ticket.get("output", 0)
        target = ticket.get("target", 0)
        performance = f"{ticket.get('performance', 0):.2f}%"
        quality = f"{ticket.get('quality', 0):.2f}%"
        downtime = f"{ticket.get('downtime', 0):.2f} min"
        description = ticket.get("description", "N/A")
        issue_type = ticket.get("issue_type", "N/A")
        ticket_id = ticket.get("ticket_id", "N/A")
        ticket_link = f"https://npm-tickets.streamlit.app/?ticket_id={ticket_id}"
        
        table_html += f"<tr style='background-color: {bg_color}; transition: background-color 0.3s;'>"
        table_html += f"<td style='padding: 10px 15px; border: 1px solid #e2e8f0;'>{line_no}</td>"
        table_html += f"<td style='padding: 10px 15px; border: 1px solid #e2e8f0;'>{oee}</td>"
        table_html += f"<td style='padding: 10px 15px; border: 1px solid #e2e8f0;'>{output}</td>"
        table_html += f"<td style='padding: 10px 15px; border: 1px solid #e2e8f0;'>{target}</td>"
        table_html += f"<td style='padding: 10px 15px; border: 1px solid #e2e8f0;'>{performance}</td>"
        table_html += f"<td style='padding: 10px 15px; border: 1px solid #e2e8f0;'>{quality}</td>"
        table_html += f"<td style='padding: 10px 15px; border: 1px solid #e2e8f0;'>{downtime}</td>"
        table_html += f"<td style='padding: 10px 15px; border: 1px solid #e2e8f0;'>{description}</td>"
        table_html += f"<td style='padding: 10px 15px; border: 1px solid #e2e8f0;'>{issue_type}</td>"
        table_html += f"<td style='padding: 10px 15px; border: 1px solid #e2e8f0;'><a href='{ticket_link}' style='color: #2b6cb0; text-decoration: none; font-weight: 500;'>View Ticket</a></td>"
        table_html += "</tr>"
    table_html += "</tbody></table>"
    
    # Compose email body with professional styling
    time_range = f"{one_hour_ago.strftime('%Y-%m-%d %H:%M:%S')} to {now.strftime('%Y-%m-%d %H:%M:%S')} (UTC)"
    body = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <style>
            body {{
                font-family: Arial, sans-serif;
                line-height: 1.6;
                color: #2d3748;
                max-width: 800px;
                margin: 0 auto;
                padding: 20px;
                background-color: #f7fafc;
            }}
            .header {{
                background: linear-gradient(90deg, #2b6cb0, #2c5282);
                color: white;
                padding: 15px 20px;
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
            p {{
                margin: 10px 0;
            }}
            a:hover {{
                text-decoration: underline;
            }}
        </style>
    </head>
    <body>
        <div class="header">
            <h2>Production Line Ticket Summary</h2>
        </div>
        <div class="content">
            <p><strong>Time Range (UTC):</strong> {time_range}</p>
            <p>Dear Team,</p>
            <p>Below is the summary of tickets generated during the latest production cycle:</p>
            {table_html}
            <p>Please review the tickets and take necessary actions. Click on the "View Ticket" links to access detailed information in the Streamlit application.</p>
            <p>Best regards,<br>Data Analytics </p>
        </div>
        <div class="footer">
            <p>Generated automatically by NPM Monitoring System | {now.strftime('%Y-%m-%d %H:%M:%S')} UTC</p>
        </div>
    </body>
    </html>
    """
    
    # Set up email
    msg = MIMEMultipart()
    msg["From"] = FROM_EMAIL
    msg["To"] = ", ".join(TO_EMAILS)
    msg["Subject"] = "Production Line Ticket Summary - Latest Cycle"
    
    msg.attach(MIMEText(body, "html"))
    
    # Send email
    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USERNAME, SMTP_PASSWORD)
            server.sendmail(FROM_EMAIL, TO_EMAILS, msg.as_string())
        print("Email sent successfully.")
    except Exception as e:
        print(f"Failed to send email: {str(e)}")

if __name__ == "__main__":
    send_ticket_summary_email()
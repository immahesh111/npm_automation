import time
from datetime import datetime
import pytz
import logging
from apscheduler.schedulers.background import BackgroundScheduler
from npmmongo import run_data_collection
from npmmail import send_production_summary_email

# Set up logging to both console and file
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('orchestrator.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def job():
    """Run data collection and email sending sequentially."""
    try:
        logger.info(f"Starting data collection at {datetime.now(pytz.timezone('Asia/Kolkata'))}")
        results = run_data_collection()
        logger.info(f"Data collection completed with {len(results)} results")
        logger.info(f"Sending email at {datetime.now(pytz.timezone('Asia/Kolkata'))}")
        email_success = send_production_summary_email()
        if email_success:
            logger.info("Email sent successfully")
        else:
            logger.error("Email sending failed")
    except Exception as e:
        logger.error(f"Error in job execution: {str(e)}", exc_info=True)

def main():
    """Set up and start the scheduler."""
    scheduler = BackgroundScheduler(timezone="Asia/Kolkata")
    scheduler.add_job(job, 'interval', hours=1, next_run_time=datetime.now(pytz.timezone('Asia/Kolkata')))
    logger.info("Scheduler started")
    scheduler.start()
    try:
        while True:
            time.sleep(60)  # Keep the script running
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logger.info("Scheduler stopped")

if __name__ == "__main__":
    main()
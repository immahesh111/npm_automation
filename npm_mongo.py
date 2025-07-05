import pymongo
from pymongo import MongoClient
import uuid
from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.edge.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
from datetime import datetime, timedelta
import re
import pytz
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# MongoDB connection
MONGO_URI = "mongodb+srv://maheshkumar17032k3:kooK8I6bgdRUcUx9@npmticket.knzwnmz.mongodb.net/?retryWrites=true&w=majority&appName=npmticket"  # Replace with your MongoDB Cloud URL
client = MongoClient(MONGO_URI)
db = client["production_db"]

# Ticketing thresholds
TICKETING_THRESHOLDS = {
    'min_oee': 60,              # OEE < 60% triggers a ticket
    'min_output_ratio': 0.4     # Output/Target < 40% triggers a ticket
}

# Line configuration for all lines
LINES = {
    'Line_1': {
        'web_ip': 'https://192.168.10.31:9443/lws/LwsInitialize',
        'database': 'Line_1_datas',
        'line_number': 1,
        'machine_tables': list(range(4, 17)),
        'feeder_table': 21,
        'nozzle_table': 24,
        'model_xpath': '/html/body/table[20]/tbody/tr[2]/td[6]',
        'output_xpath': '/html/body/table[4]/tbody/tr[162]/td[3]',
        'num_machines': 12
    },
    'Line_2': {
        'web_ip': 'https://192.168.10.41:9443/lws/LwsInitialize',
        'database': 'Line_2_datas',
        'line_number': 2,
        'machine_tables': list(range(4, 14)),
        'feeder_table': 17,
        'nozzle_table': 20,
        'model_xpath': '/html/body/table[16]/tbody/tr[2]/td[6]',
        'output_xpath': '/html/body/table[4]/tbody/tr[98]/td[3]',
        'num_machines': 8
    },
    'Line_3': {
        'web_ip': 'https://192.168.10.51:9443/lws/LwsInitialize',
        'database': 'Line_3_datas',
        'line_number': 3,
        'machine_tables': list(range(4, 15)),
        'feeder_table': 19,
        'nozzle_table': 22,
        'model_xpath': '/html/body/table[18]/tbody/tr[2]/td[6]',
        'output_xpath': '/html/body/table[4]/tbody/tr[130]/td[3]',
        'num_machines': 10
    },
    'Line_4': {
        'web_ip': 'https://192.168.10.61:9443/lws/LwsInitialize',
        'database': 'Line_4_datas',
        'line_number': 4,
        'machine_tables': list(range(4, 15)),
        'feeder_table': 19,
        'nozzle_table': 22,
        'model_xpath': '/html/body/table[18]/tbody/tr[2]/td[6]',
        'output_xpath': '/html/body/table[4]/tbody/tr[130]/td[3]',
        'num_machines': 10
    },
    'Line_6': {
        'web_ip': 'https://192.168.10.71:9443/lws/LwsInitialize',
        'database': 'Line_6_datas',
        'line_number': 6,
        'machine_tables': list(range(4, 15)),
        'feeder_table': 19,
        'nozzle_table': 22,
        'model_xpath': '/html/body/table[18]/tbody/tr[2]/td[6]',
        'output_xpath': '/html/body/table[4]/tbody/tr[130]/td[3]',
        'num_machines': 10
    },
    'Line_7': {
        'web_ip': 'https://192.168.10.101:9443/lws/LwsInitialize',
        'database': 'Line_7_datas',
        'line_number': 7,
        'machine_tables': list(range(4, 19)),
        'feeder_table': 22,
        'nozzle_table': 25,
        'model_xpath': '/html/body/table[21]/tbody/tr[2]/td[6]',
        'output_xpath': '/html/body/table[4]/tbody/tr[178]/td[3]',
        'num_machines': 13
    },
    'Line_8': {
        'web_ip': 'https://192.168.10.91:9443/lws/LwsInitialize',
        'database': 'Line_8_datas',
        'line_number': 8,
        'machine_tables': list(range(4, 14)),
        'feeder_table': 17,
        'nozzle_table': 20,
        'model_xpath': '/html/body/table[16]/tbody/tr[2]/td[6]',
        'output_xpath': '/html/body/table[4]/tbody/tr[98]/td[3]',
        'num_machines': 8
    },
    'Line_9': {
        'web_ip': 'https://192.168.10.81:9443/lws/LwsInitialize',
        'database': 'Line_9_datas',
        'line_number': 9,
        'machine_tables': list(range(4, 20)),
        'feeder_table': 23,
        'nozzle_table': 26,
        'model_xpath': '/html/body/table[22]/tbody/tr[2]/td[6]',
        'output_xpath': '/html/body/table[4]/tbody/tr[178]/td[3]',
        'num_machines': 14
    },
    'Line_11': {
        'web_ip': 'https://192.168.10.11:9443/lws/LwsInitialize',
        'database': 'Line_11_datas',
        'line_number': 11,
        'machine_tables': list(range(4, 15)),
        'feeder_table': 19,
        'nozzle_table': 22,
        'model_xpath': '/html/body/table[18]/tbody/tr[2]/td[6]',
        'output_xpath': '/html/body/table[4]/tbody/tr[130]/td[3]',
        'num_machines': 10
    },
    'Line_13': {
        'web_ip': 'https://192.168.10.141:9443/lws/LwsInitialize',
        'database': 'Line_13_datas',
        'line_number': 13,
        'machine_tables': list(range(4, 16)),
        'feeder_table': 20,
        'nozzle_table': 23,
        'model_xpath': '/html/body/table[19]/tbody/tr[2]/td[6]',
        'output_xpath': '/html/body/table[4]/tbody/tr[146]/td[3]',
        'num_machines': 11
    },
    'Line_14': {
        'web_ip': 'https://192.168.10.151:9443/lws/LwsInitialize',
        'database': 'Line_14_datas',
        'line_number': 14,
        'machine_tables': list(range(4, 15)),
        'feeder_table': 19,
        'nozzle_table': 22,
        'model_xpath': '/html/body/table[18]/tbody/tr[2]/td[6]',
        'output_xpath': '/html/body/table[4]/tbody/tr[130]/td[3]',
        'num_machines': 10
    },
    'Line_16': {
        'web_ip': 'https://192.168.10.171:9443/lws/LwsInitialize',
        'database': 'Line_16_datas',
        'line_number': 16,
        'machine_tables': list(range(4, 13)),
        'feeder_table': 16,
        'nozzle_table': 19,
        'model_xpath': '/html/body/table[15]/tbody/tr[2]/td[6]',
        'output_xpath': '/html/body/table[4]/tbody/tr[82]/td[3]',
        'num_machines': 7
    }
}

# Initialize lines in MongoDB
def init_lines():
    for line_name, config in LINES.items():
        db.lines.update_one(
            {"_id": line_name},
            {"$set": {"name": line_name, "config": config}},
            upsert=True
        )
    logging.info("Lines initialized in MongoDB")

# Create a ticket
def create_ticket(line_name, issue_type, description, metrics, hourly_data_id, priority='Medium'):
    ticket_id = f"TICKET_{datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"
    ticket = {
        'ticket_id': ticket_id,
        'hourly_data_id': hourly_data_id,
        'line_id': line_name,
        'issue_type': issue_type,
        'description': description,
        'timestamp': datetime.now(pytz.utc),
        'priority': priority,
        'status': 'Open',
        'oee': metrics['oee'],
        'downtime': metrics['downtime'],
        'output': metrics['output'],
        'target': metrics['target'],
        'performance': metrics['performance'],
        'quality': metrics['quality']
    }
    db.tickets.insert_one(ticket)
    logging.info(f"Ticket {ticket_id} saved to MongoDB")
    return ticket

# Initialize WebDriver
def initialize_driver():
    webdriver_path = 'C:\\Users\\admin\\Downloads\\edgedriver_win64\\msedgedriver.exe'
    edge_options = Options()
    edge_options.add_argument("--headless")
    edge_options.add_argument('--ignore-certificate-errors')
    edge_options.add_argument('window-size=1920,1080')
    service = Service(webdriver_path)
    try:
        driver = webdriver.Edge(service=service, options=edge_options)
        logging.info("WebDriver initialized successfully")
        return driver
    except Exception as e:
        logging.error(f"Failed to initialize WebDriver: {str(e)}")
        raise

# Login function
def login(driver, web_ip):
    try:
        driver.get(web_ip)
        time.sleep(1)
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "input#userid"))).send_keys('ADMINISTRATOR')
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "input#password"))).send_keys('NPM@LNB01' + Keys.RETURN)
        WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'OK')]"))).click()
        logging.info(f"Login successful for {web_ip}")
    except Exception as e:
        logging.error(f"Login failed for {web_ip}: {str(e)}")
        raise

# Navigation function for runtime report
def navigate_to_report(driver):
    try:
        WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//*[@id='menu']/table/tbody/tr/td/table/tbody/tr[3]/td/a"))).click()
        WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//*[@id='main']/table[3]/tbody/tr[2]/td/table/tbody/tr/td[1]/a"))).click()
        logging.info("Navigated to report page")
    except Exception as e:
        logging.error(f"Navigation to report failed: {str(e)}")
        raise

# Set report time for one-hour window
def set_report_time(driver):
    try:
        now = datetime.now(pytz.timezone('Asia/Kolkata'))
        end_datetime = now.replace(minute=0, second=0, microsecond=0)
        start_datetime = end_datetime - timedelta(hours=1)
        start_date_str = start_datetime.strftime('%m/%d/%Y')
        end_date_str = end_datetime.strftime('%m/%d/%Y')
        start_hour = start_datetime.strftime('%H')
        end_hour = end_datetime.strftime('%H')
        time_range = f"{start_datetime.strftime('%H:%M:%S')} - {end_datetime.strftime('%H:%M:%S')}"

        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//*[@id='startdate']"))).send_keys(start_date_str + Keys.RETURN)
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//*[@id='starthour']"))).send_keys(start_hour + Keys.RETURN)
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//*[@id='enddate']"))).send_keys(end_date_str + Keys.RETURN)
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//*[@id='endhour']"))).send_keys(end_hour)
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//*[@id='endminute']"))).send_keys("00")
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//*[@id='endsecond']"))).send_keys("00" + Keys.RETURN)
        WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//*[@id='productManageReportTimeSettingForm']/table/tbody/tr[5]/td/input[2]"))).click()
        time.sleep(10)

        WebDriverWait(driver, 30).until(lambda d: len(d.window_handles) > 1)
        driver.switch_to.window(driver.window_handles[-1])
        time.sleep(1)

        logging.info(f"Report time set: {start_datetime} to {end_datetime} ({time_range})")
        return start_datetime, end_datetime, time_range
    except Exception as e:
        logging.error(f"Failed to set report time: {str(e)}")
        raise

# Data cleaning functions
def clean_numeric_value(value):
    if not value or value == 'N/A':
        return 0
    try:
        cleaned = re.sub(r'[^\d.-]', '', value)
        return float(cleaned) if '.' in cleaned else int(cleaned)
    except:
        logging.warning(f"Failed to clean numeric value: {value}")
        return 0

def clean_time_value(value, convert_to_minutes=False):
    if not value or value == 'N/A':
        return 0 if convert_to_minutes else None
    if convert_to_minutes:
        try:
            parts = value.split(':')
            if len(parts) == 3:
                hours = int(parts[0])
                minutes = int(parts[1])
                seconds = int(parts[2])
                return hours * 60 + minutes + seconds / 60
            elif len(parts) == 2:
                minutes = int(parts[0])
                seconds = int(parts[1])
                return minutes + seconds / 60
            else:
                return 0
        except:
            logging.warning(f"Failed to convert time to minutes: {value}")
            return 0
    try:
        parts = value.split(':')
        if len(parts) == 3:
            return f"{parts[0].zfill(2)}:{parts[1].zfill(2)}:{parts[2].zfill(2)}"
        elif len(parts) == 2:
            return f"00:{parts[0].zfill(2)}:{parts[1].zfill(2)}"
        return value
    except:
        logging.warning(f"Failed to clean time value: {value}")
        return value

# Extract model and cycle times from Tabulator grid
def extract_model_and_cycle_times(driver, line_name, config):
    logging.info(f"Processing {line_name} for cycle times, model, and target")
    try:
        WebDriverWait(driver, 30).until(EC.element_to_be_clickable(
            (By.XPATH, "//*[@id='menu']/table/tbody/tr/td/table/tbody/tr[10]/td/a/img"))).click()
        logging.info("Menu item clicked")

        driver.switch_to.default_content()
        iframes = driver.find_elements(By.TAG_NAME, "iframe")
        if iframes:
            logging.info(f"Found {len(iframes)} iframes, switching to first iframe")
            driver.switch_to.frame(iframes[0])
        else:
            logging.warning("No iframes found on the page")

        logging.info("Clicking lnkChangeDisplay link...")
        WebDriverWait(driver, 30).until(EC.element_to_be_clickable(
            (By.XPATH, "//*[@id='lnkChangeDisplay']"))).click()
        time.sleep(5)
        logging.info("lnkChangeDisplay clicked")

        driver.switch_to.default_content()
        iframes = driver.find_elements(By.TAG_NAME, "iframe")
        if iframes:
            logging.info(f"Found {len(iframes)} iframes, switching to first iframe")
            driver.switch_to.frame(iframes[0])

        WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.ID, "ctlGrActual")))
        logging.info("Tabulator grid found")
        rows = driver.find_elements(By.CSS_SELECTOR, "div.tabulator-row")
        logging.info(f"Found {len(rows)} rows in the grid")

        cycle_times = []
        model_name = "ModelNotFound"
        target = 0

        if rows:
            last_rows = rows[-4:] if len(rows) >= 4 else rows
            logging.info(f"Processing last {len(last_rows)} rows")

            for i, row in enumerate(last_rows):
                try:
                    logging.info(f"Processing row {i+1}: {row.text[:50]}...")
                    actCTime = row.find_element(By.CSS_SELECTOR, 'div[tabulator-field="actCTime"]').text.strip()
                    if actCTime:
                        try:
                            cycle_time = float(actCTime)
                            if cycle_time <= 89.99:
                                cycle_times.append(cycle_time)
                                logging.info(f"Added cycle time: {cycle_time}")
                            else:
                                logging.info(f"Skipped cycle time {cycle_time} (exceeds 89.99)")
                        except ValueError:
                            logging.warning(f"Invalid actCTime value '{actCTime}' in row {i+1}, skipping")
                    else:
                        logging.info(f"Empty actCTime in row {i+1}, skipping")
                except Exception as e:
                    logging.error(f"Error processing row {i+1} for cycle time: {str(e)}")
                    continue

            try:
                last_row = rows[-1]
                lot_field_names = ["LOT", "Lot", "lot", "LotNo", "MODEL"]
                lot_value = ""
                for field in lot_field_names:
                    try:
                        lot_value = last_row.find_element(By.CSS_SELECTOR, f'div[tabulator-field="{field}"]').text.strip()
                        logging.info(f"LOT value from last row (field={field}): {lot_value}")
                        break
                    except:
                        continue
                if lot_value:
                    parts = lot_value.split('_')
                    model_name = '_'.join(parts[:2]) if len(parts) >= 3 else lot_value
                    logging.info(f"Processed model name: {model_name}")
                else:
                    logging.warning("Empty or missing LOT value in last row")
            except Exception as e:
                logging.error(f"Failed to extract LOT from last row: {str(e)}")

            if cycle_times:
                try:
                    max_cycle_time = max(cycle_times)
                    is_subboard = 'SB' in model_name.upper()
                    multiplier = 10 if is_subboard else 4
                    target = (3600 / max_cycle_time) * multiplier if max_cycle_time > 0 else 0
                    logging.info(f"Calculated target: {target} (max cycle time: {max_cycle_time}, model: {model_name}, is_subboard: {is_subboard}, multiplier: {multiplier})")
                except (ValueError, ZeroDivisionError) as e:
                    logging.error(f"Error calculating target: {str(e)}")
                    target = 0
            else:
                logging.warning(f"No valid cycle times found for {line_name}, setting target to 0")

        return {'model': model_name, 'target': target, 'cycle_times': cycle_times}

    except Exception as e:
        logging.error(f"Error processing {line_name} for cycle times: {str(e)}")
        return {'model': 'ModelNotFound', 'target': 0, 'cycle_times': []}

# Data extraction functions
def extract_output_value(driver, config):
    logging.info(f"Extracting Output Value for Line_{config['line_number']}")
    try:
        output_element = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.XPATH, config['output_xpath'])))
        output_value = clean_numeric_value(output_element.text.strip())
        logging.info(f"Line_{config['line_number']} - Extracted output value: {output_value}")
        return output_value
    except Exception as e:
        logging.error(f"Line_{config['line_number']} - Error extracting output value: {str(e)}")
        return 0

def extract_quality_value(driver, config):
    logging.info(f"Extracting Quality Value for Line_{config['line_number']}")
    try:
        quality_element = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.XPATH, "/html/body/table[3]/tbody/tr[1]/td[2]")))
        quality_value = clean_numeric_value(quality_element.text.strip())
        logging.info(f"Line_{config['line_number']} - Extracted quality value: {quality_value}")
        return quality_value
    except Exception as e:
        logging.error(f"Line_{config['line_number']} - Error extracting quality value: {str(e)}")
        return 0

def extract_runtime_data(driver, config):
    logging.info(f"Extracting Runtime Data for Line_{config['line_number']} (Tables {config['machine_tables'][1:]})")
    runtime_data = []
    for table_num in config['machine_tables'][1:]:
        machine_num = table_num - 4
        if machine_num > config['num_machines']:
            logging.info(f"Line_{config['line_number']} - Skipping table {table_num} as it exceeds number of machines ({config['num_machines']})")
            continue
        try:
            def get_cell_text(tr, td):
                try:
                    xpath = f"/html/body/table[{table_num}]/tbody/tr[{tr}]/td[{td}]"
                    return driver.find_element(By.XPATH, xpath).text.strip()
                except:
                    try:
                        xpath = f"/html/body/table[{table_num}]/tr[{tr}]/td[{td}]"
                        return driver.find_element(By.XPATH, xpath).text.strip()
                    except:
                        return "N/A"
            
            power_on_time = clean_time_value(get_cell_text(1, 3))
            real_running_time = clean_time_value(get_cell_text(2, 3))
            power_on_minutes = clean_time_value(get_cell_text(1, 3), convert_to_minutes=True)
            real_running_minutes = clean_time_value(get_cell_text(2, 3), convert_to_minutes=True)
            
            runtime_data.append({
                'machine_number': machine_num,
                'power_on_time': power_on_time,
                'real_running_time': real_running_time,
                'power_on_minutes': power_on_minutes,
                'real_running_minutes': real_running_minutes,
                'power_on_hours': power_on_minutes / 60 if power_on_minutes is not None else 0,
                'real_running_hours': real_running_minutes / 60 if real_running_minutes is not None else 0
            })
        except Exception as e:
            logging.warning(f"Line_{config['line_number']} - Error processing runtime table {table_num}: {str(e)}")
            continue
    logging.info(f"Line_{config['line_number']} - Extracted runtime data: {len(runtime_data)} record(s)")
    return runtime_data

# Calculate OEE
def calculate_oee(run_time, down_time, output, target, quality_value):
    try:
        run_time_hours = run_time / 60
        down_time_hours = down_time / 60
        availability = ((run_time_hours - down_time_hours) / run_time_hours * 100) if run_time_hours > 0 else 0
        performance = (output / target * 100) if target > 0 else 0
        quality = (1 - quality_value / 1000000) * 100 if quality_value >= 0 else 0
        oee = (availability / 100) * (performance / 100) * (quality / 100) * 100
        logging.info(f"Availability: {availability:.2f}%, Performance: {performance:.2f}%, Quality: {quality:.2f}%, OEE: {oee:.2f}%")
        return {
            'availability': availability,
            'performance': performance,
            'quality': quality,
            'oee': oee
        }
    except Exception as e:
        logging.error(f"Error calculating OEE: {str(e)}")
        return {
            'availability': 0,
            'performance': 0,
            'quality': 0,
            'oee': 0
        }

# Process a single line
def process_line(line_name, config):
    driver = None
    try:
        print(f"\nStarting OEE calculation for {line_name} at {datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')}")
        driver = initialize_driver()
        login(driver, config['web_ip'])
        model_info = extract_model_and_cycle_times(driver, line_name, config)
        login(driver, config['web_ip'])
        navigate_to_report(driver)
        start_datetime, end_datetime, time_range = set_report_time(driver)
        output_value = extract_output_value(driver, config)
        quality_value = extract_quality_value(driver, config)
        runtime_data = extract_runtime_data(driver, config)

        run_time = max([item['power_on_minutes'] for item in runtime_data]) if runtime_data else 0
        down_times = [(item['power_on_minutes'] - item['real_running_minutes']) for item in runtime_data if item['power_on_minutes'] is not None and item['real_running_minutes'] is not None]
        down_time = max(down_times) if down_times else 0
        oee_metrics = calculate_oee(run_time, down_time, output_value, model_info['target'], quality_value)

        # Prepare hourly_data document
        hourly_data_doc = {
            'line_id': line_name,
            'timestamp': start_datetime.astimezone(pytz.utc),
            'start_datetime': start_datetime.astimezone(pytz.utc),
            'end_datetime': end_datetime.astimezone(pytz.utc),
            'model_name': model_info['model'],
            'target_uph': model_info['target'],
            'output': output_value,
            'quality_value': quality_value,
            'run_time': run_time,
            'down_time': down_time,
            'availability': oee_metrics['availability'],
            'performance': oee_metrics['performance'],
            'quality': oee_metrics['quality'],
            'oee': oee_metrics['oee'],
            'cycle_times': model_info['cycle_times'],
            'runtime_data': runtime_data
        }

        # Insert or update hourly_data
        result = db.hourly_data.update_one(
            {"line_id": line_name, "timestamp": start_datetime.astimezone(pytz.utc)},
            {"$set": hourly_data_doc},
            upsert=True
        )
        hourly_data_id = result.upserted_id if result.upserted_id else \
            db.hourly_data.find_one({"line_id": line_name, "timestamp": start_datetime.astimezone(pytz.utc)})["_id"]

        # Check ticketing conditions
        tickets = []
        metrics = {
            'oee': oee_metrics['oee'],
            'downtime': down_time,
            'output': output_value,
            'target': model_info['target'],
            'performance': oee_metrics['performance'],
            'quality': oee_metrics['quality']
        }

        if oee_metrics['oee'] < TICKETING_THRESHOLDS['min_oee']:
            description = f"Low OEE detected: {oee_metrics['oee']:.2f}% is below threshold of {TICKETING_THRESHOLDS['min_oee']}%. Downtime: {down_time:.2f} minutes."
            tickets.append(create_ticket(line_name, "Low OEE", description, metrics, hourly_data_id, priority="High" if oee_metrics['oee'] < 50 else "Medium"))

        output_ratio = (output_value / model_info['target'] if model_info['target'] > 0 else 0)
        if output_ratio < TICKETING_THRESHOLDS['min_output_ratio']:
            description = f"Low output relative to target: Output/Target ratio is {output_ratio:.2%}, below threshold of {TICKETING_THRESHOLDS['min_output_ratio']:.2%}."
            tickets.append(create_ticket(line_name, "Low Output Ratio", description, metrics, hourly_data_id, priority="Medium"))

        # Print results
        print(f"\n=== OEE Results for {line_name} ===")
        print(f"Time Range: {time_range}")
        print(f"Model: {model_info['model']}")
        print(f"Target UPH: {round(model_info['target'], 2)}")
        print(f"Output: {output_value}")
        print(f"Quality Value: {quality_value}")
        print(f"Run Time: {run_time:.2f} minutes")
        print(f"Down Time: {down_time:.2f} minutes")
        print(f"Availability: {oee_metrics['availability']:.2f}%")
        print(f"Performance: {oee_metrics['performance']:.2f}%")
        print(f"Quality: {oee_metrics['quality']:.2f}%")
        print(f"OEE: {oee_metrics['oee']:.2f}%")
        print(f"Cycle Times: {model_info['cycle_times']}")
        print("\nRuntime Data:")
        for item in runtime_data:
            print(f"Machine {item['machine_number']}: PowerOnTime={item['power_on_time']} ({item['power_on_minutes']} min, {item['power_on_hours']:.2f} hr), "
                  f"RealRunningTime={item['real_running_time']} ({item['real_running_minutes']} min, {item['real_running_hours']:.2f} hr)")
        if tickets:
            print("\nGenerated Tickets:")
            for ticket in tickets:
                print(f"  - Ticket {ticket['ticket_id']}: {ticket['issue_type']} - {ticket['description']} (Priority: {ticket['priority']})")

        return {
            'line_name': line_name,
            'down_time': down_time,
            'output': output_value,
            'target': model_info['target'],
            'oee': oee_metrics['oee'],
            'performance': oee_metrics['performance'],
            'run_time': run_time,
            'success': True,
            'tickets': tickets
        }

    except Exception as e:
        logging.error(f"Main execution error for {line_name}: {str(e)}")
        try:
            if driver:
                driver.save_screenshot(f"error_{line_name}.png")
                print(f"Error screenshot saved as 'error_{line_name}.png'")
        except:
            pass
        return {
            'line_name': line_name,
            'down_time': 0,
            'output': 0,
            'target': 0,
            'oee': 0,
            'performance': 0,
            'run_time': 0,
            'success': False,
            'tickets': []
        }
    finally:
        if driver:
            driver.quit()
            logging.info(f"WebDriver closed for {line_name}")

# Main function to process all lines and summarize
def main():
    init_lines()
    results = []
    
    for line_name, config in LINES.items():
        result = process_line(line_name, config)
        results.append(result)
        print(f"\nCompleted processing {line_name}\n{'-'*50}")

    print("\n=== Production Summary ===")
    successful_results = [r for r in results if r['success']]
    
    if not successful_results:
        print("No successful data collected from any line.")
        return

    max_downtime = max(successful_results, key=lambda x: x['down_time'], default={'line_name': 'None', 'down_time': 0})
    print(f"Line with Highest Downtime: {max_downtime['line_name']} ({max_downtime['down_time']:.2f} minutes)")

    output_ratios = [(r['line_name'], r['output'] / r['target'] if r['target'] > 0 else 0) for r in successful_results]
    min_output_ratio = min(output_ratios, key=lambda x: x[1], default=('None', 0))
    print(f"Line with Lowest Output Relative to Target: {min_output_ratio[0]} (Output/Target Ratio: {min_output_ratio[1]:.2%})")

    min_oee = min(successful_results, key=lambda x: x['oee'], default={'line_name': 'None', 'oee': 0})
    print(f"Line with Lowest OEE: {min_oee['line_name']} ({min_oee['oee']:.2f}%)")

    min_performance = min(successful_results, key=lambda x: x['performance'], default={'line_name': 'None', 'performance': 0})
    print(f"Line with Lowest Performance: {min_performance['line_name']} ({min_performance['performance']:.2f}%)")

    poor_production_lines = [r for r in successful_results if r['oee'] < 60]
    if poor_production_lines:
        print(f"\nLines with Poor Production (OEE < 60%):")
        for line in poor_production_lines:
            print(f"  - {line['line_name']}: Downtime={line['down_time']:.2f} minutes, UPH={line['target']:.2f}, Output={line['output']}, OEE={line['oee']:.2f}%")
    else:
        print("\nNo lines have OEE below 60%.")

    low_performance_lines = [r for r in successful_results if r['performance'] < 60]
    if low_performance_lines:
        print(f"\nLines with Low Performance (Performance < 60%):")
        for line in low_performance_lines:
            print(f"  - {line['line_name']}: Performance={line['performance']:.2f}%, Output={line['output']}, UPH={line['target']:.2f}")
    else:
        print("\nNo lines have performance below 60%.")

    all_tickets = [ticket for result in results for ticket in result.get('tickets', [])]
    if all_tickets:
        print(f"\n=== Ticket Summary ===")
        print(f"Total Tickets Generated: {len(all_tickets)}")
        for ticket in all_tickets:
            print(f"  - Ticket {ticket['ticket_id']}: {ticket['issue_type']} on {ticket['line_id']} - {ticket['description']} (Priority: {ticket['priority']}, Status: {ticket['status']})")
    else:
        print("\nNo tickets generated.")

if __name__ == "__main__":
    main()
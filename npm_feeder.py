import pymongo
from pymongo import MongoClient
import sqlite3
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


# MongoDB connection details (Replace with your MongoDB cloud URL)
MONGO_URL = "mongodb+srv://maheshkumar17032k3:<db_password>@npmticket.knzwnmz.mongodb.net/?retryWrites=true&w=majority&appName=npmticket"
DB_NAME = "production_data"

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Ticketing thresholds
TICKETING_THRESHOLDS = {
    'min_performance': 60,      # Performance < 60% triggers a ticket
    'min_output_ratio': 0.4,    # Output/Target < 40% triggers a ticket
    'max_downtime': 10           # Downtime > 10 minutes triggers a ticket
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

# Desired columns for feeder and nozzle data
FEEDER_COLUMNS = [
    'Machine Order', 'T', 'FA', 'S', 'Feeder Type', 'Feeder Serial', 'TG', 'TG Serial',
    'Part Number', 'Library Name', 'Pickup Count', 'Error Count', 'Spoilage Rate [PPM]',
    'Error Rate[%]', 'Pickup Error Count', 'Recognition Error Count'
]
NOZZLE_COLUMNS = [
    'Machine Order', 'H', 'CA', 'Nozzle Number', 'Nozzle Serial', 'Pickup Count',
    'Error Count', 'Spoilage Rate [PPM]', 'Error Rate[%]', 'Pickup Error Count',
    'Recognition Error Count'
]

# Initialize MongoDB client
client = MongoClient(MONGO_URL)
db = client[DB_NAME]

# Initialize ticket database (SQLite)
def init_ticket_db():
    conn = sqlite3.connect('tickets.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tickets (
            ticket_id TEXT PRIMARY KEY,
            line_name TEXT,
            issue_type TEXT,
            description TEXT,
            timestamp TEXT,
            priority TEXT,
            status TEXT,
            metrics TEXT
        )
    ''')
    conn.commit()
    conn.close()
    logging.info("Ticket database initialized")

def save_ticket_to_db(ticket):
    conn = sqlite3.connect('tickets.db')
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO tickets (ticket_id, line_name, issue_type, description, timestamp, priority, status, metrics)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        ticket['ticket_id'],
        ticket['line_name'],
        ticket['issue_type'],
        ticket['description'],
        ticket['timestamp'],
        ticket['priority'],
        ticket['status'],
        str(ticket['metrics'])
    ))
    conn.commit()
    conn.close()
    logging.info(f"Ticket {ticket['ticket_id']} saved to SQLite database")

def create_ticket(line_name, issue_type, description, metrics, priority='Medium'):
    ticket_id = f"TICKET_{datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"
    ticket = {
        'ticket_id': ticket_id,
        'line_name': line_name,
        'issue_type': issue_type,
        'description': description,
        'timestamp': datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S'),
        'priority': priority,
        'status': 'Open',
        'metrics': metrics
    }
    save_ticket_to_db(ticket)
    return ticket

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

def navigate_to_report(driver):
    try:
        WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//*[@id='menu']/table/tbody/tr/td/table/tbody/tr[3]/td/a"))).click()
        WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//*[@id='main']/table[3]/tbody/tr[2]/td/table/tbody/tr/td[1]/a"))).click()
        logging.info("Navigated to report page")
    except Exception as e:
        logging.error(f"Navigation to report failed: {str(e)}")
        raise

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

def clean_numeric_value(value):
    if not value or value == 'N/A':
        return 0
    try:
        cleaned = re.sub(r'[^\d.-]', '', value)
        return float(cleaned) if '.' in cleaned else int(cleaned)
    except:
        logging.warning(f"Failed to clean numeric value: '{value}'")
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
            logging.warning(f"Failed to convert time to minutes: '{value}'")
            return 0
    try:
        parts = value.split(':')
        if len(parts) == 3:
            return f"{parts[0].zfill(2)}:{parts[1].zfill(2)}:{parts[2].zfill(2)}"
        elif len(parts) == 2:
            return f"00:{parts[0].zfill(2)}:{parts[1].zfill(2)}"
        return value
    except:
        logging.warning(f"Failed to clean time value: '{value}'")
        return value

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

def extract_runtime_data(driver, config):
    logging.info(f"Extracting Runtime Data for Line_{config['line_number']} (Tables {config['machine_tables'][1:]})")
    runtime_data = []
    for table_num in config['machine_tables'][1:]:
        machine_num = table_num - 4
        if machine_num >= config['num_machines']:
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
                'MachineNumber': machine_num,
                'PowerOnTime': power_on_time,
                'RealRunningTime': real_running_time,
                'PowerOnMinutes': power_on_minutes,
                'RealRunningMinutes': real_running_minutes,
                'PowerOnHours': power_on_minutes / 60 if power_on_minutes is not None else 0,
                'RealRunningHours': real_running_minutes / 60 if real_running_minutes is not None else 0
            })
        except Exception as e:
            logging.warning(f"Line_{config['line_number']} - Error processing runtime table {table_num}: {str(e)}")
            continue
    logging.info(f"Line_{config['line_number']} - Extracted runtime data: {len(runtime_data)} record(s)")
    return runtime_data

def extract_table_data(driver, table_number):
    data = []
    try:
        rows = driver.find_elements(By.XPATH, f"/html/body/table[{table_number}]/tbody/tr")
        if not rows:
            rows = driver.find_elements(By.XPATH, f"/html/body/table[{table_number}]/tr")
        for row in rows:
            cols = row.find_elements(By.TAG_NAME, "td")
            if not cols:
                cols = row.find_elements(By.TAG_NAME, "th")
            data.append([col.text.strip() for col in cols])
        logging.info(f"Extracted {len(data)} rows from table {table_number}")
    except Exception as e:
        logging.error(f"Error extracting table {table_number}: {str(e)}")
    return data

def get_top_error_rows(table_data, top_n=5):
    if not table_data or len(table_data) < 2:
        return []
    header = table_data[0]
    try:
        error_col_idx = next(i for i, col in enumerate(header) if 'error count' in col.lower())
    except StopIteration:
        logging.warning("Error Count column not found in table")
        return []
    rows_with_error = []
    for row in table_data[1:]:
        try:
            error_val = int(re.sub(r'[^\d]', '', row[error_col_idx]))
        except:
            error_val = 0
        rows_with_error.append((error_val, row))
    rows_with_error.sort(key=lambda x: x[0], reverse=True)
    top_rows = [header] + [r[1] for r in rows_with_error[:top_n]]
    return top_rows

def filter_table_data(table_data, columns_to_keep):
    if not table_data or len(table_data) < 1:
        return []
    header = table_data[0]
    indices = [header.index(col) for col in columns_to_keep if col in header]
    return [[row[i] for i in indices] for row in table_data]

def extract_model_and_cycle_times(driver, line_name, config):
    logging.info(f"Extracting cycle times and calculating target for {line_name}")
    try:
        WebDriverWait(driver, 30).until(EC.element_to_be_clickable(
            (By.XPATH, "//*[@id='menu']/table/tbody/tr/td/table/tbody/tr[10]/td/a/img"))).click()
        driver.switch_to.default_content()
        iframes = driver.find_elements(By.TAG_NAME, "iframe")
        if iframes:
            driver.switch_to.frame(iframes[0])
        WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH, "//*[@id='lnkChangeDisplay']"))).click()
        time.sleep(5)
        driver.switch_to.default_content()
        iframes = driver.find_elements(By.TAG_NAME, "iframe")
        if iframes:
            driver.switch_to.frame(iframes[0])
        WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.ID, "ctlGrActual")))
        rows = driver.find_elements(By.CSS_SELECTOR, "div.tabulator-row")
        cycle_times = []
        model_name = "ModelNotFound"
        if rows:
            last_rows = rows[-4:] if len(rows) >= 4 else rows
            for row in last_rows:
                try:
                    actCTime = row.find_element(By.CSS_SELECTOR, 'div[tabulator-field="actCTime"]').text.strip()
                    if actCTime:
                        cycle_time = float(actCTime)
                        if cycle_time <= 89.99:
                            cycle_times.append(cycle_time)
                except Exception as e:
                    logging.warning(f"Skipping row due to error: {str(e)}")
            try:
                last_row = rows[-1]
                lot_fields = ["LOT", "Lot", "lot", "LotNo", "MODEL"]
                lot_value = ""
                for field in lot_fields:
                    try:
                        lot_value = last_row.find_element(By.CSS_SELECTOR, f'div[tabulator-field="{field}"]').text.strip()
                        if lot_value:
                            break
                    except:
                        continue
                if lot_value:
                    parts = lot_value.split('_')
                    model_name = '_'.join(parts[:2]) if len(parts) >= 3 else lot_value
            except Exception as e:
                logging.warning(f"Could not extract model name: {str(e)}")
            if cycle_times:
                max_cycle_time = max(cycle_times)
                is_subboard = 'SB' in model_name.upper()
                multiplier = 10 if is_subboard else 4
                target = (3600 / max_cycle_time) * multiplier if max_cycle_time > 0 else 0
                logging.info(f"Calculated target: {target} (max cycle time: {max_cycle_time}, model: {model_name}, multiplier: {multiplier})")
            else:
                target = 0
                logging.warning(f"No valid cycle times found for {line_name}. Target set to 0.")
        else:
            target = 0
            logging.warning(f"No rows found in cycle time grid for {line_name}. Target set to 0.")
        return {'model': model_name, 'target': target, 'cycle_times': cycle_times}
    except Exception as e:
        logging.error(f"Error extracting cycle times for {line_name}: {str(e)}")
        return {'model': 'ModelNotFound', 'target': 0, 'cycle_times': []}

def process_line(line_name, config):
    driver = None
    try:
        print(f"\nStarting data extraction for {line_name} at {datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')}")
        driver = initialize_driver()
        login(driver, config['web_ip'])
        model_info = extract_model_and_cycle_times(driver, line_name, config)
        login(driver, config['web_ip'])
        navigate_to_report(driver)
        start_datetime, end_datetime, time_range = set_report_time(driver)
        output_value = extract_output_value(driver, config)
        runtime_data = extract_runtime_data(driver, config)
        feeder_data = extract_table_data(driver, config['feeder_table'])
        nozzle_data = extract_table_data(driver, config['nozzle_table'])
        feeder_top_errors = get_top_error_rows(feeder_data, top_n=5)
        nozzle_top_errors = get_top_error_rows(nozzle_data, top_n=5)
        filtered_feeder_data = filter_table_data(feeder_top_errors, FEEDER_COLUMNS)
        filtered_nozzle_data = filter_table_data(nozzle_top_errors, NOZZLE_COLUMNS)
        run_time = max([item['PowerOnMinutes'] for item in runtime_data]) if runtime_data else 0
        down_times = [(item['PowerOnMinutes'] - item['RealRunningMinutes']) for item in runtime_data if item['PowerOnMinutes'] is not None and item['RealRunningMinutes'] is not None]
        down_time = max(down_times) if down_times else 0
        target = model_info['target']
        performance = (output_value / target) * 100 if target > 0 else 0
        output_ratio = (output_value / target) if target > 0 else 0
        metrics = {
            'downtime': down_time,
            'output': output_value,
            'target': target,
            'performance': performance
        }
        tickets = []
        if down_time > TICKETING_THRESHOLDS['max_downtime']:
            description = (f"High downtime detected: {down_time:.2f} minutes exceeds threshold of "
                           f"{TICKETING_THRESHOLDS['max_downtime']} minute(s). Time range: {time_range}.")
            tickets.append(create_ticket(line_name, "High Downtime", description, metrics, priority="High"))
        if performance < TICKETING_THRESHOLDS['min_performance']:
            description = (f"Low performance detected: {performance:.2f}% is below threshold of "
                           f"{TICKETING_THRESHOLDS['min_performance']}%. Time range: {time_range}.")
            tickets.append(create_ticket(line_name, "Low Performance", description, metrics, priority="Medium"))
        if output_ratio < TICKETING_THRESHOLDS['min_output_ratio']:
            description = (f"Low output relative to target: Output/Target ratio is {output_ratio:.2%}, "
                           f"below threshold of {TICKETING_THRESHOLDS['min_output_ratio']:.2%}. Time range: {time_range}.")
            tickets.append(create_ticket(line_name, "Low Output Ratio", description, metrics, priority="Medium"))

        # Store data in MongoDB
        data = {
            'time_range': time_range,
            'model': model_info['model'],
            'target_uph': target,
            'output': output_value,
            'performance': performance,
            'run_time': run_time,
            'down_time': down_time,
            'feeder_errors': filtered_feeder_data,
            'nozzle_errors': filtered_nozzle_data,
            'tickets': tickets,
            'timestamp': datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
        }
        collection = db[line_name]
        collection.insert_one(data)
        logging.info(f"Data for {line_name} stored in MongoDB")

        print(f"\n=== Results for {line_name} ===")
        print(f"Time Range: {time_range}")
        print(f"Model: {model_info['model']}")
        print(f"Target UPH: {target:.2f}")
        print(f"Output: {output_value}")
        print(f"Performance: {performance:.2f}%")
        print(f"Run Time: {run_time:.2f} minutes")
        print(f"Down Time: {down_time:.2f} minutes")
        print(f"\nTop 5 Feeder Table Rows by Error Count (Table {config['feeder_table']}):")
        if feeder_top_errors:
            for row in filtered_feeder_data:
                print(row)
        else:
            print("  No feeder data or 'Error Count' column found.")
        print(f"\nTop 5 Nozzle Table Rows by Error Count (Table {config['nozzle_table']}):")
        if nozzle_top_errors:
            for row in filtered_nozzle_data:
                print(row)
        else:
            print("  No nozzle data or 'Error Count' column found.")
        if tickets:
            print("\nGenerated Tickets:")
            for ticket in tickets:
                print(f"  - Ticket {ticket['ticket_id']}: {ticket['issue_type']} - {ticket['description']} (Priority: {ticket['priority']})")
        return {
            'line_name': line_name,
            'down_time': down_time,
            'output': output_value,
            'target': target,
            'performance': performance,
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
            'performance': 0,
            'run_time': 0,
            'success': False,
            'tickets': []
        }
    finally:
        if driver:
            driver.quit()
            logging.info(f"WebDriver closed for {line_name}")

def main():
    init_ticket_db()
    while True:
        results = []
        for line_name, config in LINES.items():
            result = process_line(line_name, config)
            results.append(result)
            print(f"\nCompleted processing {line_name}\n{'-'*50}")
        print("\n=== Production Summary ===")
        successful_results = [r for r in results if r['success']]
        if not successful_results:
            print("No successful data collected from any line.")
        else:
            max_downtime = max(successful_results, key=lambda x: x['down_time'], default={'line_name': 'None', 'down_time': 0})
            print(f"Line with Highest Downtime: {max_downtime['line_name']} ({max_downtime['down_time']:.2f} minutes)")
            output_ratios = [(r['line_name'], r['output'] / r['target'] if r['target'] > 0 else 0) for r in successful_results]
            min_output_ratio = min(output_ratios, key=lambda x: x[1], default=('None', 0))
            print(f"Line with Lowest Output Relative to Target: {min_output_ratio[0]} (Output/Target Ratio: {min_output_ratio[1]:.2%})")
            min_performance = min(successful_results, key=lambda x: x['performance'], default={'line_name': 'None', 'performance': 0})
            print(f"Line with Lowest Performance: {min_performance['line_name']} ({min_performance['performance']:.2f}%)")
            poor_performance_lines = [r for r in successful_results if r['performance'] < TICKETING_THRESHOLDS['min_performance']]
            if poor_performance_lines:
                print(f"\nLines with Poor Performance (Performance < {TICKETING_THRESHOLDS['min_performance']}%):")
                for line in poor_performance_lines:
                    print(f"  - {line['line_name']}: Performance={line['performance']:.2f}%, Output={line['output']}, Target={line['target']:.2f}")
            else:
                print(f"\nNo lines have performance below {TICKETING_THRESHOLDS['min_performance']}%.")
            all_tickets = [ticket for result in results for ticket in result.get('tickets', [])]
            if all_tickets:
                print(f"\n=== Ticket Summary ===")
                print(f"Total Tickets Generated: {len(all_tickets)}")
                for ticket in all_tickets:
                    print(f"  - Ticket {ticket['ticket_id']}: {ticket['issue_type']} on {ticket['line_name']} - {ticket['description']} (Priority: {ticket['priority']}, Status: {ticket['status']})")
            else:
                print("\nNo tickets generated.")
        
        # Sleep until the next hour
        now = datetime.now(pytz.timezone('Asia/Kolkata'))
        next_run = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
        sleep_seconds = (next_run - now).total_seconds()
        logging.info(f"Sleeping for {sleep_seconds} seconds until {next_run}")
        time.sleep(sleep_seconds)

if __name__ == "__main__":
    main()
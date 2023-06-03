# This contains the scripts to gather, clean, and transform the data.
import gc
import logging
import time
import traceback
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
from selenium import webdriver
from selenium.common import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebElement
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from logger import Logger
from data_handler import DataHandler

DRIVER_PATH = '/Users/deni/chromedriver_mac64/chromedriver'
DRIVER_TYPE = 'chrome'
HEADLESS_MODE = False
TIME_OUT = 15
SAVE_THRESHOLD = 10
FETCH_HOURS = ["01:00", "07:00", "13:00", "19:00"]
LOG_LEVEL = Logger.INFO
DF_COLUMNS = ["Station", "Date", "Time", "Pressure", "RH", "Temp", "WD", "WS", "PREC", "NO", "NO2", "NOX", "O3", "PM10", "PM2.5"]
IS_CONTINUE = True
# START_DATE = {"YEAR": 2018, "MONTH": 1, "DAY": 1}
# START_CONTINUE_DATE = {"YEAR": 2019, "MONTH": 10, "DAY": 2}


class WebScraper:
    SCRAPE_COLUMNS = ["Station", "Date", "Time", "NO", "NO2", "NOX", "O3", "PM10", "PM2.5", "RH", "Temp (C)", "WD", "WS"]
    SCRAPE_URL = 'https://air.sviva.gov.il'
    ROW_DATA_PATH = '../data/raw/'
    ROW_TABLE_FILE_NAME = 'climate_air_quality.csv'
    driver = None
    stations_names = []
    station_data_counter = 0
    latest_fetched_date: str
    current_station_element = None
    current_station_name = ''

    def __init__(self, site_url: str, driver_type: str, driver_path: str, df: pd.DataFrame = None):
        self.url = site_url
        self.driver_path = driver_path
        self.df = df
        self.data_handler = DataHandler()
        self.stations_elements = []
        self.selected_stations = self.data_handler.load_data_params("SELECTED_STATIONS")
        if not self.selected_stations:
            logger.log(f"Failed to load stations from file", logging.ERROR)
        if driver_type.lower() == 'chrome':
            chrome_options = None
            if HEADLESS_MODE:
                chrome_options = Options()
                chrome_options.add_argument("--headless")  # Enable headless mode
            service = Service(driver_path)
            self.driver = webdriver.Chrome(service=service, options=chrome_options)

        self.driver.get(self.url)
        self.wait = WebDriverWait(self.driver, TIME_OUT)
        logger.log(f"scrape website: {self.url} \n{self.driver.title}")

    def __find_element_by_class_and_text(self, class_name, text):
        element = self.wait.until(
            EC.presence_of_element_located((By.XPATH, f"//span[contains(@class, '{class_name}') and text()='{text}']")))
        return element

    def __init_list_of_stations(self):
        if not self.stations_elements:
            time.sleep(3)
            main_ul = self.wait.until(EC.presence_of_element_located(
                (By.XPATH, "//ul[contains(@class, 'k-group') and contains(@class, 'k-treeview-lines')]")))
            lis = main_ul.find_elements(By.XPATH, "./li")
            for li in lis:
                div = li.find_element(By.XPATH, ".//div[@class='k-top' or @class='k-mid']")
                self.stations_elements.append(div)
            self.stations_names = list(self.selected_stations.keys())
            selected_stations_with_elements = []
            for station_name, station_data in self.selected_stations.items():
                for station_element in self.stations_elements:
                    if station_element.text == station_name:
                        station_data["station_element"] = station_element
                        break
                if not station_data.get("station_element"):
                    logger.log(f"Error, didn't find element for station {station_name}")

            logger.log(f"{len(self.stations_elements)} stations found")
            logger.log(f"{len(self.selected_stations.items())} selected stations")
            logger.log(f"Stations are: {self.stations_names}", logger.DEBUG)

    def open_menu(self):
        logger.log(f"Opening menu...")
        main_menu_icon = self.wait.until(EC.element_to_be_clickable((By.ID, 'mainMenuIcon')))
        time.sleep(1)  # Add a delay before clicking the button
        main_menu_icon.click()
        sub_menu_icon = self.__find_element_by_class_and_text("k-link", "נתוני ניטור אוויר")
        sub_menu_icon.click()
        time.sleep(1)  # Add a delay before clicking the button
        sub_menu2_icon = self.__find_element_by_class_and_text("k-link", "נתונים שעתיים")
        sub_menu2_icon.click()

    def select_station(self, station: WebElement):
        # Get stations
        if not self.stations_elements:
            self.__init_list_of_stations()
        if not station:
            logger.log(f"Station not found for selection, {self.current_station_name}", logger.ERROR)
            return False
        logger.log(f"Selecting station: {station.text}")
        time.sleep(1)
        checkbox = station.find_element(By.CSS_SELECTOR, "span.k-checkbox-label.checkbox-span")
        self.scroll_to_element(checkbox)
        # Perform a click on the station element
        # Not using checkbox.click() because it didn't click
        self.driver.execute_script("arguments[0].click();", checkbox)
        return True

    def get_date_picker(self):
        return self.wait.until(EC.element_to_be_clickable((By.ID, 'fromDateDatePicker')))

    @staticmethod
    def date_to_str(date: datetime):
        return date.strftime('%d/%m/%Y')

    @staticmethod
    def str_to_date(date: str, pattern: str = None):
        return datetime.strptime(date, pattern if pattern else "%d/%m/%Y")

    def split_to_str_date_and_hour(self, date: str, pattern: str = None):
        date_and_hour = self.str_to_date(date, pattern if pattern else "%H:%M %d/%m/%Y")
        return date_and_hour.strftime("%d/%m/%Y"), date_and_hour.strftime("%H:%M")

    def select_date(self, date: datetime):
        date_string = self.date_to_str(date)
        logger.log(f"Selecting date {date_string}")
        date_input_selector = self.get_date_picker()
        date_input_selector.clear()  # Clear any existing value
        date_input_selector.send_keys(date_string)

    def acquire_stations_data(self, start_date: datetime = None):
        self.__init_list_of_stations()
        for station_name, station_data in self.selected_stations.items():
            station_date = station_data.get("latest_fetched_date")
            if self.str_to_date(station_date).date() == datetime.today().date():
                continue
            self.current_station_element = station_data.get("station_element")
            self.current_station_name = station_name
            self.station_data_counter = 0
            if self.select_station(self.current_station_element):
                self.select_date(start_date if start_date else self.str_to_date(station_date))
                self.show_station_page()
                if self.get_current_station_and_date_data(station_name):
                    self.exit_station_page()
                # Unselect
                self.select_station(self.current_station_element)
                logger.log(f"Total {self.station_data_counter} fetched data [rXc] for {station_name} station")

    def get_current_station_and_date_data(self, station_name):
        # Add a delay
        logger.log(f"Scraping {station_name}")
        current_date: datetime = None
        path = Path(self.ROW_DATA_PATH) / self.ROW_TABLE_FILE_NAME
        next_page = True
        row_number = 1
        rows_data = []
        while next_page:
            init_page_params_result = self.init_page_params()
            if init_page_params_result is None:
                if len(self.df) != 1:
                    self.save_df(path, station_name, self.latest_fetched_date)
                return False
            else:
                dates_column, rows_data, columns = init_page_params_result
                logger.log(f"row num- {row_number}", logger.DEBUG)
                # Find all td elements with role "gridcell" within the first table
                data_and_hours_elements = dates_column.find_elements(By.CSS_SELECTOR, "tr.k-master-row, tr.k-alt.k-master-row")
                rows_to_fetch = []
                dates_to_fetch = []
                for index, element in enumerate(data_and_hours_elements):
                    self.scroll_to_element(element)
                    if element.text.split(' ')[0] in FETCH_HOURS:
                        dates_to_fetch.append(element.text)
                        rows_to_fetch.append(rows_data[index])

                self.get_data_from_table(station_name, dates_to_fetch, rows_to_fetch, columns)
                if row_number >= SAVE_THRESHOLD:
                    self.save_df(path, station_name, self.latest_fetched_date)
                    row_number = 1

                row_number += 1
                next_page = self.next_page()
        self.save_df(path, station_name, self.latest_fetched_date)
        return True


    def get_data_from_table(self, station_name, dates_to_fetch, rows_to_fetch, columns):
        date_format = "%H:%M %d/%m/%Y"
        # Process each row
        extended_columns = columns.copy()
        extended_columns.extend([DF_COLUMNS[0], DF_COLUMNS[1], DF_COLUMNS[2]])
        if self.station_data_counter == 0:
            logger.log(f"{len(columns)} columns found for station.")
        rows = {col: [] for col in extended_columns}
        date_string: str = ''

        # Process each row
        for i, row in enumerate(rows_to_fetch):
            date_string, hour_string = self.split_to_str_date_and_hour(dates_to_fetch[i])

            rows.get(DF_COLUMNS[0]).append(station_name)
            rows.get(DF_COLUMNS[1]).append(date_string)
            rows.get(DF_COLUMNS[2]).append(hour_string)

            # Find all td elements within the row
            td_elements = row.find_elements(By.TAG_NAME, "td")

            # Process each column of current row
            for j, td_element in enumerate(td_elements):
                self.scroll_to_element(td_element)
                td_text = td_element.find_element(By.TAG_NAME, "div").text
                if j < len(columns):
                    rows.get(columns[j]).append(td_text)
                else:
                    logger.log(f"Failed to find column, index {j} if td element but having total {len(columns)} columns")
                    break
                time.sleep(0.1)

        self.latest_fetched_date = date_string
        self.station_data_counter += (len(columns) * len(rows_to_fetch))
        new_df = pd.DataFrame.from_dict(rows)
        # Get the common columns between self.df and new_df
        common_columns = list(set(self.df.columns) & set(new_df.columns))

        # Update the merged_df with data from self.df
        self.df = pd.concat([self.df, new_df[common_columns]], ignore_index=True, axis=0)

    def init_page_params(self):
        max_retries = 10
        for retry in range(max_retries):
            time.sleep(2)
            try:
                # title_element = self.driver.find_element(By.ID, "resultPeriodTitle")
                title_element = self.wait.until(EC.visibility_of_element_located((By.ID, "resultPeriodTitle")))
                self.driver.execute_script("arguments[0].scrollIntoView();", title_element)
                self.latest_fetched_date = title_element.text
                # self.latest_fetched_date = self.wait.until(EC.visibility_of_element_located((By.ID, "resultPeriodTitle"))).text
                # self.latest_fetched_date = datetime.strptime(date_title.text, "%d/%m/%Y")
                logger.log(f"Date {self.latest_fetched_date}", logger.DEBUG)
                dates_column = self.wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "table.k-selectable")))
                # Find the second table with class "k-selectable"
                data_table = self.driver.find_elements(By.CSS_SELECTOR, "table.k-selectable")[1]
                # Find all rows (tr) with class "k-master-row" within the second table
                rows_data = data_table.find_elements(By.CSS_SELECTOR, "tr.k-master-row")
                columns = self.driver.find_elements(By.XPATH,'//*[@data-colspan="1"]')[:int(self.driver.find_elements(By.XPATH,'//*[@data-colspan="1"]').__len__()/2)]
                columns_text = []
                for column in columns:
                    self.scroll_to_element(column)
                    columns_text.append(column.text)
                return dates_column, rows_data, columns_text
            except TimeoutException as toe:
                if retry < max_retries - 1:
                    # Need to add one more day on first retry
                    if retry == 0:
                        self.add_days_to_date(1)
                    # Add one day using timedelta
                    if self.str_to_date(self.latest_fetched_date).date() >= datetime.today().date():
                        return None
                    self.add_days_to_date(1)
                    logger.log(f"{self.latest_fetched_date} date is N/A. Retrying... ({retry+1}/{max_retries})", logger.ERROR)
                    self.select_date(self.str_to_date(self.latest_fetched_date))
                    self.show_station_page()
                    time.sleep(2)  # Add any necessary delay before retrying
                else:
                    logger.log(f"Maximum retries reached. Unable to load page.", logger.ERROR)
                    return None
            except Exception as e:
                logger.log(f"Error occurred while loading page: {e}", logger.ERROR)
                return None

    def next_page(self):
        next_page_button = self.wait.until(EC.presence_of_element_located((By.ID,  'reportForward')))
        next_page_button = self.wait.until(EC.visibility_of_element_located((By.ID,  'reportForward')))
        self.scroll_to_element(next_page_button)
        next_page = "disable" not in next_page_button.get_attribute("class")
        if next_page:
            time.sleep(2)
            is_clickable = next_page_button.is_enabled() and next_page_button.is_displayed()
            # Debugging selenium.common.exceptions.ElementClickInterceptedException:
            # if not is_clickable:
            overlapping_element = self.driver.find_element(By.CSS_SELECTOR, "label[for='ownerBox']")
            # logger.log(f"ElementClickInterceptedException occurred. Clickable element: {next_page_button.get_attribute('outerHTML')}", logger.ERROR)
            logger.log(f"Overlapping element: {overlapping_element.get_attribute('outerHTML')}", logger.DEBUG)
                # logger.log(f"Page layout changed. Waiting for element to be clickable again.", logger.ERROR)
            next_page_button.click()
        else:
            logger.log("Finished with station (next page button disabled)")
        return next_page

    def save_df(self, path, station_name: str, date: str):
        logger.log(f"Saving... {station_name} {date}")
        self.selected_stations.get(station_name)['latest_fetched_date'] = date
        modified_selected_stations = {}
        for station_name, station_data in self.selected_stations.items():
            modified_selected_stations[station_name] = {k:v for k,v in self.selected_stations[station_name].items() if k != 'station_element'}
        self.data_handler.save_data_params("SELECTED_STATIONS", modified_selected_stations)
        self.data_handler.write_to_file(path, self.df, 'a')
        self.df = self.df.head(1)

    def show_station_page(self):
        time.sleep(1)
        show_button = self.wait.until(EC.element_to_be_clickable((By.ID, 'showResultsBtn')))
        show_button.click()

    def exit_station_page(self):
        container = self.driver.find_element(By.ID, "report_result")
        exit_button = container.find_element(By.XPATH, ".//img[@class='popupExitIcon popupWhiteExitIcon']")
        exit_button.click()

    #Scroll to the elements using JavaScript:
    def scroll_to_element(self, element):
        self.driver.execute_script("arguments[0].scrollIntoView();", element)

    def add_days_to_date(self, delta: int, date: datetime = None):
        if date:
            next_date = date + timedelta(days=delta)
        else:
            next_date = self.str_to_date(self.latest_fetched_date) + timedelta(days=1)
            self.latest_fetched_date = self.date_to_str(next_date)
        return next_date


def retry_acquire_stations_data(df):
    retry_count = 50  # Number of retries
    retry_delay = 5  # Delay between retries in seconds
    while retry_count > 0:
        try:
            web_scraper = None
            web_scraper = WebScraper(WebScraper.SCRAPE_URL, DRIVER_TYPE, DRIVER_PATH, df)
            logger.log(f"web_scraper.stations_elements {hex(id(web_scraper.stations_elements))}")
            web_scraper.open_menu()
            web_scraper.acquire_stations_data()

            # Clear variables underneath web_scraper
            # web_scraper.stations_elements = None
            # web_scraper = None
            # Set other variables to None if applicable
            # del web_scraper  # Delete the reference to the object
            # gc.collect()  # Force garbage collection
        except Exception as e:
            log_message = f"An exception occurred: {str(e)}"
            traceback_str = traceback.format_tb(e.__traceback__)
            log_message += f"\nTraceback:\n{traceback_str}"
            logger.log(log_message, logger.WARNING)
            traceback.print_tb(e.__traceback__)
            print('-' * 40)
            retry_count -= 1
            if web_scraper:
                web_scraper.driver.quit()
            if retry_count == 0:
                logger.log("Maximum retry attempts reached. Exiting...", logger.ERROR)
            else:
                logger.log(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)

if __name__ == '__main__':
    logger = Logger(log_level=LOG_LEVEL)
    # Create an empty DataFrame
    skeleton_df = pd.DataFrame(columns=DF_COLUMNS)

    retry_acquire_stations_data(skeleton_df)

# stations = ['אלון שבות, גוש עציון',
# 'אריאל, אריאל',
# 'באר שבע, שכונה ו',
# 'בית שמש, בית שמש',
# 'בני ברק, כביש 4',
# "בני ברק, רחוב ז'בוטינסקי",
# 'גליל עליון, מכללת תל חי',
# 'חולון, חולון',
# 'חיפה, עצמאות חיפה',
# 'חריש, חריש',
# 'יבנה, ניידת - 2',
# 'ירושלים, בקעה',
# 'ירושלים, כיכר ספרא',
# 'ירושלים, מלכי ישראל',
# 'ירושלים, ניידת - 6',
# 'ירושלים, רחוב בר אילן',
# 'ירושלים, רחוב דבורה הנביאה',
# 'כסייפה, כסייפה',
# 'כפר מסריק, כפר מסריק החדשה',
# 'כפר סבא, כפר סבא',
# 'כרמיאל, גליל מערבי',
# 'נתניה, ניידת -1',
# 'עפולה, עפולה',
# 'ערד, נגב מזרחי',
# 'פתח תקווה, רחוב גיסין',
# 'קטורה, קטורה',
# 'קריית אתא, ניידת - 4',
# 'ראש העין, ראש העין',
# 'ראשון לציון, רחוב הרצל',
# 'רחובות, רחובות',
# 'רמלה, ניידת - 7',
# 'רעננה, רחוב אחוזה',
# 'תל אביב-יפו, אוניברסיטה',
# 'תל אביב-יפו, ניידת - 5',
# 'תל אביב-יפו, רחוב יהודה המכבי',
# 'תל אביב-יפו, רחוב יפת',
# 'תל אביב-יפו, רחוב לחי']

stations = [
    'אלון שבות, גוש עציון',
    # 'אריאל, אריאל',
    'באר שבע, שכונה ו',
    # 'בית שמש, בית שמש', m
    # 'בני ברק, כביש 4',
    # "בני ברק, רחוב ז'בוטינסקי",
    # 'גליל עליון, מכללת תל חי',
    # 'חולון, חולון', h
    # 'חיפה, עצמאות חיפה', m
    # 'חריש, חריש',
    # 'יבנה, ניידת - 2',
    # 'ירושלים, בקעה', h
    # 'ירושלים, כיכר ספרא', m
    # 'ירושלים, מלכי ישראל',
    # 'ירושלים, ניידת - 6',
    # 'ירושלים, רחוב בר אילן',
    # 'ירושלים, רחוב דבורה הנביאה',
    # 'כסייפה, כסייפה',
    # 'כפר מסריק, כפר מסריק החדשה',
    # 'כפר סבא, כפר סבא',
    'כרמיאל, גליל מערבי',
    # 'נתניה, ניידת -1',
    'עפולה, עפולה',
    # 'ערד, נגב מזרחי',
    # 'פתח תקווה, רחוב גיסין', h
    # 'קטורה, קטורה',
    # 'קריית אתא, ניידת - 4',
    # 'ראש העין, ראש העין',
    # 'ראשון לציון, רחוב הרצל', l
    # 'רחובות, רחובות', h
    # 'רמלה, ניידת - 7',
    # 'רעננה, רחוב אחוזה',l
    # 'תל אביב-יפו, אוניברסיטה', h
    # 'תל אביב-יפו, ניידת - 5',
    # 'תל אביב-יפו, רחוב יהודה המכבי 'l
# 'תל אביב-יפו, רחוב יפת',l
# 'תל אביב-יפו, רחוב לחי'] l
]


# TOTAL_C_R = 15*5*4*365*5
# 15 columns
# 5 stations
# 4 rows per day
# 5 years
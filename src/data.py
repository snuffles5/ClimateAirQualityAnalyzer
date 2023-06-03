"""
This file contains the scripts to gather, clean, and transform the data.
Dataset Overview:
- Stations: The dataset includes weather data from multiple stations.
- Columns: There are 15 columns in the dataset, representing different weather variables.
- Rows: Each row represents weather data for a specific station, date, and time.
- Stations: There are 5 weather stations included in the dataset.
- Time Period: The dataset covers a span of 5 years.
- Temporal Granularity: There are 4 rows per day, representing 4 different hours.

Estimated Total Rows (before cleaning): 15 * 5 * 4 * 365 * 5 = 547,500
"""

from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.remote.webdriver import WebElement
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common import TimeoutException
from selenium.webdriver.common.by import By
from selenium import webdriver

from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import traceback
import logging
import time

from logger import Logger
from data_handler import DataHandler

LOG_LEVEL = Logger.INFO
DF_COLUMNS = ["Station", "Date", "Time", "Pressure", "RH", "Temp", "WD", "WS", "PREC", "NO", "NO2", "NOX", "O3", "PM10", "PM2.5"]


class WebScraper:
    # General Settings
    ROW_DATA_PATH = '../data/raw/'
    # Constants
    DRIVER_PATH = '/Users/deni/chromedriver_mac64/chromedriver'
    ROW_TABLE_FILE_NAME = 'climate_air_quality.csv'
    SCRAPE_URL = 'https://air.sviva.gov.il'
    DRIVER_TYPE = 'chrome'
    # Scraping Settings
    FETCH_HOURS = ["01:00", "07:00", "13:00", "19:00"]
    HEADLESS_MODE = False
    TIME_OUT = 15
    SAVE_THRESHOLD = 10

    # Scraping Variables
    station_data_counter = 0
    current_station_element = None
    driver = None
    latest_fetched_date: str
    current_station_name: str = ''

    def __init__(self, site_url: str,  df: pd.DataFrame = None, driver_type: str = DRIVER_TYPE, driver_path: str = DRIVER_PATH):
        self.url = site_url
        self.driver_path = driver_path
        self.df = df
        self.data_handler = DataHandler()
        self.stations_elements = []
        self.selected_stations = self.data_handler.load_data_params("SELECTED_STATIONS")
        if not self.selected_stations:
            logger.log(f"Failed to load stations from file", logging.CRITICAL)
            quit()

        # Set up the web driver based on the specified driver type
        if driver_type.lower() == 'chrome':
            chrome_options = None

            # Configure Chrome options for headless mode if enabled
            if self.HEADLESS_MODE:
                chrome_options = Options()
                chrome_options.add_argument("--headless")  # Enable headless mode

            # Set up the Chrome web driver
            service = Service(driver_path)
            self.driver = webdriver.Chrome(service=service, options=chrome_options)

        # Load the URL in the web driver
        self.driver.get(self.url)

        # Set up the web driver wait for explicit waits
        self.wait = WebDriverWait(self.driver, self.TIME_OUT)
        logger.log(f"scrape website: {self.url} \n{self.driver.title}")

    def open_menu(self):
        logger.log(f"Opening menu...")
        main_menu_icon = self.wait.until(EC.element_to_be_clickable((By.ID, 'mainMenuIcon')))
        time.sleep(1)
        main_menu_icon.click()
        sub_menu_icon = self.__find_element_by_class_and_text("k-link", "נתוני ניטור אוויר")
        sub_menu_icon.click()
        time.sleep(1)
        sub_menu2_icon = self.__find_element_by_class_and_text("k-link", "נתונים שעתיים")
        sub_menu2_icon.click()

    def __find_element_by_class_and_text(self, class_name, text):
        """
        Finds an element by class name and text.

        Args:
            class_name: The class name of the element to find.
            text: The expected text of the element.

        Returns:
            WebElement: The found element if it exists, None otherwise.
        """
        element = self.wait.until(
            EC.presence_of_element_located((By.XPATH, f"//span[contains(@class, '{class_name}') and text()='{text}']")))
        return element

    def acquire_stations_data(self, start_date: datetime = None):
        """
        Acquires data for the selected stations.

        This method initializes the list of station elements, and then iterates over the selected stations.
        For each station, it checks if the latest fetched date is up-to-date.
        If not, it selects the station, selects the date from the menu, shows the station page, fetches the data,
        and exits the station page.

        Args:
            start_date: The start date from which to acquire data. If not provided, it defaults to the next day after
                    the latest fetched date.

        Returns:
            None
        """

        self.__init_list_of_stations()
        for station_name, station_data in self.selected_stations.items():
            latest_fetched_date = station_data.get("latest_fetched_date")
            if self.str_to_date(latest_fetched_date).date() >= datetime.today().date():
                continue
            self.current_station_element = station_data.get("station_element")
            self.current_station_name = station_name
            self.station_data_counter = 0
            if self.__select_station_from_menu(self.current_station_element):
                self.__select_date_from_menu(start_date if start_date else self.add_days_to_date(1, latest_fetched_date))
                self.show_station_page()
                if self.__get_current_station_and_date_data(station_name):
                    self.exit_station_page()
                # Unselect
                self.__select_station_from_menu(self.current_station_element)
                logger.log(f"Total {self.station_data_counter} fetched data [rXc] for {station_name} station")

    def __init_list_of_stations(self):
        """
        Initializes the list of station elements based on the selected stations.

        This method finds and associates the station elements with the corresponding selected stations.

        Returns:
            None
        """
        # Wait for the station elements to be present on the page
        if not self.stations_elements:
            time.sleep(3)
            main_ul = self.wait.until(EC.presence_of_element_located(
                (By.XPATH, "//ul[contains(@class, 'k-group') and contains(@class, 'k-treeview-lines')]")))

            # Find all the station elements
            lis = main_ul.find_elements(By.XPATH, "./li")
            for li in lis:
                div = li.find_element(By.XPATH, ".//div[@class='k-top' or @class='k-mid']")
                self.stations_elements.append(div)

            # Associate the station elements with the selected stations
            stations_names = list(self.selected_stations.keys())
            for station_name, station_data in self.selected_stations.items():
                for station_element in self.stations_elements:
                    if station_element.text == station_name:
                        station_data["station_element"] = station_element
                        break
                if not station_data.get("station_element"):
                    logger.log(f"Error, didn't find element for station {station_name}")

            logger.log(f"{len(self.stations_elements)} stations found")
            logger.log(f"{len(self.selected_stations.items())} selected stations")
            logger.log(f"Stations are: {stations_names}", logger.DEBUG)

    def __select_date_from_menu(self, date: datetime):
        """
        Selects a date from the menu.

        This private method selects a specific date from the menu by clearing any existing value and sending
        the desired date string.

        Args:
           date: The date to be selected.

        Returns:
           None
        """
        date_string = self.date_to_str(date)
        date_input_selector = self.get_date_picker()
        date_input_selector.clear()
        date_input_selector.send_keys(date_string)
        logger.log(f"Selecting date {date_string}")

    def get_date_picker(self):
        return self.wait.until(EC.element_to_be_clickable((By.ID, 'fromDateDatePicker')))

    def __select_station_from_menu(self, station: WebElement):
        """
        Selects a station from the menu.

        This private method selects a specific station from the menu by performing a click on the station element.

        Args:
            station: The WebElement representing the station element.

        Returns:
            bool: True if the station selection is successful, False otherwise.
        """

        # Initialize the list of station elements if not already initialized
        if not self.stations_elements:
            self.__init_list_of_stations()

        # Check if the station element exists
        if not station:
            logger.log(f"Station not found for selection, {self.current_station_name}", logger.ERROR)
            return False

        logger.log(f"Selecting station: {station.text}")
        time.sleep(1)
        checkbox = station.find_element(By.CSS_SELECTOR, "span.k-checkbox-label.checkbox-span")
        self.scroll_to_element(checkbox)

        # Perform a click on the station element using JavaScript executor
        # Using JavaScript executor as a workaround for cases where checkbox.click() does not work
        self.driver.execute_script("arguments[0].click();", checkbox)
        return True

    def show_station_page(self):
        time.sleep(1)
        show_button = self.wait.until(EC.element_to_be_clickable((By.ID, 'showResultsBtn')))
        show_button.click()

    def __get_current_station_and_date_data(self, station_name):
        """
        Retrieves data for the current station and date.

        This private method retrieves the data for the current station and date by iterating through the pages,
        fetching the data from the table, and saving the DataFrame periodically.

        Args:
            station_name: The name of the current station.

        Returns:
            bool: True if the data retrieval is successful, False otherwise.
        """
        logger.log(f"Scraping {station_name}")
        path = Path(self.ROW_DATA_PATH) / self.ROW_TABLE_FILE_NAME
        next_page = True
        row_number = 1
        rows_data = []
        while next_page:
            # Initialize the page parameters
            init_page_params_result = self.init_page_params()

            if init_page_params_result is None:
                # Save the DataFrame and return if page initialization is unsuccessful
                if len(self.df) != 1:
                    self.save_df(path, station_name, self.latest_fetched_date)
                return False

            else:
                # Retrieve the parameters from the page initialization
                dates_column, rows_data, columns = init_page_params_result
                logger.log(f"row num- {row_number}", logger.DEBUG)

                # Find the data and hours elements to fetch
                data_and_hours_elements = dates_column.find_elements(By.CSS_SELECTOR, "tr.k-master-row, tr.k-alt.k-master-row")
                rows_to_fetch = []
                dates_to_fetch = []

                # Iterate through the data and hours elements
                for index, element in enumerate(data_and_hours_elements):
                    self.scroll_to_element(element)
                    if element.text.split(' ')[0] in self.FETCH_HOURS:
                        dates_to_fetch.append(element.text)
                        rows_to_fetch.append(rows_data[index])

                # Get the data from the table for the selected dates and rows
                self.get_data_from_table(station_name, dates_to_fetch, rows_to_fetch, columns)

                # Save the DataFrame and reset the row number if the save threshold is reached
                if row_number >= self.SAVE_THRESHOLD:
                    self.save_df(path, station_name, self.latest_fetched_date)
                    row_number = 1

                row_number += 1
                next_page = self.next_page()
        self.save_df(path, station_name, self.latest_fetched_date)
        return True

    def __init_page_params(self):
        """
        Initializes the page parameters.

        This private method initializes the parameters required for retrieving data from the page,
        such as the latest fetched date, dates column, data table rows, and columns.

        Returns:
            Tuple[WebElement, List[WebElement], List[str]]: A tuple containing the dates column element,
            data table rows, and column names if successful, None otherwise.
        """

        max_retries = 10
        for retry in range(max_retries):
            time.sleep(2)
            try:
                # Find the title element with ID "resultPeriodTitle"
                title_element = self.wait.until(EC.visibility_of_element_located((By.ID, "resultPeriodTitle")))
                self.driver.execute_script("arguments[0].scrollIntoView();", title_element)

                # Get the latest fetched date from the title element
                self.latest_fetched_date = title_element.text
                logger.log(f"Date {self.latest_fetched_date}", logger.DEBUG)

                # Find the dates column element
                dates_column = self.wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "table.k-selectable")))

                # Find the second table (the data table)
                data_table = self.driver.find_elements(By.CSS_SELECTOR, "table.k-selectable")[1]

                # Find all relevant data rows int the data table
                rows_data = data_table.find_elements(By.CSS_SELECTOR, "tr.k-master-row")
                columns = self.driver.find_elements(By.XPATH,'//*[@data-colspan="1"]')[:int(self.driver.find_elements(By.XPATH,'//*[@data-colspan="1"]').__len__()/2)]
                columns_text = []
                for column in columns:
                    self.scroll_to_element(column)
                    columns_text.append(column.text)

                return dates_column, rows_data, columns_text
            except TimeoutException as toe:
                if retry < max_retries - 1:
                    # Need to add one more day on the first retry
                    if retry == 0:
                        self.add_days_to_date(1)

                    # If latest fetch date is in the future, stop the retry
                    if self.str_to_date(self.latest_fetched_date).date() >= datetime.today().date():
                        return None
                    logger.log(f"{self.latest_fetched_date} date is N/A. Retrying... ({retry+1}/{max_retries})", logger.ERROR)
                    self.add_days_to_date(1)
                    self.__select_date_from_menu(self.str_to_date(self.latest_fetched_date))
                    self.show_station_page()

                    # Add any necessary delay before retrying
                    time.sleep(2)
                else:
                    logger.log(f"Maximum retries reached. Unable to load page.", logger.ERROR)
                    return None
            except Exception as e:
                logger.log(f"Error occurred while loading page: {e}", logger.ERROR)
                return None

    def __get_data_from_table(self, station_name, dates_to_fetch, rows_to_fetch, columns):
        """
        Retrieves data from the table for the specified station, dates, and rows.

        This private method retrieves data from the table for the given station, dates, and rows.
        It populates the `rows` dictionary with the retrieved data and updates the DataFrame (`self.df`) accordingly.

        Args:
            station_name: The name of the station.
            dates_to_fetch: The list of dates to fetch.
            rows_to_fetch: The list of rows to fetch.
            columns: The list of columns in the table.

        Returns:
            None
        """

        # Create an extended list of columns to include station name, date, and hour
        extended_columns = columns.copy()
        extended_columns.extend([DF_COLUMNS[0], DF_COLUMNS[1], DF_COLUMNS[2]])

        # Log the number of columns found for the station - Once per station
        if self.station_data_counter == 0:
            logger.log(f"{len(columns)} columns found for station.")

        # Initialize the rows dictionary and date_string
        rows = {col: [] for col in extended_columns}
        date_string: str = ''

        # Iterate through the rows to fetch and populate the rows dictionary
        for i, row in enumerate(rows_to_fetch):
            date_string, hour_string = self.split_to_str_date_and_hour(dates_to_fetch[i])

            rows.get(DF_COLUMNS[0]).append(station_name)
            rows.get(DF_COLUMNS[1]).append(date_string)
            rows.get(DF_COLUMNS[2]).append(hour_string)

            td_elements = row.find_elements(By.TAG_NAME, "td")

            for j, td_element in enumerate(td_elements):
                self.scroll_to_element(td_element)
                td_text = td_element.find_element(By.TAG_NAME, "div").text
                if j < len(columns):
                    rows.get(columns[j]).append(td_text)
                else:
                    logger.log(f"Failed to find column, index {j} if td element but having total {len(columns)} columns")
                    break
                time.sleep(0.1)

        # Update the latest fetched date and station data counter
        self.latest_fetched_date = date_string
        self.station_data_counter += (len(columns) * len(rows_to_fetch))

        # Create a new DataFrame from the rows dictionary
        new_df = pd.DataFrame.from_dict(rows)

        # Get the common columns between self.df and new_df
        common_columns = list(set(self.df.columns) & set(new_df.columns))

        # Update the merged_df with data from self.df
        self.df = pd.concat([self.df, new_df[common_columns]], ignore_index=True, axis=0)

    def save_df(self, path, station_name: str, date: str):
        """
       Saves the DataFrame to a file.

       This method saves the DataFrame to a file, updates the latest fetched date for the station,
       and saves the modified selected stations dictionary to a data file.

       Args:
           path: The path to the file where the DataFrame will be saved.
           station_name: The name of the station.
           date: The latest fetched date for the station.

       Returns:
           None
       """
        logger.log(f"Saving... {station_name} {date}")
        self.selected_stations.get(station_name)['latest_fetched_date'] = date

        # Remove the 'station_element' key from the selected stations' dictionary to avoid serialization issues
        modified_selected_stations = {}
        for station_name, station_data in self.selected_stations.items():
            modified_selected_stations[station_name] = {k:v for k,v in self.selected_stations[station_name].items() if k != 'station_element'}

        self.data_handler.save_data_params("SELECTED_STATIONS", modified_selected_stations)
        self.data_handler.write_to_file(path, self.df, 'a')
        self.df = self.df.head(1)

    def next_page(self):
        """
        Navigates to the next page.

        This method clicks the next page button to navigate to the next page if it is enabled.
        It handles cases where the next page button is disabled or encounters an ElementClickInterceptedException.

        Returns:
            bool: True if there is a next page and successfully navigates to it, False otherwise.
        """
        # Find the next page button element
        next_page_button = self.wait.until(EC.presence_of_element_located((By.ID,  'reportForward')))
        next_page_button = self.wait.until(EC.visibility_of_element_located((By.ID,  'reportForward')))
        self.scroll_to_element(next_page_button)
        next_page = "disable" not in next_page_button.get_attribute("class")
        if next_page:
            time.sleep(2)
            is_clickable = next_page_button.is_enabled() and next_page_button.is_displayed()
            next_page_button.click()
        else:
            logger.log(f"Finished with {self.current_station_name} station ")
        return next_page

    def exit_station_page(self):
        container = self.driver.find_element(By.ID, "report_result")
        exit_button = container.find_element(By.XPATH, ".//img[@class='popupExitIcon popupWhiteExitIcon']")
        exit_button.click()

    # Date Conversion and Util Methods
    # ------------------------------------
    @staticmethod
    def date_to_str(date: datetime):
        return date.strftime('%d/%m/%Y')

    @staticmethod
    def str_to_date(date: str, pattern: str = None):
        return datetime.strptime(date, pattern if pattern else "%d/%m/%Y")

    def split_to_str_date_and_hour(self, date: str, pattern: str = None):
        date_and_hour = self.str_to_date(date, pattern if pattern else "%H:%M %d/%m/%Y")
        return date_and_hour.strftime("%d/%m/%Y"), date_and_hour.strftime("%H:%M")

    def add_days_to_date(self, delta: int, date=None):
        if date:
            if isinstance(date, str):
                date = self.str_to_date(date)
            next_date = date + timedelta(days=delta)
        else:
            next_date = self.str_to_date(self.latest_fetched_date) + timedelta(days=1)
            self.latest_fetched_date = self.date_to_str(next_date)
        return next_date

    #Scroll to the elements using JavaScript:
    def scroll_to_element(self, element):
        self.driver.execute_script("arguments[0].scrollIntoView();", element)


def scrape_data(df):
    """
    This function handles the scraping phase, attempting to acquire stations data with retries.

    Args:
        df: DataFrame containing the headers (columns) only.

    Returns:
        None
    """
    retry_count = 50  # Number of retries
    retry_delay = 5  # Delay between retries in seconds
    web_scraper = None
    while retry_count > 0:
        try:
            web_scraper = None
            web_scraper = WebScraper(WebScraper.SCRAPE_URL, df)
            logger.log(f"web_scraper.stations_elements {hex(id(web_scraper.stations_elements))}")
            web_scraper.open_menu()
            web_scraper.acquire_stations_data()

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
    # scrape_data(skeleton_df)


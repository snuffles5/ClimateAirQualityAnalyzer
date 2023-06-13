import calendar
import csv
import json
import logging
import os
from datetime import datetime
from enum import Enum
from pathlib import Path

from dateutil import rrule
from openpyxl import Workbook, load_workbook
import pandas as pd
from src.logger import Logger


class DataUtils:
    # Define the project root directory
    PROJECT_ROOT = Path(__file__).resolve().parent
    # Define relative paths
    RAW_DATA_PATH = PROJECT_ROOT / 'data' / 'raw'
    PROCESSED_DATA_PATH = PROJECT_ROOT / 'data' / 'processed'
    RAW_TABLE_FILE_NAME = 'climate_air_quality.csv'
    RAW_DATA_FULL_PATH = RAW_DATA_PATH / RAW_TABLE_FILE_NAME
    PROCESSED_TABLE_FILE_NAME = 'climate_air_quality_proc.csv'
    PROCESSED_DATA_FULL_PATH = PROCESSED_DATA_PATH / PROCESSED_TABLE_FILE_NAME

    def __init__(self):
        self.logger = Logger()

    def verify_path(self, file_path: Path) -> bool:
        if file_path is None:
            self.logger.log("Path is None")
            return False

        if file_path.exists():
            self.logger.log("Path exists.")
            return True
        else:
            self.logger.log("Path does not exist.")
            return False

    @staticmethod
    def save_to_file(file_path: Path, df, mode: str = 'w', encode: str = 'utf-8'):
        file_ext = file_path.suffix.lower()

        if file_ext == '.csv':
            if df is None:
                raise ValueError("No data to save. DataFrame is None.")
            if mode == 'a' and file_path.exists():
                df.to_csv(file_path, mode='a', header=False, index=False, encoding=encode)
            else:
                # Check if the file exists, create it if not
                if not os.path.exists(file_path):
                    open(file_path, 'w').close()
                df.to_csv(file_path, mode='w', index=False, encoding=encode)
        else:
            raise ValueError("Unsupported file format. Only CSV files are supported.")

    def load_file_to_pandas(self, file_path: Path, sheet_name: str = None) -> pd.DataFrame:
        if file_path is None:
            self.logger.log("File path is None")
            return None

        if not file_path.exists():
            self.logger.log("File does not exist")
            return None

        file_format = file_path.suffix.lower()[1:]  # Remove the dot (.) from the extension

        if file_format == 'csv':
            return self.__load_csv_to_pandas(file_path)
        elif file_format == 'xlsx':
            return self.__load_excel_to_pandas(file_path, sheet_name)
        else:
            self.logger.log("Unsupported file format")
            return None

    def __load_csv_to_pandas(self, file_path: Path) -> pd.DataFrame:
        if not self.verify_path(file_path):
            return None

        try:
            df = pd.read_csv(file_path)
            self.logger.log("CSV file loaded into pandas DataFrame successfully.")
            return df
        except Exception as e:
            self.logger.log(f"Error occurred during loading CSV to pandas: {e}")

    def __load_excel_to_pandas(self, file_path: Path, sheet_name: str = None) -> pd.DataFrame:
        if not self.verify_path(file_path):
            return None

        try:
            workbook = load_workbook(file_path)
            if sheet_name:
                sheet = workbook[sheet_name]
            else:
                sheet = workbook.active
            data = sheet.values
            headers = next(data)
            df = pd.DataFrame(data, columns=headers)
            self.logger.log("Excel file loaded into pandas DataFrame successfully.")
            return df
        except Exception as e:
            self.logger.log(f"Error occurred during loading Excel to pandas: {e}")

    def save_data_params(self, key, data, mode: str = 'w', file_path=None, file_name=None):
        # Set default file path if not provided
        if file_path is None:
            file_path = Path("../data/")

        # Create directory if it doesn't exist
        os.makedirs(file_path, exist_ok=True)

        # Create file path with key as the file name
        file_path = file_path / f"{file_name if file_name else 'data_params'}.json"

        try:
            # Load existing JSON data from file
            with open(file_path, 'r', encoding='utf-8') as file:
                existing_data = json.load(file)

            # Update the specific key with new data
            existing_data[key] = data

            # Save updated data back to JSON file
            with open(file_path, mode, encoding='utf-8') as file:
                json.dump(existing_data, file, indent=4, ensure_ascii=False)

            # Log success message
            self.logger.log(f"Data saved successfully for key: {key}", self.logger.DEBUG)

        except Exception as e:
            # Log failure message
            self.logger.log(f"Failed to save data for key: {key}. Error: {e}", self.logger.DEBUG)


    def load_data_params(self, key, file_path=None, file_name=None):
        # Set default file path if not provided
        if file_path is None:
            file_path = Path(self.PROJECT_ROOT) / "data/"

        # Create file path with key as the file name
        file_path = file_path / f"{file_name if file_name else 'data_params'}.json"

        try:
            # Check if file exists
            if file_path.exists():
                # Read data from file
                with open(file_path, "r") as file:
                    data = json.load(file)

                # Check if key exists in the loaded data
                if key in data:
                    # Return the value associated with the key
                    return data[key]

            # Log message if key or file not found
            self.logger.log(f"Data not found for key: {key} or file not found: {file_path}", self.logger.DEBUG)

        except Exception as e:
            # Log failure message
            self.logger.log(f"Failed to load data for key: {key}. Error: {e}", self.logger.DEBUG)

        # Return None if data not found or an error occurred
        return None

    # Date Conversion and Util Methods
    # ------------------------------------
    @staticmethod
    def date_to_str(date: datetime, pattern: str = None):
        return date.strftime(pattern if pattern else "%d/%m/%Y")

    @staticmethod
    def str_to_date(date: str, pattern: str = None):
        return datetime.strptime(date, pattern if pattern else "%d/%m/%Y")

    def split_to_str_date_and_hour(self, date: str, pattern: str = None):
        date_and_hour = self.str_to_date(date, pattern if pattern else "%H:%M %d/%m/%Y")
        return date_and_hour.strftime("%d/%m/%Y"), date_and_hour.strftime("%H:%M")

    @staticmethod
    def get_all_months(start_date: str, end_date: str, pattern: str = None):
        start_date = datetime.strptime(start_date, pattern if pattern else "%d/%m/%Y")
        end_date = datetime.strptime(end_date, pattern if pattern else "%d/%m/%Y")
        months_list = list(rrule.rrule(rrule.MONTHLY, dtstart=start_date, until=end_date))

        return months_list

    @staticmethod
    def get_first_last_days_of_month(month: datetime):
        first_day = datetime(month.year, month.month, 1)
        last_day = datetime(month.year, month.month, calendar.monthrange(month.year, month.month)[1])
        return first_day, last_day

class DF_COLUMNS(Enum):
    STATION = "Station"
    DATE = "Date"
    TIME = "Time"
    PRESSURE = "Pressure"
    RH = "RH"
    TEMP = "Temp"
    WD = "WD"
    WS = "WS"
    PREC = "PREC"
    NO = "NO"
    NO2 = "NO2"
    NOX = "NOX"
    O3 = "O3"
    PM10 = "PM10"
    PM25 = "PM2.5"


class WeatherVariables(Enum):
    API_PRESSURE = "BP"
    DF_PRESSURE = "Pressure"

    API_RELATIVE_HUMIDITY = "RH"
    DF_RELATIVE_HUMIDITY = "RH"

    API_GROUND_TEMPERATURE = "TG"
    DF_GROUND_TEMPERATURE = "Temp"

    API_WIND_DIRECTION = "WD"
    DF_WIND_DIRECTION = "WD"

    API_WIND_SPEED = "WS"
    DF_WIND_SPEED = "WS"

    API_PRECIPITATION = "Rain"
    DF_PRECIPITATION = "PREC"

    API_TO_DF = {
        API_PRESSURE: DF_PRESSURE,
        API_RELATIVE_HUMIDITY: DF_RELATIVE_HUMIDITY,
        API_GROUND_TEMPERATURE: DF_GROUND_TEMPERATURE,
        API_WIND_DIRECTION: DF_WIND_DIRECTION,
        API_WIND_SPEED: DF_WIND_SPEED,
        API_PRECIPITATION: DF_PRECIPITATION
    }

    HOURS_PER_DAY = ["01:00", "07:00", "13:00", "19:00"]


class StationVariables(Enum):
    API_IDS_NAMES_KEY = "API_IDS_ADN_NAMES"
    SCRAPER_NAME_KEY = "SCRAPER_NAME"

    ALON_SHVUT = {
        "API_IDS_ADN_NAMES":
            {77: 'ROSH ZURIM', 286: 'ROSH ZURIM_1m'},
        "SCRAPER_NAME": "אלון שבות, גוש עציון"
        # 31.654463, 35.125797
    }

    BEER_SHEVA = {
        "API_IDS_ADN_NAMES":
            {59: 'BEER SHEVA', 60: 'BEER SHEVA UNI', 293: 'BEER SHEVA_1m', 411: 'BEER SHEVA BGU',
                              412: 'BEER SHEVA BGU_1m'},
        "SCRAPER_NAME": "באר שבע, שכונה ו"
    }

    KARMIEL = {
        "API_IDS_ADN_NAMES":
            {205: 'ESHHAR', 325: 'ESHHAR_1m'},
        "SCRAPER_NAME": "כרמיאל, גליל מערבי"
    }
    # 32.912439, 35.288695

    AFULA = {
        "API_IDS_ADN_NAMES":
            {16: 'AFULA NIR HAEMEQ', 306: 'AFULA NIR HAEMEQ_1m'},
        "SCRAPER_NAME": "עפולה, עפולה"
    }

    TLV = {
        "API_IDS_ADN_NAMES":
            {178: 'TEL AVIV COAST', 299: 'TEL AVIV COAST_1m'},
        "SCRAPER_NAME": "תל אביב-יפו, אוניברסיטה"
    }


if __name__ == '__main__':
    dataut = DataUtils()
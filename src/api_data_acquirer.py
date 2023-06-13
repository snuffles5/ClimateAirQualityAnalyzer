"""
Data sourced from the Israeli Meteorological Service Ministry of Transportation and Road Safety API. Access to the
API was granted after obtaining a specific TOKEN through a request to the office and signing a letter of commitment.

This file contains the functionality to acquire weather data through an API.

Variable Name and Measurement Units:
- BP: Atmospheric Pressure (mb)
- Rain: Precipitation (mm)
- RH: Relative Humidity (%)
- TG: Ground Temperature (degC)
- WD: Wind Direction (deg)
- WS: Wind Speed (m/sec)
"""
from dateutil.relativedelta import relativedelta
from requests import Response, RequestException
from datetime import datetime
from pathlib import Path
import pandas as pd
import requests
import time

from urllib3.exceptions import MaxRetryError, NewConnectionError

from data_utils import StationVariables, WeatherVariables, DF_COLUMNS, DataUtils
from src.logger import Logger

# General Settings
STATION_TRY_LIMIT = 2
logger = Logger()
data_utils = DataUtils()


class APIDataAcquirer:
    stations = [StationVariables.TLV.value,
                StationVariables.AFULA.value,
                StationVariables.KARMIEL.value,
                StationVariables.ALON_SHVUT.value,
                StationVariables.BEER_SHEVA.value]
    api_keys = [WeatherVariables.API_PRESSURE.value,
                WeatherVariables.API_WIND_SPEED.value,
                WeatherVariables.API_PRECIPITATION.value,
                WeatherVariables.API_WIND_DIRECTION.value,
                WeatherVariables.API_RELATIVE_HUMIDITY.value,
                WeatherVariables.API_GROUND_TEMPERATURE.value]

    def __init__(self, api_key: str):
        self.api_key: str = api_key
        self.base_url: str = 'https://api.ims.gov.il/v1'  # Replace with the actual API base URL
        self.headers: dict = self.set_headers()
        df_columns: list = data_utils.load_data_params("DF_COLUMNS")
        self.data_frame: pd.DataFrame = pd.DataFrame(columns=df_columns)
        self.selected_stations: dict = data_utils.load_data_params("API_SELECTED_STATIONS")

    def set_headers(self):
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Accept': '*/*',
            'User-Agent': 'Snuffles v1.0',
            "Authorization": f"ApiToken {self.api_key}"
        }
        return headers

    def fetch_all_data(self):
        """
        Fetches data from all the stations stored in the 'stations' attribute of the instance.

        This method retrieves station names and IDs, then fetches data for each station.
        The fetched data is temporarily stored in the 'rows_data' dictionary, which serves as a skeleton
        for the dataframe that will be created and saved in a method called further along the process.

        Returns:
            bool: True if data was successfully fetched for all stations, False otherwise.
        """

        stations_names_ids = self.get_stations_names_ids(self.stations).items()
        logger.log(f"{len(stations_names_ids)} stations are selected, starting to fetch...")
        fetch_counter = 0
        rows_data = {col: [] for col in [member.value for member in DF_COLUMNS]}
        for station_id, station_name in stations_names_ids:
            self.__fetch_station_data(station_name, station_id, rows_data)
            fetch_counter += 1
        return fetch_counter == len(stations_names_ids)

    def __fetch_station_data(self, station_name: str, station_id: int, rows_data: dict, start_date: str = '', end_date: str = ''):
        """
        Fetches and processes data for a specific station within a date range.

        Parameters:
            station_name (str): The station name.
            station_id (int): The station ID.
            rows_data (dict): A dictionary to populate with fetched data.
            start_date (str, optional): The start date for data fetching. Defaults to station's earliest date.
            end_date (str, optional): The end date for data fetching. Defaults to station's latest date.

        Returns:
            bool: True if data fetched for all months, False if a month's data cannot be fetched after a certain number of attempts.
        """

        logger.log(f"{station_name} [{station_id}] Station")
        start_time = time.time()
        station_no_data_counter = 0
        earliest_date_res, earliest_datetime, earliest_date_str = self.get_earliest_station_data(station_id)
        logger.log(f"Earliest Date {earliest_date_str}")
        if not start_date:
            start_date = self.selected_stations.get(str(station_id))[0]
        if not end_date:
            end_date = self.selected_stations.get(str(station_id))[1]
        if data_utils.str_to_date(start_date).date() >= data_utils.str_to_date(end_date).date():
            logger.log(f"Not fetching {station_name}, start date is after end date {start_date}-{end_date}")
            return False
        if earliest_datetime.date() > data_utils.str_to_date(start_date).date():
            start_date = data_utils.date_to_str(earliest_datetime)
        months = data_utils.get_all_months(start_date, end_date)
        time.sleep(1)
        for month in months:
            first_date_of_month, last_date_of_month = data_utils.get_first_last_days_of_month(month)
            res_data_for_station = self.get_station_data_by_time_range(station_id, first_date_of_month, last_date_of_month)
            if res_data_for_station.status_code != 200:
                logger.log(f"No data for {station_name} station in {month} month \n{res_data_for_station.reason}", logger.ERROR)
                if station_no_data_counter > STATION_TRY_LIMIT:
                    logger.log(f"Reach try limit for {station_name} station", logger.ERROR)
                    return False
                station_no_data_counter += 1
                continue
            logger.log(f"Got data for {month.strftime('%B %Y')} {res_data_for_station.elapsed.total_seconds()}s")
            self.__fetch_monthly_data(res_data_for_station, rows_data, station_id, station_name, [data_utils.date_to_str(month + relativedelta(months=1)), end_date])
            logger.log(f"Done with {month.strftime('%B %Y')} for {station_name}")
        logger.log(f"Done with {station_name} [{time.time() - start_time}s]")
        return True

    def __fetch_monthly_data(self, res_data_for_station: Response, rows_data: dict,  station_id: int, station_name: str, date_range_future_fetch: list):
        """
        Fetches and processes monthly data for a specific station.

        Parameters:
            res_data_for_station (Response): The HTTP response containing the station data.
            rows_data (dict): A dictionary to populate with fetched data.
            station_id (int): The station ID.
            station_name (str): The station name.
            date_range_future_fetch (list): The date range for the next fetch operation.

        Returns:
            bool: True if data processing is successful, False otherwise.
        """
        converted_date: datetime = datetime.min
        if monthly_data_by_interval := res_data_for_station.json():
            if monthly_data_by_interval := monthly_data_by_interval.get('data'):
                for selected_row_data in monthly_data_by_interval:
                    if row_date := selected_row_data.get('datetime'):
                        converted_date = datetime.fromisoformat(row_date)
                        if data_utils.date_to_str(converted_date, '%H:%M') in WeatherVariables.HOURS_PER_DAY.value:
                            self.__extract_hourly_data(selected_row_data, rows_data, converted_date, station_id)
                    else:
                        logger.log(f"No datetime for row", logger.ERROR)
                self.selected_stations[(str(station_id))] = date_range_future_fetch
                data_utils.save_data_params("API_SELECTED_STATIONS", self.selected_stations)
                self.__save_data(DataUtils.RAW_DATA_FULL_PATH, self.data_frame, rows_data)
                logger.log(f"Waiting {res_data_for_station.elapsed.total_seconds()*2} seconds", logger.DEBUG)
                self.adaptive_throttling(res_data_for_station.elapsed.total_seconds())
            else:
                logger.log(f"No data key (in json) for station", logger.ERROR)
                return False
        else:
            logger.log(f"data json is empty for station", logger.ERROR)
            return False
        return True

    def __extract_hourly_data(self, selected_row_data: dict, rows_data: dict, converted_date: datetime, station_id: int):
        """
        Extracts and processes hourly data for a specific station.

        Parameters:
            selected_row_data (dict): The data for a selected row from the response data.
            rows_data (dict): A dictionary to populate with fetched data.
            converted_date (datetime): The converted date from the response data.
            station_id (int): The station ID.

        Returns:
            bool: True indicating successful processing of data.
        """

        fetched_data_counter = 0
        for key in rows_data:
            rows_data[key].append(None)
        row_channels = selected_row_data.get('channels')
        for channel in row_channels:
            rows_data[DF_COLUMNS.STATION.value][-1] = self.get_station_scraper_name(self.stations, station_id)
            rows_data[DF_COLUMNS.DATE.value][-1] = data_utils.date_to_str(converted_date)
            rows_data[DF_COLUMNS.TIME.value][-1] = data_utils.date_to_str(converted_date, '%H:%M')
            if channel.get('valid') and channel.get('name') in self.api_keys:
                rows_data[WeatherVariables.API_TO_DF.value.get((channel.get('name')))][-1] = channel.get('value')
                fetched_data_counter += 1
            logger.log(f"{fetched_data_counter} fetched_data", logger.DEBUG)
        return True

    @staticmethod
    def adaptive_throttling(elapsed_time: float):
        """
        Adjusts waiting time based on the elapsed time of a request to prevent overwhelming the server.

        Parameters:
            elapsed_time (float): The time taken for a previous request.

        Returns:
            None
        """
        if elapsed_time < 1.0:
            # If the response was fast, wait for a shorter period
            time.sleep(0.5)
        elif elapsed_time < 5.0:
            # Response took longer
            time.sleep(1.0)
        else:
            # Response time is very long
            time.sleep(5.0)


    # API Get Methods
    # ------------------------------------
    def get_stations(self):
        url = f"{self.base_url}/envista/stations"
        return self.__get_request(url)

    def get_station_by_num(self, station_number: int):
        url = f"{self.base_url}/envista/stations/{station_number}"
        return self.__get_request(url)

    # example: https://api.ims.gov.il/v1/envista/stations/28/data?from=2015/10/13&to=2015/10/14
    def get_station_data_by_time_range(self, station_number: int, from_date: datetime, to_date: datetime):
        time_range = self.convert_time_range_url(from_date, to_date)
        url = f"{self.base_url}/envista/stations/{station_number}/data/{time_range}"
        return self.__get_request(url)

    def get_earliest_station_data(self, station_number: int):
        url = f"{self.base_url}/envista/stations/{station_number}/data/earliest"
        earliest_date, earliest_date_str, earliest_date_time = None, None, None
        res = self.__get_request(url)
        if res:
            if data := res.json().get('data'):
                earliest_date = data[0].get('datetime') if data and len(data) > 0 else None
                if earliest_date:
                    earliest_date_time = datetime.fromisoformat(earliest_date)
                    earliest_date_str = earliest_date_time.strftime('%B %Y')
        return res, earliest_date_time, earliest_date_str

    def get_daily_station_date(self, station_number: int, date: datetime = datetime.today().date()):
        date = data_utils.date_to_str(date, "%Y/%m/%d")
        url = f"{self.base_url}/envista/stations/{station_number}/data/daily/{date}"
        return self.__get_request(url)

    def __get_request(self, url, **kwargs):
        res = None
        try:
            res = requests.get(url, headers=self.headers)
        except (RequestException, MaxRetryError, NewConnectionError) as e:
            error_message = f"Error occurred during request: {str(e)}"
            logger.log(error_message, logger.ERROR)

        return res

    @staticmethod
    def __save_data(path: Path, data: pd.DataFrame, rows: dict):
        """
        Combines new data with existing data and saves the combined data to a specified path.

        Parameters:
            path (Path): The location to save the data.
            data (pd.DataFrame): The existing data.
            rows (dict): The new data to be added.

        Returns:
            data (pd.DataFrame): A dataframe with only the skeleton (column names).
        """
        new_df = pd.DataFrame.from_dict(rows)
        common_columns = list(set(data.columns) & set(new_df.columns))
        data = pd.concat([data, new_df[common_columns]], ignore_index=True, axis=0)
        DataUtils.save_to_file(path, data, 'a')
        data = data.head(1)
        return data

    # Util Method
    # ------------------------------------
    def convert_time_range_url(self, from_date: datetime, to_date: datetime):
        from_date_str = data_utils.date_to_str(from_date, "%Y/%m/%d")
        to_date_str = data_utils.date_to_str(to_date, "%Y/%m/%d")
        date_range = f"?from={from_date_str}&to={to_date_str}"
        return date_range

    @staticmethod
    def get_all_stations_ids(stations: list) -> list[int]:
        return [id for sub_ids in [list(s.get(StationVariables.API_IDS_NAMES_KEY.value).keys()) for s in stations] for id in sub_ids]

    @staticmethod
    def get_station_scraper_name(stations: list, station_id: int) -> str:
        for station in stations:
            for id in station.get(StationVariables.API_IDS_NAMES_KEY.value).keys():
                if id == station_id:
                    return station.get(StationVariables.SCRAPER_NAME_KEY.value)

    @staticmethod
    def get_stations_names_ids(stations: list) -> dict:
        return {k: v for d in stations for k, v in d.get(StationVariables.API_IDS_NAMES_KEY.value).items()}
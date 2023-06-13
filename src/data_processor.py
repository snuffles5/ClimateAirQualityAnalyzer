"""
This file contains the functionality to handle, clean, and transform weather data.
"""
import pandas as pd
from data_utils import DataUtils, WeatherVariables, DF_COLUMNS
from src.logger import Logger

ROW_DATA_PATH = '../data/raw/'
NA_VALUES = ["Down", "InVld", "NoData", "Calib", "<Samp"]
logger = Logger()

class DataProcessor:
    def __init__(self, utils: DataUtils):
        self.data_utils = utils
        self.df_columns = self.data_utils.load_data_params('DF_COLUMNS')
        self.modified_data_frame = pd.DataFrame

    def convert_data_types(self, data: pd.DataFrame) -> pd.DataFrame:
        if data is None or data.empty:
            # return Empty Data Frame
            return pd.DataFrame() 

        self.modified_data_frame = data.copy()
        from_date_format = '%d/%m/%Y'
        to_date_format = '%Y/%m/%d'
        time_format = '%H:%M'
        base_columns = ['Station', 'Date', 'Time']
        logger.log(f"Formatting date from {from_date_format} to {to_date_format}")
        self.modified_data_frame['Date'] = pd.to_datetime(self.modified_data_frame['Date'], format=from_date_format)
        self.modified_data_frame['Date'] = self.modified_data_frame['Date'].dt.strftime(to_date_format)
        logger.log(f"Formatting time {time_format}")
        self.modified_data_frame['Time'] = pd.to_datetime(self.modified_data_frame['Time'], format=time_format).dt.time
        self.modified_data_frame = self.modified_data_frame.sort_values(by=base_columns, ascending=[True, True, True])
        logger.log(f"Sorting by {base_columns}")

        # Convert relevant columns to numeric type
        numeric_cols = [DF_COLUMNS.PRESSURE.value,
                        DF_COLUMNS.RH.value,
                        DF_COLUMNS.TEMP.value,
                        DF_COLUMNS.WD.value,
                        DF_COLUMNS.WS.value,
                        DF_COLUMNS.PREC.value,
                        DF_COLUMNS.NO.value,
                        DF_COLUMNS.NO2.value,
                        DF_COLUMNS.NOX.value,
                        DF_COLUMNS.O3.value,
                        DF_COLUMNS.PM10.value,
                        DF_COLUMNS.PM25.value]
        self.modified_data_frame[numeric_cols] = self.modified_data_frame[numeric_cols].apply(pd.to_numeric, errors='coerce')
        logger.log(f"Converting numeric cols {numeric_cols}")

        return self.modified_data_frame

    def handle_duplicates(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans the provided DataFrame by removing duplicate rows.

        This method identifies and removes rows in the DataFrame that are complete duplicates.
        Then, it groups the DataFrame by 'Station', 'Date', and 'Time' columns and calculates
        the mean of the other columns for each group. This way, the method effectively merges
        rows that have the same station, date, and time but different values for other columns.

        Parameters:
            data (pd.DataFrame): The input DataFrame to clean.

        Returns:
            pd.DataFrame: The cleaned DataFrame with duplicate rows removed or merged.
        """

        if data is None or data.empty:
            # return Empty Data Frame
            return pd.DataFrame()
            
        logger.log(f"Data Frame before cleaning duplicates: {data.shape}")
        self.modified_data_frame = data.copy()
        len_before = len(self.modified_data_frame)

        # First drop all duplicates with exactly same rows
        self.modified_data_frame.drop_duplicates(keep='last', inplace=True)

        subset_columns = ["Station", "Date", "Time"]
        self.modified_data_frame = self.modified_data_frame.groupby(subset_columns).mean().reset_index()

        # Print the number of removed duplicates and the updated self.modified_data_frameFrame shape
        num_duplicates_removed = len_before - len(self.modified_data_frame.drop_duplicates())
        logger.log(f"Removed {num_duplicates_removed} duplicate rows")
        logger.log(f"Data Frame after cleaning duplicates: {self.modified_data_frame.shape}")

        return self.modified_data_frame

    def handle_missing_values(self, data: pd.DataFrame, columns_threshold: int = 70,
                              rows_threshold: int = 3) -> pd.DataFrame:
        """
        Cleans the provided DataFrame by handling missing values.

        This method first removes rows where all columns (except 'Station', 'Date', and 'Time')
        are missing. Then it removes columns and rows where the percentage of missing data is
        above the specified thresholds.

        Parameters:
            data (pd.DataFrame): The input DataFrame to clean.
            columns_threshold (float): The missing data percentage threshold for dropping columns.
            rows_threshold (float): The missing data percentage threshold for dropping rows.

        Returns:
            pd.DataFrame: The cleaned DataFrame with missing data handled.
        """

        if data is None or data.empty:
            # return Empty Data Frame
            return pd.DataFrame()

        self.modified_data_frame = data.copy()

        # Save the original columns
        original_columns = data.columns.tolist()
        prev_rows_count = data.shape[0]

        # fill missing values with default days limit
        self.__fill_missing_values(self.modified_data_frame, self.modified_data_frame.columns[3:])

        # drop rows where all columns are missing
        subset_columns = ["Station", "Date", "Time"]
        dropped_rows = self.modified_data_frame[self.modified_data_frame.columns.difference(subset_columns)].dropna(how='all')
        self.modified_data_frame.dropna(subset=self.modified_data_frame.columns.difference(subset_columns), how='all', inplace=True)
        logger.log(f"Dropped {prev_rows_count - len(dropped_rows)} rows with all columns missing values")

        # Drop columns and rows where the percentage of missing data is above the specified thresholds
        self.modified_data_frame = self.__remove_corrupted_columns(self.modified_data_frame, columns_threshold)
        self.modified_data_frame = self.__remove_corrupt_rows(self.modified_data_frame, rows_threshold)
        dropped_columns = list(set(original_columns) - set(self.modified_data_frame.columns))
        logger.log(
            f"Dropped {len(original_columns) - len(self.modified_data_frame.columns)} columns: {dropped_columns}, having less than {columns_threshold}% self.modified_data_frame threshold")
        logger.log(f"Data Frame before cleaning corrupted columns: {data.shape}, after {self.modified_data_frame.shape}")

        return self.modified_data_frame

    def __remove_corrupted_columns(self, data: pd.DataFrame, threshold: float) -> pd.DataFrame:
        """
        Removes columns from the DataFrame based on a missing value percentage threshold.

        This method drops any column in the DataFrame where the percentage of missing values
        exceeds the specified threshold.

        Parameters:
            data (pd.DataFrame): The input DataFrame.
            threshold (float): The maximum allowed percentage of missing values per column.

        Returns:
            pd.DataFrame: The cleaned DataFrame with columns containing excessive missing values removed.
        """

        if data is None or data.empty:
            # return Empty Data Frame
            return pd.DataFrame()

        self.modified_data_frame = data.copy()
        missing_percentages = self.modified_data_frame.isnull().mean() * 100

        columns_to_remove = missing_percentages[missing_percentages > threshold].index

        self.modified_data_frame = self.modified_data_frame.drop(columns=columns_to_remove)

        return self.modified_data_frame

    def __remove_corrupt_rows(self, data: pd.DataFrame, number_of_max_missing_cols: int) -> pd.DataFrame:
        """
        Removes rows from the DataFrame based on a missing value threshold.

        This method drops any row in the DataFrame that contains more missing values
        than the specified maximum allowed number.

        Parameters:
            data (pd.DataFrame): The input DataFrame.
            number_of_max_missing_cols (int): The maximum allowed number of missing values per row.

        Returns:
            pd.DataFrame: The cleaned DataFrame with rows containing excessive missing values removed.
        """

        if data is None or data.empty:
            # return Empty Data Frame
            return pd.DataFrame()

        self.modified_data_frame = data.copy()

        initial_rows = self.modified_data_frame.shape[0]
        self.modified_data_frame = self.modified_data_frame.dropna(thresh=self.modified_data_frame.shape[1] - number_of_max_missing_cols)
        dropped_rows = initial_rows - self.modified_data_frame.shape[0]

        if dropped_rows > 0:
            logger.log(
                f"Dropped {dropped_rows} rows with {number_of_max_missing_cols} or more columns missing values")

        return self.modified_data_frame

    def __fill_missing_values(self, data: pd.DataFrame, column_to_fill: list, days_limit: int = 2):
        """
        Forward fills missing values in the specified columns of the input DataFrame up to a certain limit.

        This method fills the missing values in the selected columns by propagating the last observed non-null value
        forward up to a limit defined by 'days_limit' times the number of hours per day.

        Parameters:
            data (pd.DataFrame): The input DataFrame with missing values.
            column_to_fill (list): The list of columns in DataFrame to fill missing values.
            days_limit (int): The number of days to consider for forward fill limit. Defaults to 2.

        Returns:
            pd.DataFrame: The updated DataFrame with filled missing values.
        """

        if data is None or data.empty:
            # return Empty Data Frame
            return pd.DataFrame()

        self.modified_data_frame = data.copy()
        self.modified_data_frame[column_to_fill] = self.modified_data_frame[column_to_fill].fillna(method='ffill', limit=(
                len(WeatherVariables.HOURS_PER_DAY.value) * days_limit))
        logger.log(f"Forward filling [{days_limit} days limit]:\n" +
                        "\n".join(f"{arg}: {data[arg].isna().sum() - self.modified_data_frame[arg].isna().sum()} values "
                                  f"({self.modified_data_frame[arg].isna().sum()} still left)" for arg in column_to_fill))
        return self.modified_data_frame

    def normalize_features(self, data: pd.DataFrame, str_columns: list) -> pd.DataFrame:
        """
        Normalizes categorical features in the DataFrame by converting unique string values to integers.

        This method modifies the given DataFrame by replacing unique string values in the specified columns
        with corresponding integer values.

        Parameters:
            data (pd.DataFrame): The input DataFrame.
            str_columns (list): The list of columns in DataFrame to normalize.

        Returns:
            pd.DataFrame: The updated DataFrame with normalized features.
        """

        if data is None or data.empty:
            # return Empty Data Frame
            return pd.DataFrame()

        self.modified_data_frame = data.copy()
        for column in str_columns:
            logger.log(f"Converting unique values for column {column}")
            unique_values_column = self.modified_data_frame[column].unique()
            replace_map = {uni_val: i + 1 for i, uni_val in enumerate(unique_values_column)}
            logger.log(f"{column}, old to new values: {replace_map}")
            self.modified_data_frame[column].replace(replace_map, inplace=True)
            logger.log(self.modified_data_frame[column].describe())

        return self.modified_data_frame

    def detect_and_handle_outliers(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Detects outliers in a DataFrame based on predefined acceptable ranges. 
        Replaces detected outliers with the respective acceptable limit values.
    
        Parameters:
            data (pd.DataFrame): Input DataFrame that may contain outliers.
    
        Returns:
            pd.DataFrame: DataFrame with outliers replaced by respective acceptable limit values.
        """
    
        if data is None or data.empty:
            # return Empty Data Frame
            return pd.DataFrame()

        self.modified_data_frame = data.copy()

        # Define the acceptable ranges for each variable
        acceptable_ranges = {
            DF_COLUMNS.RH.value: (0, 100),
            DF_COLUMNS.TEMP.value: (-50, 60),
            DF_COLUMNS.WD.value: (0, 360),
            DF_COLUMNS.WS.value: (0, None),
            DF_COLUMNS.PREC.value: (0, None),
            DF_COLUMNS.NO.value: (0, None),
            DF_COLUMNS.NO2.value: (0, None),
            DF_COLUMNS.NOX.value: (0, None),
            DF_COLUMNS.O3.value: (0, None),
            DF_COLUMNS.PM10.value: (0, None)
        }

        outliers = []

        # Iterate over each variable
        for variable, (min_val, max_val) in acceptable_ranges.items():
            column = self.modified_data_frame[variable]

            # Detect outliers based on the acceptable range
            outliers_variable = column[(column < min_val) | (column > max_val)]

            if not outliers_variable.empty:
                outliers.append((variable, outliers_variable))

        # Print the outliers
        for variable, outliers_variable in outliers:
            logger.log(f"Outliers for variable {variable}:")
            logger.log(outliers_variable)

        # Handle the outliers (you can define your own handling logic here)
        for variable, outliers_variable in outliers:
            # Example: Set the outliers to the respective acceptable limits
            self.modified_data_frame.loc[outliers_variable.index, variable] = self.modified_data_frame.loc[
                outliers_variable.index, variable].clip(lower=min_val, upper=max_val)

        return self.modified_data_frame

    # def aggregate_data(self, data: pd.DataFrame) -> pd.DataFrame:
    #     """
    #     Aggregates the weather data based on selected columns.
    # 
    #     Args:
    #         data (pd.DataFrame): The weather data as a pandas DataFrame.
    # 
    #     Returns:
    #         pd.DataFrame: The aggregated weather data.
    #     """
    #     aggregated_data = data.copy()
    #     
    #     group_labels = [0, 1, 2, 3]  # 0: Good, 1: Natural, 2: Bad, 3: Severe
    #     columns = ['NO', 'NO2', 'NOX', 'O3', 'PM10']
    # 
    #     group_ranges = {
    #         'NO': [-np.inf, 10, 20, 30, np.inf],
    #         'NO2': [-np.inf, 10, 20, 30, np.inf],
    #         'NOX': [-np.inf, 20, 40, 60, np.inf],
    #         'O3': [-np.inf, 50, 100, 150, np.inf],
    #         'PM10': [-np.inf, 25, 50, 75, np.inf]
    #     }
    # 
    #     for column in columns:
    #         aggregated_data[f'{column}_Group'] = pd.cut(aggregated_data[column], bins=group_ranges[column],
    #                                                     labels=group_labels, right=False)
    # 
    #     # Print the aggregated groups for each air quality column
    #     for column in columns:
    #         logger.log(f"Aggregated groups for {column}:")
    #         logger.log(aggregated_data[f'{column}_Group'].value_counts())
    # 
    #     return aggregated_data
import logging
import os
from tqdm import tqdm
from colorama import Fore, Style


class Logger:
    CRITICAL = logging.CRITICAL
    ERROR = logging.ERROR
    WARNING = logging.WARNING
    INFO = logging.INFO
    DEBUG = logging.DEBUG

    def __init__(self, log_file='../reports/logs/log.txt', use_progress_bar=False, log_level = logging.INFO):
        self.log_file = log_file
        self.use_progress_bar = use_progress_bar

        # Create logs directory if it doesn't exist
        logs_dir = os.path.dirname(self.log_file)
        os.makedirs(logs_dir, exist_ok=True)

        # Initialize logger
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(log_level)

        # Create file handler and formatter
        file_handler = logging.FileHandler(self.log_file)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        # Add formatter to file handler and file handler to logger
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

    def print_log(self, level, message):
        if level >= self.logger.getEffectiveLevel():
            if level == logging.INFO:
                colored_message = f"{Fore.GREEN}{message}{Style.RESET_ALL}"
            elif level == logging.WARNING:
                colored_message = f"{Fore.YELLOW}{message}{Style.RESET_ALL}"
            elif level == logging.ERROR:
                colored_message = f"{Fore.RED}{message}{Style.RESET_ALL}"
            else:
                colored_message = message
            print(colored_message)


    def log(self, message, level=logging.INFO):
        self.print_log(level, message)
        self.logger.log(level, message)

    def progress_bar(self, iterable, desc='', total=None):
        if self.use_progress_bar:
            iterable = tqdm(iterable, desc=desc, total=total)
        return iterable


# if __name__ == '__main__':
#     pass
    # Initialize logger
    # logger = Logger(log_file='../reports/logs/log.txt', use_progress_bar=True)

    # Write logs
    # logger.log('Starting data collection...')
    # logger.log('Data collection completed.')

    # Progress bar example
    # data = [1, 2, 3, 4, 5]
    # for item in logger.progress_bar(data, desc='Processing data'):
    #     Perform processing on item
        # logger.log(f'Processing item: {item}')

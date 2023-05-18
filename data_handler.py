import csv
from pathlib import Path
from openpyxl import Workbook, load_workbook
import pandas as pd


class DataHandler:
    def __init__(self):
        pass
        # Constructor code here

    def verify_path(self, file_path: Path) -> bool:
        if file_path is None:
            print("Path is None")
            return False

        if file_path.exists():
            print("Path exists.")
            return True
        else:
            print("Path does not exist.")
            return False

    def write_to_file(self, file_path: Path, fields, rows, mode: str = 'w', encode: str = 'utf-8'):
        if file_path is None:
            print("Path is None")
            return

        if not file_path.parent.exists():
            print("Folder doesn't exist")
            file_path.parent.mkdir(parents=True, exist_ok=True)  # Create the parent folder if it doesn't exist

        try:
            # Determine the file format based on the file extension
            file_format = file_path.suffix.lower()[1:]  # Remove the dot (.) from the extension

            if file_format == 'csv':
                self.__write_to_csv(file_path, fields, rows, mode, encode)
            elif file_format == 'xlsx':
                self.__write_to_xlsx(file_path, fields, rows, mode)
            else:
                print("Unsupported file format")
        except Exception as e:
            print(f"Error occurred during writing to file: {e}")

    @staticmethod
    def __write_to_csv(file_path: Path, fields, rows, mode: str = 'w', encode: str = 'utf-8'):
        file_path = file_path.with_suffix('.csv')  # Use with_suffix() to change the extension
        with open(file_path, mode, newline='', encoding=encode) as file:
            csv_writer = csv.writer(file)
            if mode == 'w':
                csv_writer.writerow(fields)
            csv_writer.writerows(rows)

    @staticmethod
    def __write_to_xlsx(file_path: Path, fields, rows, mode: str = 'w'):
        workbook = Workbook()

        if mode == 'a' and file_path.exists():
            workbook = Workbook(file_path)

        sheet = workbook.active
        sheet.sheet_view.rightToLeft = True  # Set RTL direction

        if mode == 'w':
            sheet.append(fields)

        for row in rows:
            sheet.append(row)

        workbook.save(file_path)

    def load_file_to_pandas(self, file_path: Path, sheet_name: str = None) -> pd.DataFrame:
        if file_path is None:
            print("File path is None")
            return None

        if not file_path.exists():
            print("File does not exist")
            return None

        file_format = file_path.suffix.lower()[1:]  # Remove the dot (.) from the extension

        if file_format == 'csv':
            return self.__load_csv_to_pandas(file_path)
        elif file_format == 'xlsx':
            return self.__load_excel_to_pandas(file_path, sheet_name)
        else:
            print("Unsupported file format")
            return None

    def __load_csv_to_pandas(self, file_path: Path) -> pd.DataFrame:
        if not self.verify_path(file_path):
            return None

        try:
            df = pd.read_csv(file_path)
            print("CSV file loaded into pandas DataFrame successfully.")
            return df
        except Exception as e:
            print(f"Error occurred during loading CSV to pandas: {e}")

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
            print("Excel file loaded into pandas DataFrame successfully.")
            return df
        except Exception as e:
            print(f"Error occurred during loading Excel to pandas: {e}")

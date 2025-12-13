import logging
import os
import shutil
import tempfile
import zipfile
from datetime import timedelta
from pathlib import Path

import requests
from asset_model_data_storage.data_storage_service import DataStorageService

from b3.parser import B3HistFileParser
from service.business_day import BusinessDayService

SUCCESS = 200


class B3ScrapperService:
    _URL = 'https://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_D{0}.ZIP'

    def __init__(self, business_day: BusinessDayService):
        self._data_storage_handler = DataStorageService().get_storage_handler()
        self._business_day = business_day

    def fetch_data(self, target_date=None):
        if target_date is None:
            target_date = self._business_day.get_last_business_day()

        if target_date is None:
            raise ValueError("Could not determine target date for fetching data")

        max_retries = 5
        attempts = 0

        while attempts < max_retries:
            file_name = target_date.strftime("%d%m%Y")
            file_path = f'b3/assets/{file_name}.zip'

            try:
                if self._data_storage_handler.file_exists(file_path):
                    logging.info(f"Loading local file: {file_path}")
                    return self._parse_file(file_name, self._data_storage_handler.load_file(file_path))
                else:
                    logging.info(f"File {file_path} not found locally. Scraping...")
                    return self._scrape(file_name, file_path)

            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    logging.warning(f"File {file_name}.zip not found on B3. Trying previous business day...")
                    # Get previous business day relative to current target_date
                    target_date = self._business_day.get_last_business_day(target_date - timedelta(days=1))
                    if target_date is None:
                        raise ValueError("Could not determine previous business day")
                    attempts += 1
                else:
                    raise e

        raise Exception(f"Could not fetch B3 data after {max_retries} attempts.")

    def _scrape(self, file_name: str, file_path: str):
        url = self._URL.format(file_name)
        response = requests.get(url)
        response.raise_for_status()
        return self._handle_zip_file(response.content, file_name, file_path)

    def _handle_zip_file(self, content, file_name, file_path):
        self._data_storage_handler.save_file(file_path, content, content_type='application/zip')
        return self._parse_file(file_name, content)

    @staticmethod
    def _parse_file(file_name, content):
        temp_dir = tempfile.mkdtemp()
        try:
            temp_zip_path = os.path.join(temp_dir, "archive.zip")
            with open(temp_zip_path, "wb") as f:
                f.write(content)

            # Extract the zip
            with zipfile.ZipFile(temp_zip_path, "r") as z:
                z.extractall(temp_dir)
                extracted_files = z.namelist()
                print(f"Extracted files: {extracted_files}")

                # Find the .TXT file in the extracted files
                txt_file = None
                for file in extracted_files:
                    if file.endswith('.TXT'):
                        txt_file = file
                        break

                if not txt_file:
                    raise ValueError(f"No .TXT file found in the zip archive. Extracted files: {extracted_files}")

                # Get the full path to the extracted .TXT file
                extracted_file_path = Path(temp_dir) / txt_file
                print(f"Using file: {extracted_file_path}")

            return B3HistFileParser(str(extracted_file_path)).parse_b3_hist_quota()
        finally:
            shutil.rmtree(temp_dir)
            print(f"Temporary folder {temp_dir} removed.")

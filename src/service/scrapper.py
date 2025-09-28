import os
import shutil
import tempfile
import zipfile
from pathlib import Path

import requests
from asset_model_data_storage.data_storage_service import DataStorageService

from b3.parser import B3HistFileParser

SUCCESS = 200


class B3ScrapperService:
    _URL = 'https://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_D{0}.ZIP'

    def __init__(self):
        self._data_storage_handler = DataStorageService().get_storage_handler()

    def fetch_data(self):
        # file_name = datetime.now().strftime("%d%m%Y")
        file_name = "25092025"  # Using a fixed date for testing purposes
        file_path = f'b3/assets/{file_name}.zip'
        if self._data_storage_handler.file_exists(file_path):
            return self._parse_file(file_name, self._data_storage_handler.load_file(file_path))
        else:
            return self._scrape(file_name, file_path)

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

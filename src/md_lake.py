import duckdb
from b3.parser import B3HistFileParser
from b3.transformer import B3Transformer
import logging

class MotherDuckLakeService(object):
    def __init__(self):
        self._b3_parser = B3HistFileParser(file_path='assets/COTAHIST_M082025.txt')
        self._md = duckdb.connect('md:b3')

    def create_b3_lake(self):
        df = self._b3_parser.parse_b3_hist_quota()
        self._md.execute("CREATE TABLE IF NOT EXISTS b3_hist AS SELECT * FROM df")

    def create_b3_featured_lake(self):
        logging.info(f"Creating B3 featured lake..")
        df = B3Transformer.transform_b3_hist_quota(self._md.execute("SELECT * FROM b3_hist").df())
        self._md.execute("CREATE TABLE IF NOT EXISTS b3_featured AS SELECT * FROM df")


    def delete_lake(self):
        pass

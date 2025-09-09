from md_lake import MotherDuckLakeService
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

class LakeCreatorApp(object):
    def __init__(self):
        load_dotenv()
        self._lake_service = MotherDuckLakeService()

    def main(self):
        # self._lake_service.create_b3_lake()
        self._lake_service.create_b3_featured_lake()
        
if __name__ == '__main__':
    LakeCreatorApp().main()
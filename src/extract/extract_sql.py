import pandas as pd
import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%d/%m/%Y %I:%M:%S %p")
logger = logging.getLogger(__name__)

def estract_api():
    """Extracts data from csv and returns a pandas DataFrame"""
    try:
        logger.info("Starting data extraction from csv.")
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
        csv_path = os.path.join(project_root, 'data', 'the_grammy_awards.csv')
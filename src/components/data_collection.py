import os
import time 
import requests
from bs4 import BeautifulSoup
import pandas as pd
from src.logger import logging

class WeatherDataScraper:
    def __init__(self):
        self.data_dict = {'ntio': '-', 'ntyc': '.', 'ntde': '0', 'ntlm': '1', 'nttu': '2', 'ntcd': '3',
                          'ntbb': '4', 'ntzb': '5', 'nthj': '6', 'ntfs': '7', 'ntas': '8', 'nttn': '9'}
        self.logger = self._setup_logger()

    def _setup_logger(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler = logging.FileHandler('scraper.log')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        return logger

    def list1(self, url):
        try:
            response = requests.get(url)
            response.raise_for_status()
            html_content = response.text
            soup = BeautifulSoup(html_content, 'html.parser')
            table = soup.find('table', class_='medias mensuales numspan')
            row_dict = dict()
            for row in table.find_all('tr')[1:]:
                try:
                    cells = row.find_all('td')
                    row_number = row.find('td').strong.text
                    row_value = []
                    for count, cell in enumerate(cells):
                        span_count = len(cell.find_all('span'))
                        if span_count != 0:
                            span_list = cell.find_all('span')
                            sp_value = ''
                            for sp in span_list:
                                cl = sp.get('class')[0]
                                val = self.data_dict.get(cl)
                                sp_value = sp_value + val
                        else:
                            sp_value = cell.text
                        if count != 0:
                            row_value.append(sp_value)
                    row_dict[row_number] = row_value
                except Exception as e:
                    self.logger.warning(f"Error occurred while parsing row: {str(e)}")
            return row_dict
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error occurred while making a request to the URL: {str(e)}")
            raise

    def get_weather_data(self):
        df1 = pd.DataFrame()
        for year in range(2014, 2023):
            for month in range(1, 13):
                if month < 10:
                    url = 'https://en.tutiempo.net/climate/0{}-{}/ws-421820.html'.format(month, year)
                else:
                    url = 'https://en.tutiempo.net/climate/{}-{}/ws-421820.html'.format(month, year)

                try:
                    a = self.list1(url)
                    df = pd.DataFrame(a).transpose()
                    df["Month"] = month
                    df["Year"] = year
                    df1 = pd.concat([df1, df])
                except Exception as e:
                    self.logger.warning(f"Error occurred for URL: {url}, Error: {str(e)}")
        column_dict = {0: 'T', 1: 'TM', 2: 'Tm', 3: 'SLP', 4: 'H', 5: 'PP', 6: 'VV', 7: 'V', 8: 'VM', 9: 'VG',
                       10: 'RA', 11: 'SN', 12: 'TS', 13: 'FG'}
        df1.rename(columns=column_dict, inplace=True)
        return df1

    def save_to_csv(self, folder_path, file_name):
        try:
            if not os.path.exists(folder_path):
                os.makedirs(folder_path)
            file_path = os.path.join(folder_path, file_name)
            df = self.get_weather_data()
            df.to_csv(file_path, index=False)
            self.logger.info(f"Data saved successfully to CSV file: {file_path}")
        except Exception as e:
            self.logger.error(f"Error occurred while saving data to CSV: {str(e)}")


if __name__ == "__main__":
    weather_scraper = WeatherDataScraper()
    folder_path = 'data/Html_main_data'
    file_name = 'Final_Data.csv'
    weather_scraper.save_to_csv(folder_path, file_name)


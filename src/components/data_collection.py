from bs4 import BeautifulSoup
import requests
import pandas as pd
import os
from src import span_data as sd
import time


class HTMLScraper:
    def __init__(self):
        self.class_data = self.load_span_data()

    def load_span_data(self):
        # Load span data from src/span_data module
        from src import span_data as sd
        return sd.s_d()

    def scrape_data(self, url):
        response = requests.get(url)
        html_content = response.text

        # Create BeautifulSoup object
        soup = BeautifulSoup(html_content, 'html.parser')

        # Find the table element
        table = soup.find('table', class_='medias mensuales numspan')
        row_dict = {}

        # Count the number of span elements with a specific class in each td for every row
        for row in table.find_all('tr')[1:]:
            try:
                cells = row.find_all('td')
                row_number = row.find('td').strong.text
                row_value = []

                for count, cell in enumerate(cells):
                    span_count = len(cell.find_all('span'))
                    if span_count != 0:
                        try:
                            span_list = cell.find_all('span')
                        except Exception as e:
                            pass
                        sp_value = ''
                        for sp in span_list:
                            cl = sp.get('class')[0]
                            val = self.class_data.loc[
                                (self.class_data['col1'] == cl) |
                                (self.class_data['col2'] == cl) |
                                (self.class_data['col3'] == cl) |
                                (self.class_data['col4'] == cl),
                                'col5'
                            ].values[0]
                            sp_value = sp_value + val
                    else:
                        sp_value = cell.text
                    if count != 0:
                        row_value.append(sp_value)
                row_dict[row_number] = row_value
            except Exception as e:
                pass
        return row_dict

    def create_dataframe(self):
        df1 = pd.DataFrame()
        for year in range(2014, 2023):
            for month in range(1, 13):
                if month < 10:
                    url = 'https://en.tutiempo.net/climate/0{}-{}/ws-421820.html'.format(month, year)
                else:
                    url = 'https://en.tutiempo.net/climate/{}-{}/ws-421820.html'.format(month, year)
                data = self.scrape_data(url)
                df = pd.DataFrame(data).transpose()
                df["Month"] = month
                df["Year"] = year
                df1 = pd.concat([df1, df])
        return df1

    def save_dataframe(self, dataframe, folder_path):
        os.makedirs(folder_path, exist_ok=True)
        file_path = os.path.join(folder_path, 'main_data.csv')
        dataframe.to_csv(file_path)
        print(f"Data saved successfully at {file_path}")


if __name__ == "__main__":
    folder_path = 'data/Html_data'
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    scraper = HTMLScraper()
    dataframe = scraper.create_dataframe()
    scraper.save_dataframe(dataframe, folder_path)
    start_time = time.time()
    stop_time = time.time()
    print("Time taken: {}".format(stop_time - start_time))





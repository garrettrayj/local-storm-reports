from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor as PoolExecutor
from datetime import date
from requests.adapters import HTTPAdapter
from requests.exceptions import HTTPError
from requests.packages.urllib3.util.retry import Retry
import csv
import logging
import os
import re
import requests


class Downloader:
    """
    Fetches local storm report CSV files from the Storm Prediction Center's HTTP server.
    """
    base_url = 'https://www.spc.noaa.gov/climo/reports/'
    download_dates: list
    download_dir: str

    def __init__(self, download_dates: list, download_dir: str):
        self.download_dates = download_dates
        self.download_dir = download_dir

        retry_strategy = Retry(total=3)
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session = requests.Session()
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def download_hail_reports(self):
        """
        Download deduplicated hail reports.
        Example Source: https://www.spc.noaa.gov/climo/reports/200426_rpts_raw_hail.csv

        :return:
        """
        with PoolExecutor(max_workers=8) as executor:
            for _ in executor.map(self.download_hail_date, self.download_dates):
                pass

    def download_hail_date(self, download_date: date):
        url = self.base_url + download_date.strftime('%y%m%d') + '_rpts_hail.csv'
        dest = os.path.join(
            self.download_dir,
            'hail_reports_' + download_date.strftime('%Y%m%d') + '.csv'
        )

        if os.path.exists(dest):
            return

        try:
            r = self.session.get(url)
            r.raise_for_status()
        except HTTPError as e:
            logging.warning('%s', e)
            self.scrape_reports(download_date, dest)
            return

        with open(dest, 'w') as f:
            f.write(r.content.decode())

        logging.info('Downloaded hail reports for %s', download_date.isoformat())

    def scrape_reports(self, download_date: date, dest: str):
        url = 'https://www.spc.noaa.gov/climo/reports/{}_rpts.html'.format(download_date.strftime('%y%m%d'))
        r = self.session.get(url)
        soup = BeautifulSoup(r.content.decode(), 'html.parser')
        table_rows = soup.find_all('tr')
        reports = []

        def clean_content(el):
            return re.sub(' +', ' ', el.text).replace('\n', '').replace('\t', '').strip()

        for table_row in table_rows:
            if len(table_row.contents) != 8:
                continue

            size_value = clean_content(table_row.contents[1])
            if not size_value.isdigit():
                continue

            # Filter reports where size less than 1 inch, add exception for 3/4" and 7/8" reports
            size_int = int(size_value)
            if size_int <= 100:
                if size_int != 75 or size_int != 88:
                    continue

            lat_value = clean_content(table_row.contents[5])
            lon_value = clean_content(table_row.contents[6])
            lat = int(lat_value) / 100
            lon = int(lon_value) / -100


            reports.append({
                'Time': clean_content(table_row.contents[0]),
                'Size': size_value,
                'Location': clean_content(table_row.contents[2]),
                'County': clean_content(table_row.contents[3]),
                'State': clean_content(table_row.contents[4]),
                'Lat': lat,
                'Lon': lon,
                'Comments': clean_content(table_row.contents[7])
            })

        if len(reports) > 0:
            with open(dest, 'w') as output_file:
                fieldnames = ['Time', 'Size', 'Location', 'County', 'State', 'Lat', 'Lon', 'Comments']
                output_csv = csv.DictWriter(output_file, fieldnames)
                output_csv.writeheader()
                output_csv.writerows(reports)

        logging.info('Scraped %s reports from %s', len(reports), url)



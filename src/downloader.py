from datetime import date, timedelta
import requests
import logging
import os
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from concurrent.futures import ThreadPoolExecutor as PoolExecutor
from requests.exceptions import HTTPError

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
        Example Source: https://www.spc.noaa.gov/climo/reports/200426_rpts_hail.csv

        :return:
        """
        with PoolExecutor(max_workers=4) as executor:
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
            return

        with open(dest, 'w') as f:
            f.write(r.content.decode())

        logging.info('Downloaded hail reports for %s', download_date.isoformat())




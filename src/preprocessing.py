from datetime import datetime, time
import csv
import glob
import os
import re
from pprint import pprint
import logging
import pytz

class HailReportPreprocessor:
    work_dir: str
    source_dir: str

    def __init__(self, work_dir: str, source_dir: str):
        self.work_dir = work_dir
        self.source_dir = source_dir

    def wrangle_data(self):
        num_errors = num_reports = 0
        dest = os.path.join(self.work_dir, 'hail_reports.csv')
        if os.path.exists(dest):
            os.remove(dest)

        with open(dest, 'w') as output_file:
            fieldnames = ['utc_time', 'convective_date', 'hail_diameter_inches', 'location', 'county', 'state', 'lat', 'lon', 'comments']
            output_csv = csv.DictWriter(output_file, fieldnames)
            output_csv.writeheader()

            for lsr_path in glob.glob(self.source_dir + '/*.csv'):
                date_search = re.search('hail_reports_([0-9]{8}).csv', lsr_path, re.IGNORECASE)
                lsr_date = datetime.strptime(date_search.group(1), '%Y%m%d')

                with open(lsr_path, 'r') as lsr_file:
                    for num, line in enumerate(lsr_file, 1):
                        if num == 1:
                            continue

                        content = line.strip()
                        if not content:
                            continue

                        column_search = re.search(
                            '([^,;]+),([^,;]+),([^,;]*),([^,;]+),([^,;]+),([^,;]+),([^,;]+),(.*)',
                            content
                        )
                        if not column_search:
                            logging.warning(
                                'Invalid row formatting or missing data. Skipping %s, line %s',
                                lsr_path, num
                            )
                            num_errors += 1
                            continue

                        try:
                            lsr_time = time(int(column_search.group(1)[:2]), int(column_search.group(1)[2:]))
                            lsr_datetime = datetime.combine(lsr_date, lsr_time, tzinfo=pytz.timezone('Etc/GMT-12'))
                        except Exception as e:
                            logging.warning(
                                'Invalid time data "%s". Skipping %s, line %s',
                                column_search.group(1), lsr_path, num
                            )
                            num_errors += 1
                            continue

                        try:
                            diameter_in = int(column_search.group(2)) / 100
                        except:
                            logging.warning(
                                'Invalid size data "%s". Skipping %s, line %s',
                                column_search.group(2), lsr_path, num
                            )
                            num_errors += 1
                            continue

                        try:
                            lat = float(column_search.group(6))
                            lon = float(column_search.group(7))
                        except:
                            logging.warning(
                                'Invalid lat,lon coordinates "%s,%s". Skipping %s, line %s',
                                column_search.group(6), column_search.group(7), lsr_path, num
                            )
                            num_errors += 1
                            continue

                        if not 24 < lat < 50 or not -125 < lon < -66:
                            logging.warning(
                                'Coordinates out of range "%s,%s". Skipping %s, line %s',
                                lat, lon, lsr_path, num
                            )
                            num_errors += 1
                            continue

                        report = {
                            'utc_time': lsr_datetime.astimezone(pytz.utc),
                            'convective_date': lsr_datetime,
                            'hail_diameter_inches': diameter_in,
                            'location': column_search.group(3),
                            'county': column_search.group(4),
                            'state': column_search.group(5),
                            'lat': lat,
                            'lon': lon,
                            'comments': column_search.group(8)
                        }
                        output_csv.writerow(report)
                        num_reports += 1

        logging.info(
            'Preprocessing finished! Successfully parsed %s rows. %s rows were skipped due to errors',
            num_reports,
            num_errors
        )

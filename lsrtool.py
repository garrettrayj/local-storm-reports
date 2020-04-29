#!/usr/bin/env python3

from datetime import date, datetime, timedelta
from src.downloader import Downloader
import os
import argparse
import logging

from src.preprocessing import HailReportPreprocessor

parser = argparse.ArgumentParser(
    description='Utilities for working with NOAA Storm Prediction Center local storm reports'
)
parser.add_argument(
    'operation',
    type=str,
    help='The operation to perform on data for the dates in range'
)
parser.add_argument(
    'work_dir',
    nargs='?',
    type=str,
    default=os.path.join(os.path.dirname(__file__), 'lsr_data'),
    help='Working directory for data download and processing',
)
parser.add_argument(
    '--start',
    dest='start_date',
    type=datetime.fromisoformat,
    default='1999-06-01',
    help='Start operations at the given time. Default: 1999-06-01 (beginning of data availability)'
)
parser.add_argument(
    '--end',
    dest='end_date',
    type=datetime.fromisoformat,
    default=(datetime.utcnow() - timedelta(days=1)).date().isoformat(),
    help='Stop operations at the given time. Defaults to date for yesterday.'
)
args = parser.parse_args()

logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)


def get_dates_in_range(start_date: date, end_date: date) -> list:
    date_list = []
    delta = end_date - start_date
    for i in range(delta.days + 1):
        date_list.append(start_date + timedelta(days=i))
    return date_list


if __name__ == "__main__":
    work_dir = args.work_dir
    if not os.path.exists(work_dir):
        os.mkdir(work_dir)

    processing_dates = get_dates_in_range(args.start_date, args.end_date)

    if args.operation == 'download':
        hail_dir = os.path.join(work_dir, 'hail_reports')
        if not os.path.exists(hail_dir):
            os.mkdir(hail_dir)

        downloader = Downloader(processing_dates, hail_dir)
        downloader.download_hail_reports()

    if args.operation == 'preprocess':
        hail_dir = os.path.join(work_dir, 'hail_reports')
        if not os.path.exists(hail_dir):
            raise FileNotFoundError('Missing hail data')

        hail_preprocessor = HailReportPreprocessor(work_dir, hail_dir)
        hail_preprocessor.wrangle_data()



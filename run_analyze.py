"""Constructors analyze"""

import logging

from etl.etl_data import all_etl
from reports.excel import report


def main() -> None:
    """Launch main function of analyze"""
    all_etl()
    report()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    main()

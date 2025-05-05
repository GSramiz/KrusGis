# utils/date_utils.py
from datetime import datetime

def parse_month_year(month_year: str):
    month_str, year_str = month_year.strip().split()
    month = datetime.strptime(month_str, "%B").month
    year = int(year_str)
    return month, year

def is_after_may_2025(month: int, year: int) -> bool:
    return (year > 2025) or (year == 2025 and month > 5)

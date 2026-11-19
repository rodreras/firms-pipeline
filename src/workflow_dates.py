"""Choose inclusive FIRMS workflow dates and split them by publication year."""

import argparse
from collections.abc import Iterator
from datetime import date, timedelta

HISTORY_START = date(2000, 1, 1)
DAILY_WINDOW_DAYS = 7


def intervals(
    mode: str, run_day: date, start: date | None = None, end: date | None = None
) -> Iterator[tuple[date, date]]:
    if mode == "backfill":
        first, last = HISTORY_START, run_day
    else:
        last = end or run_day - timedelta(days=1)
        first = last - timedelta(days=DAILY_WINDOW_DAYS - 1)

    first, last = start or first, end or last
    if not HISTORY_START <= first <= last <= run_day:
        raise ValueError("Choose an ordered range from 2000-01-01 through the run date")
    while first <= last:
        year_end = min(date(first.year, 12, 31), last)
        yield first, year_end
        if year_end == last:
            break
        first = year_end + timedelta(days=1)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("backfill", "daily"))
    parser.add_argument("run_day", type=date.fromisoformat)
    parser.add_argument("--start-date", default="")
    parser.add_argument("--end-date", default="")
    args = parser.parse_args()
    try:
        start = date.fromisoformat(args.start_date) if args.start_date else None
        end = date.fromisoformat(args.end_date) if args.end_date else None
        for first, last in intervals(args.mode, args.run_day, start, end):
            print(first, last)
    except ValueError as error:
        parser.error(str(error))


if __name__ == "__main__":
    main()

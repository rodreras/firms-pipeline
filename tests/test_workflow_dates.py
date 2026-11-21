import unittest
from datetime import date

from src.workflow_dates import intervals


class WorkflowDateTests(unittest.TestCase):
    def test_backfill_covers_every_year_through_run_day(self):
        run = date(2026, 11, 19)
        self.assertEqual(
            list(intervals("backfill", run)),
            [(date(year, 1, 1), date(year, 12, 31)) for year in range(2000, 2026)]
            + [(date(2026, 1, 1), run)],
        )

    def test_daily_catches_up_across_year_boundary(self):
        self.assertEqual(
            list(intervals("daily", date(2026, 1, 3))),
            [
                (date(2025, 12, 27), date(2025, 12, 31)),
                (date(2026, 1, 1), date(2026, 1, 2)),
            ],
        )

    def test_next_scheduled_run_covers_a_missed_run(self):
        missed_run = date(2026, 11, 19)
        next_run = date(2026, 11, 20)
        ((_, missed_day),) = intervals("daily", missed_run)
        ((first, last),) = intervals("daily", next_run)
        self.assertEqual((first, last), (date(2026, 11, 13), date(2026, 11, 19)))
        self.assertLessEqual(first, missed_day)
        self.assertLessEqual(missed_day, last)

    def test_explicit_end_anchors_daily_lookback(self):
        end = date(2025, 10, 8)
        self.assertEqual(
            list(intervals("daily", date(2026, 11, 19), end=end)),
            [(date(2025, 10, 2), end)],
        )

    def test_invalid_ranges_fail_before_any_interval(self):
        run = date(2026, 11, 19)
        for start, end in (
            (date(1999, 12, 31), run),
            (run, date(2026, 11, 18)),
            (run, date(2026, 11, 20)),
        ):
            with self.subTest(start=start, end=end), self.assertRaises(ValueError):
                next(intervals("backfill", run, start, end))

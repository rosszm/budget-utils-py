from unittest import TestCase
from datetime import datetime
from budget import parsers


class ParseSheetTitleTestCase(TestCase):
    def test_parse_title_short(self):
        with self.subTest("uppercase"):
            dt = parsers.parse_month_datetime("JAN2020")
            self.assertEqual(dt, datetime(2020, 1, 1))

        with self.subTest("lowercase"):
            dt = parsers.parse_month_datetime("jan2020")
            self.assertEqual(dt, datetime(2020, 1, 1))

        with self.subTest("capitalized"):
            dt = parsers.parse_month_datetime("Jan2020")
            self.assertEqual(dt, datetime(2020, 1, 1))

        with self.subTest("mixed case"):
            dt = parsers.parse_month_datetime("jAn2020")
            self.assertEqual(dt, datetime(2020, 1, 1))

        with self.subTest("only month"):
            dt = parsers.parse_month_datetime("Jan")
            self.assertEqual(dt, datetime(datetime.now().year, 1, 1))

    def test_parse_title_long(self):
        with self.subTest("uppercase"):
            dt = parsers.parse_month_datetime("JANUARY2020")
            self.assertEqual(dt, datetime(2020, 1, 1))

        with self.subTest("lowercase"):
            dt = parsers.parse_month_datetime("january2020")
            self.assertEqual(dt, datetime(2020, 1, 1))

        with self.subTest("capitalized"):
            dt = parsers.parse_month_datetime("January2020")
            self.assertEqual(dt, datetime(2020, 1, 1))

        with self.subTest("mixed case"):
            dt = parsers.parse_month_datetime("jAnUAry2020")
            self.assertEqual(dt, datetime(2020, 1, 1))

        with self.subTest("only month"):
            dt = parsers.parse_month_datetime("January")
            self.assertEqual(dt, datetime(datetime.now().year, 1, 1))

    def test_parse_title_invalid(self):
        with self.subTest("empty title"):
            dt = parsers.parse_month_datetime("")
            self.assertEqual(dt, None)

        with self.subTest("missing month"):
            dt = parsers.parse_month_datetime("2020")
            self.assertEqual(dt, None)

        with self.subTest("invalid month"):
            dt = parsers.parse_month_datetime("asd2020")
            self.assertEqual(dt, None)

        with self.subTest("invalid year"):
            dt = parsers.parse_month_datetime("feb202020")
            self.assertEqual(dt, None)


class ParseMoneyTestCase(TestCase):
    def test_parse_money_invalid(self):
        with self.subTest("empty string"):
            dt = parsers.parse_money("")
            self.assertEqual(dt, None)

        with self.subTest("non-numeric"):
            dt = parsers.parse_money("hello world")
            self.assertEqual(dt, None)

    def test_parse_title_no_symbol(self):
        with self.subTest("integer"):
            dt = parsers.parse_money("5")
            self.assertEqual(dt, 5.0)

        with self.subTest("trailing zeros"):
            dt = parsers.parse_money("5.00")
            self.assertEqual(dt, 5.0)

        with self.subTest("decimal value"):
            dt = parsers.parse_money("5.67")
            self.assertEqual(dt, 5.67)

    def test_parse_money_currency_symbol(self):
        with self.subTest("decimal value"):
            dt = parsers.parse_money("$5.67")
            self.assertEqual(dt, 5.67)
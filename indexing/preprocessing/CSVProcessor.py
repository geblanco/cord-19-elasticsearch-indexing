"""
Taken from:
https://github.com/johnantonn/cord-19/blob/master/preprocessing/CSVProcessor.py  # noqa: E501
"""
import calendar
from datetime import datetime


class CSVProcessor:
    """ Class that contains methods to process csv data. """

    def fix_date(self, input_date):
        """ Function """
        months = {v: k for k, v in enumerate(calendar.month_abbr)}
        input_date = str(input_date)

        if input_date == "nan":
            return datetime(1970, 1, 1).strftime('%Y-%m-%d')

        if "-" in input_date:
            elements = input_date.split("-")
            if " " in elements[0].strip():
                new_elements = elements[0].strip().split(" ")
                year = int(new_elements[0].strip())
                month_index = int(months[new_elements[1].strip()])
            else:
                year = int(
                    elements[0].replace("[", "").replace("'", "").strip()
                )
                month_index = int(elements[1].strip())
            return datetime(year, month_index, 1).strftime('%Y-%m-%d')

        if len(input_date) == 4:
            return datetime(int(input_date), 1, 1).strftime('%Y-%m-%d')

        if " " in input_date and "[" not in input_date:
            elements = input_date.split(" ")
            year = int(elements[0].strip())

            if elements[1].strip() == "Spring":
                elements[1] = "Mar"
            elif elements[1].strip() == "Summer":
                elements[1] = "Jul"
            elif (
                elements[1].strip() == "Autumn" or
                elements[1].strip() == "Fall"
            ):
                elements[1] = "Oct"
            elif elements[1].strip() == "Winter":
                elements[1] = "Dec"

            if "-" in elements[1].strip():
                month_index = int(months[elements[1].strip().split("-")[0]])
            else:
                month_index = int(months[elements[1].strip()])

            return datetime(year, month_index, 1).strftime('%Y-%m-%d')

        print(input_date)
        return None

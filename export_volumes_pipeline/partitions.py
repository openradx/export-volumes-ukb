from dagster import DailyPartitionsDefinition

from . import constants

daily_partition = DailyPartitionsDefinition(
    start_date=constants.START_DATE, end_date=constants.END_DATE
)

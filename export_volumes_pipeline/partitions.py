import os

from dagster import DailyPartitionsDefinition

daily_partition = DailyPartitionsDefinition(
    start_date=os.environ["PARTITION_START_DATE"],
    end_date=os.environ["PARTITION_END_DATE"],
)

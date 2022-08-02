# tma-training-DE-databricks
## Challenge

About data:

The dataset for this use case contains **heartbeat data collected from health devices of 5 individuals** per hour over a period of 5 months - January 2020 to May 2020. Be mindful about the fact that 2020 was a leap year.

For each month there are two types of files:

1. File ending with month number. E.g.: health_tracker_data_2020_1.json
2. file ending with _late. This file contains records that arrived late. For instance some records for January month arrived in February. So these records will be present in 1_late.json and not in 2.json. This file may or may not be present for each month. E.g.: health_tracker_data_2020_1_late.json

File name format:

1. health_taracker_data_2020_<month>.json
2. health_taracker_data_2020_<month>_late.json

Here month value is:

1. 1 for January
2. 2 for February
3. 3 for March
4. 4 for April
5. 5 for May

Data location: [https://hadoop-and-big-data.s3-us-west-2.amazonaws.com/fitness-tracker/<file_name>.json](https://hadoop-and-big-data.s3-us-west-2.amazonaws.com/fitness-tracker/%3cfile_name%3e.json)

You can use databricks community edition to solve this challenge. Spin up a cluster in databricks workspace and open a notebook to get started.

Challenge:

For this exercise, you’ll be required to do the following:

1. Read raw file and ingest into delta table.
2. Perform batch upload of new data.
3. Perform data quality checks and find issues with quality of data.
4. If there are any issues, then rectify the data and update the table.
5. Scrub personal data of a user.
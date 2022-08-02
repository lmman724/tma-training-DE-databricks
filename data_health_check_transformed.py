import requests
import os

source_url_health_january = "https://hadoop-and-big-data.s3-us-west-2.amazonaws.com/fitness-tracker/health_tracker_data_2020_1.json"

source_url_health_February = "https://hadoop-and-big-data.s3-us-west-2.amazonaws.com/fitness-tracker/health_tracker_data_2020_2.json"

source_url_health_February_late = "https://hadoop-and-big-data.s3-us-west-2.amazonaws.com/fitness-tracker/health_tracker_data_2020_2_late.json"

source_url_health_march   = "https://hadoop-and-big-data.s3-us-west-2.amazonaws.com/fitness-tracker/health_tracker_data_2020_3.json"

source_url_health_april = "https://hadoop-and-big-data.s3-us-west-2.amazonaws.com/fitness-tracker/health_tracker_data_2020_4.json"

source_url_health_may = "https://hadoop-and-big-data.s3-us-west-2.amazonaws.com/fitness-tracker/health_tracker_data_2020_5.json"

health_tracker_data_2020_1 = requests.get(source_url_health_january)
health_tracker_data_2020_2 = requests.get(source_url_health_February)
health_tracker_data_2020_2_late = requests.get(source_url_health_February_late)
health_tracker_data_2020_3 = requests.get(source_url_health_march)
health_tracker_data_2020_4 = requests.get(source_url_health_april)
health_tracker_data_2020_5 = requests.get(source_url_health_may)


print(health_tracker_data_2020_1.text)
print(health_tracker_data_2020_2.text)
print(health_tracker_data_2020_2_late.text)
print(health_tracker_data_2020_3.text)
print(health_tracker_data_2020_4.text)
print(health_tracker_data_2020_5.text)

path_dir_1 = r"C:\Users\lmman\OneDrive\Documents\Git\TMA Project\Azure\tma-training-DE-databricks\heartbeat_data\health_tracker_data_2020_1.txt"
path_dir_2 = r"C:\Users\lmman\OneDrive\Documents\Git\TMA Project\Azure\tma-training-DE-databricks\heartbeat_data\health_tracker_data_2020_2.txt"
path_dir_2_late = r"C:\Users\lmman\OneDrive\Documents\Git\TMA Project\Azure\tma-training-DE-databricks\heartbeat_data\health_tracker_data_2020_2_late.txt"
path_dir_3 = r"C:\Users\lmman\OneDrive\Documents\Git\TMA Project\Azure\tma-training-DE-databricks\heartbeat_data\health_tracker_data_2020_3.txt"
path_dir_4 = r"C:\Users\lmman\OneDrive\Documents\Git\TMA Project\Azure\tma-training-DE-databricks\heartbeat_data\health_tracker_data_2020_4.txt"
path_dir_5 = r"C:\Users\lmman\OneDrive\Documents\Git\TMA Project\Azure\tma-training-DE-databricks\heartbeat_data\health_tracker_data_2020_5.txt"


with open(path_dir_1,'w') as file:
    file.writelines(health_tracker_data_2020_1.text)

with open(path_dir_2,'w') as file:
    file.writelines(health_tracker_data_2020_2.text)

with open(path_dir_2_late,'w') as file:
    file.writelines(health_tracker_data_2020_2_late.text)


with open(path_dir_3,'w') as file:
    file.writelines(health_tracker_data_2020_3.text)

with open(path_dir_4,'w') as file:
    file.writelines(health_tracker_data_2020_4.text)

with open(path_dir_5,'w') as file:
    file.writelines(health_tracker_data_2020_5.text)
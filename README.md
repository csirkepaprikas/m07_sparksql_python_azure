# Spark SQL Homework

### Balázs Mikes

#### github link:
https://github.com/csirkepaprikas/m06_sparkbasics_python_azure.git

This Spark SQL task is designed to introduce me to the world of Databricks and demonstrate how to implement Spark logic using this toolset. The assignment encourages the use of Delta tables, compute clusters, and connection to cloud infrastructure.
As Databricks is widely used in production environments, it is essential to be familiar with this toolset. Additionally, analyzing the Spark plan and deploying notebooks are valuable skills that will be covered in this homework.
Through this assignment, I gained an understanding of how to test Spark applications using these tools.


The actual task was to apply an ETL job – coded in python - on the source datas - saved on Azure Blob storage -: a set of hotels data in csv files and a set of weather datas in parquet format. The execution taoes place on Azure AKS. Both data sets contains longitude and latitude columns but in case of the hotels’ they might be missing or being saved in inappropiate format. The task was to clean this hotels’ data, fill with the proper coordinates by applying an API, then attach geohash to both of the data sources, then join and save them -also on Blob storage- in the same structured, partioned format as the source weather data.


## Here you can see the actual well-documented code:

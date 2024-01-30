# AIRFLOW PROJECTS

## List of projects
1. [Load data from PostgresSQL to S3](#load-data-from-postgressql-to-s3)
2. [Load data from a local storage to S3 and to RedShift](#load-data-from-a-local-storage-to-s3-then-to-redshift)
3. [Transform data from S3 to RedShift by Glue job](#transform-data-from-s3-to-redshift-by-glue-job)


## Preparation

1. Installing dependencies in EC2
```bash
sudo apt update
sudo apt install awscli
sudo apt install python3-pip
sudo apt install python3.10-venv
python3 -m venv airflow_venv
sudo pip install pandas 
sudo pip install s3fs
sudo pip install fsspec
sudo pip install apache-airflow
sudo pip install apache-airflow-providers-postgres
sudo pip install apache-airflow-providers-amazon
```

2. Configuring EC2 instant

    * Add port 8080 of custom TCP for connecting aiflow server through public IP

    ![Something](images/Screenshot%202024-01-13%20183516.png)

3. Adding connections in Airflow

    * AWS connection
    ![Something](images/Screenshot%202024-01-20%20222403.png)

    * PostgresSQL connection
    ![Something](images/Screenshot%202024-01-13%20203056.png)

    * RedShift connection
    ![Something](images/Screenshot%202024-01-21%20215513.png)


## Load data from PostgresSQL to S3
<details>
  <summary>Load more</summary>

* The used data in this project is collected from [weather](https://openweathermap.org)
* The services used in the project, are AWS RDS (PostgresSQL), AWS EC2 (Airflow), AWS S3.
* The data from API and data from [S3 file](weather-pipeline/us-city.csv) that are moved to PostgresSql and then combining tables is converted to CSV file and saved to S3.

![Something](weather-pipeline/images/graph.png)

* The Airflow graph. [code file](weather-pipeline/weather_dag.py)

![Something](weather-pipeline/images/Screenshot%202024-01-15%20180143.png)
</details>

## Load data from a local storage to S3 then to RedShift
<details>
  <summary>Load more</summary>

* The used data in this project is collected from [RapidAPI](https://rapidapi.com/)
* The services used in the project, are AWS EC2 (Airflow), AWS S3, AWS Lambda, AWS RedShift, AWS QuickSight.
* The data from API is saved into S3. The data is transformed and loaded to other S3 buckets by [Lambda triggers](zillow-pipeline/lambda/). Airflow is watching the file in S3 and loading it to RedShift. The data in RedShift is exported to QuickSight.

![Something](zillow-pipeline/images/graph.png)

* The Airflow graph. [code file](zillow-pipeline/zillow_dag.py)


![Something](zillow-pipeline/images/Screenshot%202024-01-22%20001945.png)

* The Data in RedShift. 


![Something](zillow-pipeline/images/Screenshot%202024-01-22%20002936.png)

* The Graphs in QuickSight. 


![Something](zillow-pipeline/images/Screenshot%202024-01-22%20003731.png)
</details>

## Transform data from S3 to RedShift by Glue job
<details>
  <summary>Load more</summary>

* The used data in this project is collected from [kaggle](https://kaggle.com/datasets/yeanzc/telco-customer-churn-ibm-dataset/)

* The services used in the project, are AWS EC2 (Airflow), AWS S3, AWS Glue, AWS RedShift, Power BI.

* The data is saved into S3. The data is transformed and loaded to RedShift by Glue ETL. Airflow is watching the Glue Job. The data in RedShift is exported to Power BI.

* The data can be collected by Glue Crawler. Users can analyse the data through AwS Athena.

![Something](customer-churn-pipeline/images/graph.png)

* The Glue ETL. [configurate file](customer-churn-pipeline/S3-to-redshift.py)

![Something](customer-churn-pipeline/images/Screenshot%202024-01-30%20145847.png)

* The Airflow graph. [code file](customer-churn-pipeline/customer-churn-dag.py)


![Something](customer-churn-pipeline/images/Screenshot%202024-01-26%20172734.png)

* Glue Crawlers.

![Something](customer-churn-pipeline/images/Screenshot%202024-01-30%20104834.png)


* The Data in Athena. 

![Something](customer-churn-pipeline/images/Screenshot%202024-01-24%20165542.png)

* The Graphs in PowerBI. 

![Something](customer-churn-pipeline/images/Screenshot%202024-01-26%20174233.png)
  
</details>
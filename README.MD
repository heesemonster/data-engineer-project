## Data Model 
See 'data_model.png' in the repo root

## How to Run From This Directory:
* Install `requirements.txt`
* `python ./main.py` or `path/to/python.exe ./main.py`
* Depending on OS flavor, may need to use `python3` instead

## Output Files:
- report/genre_data.csv
- report/production_data.csv

## Design

S3 delivery -> Deployed EC2 Airflow ETL -> S3 destination -> Automatic ingest with Redshift/Snowflake -> data mart-esque table served to API

Exposing this data over an HTTP API could be accomplished securely, safely, and in a scalable fashion through a few different methods. A system designed around this ETL process
would be able to safely query the end-result aggregates without worrying about running arbitrary SQL, or mucking around with the data model.

Firstly, and ideally, this transformation process would be scheduled or turned into a discrete programming task - there are several ways to do that (Lambdas, etc.), but I believe that routine, scheduled ETL tasks can work great in a task framework such as Apache Airflow.

Airflow would act as a task orchestrator and execute the ETL on a cron schedule, based around when the monthly data load is supposed to come in. Additional checks could be added to ensure that the delivered file actually exists at the destination. Furthermore, the usage of SQLite would be dropped, and I would opt for dedicated database space on a cloud database.

With "destination" in mind - the most straightforward way to store the pre-and-post ETL data would be utilizing cloud storage - specifically Amazon S3. It has the capability to fire off notifications on data ingress and egress which can act not only as a "true" indicator of new movie data arriving, but could also be used to alert subsequent data layers that new data is "ready".

After the data has ran through the ETL process, it would be stored onto S3, and then in turn an automated process would copy it to a database layer so that the serving table will have the most recent copy of the data. Snowflake has an automatic ingestion called SnowPipe, Redshift can sniff S3 directories, or be triggered, and run "COPY" SQL. I'd opt for Snowflake in this case, since there is no need for OLAP as the data comes in pre-aggregated. Plus, persisting data in a database means fault tolerance - "old" runs won't disappear if something goes wrong, schemas/data can be rolled back, and so on.

Lastly, a Node.js API could be created that would simply fetch this pre-aggregated data at different levels and return them to the end-user. This data hierarchy would be based on year, genre, and production company - the "dims" in the data model. This would be the only step that would require authentication - if the Airflow and DB instances are all deployed in the cloud on a VPC there is no user exposure. Tokens could be handled with a JWT/OAuth 2.0 flow.

Apache Airflow comes with excellent monitoring, and can deploy task failure notifications to Slack, or simply log them. Beyond that, additional application monitoring could be achieved by either: deploying the application in a cloud environment (Kubernetes, etc.) that rolls-their-own or adding in metrics/monitoring code with a framework like Sentry or Prometheus.

The most unscalable part of this system is the Python ETL itself - once data starts growing beyond what Python/SQLite can handle in-memory it will become the bottleneck to this process. Solutions would include: using a Big Data framework such as Spark, using BigTable/HBase to store the data and query in chunks, or using distributed dataframes such as Dask. Additionally, a key/value store such as redis could be utilized for row hashing to prevent redundant data from being processed.
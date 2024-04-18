# Project Architecture

This project is designed to be a robust, scalable, and automated ETL pipeline for processing real-time flight data from the Flightradar24 API, respecting the Medaillon architecture. The architecture of the application includes the following components:
### Data Extraction(Bronze)

The data extraction phase involves pulling data from the Flightradar24 API. This is done using a scheduled job that runs every hour, ensuring that the most recent data is always available for processing. The data is first extracted raw to the bronze bucket as a csv file.
I encountered a problem using the API as it only gets 1500 for every call. So I used get_zone and get_bonds to get flights for all zones, but still I get limited for some busy zones. The solution implented is dividing each zone recursivly when we reach the 1500 limit.
That's what divide_zone in extract.py file do.
The airplanes and airlines are only extracted on the first run of the pipeline and kept for future run as they don't change that frequently. They are saved as csv files on the minio bucket.

### Data Cleaning(Silver)

The data cleaning phase is an essential part of the pipeline. It involves removing duplicates, handling missing values, and performing type conversions as necessary. This phase ensures that the data is in a suitable format for analysis and reduces the likelihood of errors during the processing phase. The data coming from this phase is saved on the silver bucket as parquet file.
In our pipeline, here is the cleaning and transformation done:
    - Flights : Removed duplicates based on "id", change "time" for human readability,
                cast "longitude" and "latitude" as Floats and keep only the columns that
                we need
| id | aircraft_code | time | number | on_ground | airline_icao | origin_airport_name | origin_country | destination_airport_name | destination_country |
|----|---------------|------|--------|-----------|--------------|---------------------|----------------|--------------------------|-------------------|
| 34d26b57 | A359 | 2024-04-18 17:17:22 | SQ24 | 0 | SIA | Singapore Changi Airport | Singapore | New York John F. Kennedy International Airport | United States |

We also have longitude and latitude columns.
### Data Processing(Gold)

The data processing phase involves transforming the cleaned data into a format that is suitable for analysis. To get this bussiness ready data, we aggregate flights with airports and airline. Additionnaly we enrich data with additional information like the continent of the airports and the distance between the origin and the destination of the flight. Finally the data is stored in the gold bucket as a parquet file and ready to answer our business questions.

The silver and gold steps are done using Pyspark which is run on our spark cluster.

### Data Storage

The processed data is stored in a directory with a timestamped naming convention. This allows for easy tracking of data versions and makes it possible to revert to a previous version of the data if necessary. We use Minio for storage because it provides a high-performance, AWS S3 compatible object storage. It's open-source, lightweight, and can be deployed on a variety of hardware platforms. This makes it a flexible and cost-effective solution for our data storage needs. 

### Orchestration

The entire pipeline is orchestrated Prefect. This allows for the scheduling of the extraction, cleaning, and processing tasks, and ensures that the pipeline runs smoothly without human intervention. Prefect was chosen to orchestrate the pipeline due to its robust and flexible framework for building, scheduling, and monitoring complex workflows. It provides a high level of control over task execution and data sharing, and it has excellent error handling capabilities. Prefect's server and UI also provide a centralized location for monitoring the state of your workflows, which is crucial for production deployments. Its Pythonic API makes it easy to integrate with our existing Python-based data processing tasks. Additionally, Prefect's model of "code as config" allows us to version control our workflows just like any other codebase, which aligns with our DevOps practices.

### Monitoring

Monitoring tools can be used to track the performance of the pipeline and alert the team to any issues that arise. This could involve tracking the time taken for each phase of the pipeline, the amount of data processed, and any errors that occur. All this can be done using Prefect.

### Future Developpement

We can add dremio and nessio to load data from our buckets to a external tables to make it more available.
We could also implement dbt for data validation but our pipeline will turn into an ELT instead.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

### Prerequisites

- Docker
- Make


### Installation

#### Configuration
The `config.ini.template` file contains the configuration for the application. Before running the services, you will need to fill in the necessary details.
- The `[path]` section is for specifying the paths to your CSV and Parquet files.
- The `[MINIO]` section is for MinIO configuration. After starting the services, you will need to retrieve the MinIO secret key and access key from the console and fill them in here. You can change the user and password in the docker compose yaml file. The `MINIO_ENDPOINT` is the URL of your MinIO server, and `MINIO_BUCKET` is the name of your bucket.
- The `[SPARK]` section is for specifying the URL of your Spark master.
- The `[API]` section is for setting the limit for your API.

Remember to rename the `config.ini.template` file to `config.ini` after filling in the details.
You can also configure the resource allocated to the spark workers on the docker compose yaml file.

1. Clone the repository:
```sh
git clone
```
2. Navigate to the project directory:
```sh
cd flight-radar
```
3. Build the Docker Compose services:
```sh
make build
```
4. Start the Docker Compose services:
```sh
make up
```
Now we have:
- Minio console [localhost:9001](http://localhost:9001/)
- Prefect server [localhost:4200](http://localhost:4200/)
- spark UI [localhost:8080](http://localhost:8080/)

To run the main script, use the following command:

```sh
make run-main scale=<number of workers>
```

Navigate to the Prefect console and initiate the first run of the pipeline flow. This step is only necessary if you wish to start the flow immediately. Otherwise, the pipeline will automatically execute within an hour, and continue to run on an hourly schedule thereafter.

To run the answers script, use the following command:
```sh
make run-answers question=--help
```
Then you'll see the list of possible questions.

### Running the tests
The tests for this project are located in the tests directory. They can be run using Pytest.

### Acknowledgments

- [Spark cluster in Docker](https://github.com/bitnami/containers/tree/main/bitnami/spark)
- [Why you should use the S3 magic committer to improve performance?](https://spot.io/blog/improve-apache-spark-performance-with-the-s3-magic-committer/)
- [The prefect Hybrid model](https://medium.com/the-prefect-blog/the-prefect-hybrid-model-1b70c7fd296)
- [Prefect with its postgres db with Docker Compose](https://github.com/rpeden/prefect-docker-compose)


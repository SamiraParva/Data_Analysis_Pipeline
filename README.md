# Building an Analysis Data Pipeline with PySpark, AWS, and PostgreSQL

## Introduction

This repository presents an end-to-end solution for reading TSV files from AWS S3, processing them, and importing the data into a PostgreSQL relational database. The scope of this data pipeline covers everything from raw data to BI tables, including data collection, transformation, and preparation for the BI team's requirements.

## Requirements

The reqirement of analysis is as below:

1. **article_performance**: Aggregating simple statistics on how articles have performed.
2. **user_performance**: Aggregating simple statistics on how users interacted with the app.

### S3 Data Details

- Each line in the files represents a collected event, with the first line serving as the header for schema interpretation.
- The `EVENT_NAME` column represents the type of event collected, including:
  - `top_news_card_viewed`: A card from the Top News section viewed by the user.
  - `my_news_card_viewed`: A card from My News section viewed by the user.
  - `article_viewed`: The user clicked on the card and viewed the article in the app's web viewer.
- The `Attributes` column contains the event payload in JSON format, including fields such as `id`, `category`, `url`, `title`, and more.

### Table Structures

<u>article_performance table</u>

| article_id  | date         | title           | category   | card_views | article_views |
|-------------|--------------|-----------------|------------|------------|---------------|
| id1         |  2020-01-01  | Happy New Year! |  Politics  |  1000      |    22         |

<u>user_performance table</u>

| user_id     | date         | ctr   |
|-------------|--------------|-------|
| id1         |  2020-01-01  |0.15   |

- `ctr` (click-through rate) = number of articles viewed / number of cards viewed.
- Load the files directly from S3 instead of manually copying them locally.
- Create staging tables as necessary for any intermediate steps in the process.

## Getting Started

This section explains how to run this application. It has been designed to be straightforward.

### Prerequisites

Ensure you have the following prerequisites:

- Docker and docker-compose (version 3)
- Internet connection to download required Docker images and libraries.

## Installation

Follow the steps below to run the App. put the json files in `kafka/resources` directory. this directory from localhost is mounted to docker kafka server.

running all containers
   ```sh
   $ sudo docker-compose up -d
   ```

After the command prints `etl exited with code 0`, the database is in its final state, with the solution, the data will be inserted to postgres data base and can be accessed with any postgres sql client like psql as follow:
   ```sh
   $ psql -h localhost -p 5432 -U postgres
   ```
Password for user postgres: postgres.
change the default database to ETL:
   ```sh
   $ \c ETL;
   $ SELECT * FROM user_performance;
   $ SELECT * FROM article_performance;
   ```


Some more details:
* The docker compose file will create a Postgres container already connected to the ETL container. Postgres instance is used for storing data models and solution.

* In order to read from AWS S3 respective config should be set in `.env` file.

## Stoping Services
Enter the following command to stop the containers:

```bash
$ sudo docker-compose down -v
```

## Author

👤 **Samira Parvaniani**

- Github: [@SamiraParva](https://github.com/SamiraParva)

## Version History
* 0.1
    * Initial Release

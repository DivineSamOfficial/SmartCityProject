 
![Architecture Diagram](https://github.com/DivineSamOfficial/SmartCityProject/blob/main/Analysis-1.png)
# Smart City End to End Real-Time Data Streaming Pipeline

## Project Overview

This project aims to create a comprehensive real-time data streaming pipeline for a Smart City initiative. It captures and processes real-time data from a vehicle traveling from London to Birmingham, including vehicle data, GPS data, emergency incidents, weather conditions, and camera footage. The pipeline leverages a combination of IoT devices, Apache Kafka, Apache Spark, Docker, and AWS services to ensure efficient data ingestion, processing, storage, and visualization.

## Architecture Overview

![Architecture Diagram](https://github.com/DivineSamOfficial/SmartCityProject/blob/main/SysArch.png)

## Technologies Used

- **IoT Devices**: For capturing real-time data.
- **Apache Zookeeper**: For managing and coordinating Kafka brokers.
- **Apache Kafka**: For real-time data ingestion into different topics.
- **Apache Spark**: For real-time data processing and streaming.
- **Docker**: For containerization and orchestration of Kafka and Spark.
- **Python**: For data processing scripts.
- **AWS Cloud**: 
  - **S3**: For storing processed data as Parquet files.
  - **Glue**: For data extraction and cataloging.
  - **Athena**: For querying processed data.
  - **IAM**: For managing access and permissions.
  - **Redshift**: For data warehousing and analytics.
- **Amazon QuickSight**: For data visualization and dashboarding.

## Project Workflow

1. **Data Ingestion**:
   - IoT devices capture real-time data.
   - Data is ingested into Kafka topics configured in Docker using `docker-compose.yml`.

2. **Data Processing**:
   - Apache Spark reads data from Kafka topics.
   - Spark processes the data and writes it to AWS S3 as Parquet files.
   - Spark Streaming is used for real-time data processing with checkpointing to handle data issues.

3. **Data Storage**:
   - Processed data is stored in AWS S3.
   - AWS Glue crawlers extract data from S3 and catalog it.

4. **Data Querying**:
   - AWS Athena queries the processed and cataloged data from Glue.

5. **Data Visualization**:
   - Amazon QuickSight visualizes the queried data with interactive dashboards.

## Getting Started

### Prerequisites

- Docker and Docker Compose
- AWS Account with appropriate IAM roles and permissions
- Python 3.x
- Apache Kafka and Apache Spark setup

### Setup Instructions

1. **Clone the Repository**:
   ```sh
   git clone https://github.com/SmartCityProject/smart-city-pipeline.git
   cd smart-city-pipeline
   ```

2. **Configure Docker**:
   - Ensure Docker and Docker Compose are installed and running.
   - Configure Kafka and Spark in `docker-compose.yml`.
   - Start the services:
     ```sh
     docker-compose up -d
     ```

3. **AWS Configuration**:
   - Set up AWS IAM roles and permissions.
   - Configure AWS S3 buckets, Glue crawlers, and Athena.
   - Update the configuration files with your AWS credentials and resource details.

4. **Run Data Ingestion**:
   - Start producing data to Kafka topics using IoT data simulators.

5. **Run Spark Streaming**:
   - Submit the Spark job to process and stream data to S3:
     ```sh
     spark-submit --master local[2] your-spark-job.py
     ```

6. **Query Data with Athena**:
   - Use AWS Athena to query the processed data stored in S3.

7. **Visualize Data with QuickSight**:
   - Create an Amazon QuickSight analysis and build your dashboard with the processed data.

## Dashboard

The dashboard created in Amazon QuickSight includes the following visualizations:

- **Total Number of Entries in Each Table**: KPI widgets or bar charts.
- **Average Speed of Vehicles**: Single KPI value.
- **Number of Emergency Incidents**: Single KPI value.
- **Time Series Analysis**: Line charts for vehicle data, weather data, and emergency incidents over time.
- **Geographic Visualization**: Maps for route visualization and vehicle locations.
- **Heat Maps**: For GPS data points density.
- **Detailed Analysis**: Tables and pie charts for detailed data insights.

## Conclusion

This project demonstrates the power of modern data engineering tools to handle complex, real-time data streams and deliver actionable insights for Smart City initiatives. The use of AWS services ensures scalability, reliability, and ease of data management, making it an excellent example of an end-to-end data streaming pipeline.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Thanks to the open-source community for providing the tools and libraries used in this project.
- Special thanks to my team and mentors for their support and guidance.

```

This `README.md` provides a clear and detailed overview of the project, including the technologies used, workflow, setup instructions, and visualizations created in Amazon QuickSight. Adjust any specifics like the repository URL, architecture diagram path, and other details as needed.

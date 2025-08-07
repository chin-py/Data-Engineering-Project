# Real-Time Weather Data ETL Pipeline Using Airflow PySpark Deployed on AWS using EC2 and S3 Service 

## 🌤️ Project Overview

This project implements a comprehensive **real-time weather data ETL pipeline** that automatically extracts current weather information for Indian cities(eg. Pune), processes the data, and stores it in a scalable cloud architecture. The pipeline combines Apache Airflow for orchestration, AWS services for storage, and Apache Spark for data transformation.

##  Architecture

```
OpenWeatherMap API → Apache Airflow (EC2) → AWS S3 (Raw Data) → Databricks Spark → AWS S3 (Processed Data)
```

### Pipeline Flow:
1. **Extract & Load (EL)**: Airflow DAG fetches real-time weather data and stores raw data in S3
2. **Transform (T)**: Databricks processes the raw data using PySpark and SparkSQL
3. **Load**: Transformed data is saved back to S3 for analytics and reporting

##  Technology Stack

- **Orchestration**: Apache Airflow
- **Cloud Infrastructure**: AWS EC2, S3
- **Data Processing**: Apache Spark, PySpark, SparkSQL
- **Analytics Platform**: Databricks
- **Data Source**: OpenWeatherMap API
- **Languages**: Python
- **Data Formats**: CSV, Parquet

## 📊 Data Pipeline Components

### 1. Data Extraction & Loading (Airflow DAG)

The Airflow DAG performs the following tasks:

- **API Health Check**: Validates OpenWeatherMap API availability
- **Data Extraction**: Fetches current weather data for Pune, India
- **Data Processing**: Converts timestamps and structures the data
- **S3 Upload**: Stores processed data as timestamped CSV files

**Key Features:**
- Automatic scheduling (every 10 minutes)
- Error handling and retries
- Data validation and formatting
- Timezone conversion (UTC to IST)

### 2. Data Transformation (Databricks Notebook)

The transformation layer (`OpenWeather-ETL-project.ipynb`) includes:

#### Data Processing:
- **Type Casting**: Converting string columns to appropriate data types
- **Temperature Conversion**: Kelvin to Celsius transformation
- **Feature Engineering**: Creating derived metrics (temperature ranges, daylight hours)
- **Time Extraction**: Year, month, day extraction from timestamps

#### Data Analysis:
- **Aggregations**: Average temperature by day, weather condition counts
- **Grouping**: Statistics by weather type and time periods
- **Categorization**: Temperature binning (Cold, Moderate, Hot)
- **Quality Checks**: Null value handling and data validation

##  Sample Data Structure (raw)

```json
{
  "City": "Pune",
  "Country": "IN",
  "Weather_main": "Clouds",
  "Weather_subtype": "overcast clouds",
  "Temperature": 300.13,
  "Feels_Like": 301.81,
  "Min_Temp": 300.13,
  "Max_Temp": 300.13,
  "Pressure": 1009,
  "Humidity": 68,
  "Visibility": 10000,
  "Wind_speed": 3.02,
  "cloudiness_percent": 98,
  "Rain_mm_hour": 0.0,
  "Time_Recorded_local": "2025-08-07T10:23:58+05:30",
  "Sunrise_local": "2025-08-07T06:14:02+05:30",
  "Sunset_local": "2025-08-07T19:06:44+05:30"
}
```

```bash
### S3 Buckets
RAW_DATA_BUCKET=openweather-etl-extracted-data
PROCESSED_DATA_BUCKET=openweather-etl-transformed-data
```

### Airflow DAG Configuration
- **Schedule**: Every 10 minutes (`timedelta(seconds=600)`)
- **Retries**: 1 with 30-second delay
- **Timeout**: 30 seconds for API calls
- **Catchup**: Disabled

## 📊 Some of the Data Transformations

### Temperature Processing
- Convert Kelvin to Celsius: `(K - 273.15)`
- Create temperature categories: Cold (<10°C), Moderate (10-25°C), Hot (>25°C)
- Calculate temperature ranges and feels-like differences

### Time Processing
- Unix timestamp to ISO format conversion
- Timezone adjustment (UTC to IST +5:30)
- Daylight duration calculation
- Date component extraction (year, month, day)

### Aggregations
- Average temperature by time periods
- Weather condition frequency analysis
- Wind speed statistics by weather type
- Humidity and pressure trends

##  -------------------------------------------------------------

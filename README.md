# 🎬 Hollywood in the Cloud

> An End-to-End Serverless Data Pipeline for Box Office Analysis

A fully automated, cloud-native data engineering project that ingests, processes, and analyzes over 100 years of US Box Office data (1902-2024) using AWS services and Power BI.

## 📊 Project Overview

This project demonstrates a complete ETL pipeline built on AWS infrastructure to answer critical questions about the film industry:

- **What are the long-term trends in movie production?**
- **Which genres are most common vs. most popular?**
- **How do English and Non-English films compare?**
- **What are the most popular foreign languages in film?**

### Target Audience

- **Production Studios** – Data-driven insights for green-lighting new projects
- **Film Distributors** – Strategic analysis for optimal release windows

## 🏗️ Architecture

The pipeline follows a serverless, event-driven architecture:

```
API → Lambda → S3 → Glue → Athena → Power BI
```

### Tech Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| **Security** | AWS IAM | Role-based access control with least privilege permissions |
| **Ingest** | AWS Lambda | Automated data fetching from external API |
| **Storage** | AWS S3 | Data Lake for raw and processed CSV files |
| **Transform** | AWS Glue | Schema discovery via crawlers and data cleaning |
| **Query** | AWS Athena | SQL-based querying of optimized tables |
| **Visualize** | Microsoft Power BI | Interactive dashboard via ODBC connection |

## 🔄 Workflow

1. **Data Ingestion**: Lambda function fetches box office data from API
2. **Raw Storage**: CSV files stored in S3 Data Lake with separated locations
3. **Schema Discovery**: Glue crawlers parse metadata from each CSV
4. **Data Transformation**: Glue jobs clean and optimize data
5. **Unified Table**: Athena creates a queryable unified table
6. **Visualization**: Power BI connects via ODBC for interactive analysis

## 🚧 Challenges & Solutions

### AWS Lambda
- **Challenge**: Function timeout (initial 3 seconds)
- **Solution**: Extended timeout to 15 minutes and configured environment variables

### AWS S3
- **Challenge**: Crawler configuration requirements
- **Solution**: Split Lambda CSV outputs into separate S3 locations (mandatory for crawler operation)

### AWS Glue
- **Challenge**: Schema parsing for multiple data sources
- **Solution**: Configured individual crawler per CSV file

### AWS Athena
- **Challenge**: Unified data access
- **Solution**: Created consolidated table stored in S3

### Power BI
- **Challenge**: Cloud connectivity
- **Solution**: Implemented ODBC service for Athena connection

### AWS IAM
- **Challenge**: Security and permission management
- **Solution**: Created granular roles with specific permissions for each service

## 📈 Key Insights

### Quality vs. Quantity
Drama and Comedy lead in volume, but **Adventure and Action** genres achieve the highest popularity ratings.

### Seasonality Patterns
Peak releases concentrate in **Q4 (October/November)**, strategically targeting:
- Holiday audiences
- Award season consideration

### Language Trends
**French dominates** the foreign language market with 739 films—nearly double that of Italian or Japanese.

## 🛠️ Setup & Deployment

### Prerequisites
- AWS Account with appropriate permissions
- Power BI Desktop or Service
- ODBC driver for Amazon Athena

### AWS Services Configuration

1. **IAM Setup**
   ```bash
   # Create roles with necessary permissions for Lambda, S3, Glue, and Athena
   ```

2. **Lambda Configuration**
   - Set timeout to 15 minutes
   - Configure environment variables
   - Deploy ingestion function

3. **S3 Bucket Structure**
   ```
   s3://your-bucket/
   ├── raw/
   │   ├── dataset1/
   │   ├── dataset2/
   │   └── dataset3/
   └── processed/
   ```

4. **Glue Crawlers**
   - Create one crawler per CSV source
   - Configure to run on schedule or trigger

5. **Athena Setup**
   - Create unified table from crawled metadata
   - Configure query result location

6. **Power BI Connection**
   - Install Athena ODBC driver
   - Configure data source with AWS credentials
   - Build interactive dashboard

## 📊 Dashboard Features

- Historical trend analysis (1902-2024)
- Genre performance comparison
- Language distribution visualization
- Seasonal release pattern analysis
- Popularity vs. volume metrics

## 👤 Author

**Maher Mahmoud Maher**

---

*Presentation Date: November 22, 2025*

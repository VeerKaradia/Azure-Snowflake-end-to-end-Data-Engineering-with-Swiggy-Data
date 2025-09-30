# Data Engineering
## Azure-Snowflake end to end Data Engineering with Swiggy Data

<img width="1104" height="718" alt="Image" src="https://github.com/user-attachments/assets/5d414478-3d49-4fca-869e-6a3825d5894c" />

## Aim
Ever wondered how food delivery and quick-commerce platforms like Swiggy or Zomato handle massive amounts of orders in real time?
In this project, I will demonstrate an end-to-end data pipeline built on Azure and Snowflake for a real-life food aggregator, leveraging the Medallion architecture to ensure scalability, governance, and flexibility across ingestion, transformation, and consumption layers.
We will learn how data moves from  Azure Blob→ Stage schema→ Clean schema→ Consumption schema→ insights, with a step-by-step hands-on experience.

## What I have Learned:
- Understand the food order process flow and design an OLTP source ER diagram.
- Build a 3-layer Snowflake data warehouse (Stage → Clean → Consumption), also known as Bronze → Silver → Gold layers.
- Learn how Azure Event Grid + Storage Queue trigger Snowpipe for automatic data loading.
- Implement Streams & Tasks to handle real-time delta changes.
- Apply Slowly Changing Dimension (SCD Type 2) in the dimensional model.
- Create an interactive Streamlit dashboard for KPIs and insights.

## Design Highlights

- Stage Folder Structure & Snowflake Streams:
       1. Created database and schemas (Stage, Clean, Consumption), file formats, and external stages to connect Snowflake with Azure Blob Storage and internal stage with two folders: initial load and delta for incremental processing.
       2. Stage schema stores raw data for entities such as Customer, Location, Customer Address, Restaurant, Menu, Orders, Order Items, Delivery Agent, Delivery, and Date.
       3. Data validation: Initial and delta CSV files were manually processed across Stage → Clean → Consumption schemas to verify pipeline correctness before Snowpipe automation.
       4. Streams: Snowflake streams on Stage and Clean schemas capture incremental changes, enabling event-driven propagation to the next schema.
       5. Snowpipe: Implemented at the Stage schema level to automatically ingest data from Azure Blob Storage, using Event Grid and Storage Queue notifications to trigger the pipeline for multiple entities. 

-  Azure Cloud Setup & Integration:
       1. Resource Creation: Provisioned Azure Storage Account and Blob containers with hierarchical folder structure..
       2. Identity & Access Management: Secured Snowflake integration with Microsoft Entra ID (Azure AD), granting least-privilege access (Reader at container/folder level and Viewer role for monitoring).
       3. Event-Driven Pipeline: Built an event-driven ingestion pipeline, where Event Grid detects new blobs and notifies Snowpipe via Storage Queue.

 - Automation Using Schedulers:
       1. Task 1: Processes new files from Azure Blob → Stage schema (raw tables).
       2. Task 2: Merges data from Stage → Clean → Consumption schemas, applying transformations, validations, and business rules.
       3. Fully automated, near real-time pipeline with minimal manual intervention.

- Data Transformation & Warehouse Design:
       1. Stage → Clean: Applied data cleansing, deduplication, type casting, and enrichment. Implemented business rules such as state normalization, city tier classification, and surrogate key generation.
       2. Clean → Consumption: Implemented dimensional modeling with SCD Type 2 to track historical changes for each entity. A custom hash key is generated for every record to detect changes efficiently and maintain historical versions.

- Analytics & Visualizationn:
       1. Built interactive dashboards using Streamlit with Python, leveraging curated Snowflake views created from Snowpipe-ingested data to perform near real-time analysis.

## Key Concepts I was able to learn
- How can I load data into Snowflake directly from Azure Blob Storage?
- How does Event Grid + Snowpipe enable near real-time ingestion?
- What’s the role of Streams in capturing incremental changes (delta loads)?
- How do I structure my Stage, Clean, and Consumption schemas for ELT pipelines?
- How to query files in the Stage using $ notation before ingestion?
- How do COPY INTO commands load CSV files into Snowflake tables?
- How can I implement SCD Type 2 for handling historical changes in dimension data?
- How do I secure my pipeline using Azure Entra ID + RBAC?
- How to visualize KPIs and insights with a Streamlit dashboard connected to Snowflake?
- How to set up an automated scheduler (Tasks) for data refresh?
- What best practices ensure data quality, governance, and scalability in the pipeline?

## Azure Cloud
The images illustrate the Azure setup: the Storage Account hierarchy and folders, the Event Grid triggering when new records are added, latency and other metrics tracking, and the Storage Queue receiving notifications.

Customer folder with csv file:
<img width="1913" height="868" alt="storage account" src="https://github.com/user-attachments/assets/78ea7fb3-064b-440b-98e9-3d908720123a" />

Storage Account hierarchy:
<img width="1903" height="855" alt="storage account2" src="https://github.com/user-attachments/assets/8896fcbf-5188-42a4-9b46-d5589f66f585" />

Event Grid Metrics once new file comes in the blob:
<img width="1908" height="863" alt="event_grid" src="https://github.com/user-attachments/assets/c6e3c481-2f60-4e56-bf03-6fa4e2db0a54" />

Latency Metrics:
<img width="1899" height="857" alt="latency" src="https://github.com/user-attachments/assets/1cb4d383-1b2d-4429-a5e2-db5bddd5ee96" />

Event Subscription:
<img width="1866" height="822" alt="event subscription" src="https://github.com/user-attachments/assets/53c48bb6-6a44-4a6e-995e-843ef0ee16d7" />


When new data is added to the Storage Account, Event Grid sends a topic to the Storage Queue, and Snowpipe reads the notification to automatically ingest the data: 
Note: Notifications are intentionally shown in the Storage Queue for demonstration purposes; once Snowpipe is integrated, they are processed immediately, and the notifications page remains empty.
<img width="1860" height="859" alt="notfication" src="https://github.com/user-attachments/assets/8fe515fc-bdc5-4a11-8ee7-695ebeefe22d" />







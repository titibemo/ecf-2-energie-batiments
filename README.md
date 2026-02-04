![Logo](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/th5xamgrr6se0x5ro4g6.png)
 
# **Project Name: ECF2 - ENERGIE BATIMENTS**  
 
## Table of Contents
 
1. [About the Project](#about-the-project)
2. [Built With](#built-with)
3. [Getting Started](#getting-started)
   - [Prerequisites](#prerequisites)
   - [Project structure](#project-structure)
4. [Installation and configuration](#installation-and-configuration)
   - [Installation](#installation)
   - [Configuration](#configuration)
        -   [Docker-compose](#docker-compose)
5. [Usage](#usage)
   - [Cleaning data](#cleaning-data)
   - [Notebooks](#Notebooks)
   - [Uninstall](#uninstall)
6. [Authors](#authors)
7. [License](#license)
 
---
 
## About the Project
 
This project aims to demonstrate my skills in data engineering and management by showcasing a data processing pipeline integrated. The project includes the ingestion, cleaning, analysis, and visualization of electricity, gas, and water consumption data, enriched with weather data and building characteristics..
 
### **Project goals include:**  
 
**C2.2 – Process Structured Data Using a Programming Language**

- Implement data transformation pipelines using PySpark
- Clean and standardize data with Pandas
- Develop reusable data processing functions (Spark UDFs, Pandas functions)
- Handle errors and edge cases effectively

**C2.3 – Analyze Structured Data to Meet Business Requirements**

- Compute key energy performance indicators (KPIs)
- Identify correlations between energy consumption and external factors (weather, building characteristics)
- Detect anomalies and propose corrective actions
- Produce actionable statistical analyses

**C2.4 – Present Analyzed Data in a Clear and Insightful Manner**

- Design professional data visualizations using Matplotlib
- Perform advanced visual analyses with Seaborn
- Build a concise, multi-panel executive dashboard
- Write a summary report with actionable recommendations
---
 
## Built With
 
The following technologies are used in this project:
 
https://github.com/inttter/md-badges


- ![Python](https://img.shields.io/badge/Python-FFD43B?style=for-the-badge&logo=python&logoColor=blue) **Version 3.11** – Main programming language used for data processing and analysis.  
- ![PySpark](https://img.shields.io/badge/PySpark-F26207?logo=replit&logoColor=fff) **Version 3.5.7** – Used for large-scale data processing and cleaning.  
- ![Docker](https://img.shields.io/badge/Docker-2CA5E0?style=for-the-badge&logo=docker&logoColor=white) Used to **containerize** the application and ensure a consistent runtime environment.  
- ![GitHub](https://img.shields.io/badge/GitHub-24292F?style=for-the-badge&logo=github&logoColor=white) Used for **version control and collaboration**, keeping the project organized and maintainable.  
- ![VSCode](https://img.shields.io/badge/VSCode-007ACC?style=for-the-badge&logo=visualstudiocode&logoColor=white) **IDE** used for development, debugging, and running the project locally.  
- ![Matplotlib](https://custom-icon-badges.demolab.com/badge/Matplotlib-71D291?logo=matplotlib&logoColor=fff) Used to **create plots and visualizations** for data exploration and reporting.  
- ![Seaborn](https://img.shields.io/badge/Seaborn-EC407A?logo=seaborn&logoColor=fff) Used for **statistical data visualization**, especially heatmaps, boxplots, and regression plots.  
- ![Pandas](https://img.shields.io/badge/Pandas-150458?logo=pandas&logoColor=fff) Used for **data manipulation and analysis**, working with CSV, Parquet, and enriched datasets.  
- ![NumPy](https://img.shields.io/badge/NumPy-4DABCF?logo=numpy&logoColor=fff) Used for **numerical computing and array operations**, supporting analysis and calculations.  

 
# Getting Started
 
## Prerequisites
 
Before you can install the project, make sure you have the following installed on your machine and to have basic knowledge of the following technologies:
 
- **Docker**: For containerizing the application to ensure a consistent environment.
  - [Install Docker](https://www.docker.com/products/docker-desktop/) if you don't have it already.
- **python**: In version 3.10+.
  - [Install python](https://www.python.org/downloads/) according to your operating system.


## Project structure

Complete project structure (Finished) :

```bash

C:.
│   .gitignore                 # Git ignore file
│   docker-compose.yml          # Docker Compose setup for PySpark 3.5.7
│   README.md                   # Project documentation
│   requirements.txt            # Python libraries required for the project
│   submit.sh                   # Script to submit jobs to Spark
│
├── .venv                      # Virtual environment for Python dependencies
│
├── apps                        # Python scripts for data processing
│   └── 02_nettoyage_spark.py   # Data cleaning with PySpark
│
├── data                        # Raw data files (CSV)
│   generate_data_ecf.py        # Script to regenerate data if there are issues or in the beginning
│   │
│   ├── output                  # Processed results are saved here
│   │   ├── consommations_enrichies.csv  # Full enriched dataset (large CSV)
│   │   ├── meteo_clean.csv                # Cleaned meteorological data
│   │   │
│   │   ├── consommations_clean           # Parquet files of cleaned consumption
│   │   │   ├── ._SUCCESS.crc
│   │   │   ├── _SUCCESS
│   │   │   └── ...                        # Partitioned by date and energy type
│   │   │       └── date=2023-01-01
│   │   │           └── type_energie=eau
│   │   │               ├── .part-00000-18701f6a-6194-48ce-9952-c431446578c4.c000
│   │   │               └── part-00007-18701f6a-6194-48ce-9952-c431446578c4.c000
│   │   │
│   │   └── graphics                     # Saved visualizations
│   │       ├── 06_consommation_mensuelle.png
│   │       ├── 07_impact_solaire_electrique.png
│   │       ├── 07_impact_temperature_gaz.png
│   │       └── 07_matrice_correlation.png
│
└── notebooks                           # Jupyter notebooks for analysis
    ├── 01_exploration_spark.ipynb
    ├── 03_agregations_spark.ipynb      # Currently empty
    ├── 04_nettoyage_meteo_pandas.ipynb # Cleaning and processing weather data
    ├── 05_fusion_enrichissement.ipynb  # Merge and enrich consumption + weather + building + tariffs
    ├── 06_statistiques_descriptives.ipynb  # Descriptive stats, trends, seasonality
    ├── 07_analyse_correlations.ipynb   # Correlation analysis and visualizations
    └── 08_detection_anomalies.ipynb    # Detect anomalies in the data



```

# Installation and configuration

## Installation
 
To install and set up the project, follow these steps:
 
1. Clone the repository:
```bash
git clone https://github.com/titibemo/ecf-2-energie-batiments
```

2. Create a virtual environment to isolate dependencies : Navigate to the project root, open a terminal and run the followinf commands:
```bash
# On windows
python -m venv venv 
venv\Scripts\activate         
```

Install the required packages :
```bash
pip install -r requirements.txt  
```
 
## Configuration

### Docker-compose

```yml
services:
  spark-master:
    image: apache/spark:3.5.3
    container_name: spark-master
    hostname: spark-master
    command: /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master
    environment:
      - SPARK_MASTER_HOST=spark-master
      - SPARK_MASTER_PORT=7077
      - SPARK_MASTER_WEBUI_PORT=8080
    ports:
      - "8080:8080"
      - "7070:7077"
      - "4050:4040"
    volumes:
      - ./data:/data
      - ./apps:/apps
    networks:
      - spark-network

  spark-worker-1:
    image: apache/spark:3.5.3
    container_name: spark-worker-1
    hostname: spark-worker-1
    command: /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077
    environment:
      - SPARK_WORKER_CORES=2
      - SPARK_WORKER_MEMORY=2g
    depends_on:
      - spark-master
    volumes:
      - ./data:/data
      - ./apps:/apps
    networks:
      - spark-network

  spark-worker-2:
    image: apache/spark:3.5.3
    container_name: spark-worker-2
    hostname: spark-worker-2
    command: /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077
    environment:
      - SPARK_WORKER_CORES=2
      - SPARK_WORKER_MEMORY=2g
    depends_on:
      - spark-master
    volumes:
      - ./data:/data
      - ./apps:/apps
    networks:
      - spark-network

  spark-worker-3:
    image: apache/spark:3.5.3
    container_name: spark-worker-3
    hostname: spark-worker-3
    command: /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077
    environment:
      - SPARK_WORKER_CORES=2
      - SPARK_WORKER_MEMORY=2g
    depends_on:
      - spark-master
    volumes:
      - ./data:/data
      - ./apps:/apps
    networks:
      - spark-network

  jupyter:
    image: jupyter/pyspark-notebook:latest
    container_name: spark-jupyter
    environment:
      - JUPYTER_ENABLE_LAB=yes
      - SPARK_MASTER=spark://spark-master:7077
    ports:
      - "8888:8888"
    volumes:
      - ./notebooks:/home/jovyan/work
      - ./data:/home/jovyan/data
    depends_on:
      - spark-master
    networks:
      - spark-network

networks:
  spark-network:
    driver: bridge

```


## Usage

### Generate Data

At the beginning, the `data` folder is empty. To generate the datasets, run the following command in your IDE terminal:

```bash
python data/generate_data_ecf.py
```

This command will generate four files that will be used throughout the project:
- batiments.csv # Building characteristics
- consommations_raw.csv # Raw energy consumption data
- meteo_raw.csv # Raw meteorological data
- tarifs_energie.csv # Energy tariffs


### Cleaning data

**Note**: Before cleaning the data, it is recommended to explore the `consommations_raw.csv` file using the notebook `01_exploration_spark.ipynb`.
This notebook highlights potential issues, anomalies, and provides initial statistics that will help guide the cleaning process.

From the root of the application, open a terminal and run the following command to dockerize the PySpark application:

```bash
docker compose up -d
```

Once the application is running, open a terminal and execute the following command to clean the data:

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /apps/02_nettoyage_spark.py
```

This command cleans the data from consommations_raw.csv. It performs the following actions:

- Converts all timestamps into a unified format for consistency
- Cleans and standardizes consumption values
- Removes duplicate records

At the same time, a cleaning report is generated, and the cleaned dataset is saved in Parquet format in the /data/output/consommations_clean directory.

**Note**: It will take about 30 minutes minimum to complete the process.

# Notebooks

**Note:**  
- Each notebook serves a specific purpose; the project is composed of seven notebooks.  
- Every saved figure shown below will be stored in the `data/output/graphics` folder with a filename corresponding to the notebook number, for example: `07_impact_solaire_electrique.png`.

---

### 01_exploration_spark
Using the `consommations_raw.csv` dataset, this notebook highlights data anomalies. A report is generated at the end of the notebook.

### 03_agregations_spark
*TODO*

### 04_nettoyage_meteo_pandas
Using the `meteo_raw.csv` dataset, the data is cleaned (standardized timestamps, temperature and humidity converted to numeric columns, removal of abnormal values) and enriched by adding new columns: **day, month, season, and day_of_week**.

### 05_fusion_enrichissement
In this notebook, we retrieve the data created in `data/output/consommations_clean` and merge it with the cleaned meteorological data, building characteristics, and tariff data.  
The complete dataframe is saved to the output folder as `consommations_enrichies.csv`.  

**Note:** This CSV file is quite large (approximately 1.7 GB).

### 06_statistiques_descriptives
We load the `consommations_enrichies.csv` file and perform descriptive statistical analysis regarding energy type, municipalities, etc.  
Temporal evolution analysis (monthly trends, seasonality) is saved to the output folder as `consommations_mensuelle.png`.

### 07_analyse_correlations
In this notebook, we analyze and visualize several elements:  
- **Impact of temperature on heating consumption:** saved as `07_impact_temperature_gaz.png`.  
- **Effect of solar radiation on electricity consumption:** saved as `07_impact_solaire_electrique.png`.

### 08_agregations_spark
*TODO*

### 09_visualisations_matplotlib
In this notebook, we will use **Matplotlib** (and Seaborn) to generate visualizations for the following statistics:

- **Temporal evolution of total consumption by energy type** (line plot)  
- **Distribution of consumption by building type** (boxplot)  
- **Heatmap of average consumption by hour and day of the week**  
- **Scatter plot of temperature vs heating consumption with regression**  
- **Comparison of average consumption by energy class (DPE)** (bar chart)

### 10_visualisations_seaborn
In this notebook, we will use **seaborn** (and Seaborn) to generate visualizations for the following statistics:

- **Pairplot of energy consumption** (electricity, gas, water) by season  
- **Violin plot of electricity consumption** by building type  
- **Annotated heatmap of the complete correlation matrix**  
- **FacetGrid: monthly consumption trends by municipality** (top 6 municipalities)  
- **Jointplot: relationship between building surface area and consumption**, with marginal distributions  
- **Catplot: energy consumption by energy class (DPE) and building type**

---
## Uninstall

To uninstall the project and remove all associated volumes, run the following command:

```bash
docker compose down -v
```
---
 
## Authors
 
- [GitHub Profile](https://github.com/titibemo)
 
---
 
## License
 
This project is open-source and can be freely copied, modified, and distributed by anyone. No specific license is provided, but contributions and usage are welcome.
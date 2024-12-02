# Project Title: Insightful Data Pipeline

This repository contains the data pipeline scripts for the Insightful project, aimed at processing data step by step using modular components. The pipeline consists of preprocessing steps, followed by the main data processing, with code separated into different modules for maintainability and reuse.

## Folder Structure

```
.
├── pipeline
│   ├── __pycache__
│   ├── configs
│   │   ├── preprocessing_config.yaml
│   │   └── processing_config.yaml
│   ├── data
│   ├── mappings
│   ├── processing
│   │   ├── __init__.py
│   │   └── processing.py
│   ├── utils
│   │   ├── __init__.py
│   │   └── utils.py
│   ├── main_preprocess.py
│   └── main_process.py
├── README.md
├── poc-despot.ipynb
├── poc-njegos.ipynb
```

## Prerequisites

- Python 3.7+
- Required Python packages: Install using `requirements.txt` or manually install the dependencies

```sh
pip install -r requirements.txt
```

## Steps to Run the Pipeline

### Step 1: Clone the Repository

Clone this repository to your local machine:

```sh
git clone https://github.com/despot-logikka/Insightful-poc.git
```

Navigate to the project directory:

```sh
cd Insightful-poc
```

### Step 2: Navigate to the `pipeline` Directory

To run the main preprocessing and processing scripts, navigate to the `pipeline` directory:

```sh
cd pipeline
```

### Step 3: Preprocessing Step

The preprocessing step involves using the `main_preprocess.py` script to preprocess the raw data. This script relies on the configurations specified in `configs/preprocessing_config.yaml`.

Run the preprocessing script:

```sh
python main_preprocess.py
```

This will read the raw data, apply transformations defined in the configuration, and save the intermediate processed data to be used in the next step.

### Step 4: Main Processing Step

After preprocessing is complete, run the main processing script to complete the data pipeline.

```sh
python main_process.py
```

This script processes the preprocessed data further, performing transformations as defined in `configs/processing_config.yaml`.

### Step 5: Inspect the Results

After running the pipeline, the final output data and logs can be found in the `data` directory or any other directory defined in the configuration files.

## Overview of Code Structure

- **processing/**: Contains code related to data processing, e.g., transformations and data cleaning.
  - **processing.py**: Core functions for processing the data.
- **utils/**: Contains utility functions that assist with various operations.
  - **utils.py**: General utility functions used throughout the pipeline.
- **configs/**: YAML configuration files for setting up different parameters for preprocessing and main processing.
- **main_preprocess.py**: The entry script for data preprocessing.
- **main_process.py**: The entry script for data processing after preprocessing is complete.
# Data Cleaning ETL Pipeline

This project provides a simple Extract, Transform, and Load (ETL) pipeline for cleaning a dataset. The dataset in this example contains Chinese column names related to vehicle and personal information, which are translated into English. The pipeline performs various data cleaning tasks, including column name translation, email validation, phone number cleaning, and removal of invalid or duplicate records.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)

## Overview

This ETL pipeline processes a dataset by:
1. **Extracting** data from a CSV file.
2. **Transforming** the data to clean emails, phone numbers, and remove invalid or duplicate records.
3. **Loading** the cleaned dataset into a new CSV file and storing invalid records in a separate file.

### Key Objectives:
- Translate Chinese column names into English.
- Validate and clean email addresses.
- Clean phone numbers by removing non-digit characters.
- Remove rows missing critical fields and duplicates.
- Save the cleaned data and invalid records separately.

## Features

- **Column Translation**: Translates Chinese column names to English using a dictionary.
- **Email Validation**: Detects invalid or placeholder emails and replaces them with `NULL`.
- **Phone Number Cleaning**: Strips non-digit characters from phone numbers.
- **Critical Field Validation**: Removes records where all critical fields (VIN, Name, ID_Number, Phone, Email) are missing.
- **Duplicate Removal**: Detects and removes duplicate records based on VIN, ID number, and email.
- **File Output**: Saves the cleaned data and invalid records to separate CSV files.


## Installation

### Requirements:

- Python 3.x
- Pandas
- Regular Expressions (`re` module)

To install the required libraries, run:
```bash
pip install pandas
```
## Usage
Clone the repository:
```
git clone https://github.com/yourusername/data-cleaning-pipeline.git
cd data-cleaning-pipeline
```

# -*- coding: utf-8 -*-


"""Official Cleaner (China)"""

import pandas as pd
import re

# File paths for input, output, and garbage files
input_file = '/content/China-2020.csv'
output_file = 'cleaned_china14.csv'
garbage_file = 'RemovedRecords_14.csv'

# Corrected dictionary for translating column names
column_translation = {
    '车架号': 'VIN',
    '姓名': 'Name',
    '身份证': 'ID_Number',
    '手机': 'Phone',
    '邮箱': 'Email',
    '省': 'Province',
    '城市': 'City',
    '地址': 'Address',
    '邮编': 'Postal_Code',
    '生日': 'Birthday',
    '行业': 'Industry',
    '月薪': 'Monthly_Salary',
    '婚姻': 'Marital_Status',
    '教育': 'Education',
    'BRAND': 'Brand',
    '车系': 'Car_Series',
    '车型': 'Car_Model',
    '配置': 'Configuration',
    '颜色': 'Color',
    '发动机号': 'Engine_Number'
}

def extract(input_file):
    # Extract the dataset by reading CSV with utf-8-sig encoding
    df = pd.read_csv(input_file, encoding='UTF-8-SIG')

    # Translate column names based on corrected mapping
    df = df.rename(columns=column_translation)

    return df

def transform(df):
    # Initialize a dataframe for invalid rows
    invalid_rows = pd.DataFrame()

    # Step 1: Replace the entire email with 'NULL' if it contains 'noemail' or similar variations
    df['Email'] = df['Email'].replace(
        to_replace=r'.*(noemai|nomai|noemia|nomea|noemal|noeami|nomei|noma|noemil|noeai|NOEMAIL).*',
        value='NULL', regex=True
    )

    # Step 2: Clean and validate email formats
    def validate_email(email):
        if isinstance(email, str) and email != 'NULL':
            pattern = r"^[\w\.-]+@[\w\.-]+\.\w+$"  # Simple regex for email validation
            if re.match(pattern, email):
                return email
        return None

    # Step 3: Remove non-digit characters from Phone column
    def clean_phone(phone):
        if isinstance(phone, str):
            return re.sub(r'\D', '', phone)  # Keep only digits in Phone column
        return phone

    df_clean = df.copy()

    # Apply email validation
    df_clean['Email'] = df_clean['Email'].apply(validate_email)

    # Clean Phone column
    df_clean['Phone'] = df_clean['Phone'].apply(clean_phone)

    # Step 4: Remove rows where all critical fields (VIN, Name, ID_Number, Phone, Email) are missing
    critical_columns = ['VIN', 'Name', 'ID_Number', 'Phone']

    # Use .all(axis=1) to select rows where all critical fields are missing
    missing_critical_rows = df_clean[df_clean[critical_columns].isnull().all(axis=1)]

    if not missing_critical_rows.empty:
        invalid_rows = pd.concat([invalid_rows, missing_critical_rows], ignore_index=True)
    df_clean = df_clean.dropna(subset=critical_columns)

    # Step 5: Remove duplicates based on VIN, ID_Number, and Email
    duplicate_rows = df_clean[df_clean.duplicated(subset=['VIN', 'ID_Number', 'Email'], keep='first')]

    if not duplicate_rows.empty:
        invalid_rows = pd.concat([invalid_rows, duplicate_rows], ignore_index=True)
    df_clean = df_clean.drop_duplicates(subset=['VIN', 'ID_Number', 'Email'], keep='first')

    # Step 6: Keep only the specified columns
    columns_to_keep = ['VIN', 'Name', 'ID_Number', 'Phone', 'Email', 'Province', 'City', 'Address', 'Postal_Code', 'Birthday', 'Brand',
                       'Car_Model', 'Engine_Number']
    df_clean = df_clean[columns_to_keep]

    # Remove invalid records from the cleaned DataFrame
    invalid_rows = invalid_rows.drop_duplicates()

    return df_clean, invalid_rows

def load(df_cleaned, output_file, invalid_rows, garbage_file):
    # Save the cleaned DataFrame to a new CSV file with utf-8 encoding
    df_cleaned.to_csv(output_file, index=False, encoding='UTF-8-SIG')

    # Save the invalid/garbage records to a separate CSV file with utf-8 encoding
    invalid_rows.to_csv(garbage_file, index=False, encoding='UTF-8-SIG')

    return

# Example usage:
# df = extract(input_file)
# df_cleaned, invalid_rows = transform(df)
# load(df_cleaned, output_file, invalid_rows, garbage_file)

"""Execute ETL Process"""

import logging

logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)

try:
    # Extract, transform and load data
    df = extract(input_file)
    df_cleaned, invalid_rows = transform(df)
    load(df_cleaned, output_file,invalid_rows, garbage_file)

    logging.info("Data processing completed successfully.")

# Handle exceptions, log messages
except Exception as e:
    logging.error(f"An error occurred: {str(e)}")


import os
import glob
import csv
import pandas as pd

def generate_ddl(table_name, column_names, max_lengths):
    """Generate a DDL statement for creating a SQL table with all columns as VARCHAR."""
    ddl = f"CREATE TABLE {table_name} (\n"
    column_definitions = []
    for column_name in column_names:
        # Directly use the maximum length for VARCHAR
        length = max_lengths[column_name]
        column_def = f"    {column_name} VARCHAR({length})"
        column_definitions.append(column_def)
    ddl += ",\n".join(column_definitions)
    ddl += "\n);"
    return ddl

def clean_file_and_save_copy(file_path):
    """Reads a file, removes lines that are empty or contain only spaces, and saves a cleaned copy."""
    file_path = os.path.abspath(file_path)
    cleaned_lines = []  # List to hold cleaned lines
    
    with open(file_path, 'r', encoding='cp1252') as file:
        for line in file:
            # Check if line is not empty or doesn't contain only spaces
            if line.strip():
                cleaned_lines.append(line)
    
    # Define a new file path for the cleaned file
    directory, filename = os.path.split(file_path)
    cleaned_file_path = os.path.join(directory, f"cleaned_{filename}")
    
    # Write the cleaned lines to the new file
    with open(cleaned_file_path, 'w', encoding='utf-8') as cleaned_file:
        cleaned_file.writelines(cleaned_lines)
    
    return(cleaned_file_path)

def find_files(path_directory, flag_xlsx):
    # Search for CSV files in current directory and all subdirectories
    if flag_xlsx:
        list_files = glob.glob(os.path.join(path_directory, '', '*.xlsx'), recursive=True)
        if len(list_files) == 0:
            list_files = glob.glob(os.path.join(path_directory, '', '*.xls'), recursive=True)
    else:
        list_files = glob.glob(os.path.join(path_directory, '', '*.csv'), recursive=True)

    return list_files

def find_max_char_lengths(csv_file_path):
    """Finds the maximum number of characters for each column in a CSV file."""
    file_toCheckMax = pd.read_csv(csv_file_path, dtype = "str", encoding="cp1252")

    max_lengths = {}
    for column in file_toCheckMax.columns:
        # Ensure the column is of string type
        #max_lengths[column] = int(file_toCheckMax[column].str.len().max())
        max_character = int(file_toCheckMax[column].fillna('').astype(str).str.len().max())
        if max_character == 0: 
            max_character = 2
            
        max_lengths[column] = max_character

                
    return max_lengths

def convert_excel_to_csv_same_path(excel_file_path):
    # Extract directory and base filename without extension
    directory, base_filename = os.path.split(excel_file_path)
    base_filename_without_ext = os.path.splitext(base_filename)[0]
    
    # Construct the CSV file path
    csv_file_path = os.path.join(directory, f"{base_filename_without_ext}.csv")
    
    # Read the Excel file
    try:
        df = pd.read_excel(excel_file_path, engine='openpyxl')
    except:
        df = pd.read_excel(excel_file_path) #Missing optional dependency 'xlrd'. Install xlrd >= 2.0.1 for xls Excel support Use pip or conda to install xlrd.

    # Save the DataFrame to a CSV file
    df.to_csv(csv_file_path, index=False, quoting=csv.QUOTE_ALL)
    print(f"CSV file saved at: {csv_file_path}")

ENCODING = 'utf-8'

# All directories that will be imported to SQL
all_path_directories = ['../data/GRF17'
    ,'../data/2017-18-crdc-data/2017-18 Public-Use Files/Data/SCH/CRDC/CSV'
    ,'../data/2017-18-crdc-data/2017-18 Public-Use Files/Data/SCH/EDFacts/CSV'
    ,'../data/2017-18-crdc-data/2017-18 Public-Use Files/Data/LEA/CRDC/CSV'
    ,'../data/hmda_2017_nationwide_all-records_labels'
    ,'../data/EDGE_GEOCODE_PUBLICLEA_1718'
    ,'../data']


# All directories that will be imported to SQL
output_sql_name = ['GRF17'
                  ,'CRDC_SCH'
                  ,'CRDC_SCH_EDFacts'
                  ,'CRDC_LEA'
                  ,'HDMA'
                  ,'GEOCODE'
                  ,'ussd17']

#Change it according with the files that you have in each directory 
excel_files_exist = [False, #If first time, put TRUE (GRF17)
                     False,
                     False, 
                     False,
                     False,
                     False, 
                     True] #If first time, put TRUE (ussd17)

file_number = 0
for path_directory in all_path_directories:

    # Eliminate previously cleaned data to avoid replicas
    pattern = path_directory + '/cleaned_*'

    # List all files matching the pattern in the current directory
    for file in glob.glob(pattern):
        try:
            os.remove(file)
            print(f"Deleted file: {file}")
        except OSError as e:
            print(f"Error deleting file {file}: {e.strerror}")

    # Convert Excel Files to CSV if needed
    excel_files = excel_files_exist[file_number]
    if excel_files: 
        xlsx_files = find_files(path_directory, excel_files)
        print(xlsx_files)
        for file_xlsx in xlsx_files:
        # Example usage
            convert_excel_to_csv_same_path(file_xlsx)
        
        excel_files = False
    
    # Find all CSV files at path_directory
    csv_files = find_files(path_directory, excel_files)
    print(" ")
    print("Current File >> ")
    print("../SQL/"+output_sql_name[file_number]+".sql")

    # Check if the copy_commands.sql file exists and erase it
    if os.path.exists("../SQL/"+output_sql_name[file_number]+".sql"):
        os.remove("../SQL/"+output_sql_name[file_number]+".sql")

    # Create the file with initial SCHEMA command
    with open("../SQL/"+output_sql_name[file_number]+".sql", 'x', encoding=ENCODING) as copy_commands_file:
        copy_commands_file.write('CREATE SCHEMA IF NOT EXISTS "DataAnalytics"; \n')

    #Create the tables and copy commands 
    with open("../SQL/"+output_sql_name[file_number]+".sql", 'a', encoding=ENCODING) as copy_commands_file:
        for file_csv in csv_files:

            # Generate the table name based on the file name
            table_name = os.path.splitext(os.path.basename(file_csv))[0].replace('-', '')
            table_name = table_name.replace('(', '')
            table_name = table_name.replace(')', '')
            table_name = table_name.replace(' ', '')
            table_name = '"DataAnalytics"' + "." + table_name

            print(table_name)
            max_lengths = find_max_char_lengths(file_csv)

            # Generate the DDL statement for the table
            ddl = generate_ddl(table_name, list(max_lengths.keys()), max_lengths)

            # Assuming table_postgreSQL is your opened file for writing DDL statements
            copy_commands_file.write(ddl + "\n")

            # Pre-process the file path to escape backslashes

            #To use the files without the last empty line
            file_csv_cleaned = clean_file_and_save_copy(file_csv)
            file_csv_escaped = file_csv_cleaned.replace('\\', '/')

            # Format the COPY command for the current file using the pre-processed path
            copy_command = f"COPY {table_name} FROM '{file_csv_escaped}' DELIMITER ',' CSV HEADER ENCODING 'windows-1251';\n"

            # Write the COPY command to the file
            copy_commands_file.write(copy_command)
    file_number=file_number+1
    
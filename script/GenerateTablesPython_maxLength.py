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

def read_and_convert_csv_data(file_csv, encoding):
    first_three_rows = []
    with open(file_csv, mode='r', encoding=encoding) as file:
        csv_reader = csv.reader(file)
        
        # Read and store the header
        header = next(csv_reader)
        
        # Read and store the first three rows, converting data types where possible
        for _ in range(3):
            try:
                row = next(csv_reader)
                first_three_rows.append(row)
            except StopIteration:
                # Handles the case where the file has less than 3 rows following the header
                break
    
    return header, first_three_rows

def clean_file_and_save_copy(file_path):
    """Reads a file, removes lines that are empty or contain only spaces, and saves a cleaned copy."""
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
    else:
        list_files = glob.glob(os.path.join(path_directory, '', '*.csv'), recursive=True)

    return list_files

def find_max_char_lengths(csv_file_path):
    """Finds the maximum number of characters for each column in a CSV file."""
    file_toCheckMax = pd.read_csv(csv_file_path, dtype = "str", encoding="cp1252")

    max_lengths = {}
    for column in file_toCheckMax.columns:
        # Ensure the column is of string type
        max_lengths[column] = file_toCheckMax[column].str.len().max()

                
    return max_lengths

def convert_excel_to_csv_same_path(excel_file_path):
    # Extract directory and base filename without extension
    directory, base_filename = os.path.split(excel_file_path)
    base_filename_without_ext = os.path.splitext(base_filename)[0]
    
    # Construct the CSV file path
    csv_file_path = os.path.join(directory, f"{base_filename_without_ext}.csv")
    
    # Read the Excel file
    df = pd.read_excel(excel_file_path, engine='openpyxl')
    
    # Save the DataFrame to a CSV file
    df.to_csv(csv_file_path, index=False)
    print(f"CSV file saved at: {csv_file_path}")

ENCODING = 'utf-8'
path_directory = '../Data/GRF17'

excel_files = True
if excel_files: 
    xlsx_files = find_files(path_directory, excel_files)

    for file_xlsx in xlsx_files:
    # Example usage
        convert_excel_to_csv_same_path(file_xlsx)
    
    excel_files = False


#CSV files at path_directory
csv_files = find_files(path_directory, excel_files)

header = []  # To store the header
first_three_rows = []  # To store the first three rows

#This is to write the tables in a txt file
# table_postgreSQL = open('C:/Users/Public/EDFacts/tables_SQL.txt', "w")

#Create the tables and copy commands 
with open(path_directory + "/copy_commands.sql", 'w', encoding='utf-8') as copy_commands_file:
    for file_csv in csv_files:

        # Generate the table name based on the file name
        table_name = os.path.splitext(os.path.basename(file_csv))[0].replace('-', ' ')
        table_name = table_name.replace(' ', '')
        table_name = '"DataAnalytics"' + "." + table_name

        print(table_name)
        max_lengths = find_max_char_lengths(file_csv)

        # Generate the DDL statement for the table
        ddl = generate_ddl(table_name, list(max_lengths.keys()), max_lengths)

                
        # Assuming `table_postgreSQL` is your opened file for writing DDL statements
        copy_commands_file.write(ddl + "\n")

        # Pre-process the file path to escape backslashes

        #To use the files without the last empty line
        file_csv_cleaned = clean_file_and_save_copy(file_csv)
        file_csv_escaped = file_csv_cleaned.replace('\\', '/')

        # Format the COPY command for the current file using the pre-processed path
        copy_command = f"COPY {table_name} FROM '{file_csv_escaped}' DELIMITER ',' CSV HEADER ENCODING 'windows-1251';\n"

        # Write the COPY command to the file
        copy_commands_file.write(copy_command)



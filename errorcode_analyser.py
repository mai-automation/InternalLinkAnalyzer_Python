"""
This script can be used to analyze error codes after crawling a website and checking the status of URLs.
It processes multiple Excel files in a folder named "Files" to extract data based on specific response codes.
It filters the data based on the response codes provided, extracts relevant columns, and counts occurrences by subfolder.
The summary for each response code is saved to a separate CSV file with the counts of URLs per subfolder.
"""
import os
import pandas as pd
import re

# Define the folder path where the files are located
folder_path = "./Files"

# Define the list of response codes to process
response_codes = [301, 308, 404]    # Add more response codes as needed

# Iterate through all response codes
for code in response_codes:
    # Initialize a list to store the filtered data for the current code
    filtered_data_list = []

    # Iterate through all files in the folder
    for filename in os.listdir(folder_path):
        if filename.endswith(".xlsx"):  # Only process Excel files
            file_path = os.path.join(folder_path, filename)
            print(f"Processing file: {filename}")

            # Load the Excel file
            spreadsheet = pd.ExcelFile(file_path)

            # Process each sheet in the Excel file
            for sheet_name in spreadsheet.sheet_names:
                print(f"  Processing sheet: {sheet_name}")
                sheet_data = spreadsheet.parse(sheet_name)

                # Filter for the current response code and extract relevant columns
                if "Response Code" in sheet_data.columns and "URL (linked)" in sheet_data.columns:
                    filtered_data = sheet_data[sheet_data["Response Code"] == code][["URL (linked)", "Response Code"]]
                    filtered_data_list.append(filtered_data)

    # Combine all the data for the current response code into a single DataFrame
    if filtered_data_list:
        combined_data = pd.concat(filtered_data_list, ignore_index=True)

        # Remove duplicate URLs
        combined_data = combined_data.drop_duplicates(subset=["URL (linked)"])

        # Extract subfolder from URL
        combined_data["Subfolder"] = combined_data["URL (linked)"].str.extract(r"https?://[^/]+(/[^/]+)/")[0]

        # Count occurrences of errors by subfolder
        subfolder_counts = combined_data["Subfolder"].value_counts().reset_index()
        subfolder_counts.columns = ["(Sub)Directory", "Numbers of URLs"]

        # Save summary to a CSV file
        summary_file = f"response_code_{code}_summary.csv"
        subfolder_counts.to_csv(summary_file, index=False)
        print(f"Summary for response code {code} written to {summary_file}")
    else:
        print(f"No data found for response code {code}.")

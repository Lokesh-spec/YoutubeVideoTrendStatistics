import os
import csv
import json
from pprint import pprint

def validate_json_files(folder_path):
    # Ensure the folder exists
    if not os.path.exists(folder_path):
        print(f"Folder '{folder_path}' does not exist.")
        return
    
    # Get a list of files in the folder
    files = os.listdir(folder_path)
    
    for file_name in files:
        if file_name != "app.py" and file_name.endswith('.json'):  # Process only .json files
            file_path = os.path.join(folder_path, file_name)
            
            # Check if the path is a file
            if os.path.isfile(file_path):
                try:
                    with open(file_path, 'r') as f:
                        data = json.load(f)  # Attempt to load the file as JSON
                    print(f"VALID: {file_name}")
                    
                    # Initialize the header row for CSV
                    final_data = []
                    
                    # Check if 'items' exists in the JSON and process it
                    if 'items' in data:
                        for item in data['items']:
                            # Extract values for each key
                            kind = item.get('kind', '')
                            etag = item.get('etag', '')
                            id = item.get('id', '')
                            snippet = item.get('snippet', {})
                            channelId = snippet.get('channelId', '')
                            categoryTitle = snippet.get('title', '')
                            assignable = snippet.get('assignable', '')
                            
                            etag = etag.strip('"""')
                            
                            # Append extracted values to final_data
                            final_data.append([kind, etag, id, channelId, categoryTitle, assignable])
                            
                    
                    # Print the extracted data (for debugging)
                    pprint(final_data)
                    
                    # Create a CSV file with the same name as the JSON file (but .csv extension)
                    csv_file_name = file_name.split('.')[0] + ".csv"
                    csv_file_path = os.path.join(folder_path, csv_file_name)
                    
                    # Write to the CSV file
                    with open(csv_file_path, mode='w', newline='', encoding='utf-8') as file:
                        writer = csv.writer(file)
                        writer.writerows(final_data)  # Write all rows to CSV

                    print(f"CSV file created: {csv_file_name}")
                    
                except json.JSONDecodeError as e:
                    print(f"INVALID: {file_name} - JSONDecodeError: {e}")
                except Exception as e:
                    print(f"ERROR: {file_name} - {e}")

# Example usage
folder_path = "Data/Json"
validate_json_files(folder_path)

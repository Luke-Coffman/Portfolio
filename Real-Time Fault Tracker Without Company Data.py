import pandas as pd
import os
import schedule
import time
from datetime import datetime, timedelta
import re

# Dictionary defining locations and their corresponding directory paths
locations = {
    #Private Company Information
}

# Function to find the line number of a marker in a file
def find_marker_line(file_path, marker):
    with open(file_path, 'r') as file:
        for line_number, line in enumerate(file, start=1):
            if marker in line:
                return line_number
    return None

# Function to read a CSV file from a specified start line with a given delimiter
def read_csv_file(file_path, start_line, delimiter, index_col=False):
    try:
        df = pd.read_csv(file_path, skiprows=start_line, delimiter=delimiter, on_bad_lines='skip')
        print("CSV file read successfully. Here are the first few rows:")
        print(df.head())
    except FileNotFoundError:
        print("File not found. Ensure the file exists and the path is correct.")
        return None
    except pd.errors.ParserError as e:
        print(f"Parser error: {e}")
        return None
    return df

# Function to find the value of "Private Company Information" in a file based on the way it is built from #Private Company Information
def find_cpf_value(file_path):
    with open(file_path, 'r') as file:
        lines = file.readlines()
        for i, line in enumerate(lines):
            if "Cases per Fault" in line:
                return lines[i + 3].strip()  # Return the line 3 lines below the marker which is where CPF always is
    return None

# Process the data in a DataFrame
def process_data(df, cpf_value):
    # Define column names
    column_names = ['F', 'Date/Time', 'Duration(min)', 'Source', 'Alarm Description', 'Area', 'Code', 
                    'ErrorDescription', 'ShuttleX', 'ShuttleZ', 'Label1', 'Label2', 'XY_CMD', 'LZ_CMD']
    
    # Rename columns
    df.columns = column_names

    # Remove unnecessary columns
    df.drop(columns=['ShuttleZ', 'Label1', 'Label2', 'XY_CMD', 'LZ_CMD'], inplace=True)

    # Concatenate 'ErrorDescription' and 'ShuttleX' for specific conditions
    df['ErrorDescription'] = df.apply(
        lambda columns: f"{columns['ErrorDescription']}, {columns['ShuttleX']}" 
                    if (not any(char.isdigit() for char in str(columns['ShuttleX'])) and pd.notna(columns['ShuttleX'])) or 
                       ('sensor blocked' in str(columns['ErrorDescription']).lower() and pd.notna(columns['ShuttleX'])) or
                       ('shuttle load overhang' in str(columns['ErrorDescription']).lower() and pd.notna(columns['ShuttleX']))
                    else columns['ErrorDescription'], axis=1)

    # Remove the 'ShuttleX' column
    df.drop(columns=['ShuttleX'], inplace=True)

    # Remove all rows that do not have a '*' in the 'F' column
    df_filtered = df[df['F'] == '*'].copy()

    # Extract the aisle number from the Source column and create a new column 'Aisle'
    df_filtered['Aisle'] = df_filtered['Source'].str.extract(r'AI(\d{2})')[0].astype(int)

    # Create the 'Level' column based on the 'Alarm Description'
    def extract_level(description):
        if 'LEVEL' in description:
            try:
                return int(description.split(' ')[1])
            except (ValueError, IndexError):
                return 'Aisle Fault'
        elif 'LV' in description:
            match = re.search(r'LV(\d{2})', description)
            if match:
                return int(match.group(1))
        return 'Aisle Fault'

    df_filtered['Level'] = df_filtered['Alarm Description'].apply(extract_level)
    
    # Add the CPF value to each row
    df_filtered['CPF'] = cpf_value

    return df_filtered

# Function to count occurrences of errors within 90 minutes for each group
def count_occurrences_within_90_minutes(group):
    group = group.sort_values(by='Date/Time', ascending=False)
    count = 0
    last_time = None
    last_index = None
    levels_seen = set()

    for index, row in group.iterrows():
        if last_time is None or (last_time - row['Date/Time']).total_seconds() > 5400: # 90 minutes
            count = 1
            levels_seen = {row['Level']}
        else:
            count += 1
            levels_seen.add(row['Level'])

        last_time = row['Date/Time']
        
        if count >= 2:
            if last_index is not None:
                group.at[last_index, 'Keep'] = False
            last_index = index

        group.at[index, 'OccurrencesWithin90Minutes'] = count
        group.at[index, 'Keep'] = True
        group.at[index, 'DifferentLevels'] = len(levels_seen) > 1

    group = group[group['Keep'] == True]
    group = group.drop(columns=['Keep'])
    return group

# Function to track frequent errors in the filtered DataFrame
def track_frequent_errors(df_filtered):
    df_filtered['Date/Time'] = pd.to_datetime(df_filtered['Date/Time'], errors='coerce')
    df_filtered.dropna(subset=['Date/Time'], inplace=True)
    df_filtered['OccurrencesWithin90Minutes'] = 0
    df_filtered['Keep'] = True
    df_filtered['DifferentLevels'] = False

    df_filtered = df_filtered.groupby(['Aisle', 'Level', 'ErrorDescription'], group_keys=False).apply(count_occurrences_within_90_minutes)
    df_filtered['Date/Time'] = df_filtered['Date/Time'].dt.strftime('%Y-%m-%d %H:%M')
    df_filtered = df_filtered[df_filtered['OccurrencesWithin90Minutes'] >= 2]
    df_filtered = df_filtered.sort_values(by='Date/Time', ascending=False)

    print("Data with frequent errors tracked:")
    print(df_filtered.head())

    return df_filtered

# Function to preprocess data for a given directory and location
def preprocess_data(directory, location):
    file_pattern = '-DMSFaults.csv'
    delimiter = ','
    now = datetime.now()
    
    if now.hour == 20 and now.minute >= 30 or now.hour > 20:
        file_date = (now + timedelta(days=1)).strftime('%Y%m%d')
    else:
        file_date = now.strftime('%Y%m%d')

    file_name = f'{file_date}{file_pattern}'
    file_path = os.path.join(directory, file_name)

    print(f"Attempting to read file for {location}: {file_path}")

    marker = "*** UB Faults ***"
    marker_line = find_marker_line(file_path, marker)
    if marker_line is None:
        print(f"Marker '{marker}' not found in the file.")
        return

    cpf_value = find_cpf_value(file_path)
    if cpf_value is None:
        print("CPF value not found in the file.")
        return

    print(f"CPF value found: {cpf_value}")
    df = read_csv_file(file_path, start_line=marker_line, delimiter=delimiter)
    if df is not None:
        df_filtered = process_data(df, cpf_value)
        save_directory = r'#Private Company Information'
        processed_output_path = os.path.join(save_directory, f'filtered_data_{location}.csv')
        final_output_path = os.path.join(save_directory, f'filtered_data_with_error_tracking_{location}.csv')
        df_filtered.to_csv(processed_output_path, index=False)
        print(f"Filtered data for {location} saved to '{processed_output_path}'. Here are the first few rows:")
        print(df_filtered.head())

        df_with_errors = track_frequent_errors(df_filtered)
        df_with_errors.to_csv(final_output_path, index=False)
        print(f"Data with frequent error tracking for {location} saved to '{final_output_path}'. Here are the first few rows:")
        print(df_with_errors.head())

        generate_html(location)
    else:
        print(f"Failed to read data from the file: {file_path}")

# Function to generate an HTML file for the given location
def generate_html(location):
    save_directory = r'#Private Company Information'
    csv_file_path = os.path.join
    csv_file_path = os.path.join(save_directory, f'filtered_data_with_error_tracking_{location}.csv')

    try:
        df = pd.read_csv(csv_file_path)
        print("CSV file read successfully. Here are the first few rows:")
        print(df.head())
    except FileNotFoundError:
        print(f"File not found. Ensure the file exists at '{csv_file_path}'.")
        df = pd.DataFrame()

    if df.empty:
        print("The DataFrame is empty. No data to display.")
        no_data_message = True
    else:
        no_data_message = False

    if 'CPF' in df.columns and not df['CPF'].empty:
        cpf_value = df['CPF'].iloc[0]
    else:
        print("CPF value not found in the DataFrame.")
        cpf_value = 'N/A'

    html = f'''
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>Real-Time Fault Alerts - {location}</title>
        <meta http-equiv="refresh" content="300">
        <style>
            body {{
                font-family: Arial, sans-serif;
                font-size: 18px;
                background-color: #f4f4f9;
                color: #333;
                margin: 0;
                padding: 0;
            }}
            .header {{
                background-color: #4CAF50;
                color: white;
                padding: 20px;
                font-size: 24px;
                display: flex;
                justify-content: space-between;
                align-items: center;
            }}
            .cpf {{
                font-size: 30px;
                background-color: #ffffff;
                padding: 10px;
                border-radius: 5px;
                color: #000;
            }}
            .location-select {{
                font-size: 22px;
                background-color: #ffffff;
                padding: 10px;
                border-radius: 5px;
                color: #000;
                margin-top: -10px;
            }}
            .container {{
                padding: 20px;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                margin-top: 20px;
            }}
            th, td {{
                border: 1px solid #ddd;
                padding: 8px;
                text-align: left;
            }}
            th {{
                background-color: #4CAF50;
                color: white;
            }}
            tr:nth-child(even) {{
                background-color: #f2f2f2;
            }}
            .log-container {{
                margin-top: 20px;
            }}
            .log {{
                background-color: #fff;
                border: 1px solid #ddd;
                padding: 10px;
                max-height: 200px;
                overflow-y: auto;
                color: red;
                font-weight: bold;
            }}
            .highlight {{
                color: red;
            }}
            .no-data {{
                text-align: center;
                color: red;
                font-size: 20px;
                font-weight: bold;
                margin-top: 20px;
            }}
        </style>
        <script>
            function changeLocation() {{
                var location = document.getElementById('locationSelect').value;
                window.location.href = location + '.html';
            }}
        </script>
    </head>
    <body>
        <div class="header">
            <div class="location-select">
                <label for="locationSelect">Select Location:</label>
                <select id="locationSelect" onchange="changeLocation()">
    '''

    for loc in locations.keys():
        selected = 'selected' if loc == location else ''
        html += f'<option value="real_time_fault_alerts_{loc}" {selected}>{loc}</option>'

    html += f'''
                </select>
            </div>
            <div>
                <h1>Real-Time Fault Alerts</h1>
            </div>
            <div class="cpf">Cases per Fault: {cpf_value}</div>
        </div>
        <div class="container">
    '''

    if no_data_message:
        html += '''
            <div class="no-data">No data available yet.</div>
        '''
    else:
        html += '''
            <table id="alertsTable">
                <thead>
                    <tr>
                        <th>Aisle</th>
                        <th>Level</th>
                        <th>Fault Description</th>
                        <th>Timestamp of most recent fault</th>
                        <th>Occurrences Within 90 Minutes</th>
                    </tr>
                </thead>
                <tbody>
        '''

        for index, row in df.iterrows():
            highlight_class = ''
            if row['ErrorDescription'] == 'No Communications with Shuttle' or row['ErrorDescription'].startswith('W:'):
                highlight_class = 'highlight'
            
            html += f'''
                        <tr class="{highlight_class}">
                            <td>{row['Aisle']}</td>
                            <td>{row['Level']}</td>
                            <td>{row['ErrorDescription']}</td>
                            <td>{row['Date/Time']}</td>
                            <td>{row['OccurrencesWithin90Minutes']}</td>
                        </tr>
            '''

        html += '''
                    </tbody>
                </table>
            </div>
        '''

    html += '''
        </div>
    </body>
    </html>
    '''

    html_output_path = os.path.join(save_directory, f'real_time_fault_alerts_{location}.html')
    with open(html_output_path, 'w') as file:
        file.write(html)

    print(f"HTML file for {location} generated successfully at '{html_output_path}'.")

# Main function to process data for each location and schedule the task
def process_locations():
    for location, directory in locations.items():
        preprocess_data(directory, location)

def main():
    process_locations()
    schedule.every(5).minutes.do(process_locations)  # Schedule the process_locations function to run every 5 minutes

    while True:
        schedule.run_pending()  # Run pending scheduled tasks
        time.sleep(1)  # Sleep for 1 second

if __name__ == "__main__":
    main()

import os
import pandas as pd

def append_local_to_apps(mappings_apps):
    """Appends '-Local' to the app_mapping_v2 column in mappings_apps."""
    mappings_apps['app_mapping_v2'] = mappings_apps['app_mapping_v2'] + "-Local"
    return mappings_apps

def drop_na_sites(mappings_sites):
    """Drops rows where 'site' is NaN in mappings_sites."""
    return mappings_sites.dropna(subset=['site'])

def load_and_process_csv_files(input_directory, output_filepath):
    """
    Loads all CSV files from the specified directory, concatenates them, drops duplicates,
    sorts by 'employeeId', and saves the result to the specified output file path.

    Parameters:
    - input_directory: str, the directory containing CSV files to be loaded.
    - output_filepath: str, the path to save the processed CSV file.
    """
    # List all CSV files in the specified directory
    csv_files = [f for f in os.listdir(input_directory) if f.endswith('.csv')]

    # Load and concatenate all CSV files
    dataframes = []
    for file in csv_files:
        file_path = os.path.join(input_directory, file)
        df = pd.read_csv(file_path)
        dataframes.append(df)
    
    combined_df = pd.concat(dataframes, ignore_index=True)

    # Drop duplicates
    combined_df = combined_df.drop_duplicates()

    # Sort by 'employeeId'
    combined_df = combined_df.sort_values(by='employeeId').reset_index(drop=True)

    # Save the processed DataFrame to the specified output file path
    combined_df.to_csv(output_filepath, index=False)

    return combined_df

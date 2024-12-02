import yaml
from utils import load_and_process_csv_files

# Load configuration from 'preprocessing.yaml'
with open('preprocessing_config.yaml', 'r') as config_file:
    config = yaml.safe_load(config_file)

# Define the input directory and output path for combined CSV from the config file
input_directory = config['paths']['input_dir']
output_filepath = config['paths']['output_path']

# Combine and save CSV files
combined_df = load_and_process_csv_files(input_directory, output_filepath)
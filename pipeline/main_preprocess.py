import os
import yaml
from utils import load_and_process_csv_files

# Get the directory of the current script
current_dir = os.path.dirname(os.path.abspath(__file__))

print(current_dir)

# Build the full path to the config file
config_path = os.path.join(current_dir, 'configs', 'preprocessing_config.yaml')

print(config_path)

# Load configuration from 'preprocessing.yaml'
with open(config_path, 'r') as config_file:
    config = yaml.safe_load(config_file)

# Define the input directory and output path for combined CSV from the config file
input_directory = config['paths']['input_dir']
output_filepath = config['paths']['output_path']

# Combine and save CSV files
load_and_process_csv_files(input_directory, output_filepath)
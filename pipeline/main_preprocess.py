import os
import yaml
from utils import load_and_process_csv_files

# Get the directory of the current script
current_dir = os.path.dirname(os.path.abspath(__file__))

# Build the full path to the config file
config_path = os.path.join(current_dir, 'configs', 'preprocessing_config.yaml')

# Load configuration from 'preprocessing.yaml'
with open(config_path, 'r') as config_file:
    config = yaml.safe_load(config_file)

try:
    print(os.listdir('/'))
    print(os.listdir('/data/'))
    print(os.listdir('/exp/'))
    print(os.listdir('/out/'))
except Exception as e:
    print(e)

input_dir = config['paths']['input_dir']
output_path = config['paths']['output_path']

print(f"Input directory: {input_dir}")
print(f"Output path: {output_path}")

# Combine and save CSV files
load_and_process_csv_files(input_dir, output_path)
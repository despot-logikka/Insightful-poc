
import os
import pandas as pd
import yaml
from pathlib import Path
import json
from datetime import timedelta
from utils import append_local_to_apps, drop_na_sites


class WorkdayProcessor:
    def __init__(self, config_path):
        self.config = self.load_config(config_path)

    @staticmethod
    def load_config(config_path):
        """Load configuration from a YAML file."""
        with open(config_path, 'r', encoding='utf-8') as fp:
            return yaml.safe_load(fp)

    def load_csv(self, file_path, sep=','):
        """Load a CSV file into a DataFrame."""
        with open(file_path, 'r', encoding='utf-8') as fp:
            return pd.read_csv(fp, sep=sep)

    def load_json(self, file_path):
        """Load a JSON file."""
        with open(file_path, 'r', encoding='utf-8') as fp:
            return json.load(fp)

    @staticmethod
    def prepare_initial_data(df, browsers):
        """
        Prepares the initial DataFrame:
        - Adds 'Concentration Lost' for inactive apps.
        - Converts 'start' and 'end' to datetime.
        - Rewrites 'app' as 'Private Links' for apps in unique_apps where 'site' is NaN.
        """
        unique_apps = browsers['browsers'].unique()

        # Add 'Concentration Lost' where app is inactive
        df.loc[df['active'] == False, 'app'] = 'Concentration Lost'

        # Convert 'start' and 'end' to datetime
        df['start_time'] = pd.to_datetime(df['start'], unit='ms')
        df['end_time'] = pd.to_datetime(df['end'], unit='ms')
        df = df.sort_values(by=['employeeId', 'start_time'])

        # Filling NaN values in the DataFrame for specific columns
        df['mouseClicks'].fillna(0, inplace=True)
        df['keystrokes'].fillna(0, inplace=True)
        df['mouseScroll'].fillna(0, inplace=True)
        df['mic'].fillna(False, inplace=True)
        df['camera'].fillna(False, inplace=True)

        # Rewrite 'app' as 'Private Links' where 'app' is in unique_apps and 'site' is NaN
        df.loc[df['app'].isin(unique_apps) & df['site'].isna(), 'app'] = 'Private Links'

        # Keep only necessary columns
        return df[['employeeId',
                   'app',
                   'site',
                   'start_time',
                   'end_time',
                   'mouseClicks',
                   'keystrokes',
                   'mic',
                   'mouseScroll',
                   'camera'
                ]]

    def preprocess_data(self, df):
        """Main method to process datasets."""
        paths = self.config['paths']

        # Load mappings
        mappings_apps = self.load_csv(paths['app_mappings'])
        mappings_sites = self.load_csv(paths['site_mappings'])
        exclude_mappings = self.load_json(paths['exclude_mappings'])

        # Apply utility functions
        mappings_apps = append_local_to_apps(mappings_apps)
        mappings_sites = drop_na_sites(mappings_sites)

        # Step 1: Map excluded sites to themselves in mappings_sites
        mappings_sites.loc[mappings_sites['site'].isin(exclude_mappings['sites']), 'site_mapping'] = mappings_sites['site']
        df = df.merge(mappings_sites[['site', 'site_mapping']], on='site', how='left')
        df['app'] = df['site_mapping'].combine_first(df['app'])
        df.drop(columns=['site_mapping'], inplace=True)

        # Step 2: Map excluded apps to themselves in mappings_apps
        mappings_apps.loc[mappings_apps['app'].isin(exclude_mappings['apps']), 'app_mapping_v2'] = mappings_apps['app']
        df = df.merge(mappings_apps[['app', 'app_mapping_v2']], on='app', how='left')
        df['app'] = df['app_mapping_v2'].combine_first(df['app'])
        df.drop(columns=['app_mapping_v2'], inplace=True)

        df['app'] = df['app'].str.replace(r'\s+', '_', regex=True)

        # Step 4: Merge consecutive app usage rows
        df = self.merge_consecutive_rows(df)

        return df

    @staticmethod
    def merge_consecutive_rows(df):
        """Merge consecutive rows with the same app and employeeId."""
        df = df.sort_values(by=['employeeId', 'start_time']).reset_index(drop=True)
        df['new_group'] = (
            (df['employeeId'] != df['employeeId'].shift()) |
            (df['app'] != df['app'].shift()) |
            (df['start_time'] != df['end_time'].shift())
        )
        df['group_id'] = df['new_group'].cumsum()
        df = df.groupby(['employeeId', 'group_id'], as_index=False).agg({
            'start_time': 'first',
            'end_time': 'last',
            'app': 'first',
            'mouseClicks': 'sum',
            'keystrokes': 'sum',
            'mic': 'max',  # OR operation equivalent for boolean values
            'mouseScroll': 'sum',
            'camera': 'max'  # OR operation equivalent for boolean values
        })
        df = df.drop(columns=['group_id']).sort_values(by=['employeeId', 'start_time']).reset_index(drop=True)
        return df

    @staticmethod
    def create_working_day(df, max_workday_gap=timedelta(hours=2)):
        result = []
        
        for employee_id, group in df.groupby('employeeId'):
            # Sort the group by 'start_time' to process logs in chronological order
            group = group.sort_values('start_time').reset_index(drop=True)
            
            daily_apps = []
            daily_durations = []
            daily_app_start_times = []
            daily_app_end_times = []
            daily_mouse_clicks = []
            daily_keystrokes = []
            daily_mic = []
            daily_mouse_scroll = []
            daily_camera = []
            day_counter = 1
            last_end = None
            start_time_of_workday = None

            for idx, row in group.iterrows():
                app_name = row['app']
                start_time = row['start_time']
                end_time = row['end_time']
                mouse_clicks = row['mouseClicks']
                keystrokes = row['keystrokes']
                mic = row['mic']
                mouse_scroll = row['mouseScroll']
                camera = row['camera']

                if last_end is None:
                    # Initialize start_time_of_workday and last_end
                    start_time_of_workday = start_time
                    last_end = end_time

                    daily_apps.append(app_name)
                    daily_durations.append((end_time - start_time).total_seconds() / 60)
                    daily_app_start_times.append(start_time)
                    daily_app_end_times.append(end_time)
                    daily_mouse_clicks.append(mouse_clicks)
                    daily_keystrokes.append(keystrokes)
                    daily_mic.append(mic)
                    daily_mouse_scroll.append(mouse_scroll)
                    daily_camera.append(camera)
                    continue

                gap = start_time - last_end

                # If the gap is larger than or equal to max_workday_gap, start a new workday
                if gap >= max_workday_gap:
                    # Append the current workday to result
                    result.append({
                        'employeeId': f'{employee_id}_{day_counter}',
                        'app': daily_apps,
                        'app_durations': daily_durations,
                        'app_start_times': daily_app_start_times,
                        'app_end_times': daily_app_end_times,
                        'mouseClicks': daily_mouse_clicks,
                        'keystrokes': daily_keystrokes,
                        'mic': daily_mic,
                        'mouseScroll': daily_mouse_scroll,
                        'camera': daily_camera,
                        'start_time': start_time_of_workday,
                        'end_time': last_end
                    })
                    
                    day_counter += 1
                    daily_apps = []
                    daily_durations = []
                    daily_app_start_times = []
                    daily_app_end_times = []
                    daily_mouse_clicks = []
                    daily_keystrokes = []
                    daily_mic = []
                    daily_mouse_scroll = []
                    daily_camera = []
                    start_time_of_workday = start_time
                    last_end = end_time

                    # Start the new workday with the current app
                    daily_apps.append(app_name)
                    daily_durations.append((end_time - start_time).total_seconds() / 60)
                    daily_app_start_times.append(start_time)
                    daily_app_end_times.append(end_time)
                    daily_mouse_clicks.append(mouse_clicks)
                    daily_keystrokes.append(keystrokes)
                    daily_mic.append(mic)
                    daily_mouse_scroll.append(mouse_scroll)
                    daily_camera.append(camera)
                    continue

                # If the gap is 20 seconds or less, label as "Log Lost/Software Bug"
                if timedelta(seconds=0) < gap <= timedelta(seconds=20):
                    daily_apps.append('Log Lost/Software Bug')
                    daily_durations.append(gap.total_seconds() / 60)
                    daily_app_start_times.append(last_end)
                    daily_app_end_times.append(start_time)
                    daily_mouse_clicks.append(0)
                    daily_keystrokes.append(0)
                    daily_mic.append(False)
                    daily_mouse_scroll.append(0)
                    daily_camera.append(False)
                
                # If the gap is larger than 20 seconds and less than max_workday_gap, label as "Pause"
                elif timedelta(seconds=20) < gap < max_workday_gap:
                    daily_apps.append('Pause')
                    daily_durations.append(gap.total_seconds() / 60)
                    daily_app_start_times.append(last_end)
                    daily_app_end_times.append(start_time)
                    daily_mouse_clicks.append(0)
                    daily_keystrokes.append(0)
                    daily_mic.append(False)
                    daily_mouse_scroll.append(0)
                    daily_camera.append(False)

                # Add the current app duration
                daily_apps.append(app_name)
                daily_durations.append((end_time - start_time).total_seconds() / 60)
                daily_app_start_times.append(start_time)
                daily_app_end_times.append(end_time)
                daily_mouse_clicks.append(mouse_clicks)
                daily_keystrokes.append(keystrokes)
                daily_mic.append(mic)
                daily_mouse_scroll.append(mouse_scroll)
                daily_camera.append(camera)

                # Update last_end to the current app's end time
                last_end = end_time

            # Append any remaining apps and durations for the final day
            if daily_apps:
                result.append({
                    'employeeId': f'{employee_id}_{day_counter}',
                    'app': daily_apps,
                    'app_durations': daily_durations,
                    'app_start_times': daily_app_start_times,
                    'app_end_times': daily_app_end_times,
                    'mouseClicks': daily_mouse_clicks,
                    'keystrokes': daily_keystrokes,
                    'mic': daily_mic,
                    'mouseScroll': daily_mouse_scroll,
                    'camera': daily_camera,
                    'start_time': start_time_of_workday,
                    'end_time': last_end
                })

        # Create a DataFrame from the result list
        result_df = pd.DataFrame(result)
        
        return result_df
    
    @staticmethod
    def merge_log_lost_and_same_apps(df):
        """
        Processes the DataFrame to handle 'Log Lost/Software Bug' entries and merge consecutive same apps.
        """
        processed_rows = []
        
        for idx, row in df.iterrows():
            apps = row['app']
            durations = row['app_durations']
            app_starts = row['app_start_times']
            app_ends = row['app_end_times']
            mouse_clicks = row['mouseClicks']
            keystrokes = row['keystrokes']
            mic = row['mic']
            mouse_scroll = row['mouseScroll']
            camera = row['camera']
            employeeId = row['employeeId']
            day_start = row['start_time']
            day_end = row['end_time']
            
            # Initialize lists for processed data
            new_apps = []
            new_durations = []
            new_app_starts = []
            new_app_ends = []
            new_mouse_clicks = []
            new_keystrokes = []
            new_mic = []
            new_mouse_scroll = []
            new_camera = []
            
            i = 0
            while i < len(apps):
                app_name = apps[i]
                duration = durations[i]
                start_time = app_starts[i]
                end_time = app_ends[i]
                clicks = mouse_clicks[i]
                keys = keystrokes[i]
                mic_active = mic[i]
                scroll = mouse_scroll[i]
                cam_active = camera[i]
                
                # If the app is 'Log Lost/Software Bug'
                if app_name == 'Log Lost/Software Bug':
                    # Merge with previous app
                    if len(new_apps) > 0:
                        # Extend the previous app's end time and duration
                        new_durations[-1] += duration
                        new_app_ends[-1] = end_time
                        new_mouse_clicks[-1] += clicks
                        new_keystrokes[-1] += keys
                        new_mic[-1] = new_mic[-1] or mic_active
                        new_mouse_scroll[-1] += scroll
                        new_camera[-1] = new_camera[-1] or cam_active
                    i += 1
                    continue
                
                # If current app is same as previous and end time of previous is same as start time
                if (len(new_apps) > 0 and
                    app_name == new_apps[-1] and
                    new_app_ends[-1] == start_time):
                    # Merge with previous app
                    new_durations[-1] += duration
                    new_app_ends[-1] = end_time
                    new_mouse_clicks[-1] += clicks
                    new_keystrokes[-1] += keys
                    new_mic[-1] = new_mic[-1] or mic_active
                    new_mouse_scroll[-1] += scroll
                    new_camera[-1] = new_camera[-1] or cam_active
                else:
                    # Add new app
                    new_apps.append(app_name)
                    new_durations.append(duration)
                    new_app_starts.append(start_time)
                    new_app_ends.append(end_time)
                    new_mouse_clicks.append(clicks)
                    new_keystrokes.append(keys)
                    new_mic.append(mic_active)
                    new_mouse_scroll.append(scroll)
                    new_camera.append(cam_active)
                i += 1
            
            # Create a new row with the processed data
            new_row = {
                'employeeId': employeeId,
                'app': new_apps,
                'app_durations': new_durations,
                'app_start_times': new_app_starts,
                'app_end_times': new_app_ends,
                'mouseClicks': new_mouse_clicks,
                'keystrokes': new_keystrokes,
                'mic': new_mic,
                'mouseScroll': new_mouse_scroll,
                'camera': new_camera,
                'start_time': day_start,
                'end_time': day_end
            }
            processed_rows.append(new_row)
        
        # Create a new DataFrame
        return pd.DataFrame(processed_rows)


    @staticmethod
    def delete_working_days(df):
        """
        Removes rows where 'start_time' is between 5th of September 2024 and 13th of September 2024.
        """
        # Ensure 'start_time' is in datetime format
        df['start_time'] = pd.to_datetime(df['start_time'])

        # Define the date range
        start_date = pd.to_datetime('2024-09-05')
        end_date = pd.to_datetime('2024-09-13')

        # Filter out rows within the specified date range
        df_filtered = df[~((df['start_time'] >= start_date) & (df['start_time'] <= end_date))]

        # Reset index if necessary
        return df_filtered.reset_index(drop=True)

    @staticmethod
    def add_workday_features(df):
        """
        Adds calculated features to the DataFrame:
        - Base employee ID extracted from 'employeeId'.
        - Hours until the next workday ('hours_until_next_workday').
        - Duration of the workday in minutes ('workday_duration').
        """
        # Convert 'start_time' and 'end_time' to datetime objects
        df['start_time'] = pd.to_datetime(df['start_time'])
        df['end_time'] = pd.to_datetime(df['end_time'])

        # Extract base employee ID
        df['base_employeeId'] = df['employeeId'].str.extract(r'^(.*)_\d+$')
        df['base_employeeId'] = df['base_employeeId'].fillna(df['employeeId'])

        # Sort the DataFrame
        df = df.sort_values(by=['base_employeeId', 'start_time']).reset_index(drop=True)

        # Calculate 'next_start_time'
        df['next_start_time'] = df.groupby('base_employeeId')['start_time'].shift(-1)

        # Calculate 'hours_until_next_workday'
        df['hours_until_next_workday'] = (df['next_start_time'] - df['end_time']).dt.total_seconds() / 3600
        df['hours_until_next_workday'] = df['hours_until_next_workday'].fillna(-1)

        # Calculate 'workday_duration' in minutes
        df['workday_duration'] = (df['end_time'] - df['start_time']).dt.total_seconds() / 60

        # Drop temporary columns if not needed
        df = df.drop(columns=['next_start_time', 'base_employeeId'])

        return df
    
    @staticmethod
    def process_workdays(df):
        # Ensure 'start_time' and 'end_time' are datetime objects
        df['start_time'] = pd.to_datetime(df['start_time'])
        df['end_time'] = pd.to_datetime(df['end_time'])

        # Extract base employee ID (e.g., 'emp_1' from 'emp_1_30')
        df['base_employeeId'] = df['employeeId'].str.extract(r'^(.*)_\d+$')
        df['base_employeeId'] = df['base_employeeId'].fillna(df['employeeId'])

        # Sort the DataFrame by 'base_employeeId' and 'start_time'
        df = df.sort_values(by=['base_employeeId', 'start_time']).reset_index(drop=True)

        # Delete all rows where 'workday_duration' < 45 minutes
        df = df[df['workday_duration'] >= 45].reset_index(drop=True)

        # Recalculate 'hours_until_next_workday' after deletion
        def recalculate_hours_until_next_workday(emp_df):
            emp_df = emp_df.sort_values('start_time').reset_index(drop=True)
            emp_df['next_start_time'] = emp_df['start_time'].shift(-1)
            emp_df['hours_until_next_workday'] = (emp_df['next_start_time'] - emp_df['end_time']).dt.total_seconds() / 3600
            emp_df['hours_until_next_workday'] = emp_df['hours_until_next_workday'].fillna(-1)
            return emp_df.drop(columns=['next_start_time'])

        # Reset index to ensure unique indices before grouping
        df = df.reset_index(drop=True)

        # Add a temporary unique identifier for each row
        df['unique_id'] = df.index

        # Apply the function and reset the index to avoid duplicate labels
        df = df.groupby('base_employeeId', group_keys=False).apply(recalculate_hours_until_next_workday).reset_index(drop=True)

        # Now, for each employee, merge adjacent workdays where 'hours_until_next_workday' < 3 hours
        indices_to_drop = set()
        for employee_id in df['base_employeeId'].unique():
            emp_df = df[df['base_employeeId'] == employee_id].sort_values('start_time').reset_index(drop=True)
            idx = 0
            while idx < len(emp_df) - 1:
                current_row = emp_df.loc[idx]
                next_row = emp_df.loc[idx + 1]
                current_unique_id = current_row['unique_id']
                next_unique_id = next_row['unique_id']

                # Calculate actual gap between current end_time and next start_time
                actual_gap_hours = (next_row['start_time'] - current_row['end_time']).total_seconds() / 3600

                # Continue to merge while actual gap is less than 3 hours
                if 0 <= actual_gap_hours < 3:
                    merge_unique_ids = [current_unique_id, next_unique_id]
                    total_pause_duration = actual_gap_hours * 60  # Convert to minutes

                    # Merge the workdays
                    first_unique_id = merge_unique_ids[0]
                    first_row_index = df.index[df['unique_id'] == first_unique_id][0]

                    # Initialize merged lists with the first workday's data
                    first_row = df.loc[df['unique_id'] == first_unique_id].iloc[0]
                    merged_app = first_row['app'].copy()
                    merged_app_durations = first_row['app_durations'].copy()
                    merged_app_start_times = first_row['app_start_times'].copy()
                    merged_app_end_times = first_row['app_end_times'].copy()
                    merged_mouse_clicks = first_row['mouseClicks'].copy()
                    merged_keystrokes = first_row['keystrokes'].copy()
                    merged_mic = first_row['mic'].copy()
                    merged_mouse_scroll = first_row['mouseScroll'].copy()
                    merged_camera = first_row['camera'].copy()

                    # Loop through the rest of the workdays to merge
                    for i in range(1, len(merge_unique_ids)):
                        uid_prev = merge_unique_ids[i - 1]
                        uid_curr = merge_unique_ids[i]

                        # Insert 'Pause' between workdays
                        merged_app.append('Pause')

                        # Retrieve times correctly
                        pause_start_time = df.loc[df['unique_id'] == uid_prev, 'end_time'].iloc[0]
                        pause_end_time = df.loc[df['unique_id'] == uid_curr, 'start_time'].iloc[0]

                        # Calculate pause duration
                        pause_duration_minutes = (pause_end_time - pause_start_time).total_seconds() / 60
                        merged_app_durations.append(pause_duration_minutes)
                        merged_app_start_times.append(pause_start_time)
                        merged_app_end_times.append(pause_end_time)
                        merged_mouse_clicks.append(0)
                        merged_keystrokes.append(0)
                        merged_mic.append(False)
                        merged_mouse_scroll.append(0)
                        merged_camera.append(False)

                        # Append the lists from the current workday
                        row_curr = df.loc[df['unique_id'] == uid_curr].iloc[0]
                        merged_app.extend(row_curr['app'])
                        merged_app_durations.extend(row_curr['app_durations'])
                        merged_app_start_times.extend(row_curr['app_start_times'])
                        merged_app_end_times.extend(row_curr['app_end_times'])
                        merged_mouse_clicks.extend(row_curr['mouseClicks'])
                        merged_keystrokes.extend(row_curr['keystrokes'])
                        merged_mic.extend(row_curr['mic'])
                        merged_mouse_scroll.extend(row_curr['mouseScroll'])
                        merged_camera.extend(row_curr['camera'])

                    # Update 'end_time', 'workday_duration', 'hours_until_next_workday'
                    last_unique_id = merge_unique_ids[-1]
                    last_row_index = df.index[df['unique_id'] == last_unique_id][0]
                    df.at[first_row_index, 'end_time'] = df.at[last_row_index, 'end_time']
                    total_workday_duration = df[df['unique_id'].isin(merge_unique_ids)]['workday_duration'].sum()
                    total_workday_duration += total_pause_duration
                    df.at[first_row_index, 'workday_duration'] = total_workday_duration
                    df.at[first_row_index, 'hours_until_next_workday'] = df.at[last_row_index, 'hours_until_next_workday']

                    # Update the merged lists
                    df.at[first_row_index, 'app'] = merged_app
                    df.at[first_row_index, 'app_durations'] = merged_app_durations
                    df.at[first_row_index, 'app_start_times'] = merged_app_start_times
                    df.at[first_row_index, 'app_end_times'] = merged_app_end_times
                    df.at[first_row_index, 'mouseClicks'] = merged_mouse_clicks
                    df.at[first_row_index, 'keystrokes'] = merged_keystrokes
                    df.at[first_row_index, 'mic'] = merged_mic
                    df.at[first_row_index, 'mouseScroll'] = merged_mouse_scroll
                    df.at[first_row_index, 'camera'] = merged_camera

                    # Mark other workdays for dropping
                    indices_to_drop.update(merge_unique_ids[1:])

                    # Update emp_df to reflect changes
                    emp_df.loc[idx, 'end_time'] = df.at[first_row_index, 'end_time']
                    emp_df.loc[idx, 'workday_duration'] = df.at[first_row_index, 'workday_duration']
                    emp_df.loc[idx, 'hours_until_next_workday'] = df.at[first_row_index, 'hours_until_next_workday']
                    emp_df = emp_df.drop(idx + 1).reset_index(drop=True)

                    # Do not increment idx to check for further merges
                else:
                    idx += 1  # Move to the next workday

            # Recalculate 'hours_until_next_workday' after merging
            emp_df = emp_df.sort_values('start_time').reset_index(drop=True)
            emp_df['next_start_time'] = emp_df['start_time'].shift(-1)
            emp_df['hours_until_next_workday'] = (emp_df['next_start_time'] - emp_df['end_time']).dt.total_seconds() / 3600
            emp_df['hours_until_next_workday'] = emp_df['hours_until_next_workday'].fillna(-1)

            # Update the main df with recalculated 'hours_until_next_workday'
            for idx2, row in emp_df.iterrows():
                df_index = df.index[df['unique_id'] == row['unique_id']][0]
                df.at[df_index, 'hours_until_next_workday'] = row['hours_until_next_workday']

        # Drop the rows marked for dropping
        df = df[~df['unique_id'].isin(indices_to_drop)].reset_index(drop=True)

        # Drop the temporary columns as they're no longer needed
        df = df.drop(columns=['base_employeeId', 'unique_id'])
        return df
    
    @staticmethod
    def save_processed_data(df, save_path):
        """
        Saves the processed DataFrame to the specified path.
        Ensures the directory exists and replaces the file if it already exists.
        """
        # Ensure the directory exists
        os.makedirs(os.path.dirname(save_path), exist_ok=True)

        # Save the DataFrame to the specified file
        df.to_csv(save_path, index=False, encoding='utf-8')
        print(f"Processed data saved to {save_path}")





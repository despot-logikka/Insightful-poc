from datetime import timedelta
from processing import WorkdayProcessor

if __name__ == "__main__":
    # Step 1: Load configuration
    config_path = "config.yaml"
    processor = WorkdayProcessor(config_path)

    # Step 2: Load the raw dataset
    raw_df = processor.load_csv(processor.config['paths']['input_data'], sep=';')

    # Step 3: Load browsers dataset for unique apps
    browsers = processor.load_csv(processor.config['paths']['browsers'], sep=';')

    # Step 4: Prepare initial data (refine timestamps, handle inactive apps, and private links)
    prepared_df = processor.prepare_initial_data(raw_df, browsers)

    # Step 5: Preprocess data (apply mappings, clean columns, and merge rows)
    df_processed = processor.preprocess_data(prepared_df)

    # Step 6: Create working days (split data into workday chunks)
    working_day_df = processor.create_working_day(df_processed, max_workday_gap=timedelta(hours=1))

    # Step 7: Merge 'Log Lost/Software Bug' entries and consecutive same apps
    working_day_df = processor.merge_log_lost_and_same_apps(working_day_df)

    # Step 8: Delete working days in the specified date range
    filtered_df = processor.delete_working_days(working_day_df)

    # Step 9: Add additional workday features
    enriched_df = processor.add_workday_features(filtered_df)

    # Step 10: Process workdays (final adjustments, merging, and filtering)
    final_df = processor.process_workdays(enriched_df)

    # Step 11: Save the final processed DataFrame
    save_path = processor.config['paths']['processed_data']
    processor.save_processed_data(final_df, save_path)

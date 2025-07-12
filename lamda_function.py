import awswrangler as wr
import pandas as pd
import urllib.parse
import os
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def validate_environment():
    """Validate required environment variables"""
    required_vars = [
        's3_cleansed_layer',
        'glue_catalog_db_name',
        'glue_catalog_table_name',
        'write_data_operation'
    ]
    missing_vars = [var for var in required_vars if var not in os.environ]
    if missing_vars:
        raise EnvironmentError(f"Missing environment variables: {missing_vars}")
    
    return {
        'cleansed_path': os.environ['s3_cleansed_layer'],
        'db_name': os.environ['glue_catalog_db_name'],
        'table_name': os.environ['glue_catalog_table_name'],
        'write_mode': os.environ['write_data_operation']
    }

def lambda_handler(event, context):
    try:
        # Validate environment
        env = validate_environment()
        logger.info(f"Environment config: {env}")
        
        # Parse S3 event
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
        s3_path = f"s3://{bucket}/{key}"
        logger.info(f"Processing file: {s3_path}")

        # Read and transform data
        df_raw = wr.s3.read_json(s3_path)
        logger.info(f"Raw data columns: {df_raw.columns.tolist()}")
        
        df_processed = pd.json_normalize(df_raw['items'])
        logger.info(f"Processed data shape: {df_processed.shape}")
        logger.debug(f"Sample data:\n{df_processed.head(2)}")

        # Write to S3 and Glue Catalog
        wr.s3.to_parquet(
            df=df_processed,
            path=env['cleansed_path'],
            dataset=True,
            database=env['db_name'],
            table=env['table_name'],
            mode=env['write_mode']
        )
        logger.info(f"Data written to: {env['cleansed_path']}")

        return {
            'statusCode': 200,
            'body': f"Successfully processed {key}",
            'output_location': env['cleansed_path'],
            'processed_rows': len(df_processed)
        }

    except wr.exceptions.NoFilesFound as e:
        logger.error(f"S3 file not found: {str(e)}")
        raise
    except pd.errors.EmptyDataError as e:
        logger.error(f"Empty/invalid JSON: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        raise

# For local testing (if needed)
if __name__ == "__main__":
    mock_event = {
        "Records": [{
            "s3": {
                "bucket": {"name": "test-bucket"},
                "object": {"key": "test/key.json"}
            }
        }]
    }
    os.environ.update({
        's3_cleansed_layer': 's3://test-output/',
        'glue_catalog_db_name': 'test_db',
        'glue_catalog_table_name': 'test_table',
        'write_data_operation': 'overwrite'
    })
    print(lambda_handler(mock_event, None))
import argparse
import logging
import json
import os
from backup import db_connect,backup,restore, compression,notify

def setup_logging(log_file):
    """
    Set up logging configuration. Logs to the specified file.
    """
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logging.info(f"Logging initialized. Log file: {log_file}")


def main():
    parser = argparse.ArgumentParser(description="Database Backup CLI Tool")
    # Main arguments
    parser.add_argument('operation', choices=['backup', 'restore'], help="Operation to perform: backup or restore")

    # Database connection args
    parser.add_argument('--db-type', required=True,choices=["mysql", "postgresql", "mongodb", "sqlite"], help="Database type (mysql, postgresql, mongodb, sqlite)")
    parser.add_argument('--config', required=True, help="Path to JSON configuration file for database connection")

    # Backup args
    parser.add_argument('--output', help="Output backup file path")
    parser.add_argument('--compress', action='store_true', help="Compress the backup")

    # Storage args
    parser.add_argument('--cloud', choices=['s3', 'gcs', 'azure'], help="Cloud storage option")
    parser.add_argument('--bucket', help="Cloud storage bucket name")

    # Log file argument
    parser.add_argument('--log-file', help="Path to the log file", default='backup.log')

    # Parse the arguments
    args = parser.parse_args()

    logging.info("operation",args.operation)
    logging.info("db_type",args.db_type)

    # Set up logging with custom or default log file
    setup_logging(args.log_file)

    # Log start of the operation
    logging.info(f"Starting {args.operation} operation.")

    #Load database configuration from JSON file
    try:
        with open (args.config,'r') as config_file:
            config = json.load(config_file)
            db_config = config['database']
    except Exception as e:
        logging.error(f"Failed to read config file: {e}")
        print(f"Failed to read config file: {e}")
        return

    # Ensure all required db_config fields are present
    required_keys = ['host','port','user','password','database']
    if not all(key in db_config for key in required_keys):
        logging.error("Config file is missing required database configuration keys.")
        print("Config file is missing required database configuration keys.")
        return

    try:
        # Operation: Backup
        if args.operation == "backup":
            conn = db_connect.connect_to_db(args.db_type,db_config)
            if conn:
                logging.info(f"Successfully connected to {args.db_type} database.")
                backup_file = backup.full_backup(args.db_type, db_config['database'], db_config, args.output, logging)
                logging.info(f"Backup created at {args.output}.")

                if args.compress:
                    compression.compress_backup(backup_file, args.output + ".tar.gz", logger=logging)
                    logging.info(f"Backup compressed to {args.output}.tar.gz")
            else:
                logging.error(f"Failed to connect to {args.db_type} database.")
        #operation: restore
        elif args.operation == 'restore':
            if not args.backup_file:
                logging.error("Error: --backup-file is required for restore operations.")
                print("Error: --backup-file is required for restore operations.")
                return

            restore.restore_backup(args.db_type, args.backup_file, db_config, logger=logging)
            logging.info(f"Database restored from {args.backup_file}.")

    except Exception as e:
        logging.error(f"An error occurred during the {args.operation} operation: {e}")
        print(f"An error occurred: {e}")


    # Log the end of the operation
    logging.info(f"{args.operation.capitalize()} operation completed.")




if __name__ == '__main__':
    main()
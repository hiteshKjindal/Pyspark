import pandas as pd
import pyarrow
from sqlalchemy import create_engine
import pyodbc
import logging
from asql import cnxn # to  import database connection

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()


# Function to execute stored procedures
def execute_stored_procedure(conn, procedure_name):
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"EXEC {procedure_name}")
            logger.info(f"Stored procedure {procedure_name} executed successfully.")
        conn.commit()  # Commit the changes after stored procedure execution
    except Exception as e:
        logger.error(f"Error executing stored procedure {procedure_name}: {e}")
        raise

def main():
    try:
            conn=cnxn
            # Truncate the staging table
            with conn.cursor() as cursor:
                cursor.execute('TRUNCATE TABLE ride_staging')
                conn.commit()
                logger.info("Staging table 'ride_staging' truncated.")

            # Load data into the staging table
            url = r'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet'
            df = pd.read_parquet(url)
            df.rename(columns={'PULocationID':'pickup_location_id','DOLocationID':'drop_location_id'}, inplace=True)
            df.to_sql('ride_staging', create_engine('mssql+pyodbc://', creator=lambda: conn), if_exists='append', index=False)
            conn.commit()
            logger.info("Data loaded into 'ride_staging' table.")

            # Execute stored procedures
            procedures = ['sp_InsertIntoDatetimeDim', 'sp_InsertIntovendorDim', 'sp_InsertIntoratecodeDim', 'sp_InsertIntopaymentDim']
            for proc in procedures:
                execute_stored_procedure(conn, proc)

    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        conn.commit()
        cursor.close()
        conn.close()
        logger.info("Closing database connection.")
    
if __name__ == "__main__":
    main()

import psycopg2
import pandas as pd
import json
import os
import math
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_credentials(credentials_path="/Users/stephanielaface/Documents/GitHub/encounters/credentials.json"):
    """
    Load credentials from JSON file
    
    Args:
        credentials_path: Path to credentials.json file
    
    Returns:
        Dictionary containing credentials
    """
    try:
        with open(credentials_path, 'r') as f:
            credentials = json.load(f)
        return credentials
    except FileNotFoundError:
        print(f"Error: credentials.json not found at {credentials_path}")
        print("Please ensure the file exists in the current directory or provide the correct path")
        return None
    except json.JSONDecodeError:
        print("Error: Invalid JSON format in credentials.json")
        return None

def read_table_data(connection_params, table_name):
    """
    Read data from a specific table using psycopg2
    
    Args:
        connection_params: Dictionary containing database connection parameters
        table_name: Full table name (schema.table)
    
    Returns:
        Pandas DataFrame with the table data
    """
    conn = None
    cursor = None
    
    try:
        # Connect to Redshift
        conn = psycopg2.connect(**connection_params)
        cursor = conn.cursor()
        
        # Query to get data
        query = f"SELECT * FROM {table_name};"
        logging.info(f"Executing query for {table_name}...")
        
        # Use pandas to read the query results directly
        df = pd.read_sql_query(query, conn)
        
        logging.info(f"✓ Successfully read {len(df)} rows from {table_name}")
        return df
        
    except Exception as e:
        logging.error(f"Error reading {table_name}: {e}")
        return pd.DataFrame()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def read_all_encounter_tables(connection_params):
    """
    Read all TCHP encounter tables from Redshift
    
    Args:
        connection_params: Dictionary containing database connection parameters
    
    Returns:
        Dictionary containing all dataframes
    """
    tables = {
        '00': 'srh_encounters.tx_tchp_encounters_00',
        '100': 'srh_encounters.tx_tchp_encounters_100',
        '150': 'srh_encounters.tx_tchp_encounters_150',
        '20p': 'srh_encounters.tx_tchp_encounters_20p',
        '310': 'srh_encounters.tx_tchp_encounters_310',
        '40p': 'srh_encounters.tx_tchp_encounters_40p',
        '431': 'srh_encounters.tx_tchp_encounters_431',
        'trailer': 'srh_encounters.tx_tchp_encounters_trailer'
    }
    
    dataframes = {}
    
    logging.info("Starting to read encounter tables from Redshift")
    for key, table_name in tables.items():
        df = read_table_data(connection_params, table_name)
        dataframes[key] = df
    
    return dataframes

def test_connection(connection_params):
    """
    Test the database connection
    
    Args:
        connection_params: Dictionary containing database connection parameters
    
    Returns:
        Boolean indicating if connection was successful
    """
    conn = None
    cursor = None
    
    try:
        # Debug: Print connection params (without password)
        debug_params = {k: v if k != 'password' else '***' for k, v in connection_params.items()}
        logging.info(f"Attempting connection with params: {debug_params}")
        
        # Connect to Redshift
        conn = psycopg2.connect(**connection_params)
        cursor = conn.cursor()
        
        # Test query
        cursor.execute("SELECT current_database(), current_user, version();")
        result = cursor.fetchone()
        
        logging.info(f"✓ Successfully connected to Redshift")
        logging.info(f"  Database: {result[0]}")
        logging.info(f"  User: {result[1]}")
        
        return True
        
    except Exception as e:
        logging.error(f"Failed to connect to Redshift: {e}")
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def write_encounter_file(dataframes, output_directory="/Users/stephanielaface/Documents/GitHub/encounters/tchp/output"):
    """
    Write encounter data to flat file in proper hierarchical order
    
    Args:
        dataframes: Dictionary containing all encounter dataframes
        output_directory: Directory to save the output file
    
    Returns:
        Path to the output file
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_directory, exist_ok=True)
    
    # Generate timestamp for filename
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    output_file_path = os.path.join(output_directory, f"CSV_SFR_TCH_PROF_TX_CAID_{timestamp}.DAT")
    
    try:
        # Check if we have data
        total_records = sum(len(df) for df in dataframes.values())
        if total_records == 0:
            logging.warning("No data to write - all tables are empty")
            return None
        
        logging.info(f"Writing {total_records} total records to output file")
        
        # Write 00 (header) record
        with open(output_file_path, 'w', newline="") as output_file:
            if not dataframes['00'].empty:
                dataframes['00'].to_csv(output_file, index=False, sep='|', header=False)
                logging.info(f"  Written {len(dataframes['00'])} header (00) records")
        
        # Write hierarchical records for each 100 record
        records_processed = 0
        with open(output_file_path, 'a', newline="") as output_file:
            
            if not dataframes['100'].empty:
                for idx, row_100 in dataframes['100'].iterrows():
                    # Get the key from the 100 record
                    key_record = row_100.get("c_100_key")
                    
                    if pd.isna(key_record):
                        logging.warning(f"  Skipping 100 record at index {idx} - no key found")
                        continue
                    
                    # Write 100 record
                    row_100.to_frame().T.to_csv(output_file, index=False, sep="|", header=False)
                    records_processed += 1
                    
                    # Write corresponding 150 records
                    if not dataframes['150'].empty and 'c_150_key' in dataframes['150'].columns:
                        df_150_subset = dataframes['150'][dataframes['150']['c_150_key'] == key_record]
                        if not df_150_subset.empty:
                            df_150_subset.to_csv(output_file, index=False, sep='|', header=False)
                    
                    # Write corresponding 20p records
                    if not dataframes['20p'].empty and 'c_20p_key' in dataframes['20p'].columns:
                        df_20p_subset = dataframes['20p'][dataframes['20p']['c_20p_key'] == key_record]
                        if not df_20p_subset.empty:
                            df_20p_subset.to_csv(output_file, index=False, sep='|', header=False)
                    
                    # Write corresponding 310 records
                    if not dataframes['310'].empty and 'c_310_key' in dataframes['310'].columns:
                        df_310_subset = dataframes['310'][dataframes['310']['c_310_key'] == key_record]
                        if not df_310_subset.empty:
                            df_310_subset.to_csv(output_file, index=False, sep='|', header=False)
                    
                    # Write corresponding 40p records
                    if not dataframes['40p'].empty and 'c_40p_key' in dataframes['40p'].columns:
                        df_40p_subset = dataframes['40p'][dataframes['40p']['c_40p_key'] == key_record]
                        if not df_40p_subset.empty:
                            df_40p_subset.to_csv(output_file, index=False, sep='|', header=False)
                    
                    # Write corresponding 431 records
                    if not dataframes['431'].empty and 'c_431_key' in dataframes['431'].columns:
                        df_431_subset = dataframes['431'][dataframes['431']['c_431_key'] == key_record]
                        if not df_431_subset.empty:
                            df_431_subset.to_csv(output_file, index=False, sep='|', header=False)
                
                logging.info(f"  Processed {records_processed} claim (100) records with their detail records")
        
        # Write trailer record
        with open(output_file_path, 'a', newline="") as output_file:
            if not dataframes['trailer'].empty:
                dataframes['trailer'].to_csv(output_file, index=False, sep='|', header=False)
                logging.info(f"  Written {len(dataframes['trailer'])} trailer records")
        
        # Get file size
        file_size = os.path.getsize(output_file_path) / (1024 * 1024)  # Convert to MB
        logging.info(f"✓ Successfully created output file: {output_file_path}")
        logging.info(f"  File size: {file_size:.2f} MB")
        
        return output_file_path
    
    except Exception as e:
        logging.error(f"Failed to write output file: {str(e)}")
        return None

def main():
    """
    Main function to orchestrate the TCHP encounter data extraction
    """
    print("\n" + "="*60)
    print("TCHP ENCOUNTER DATA EXTRACTION TOOL")
    print("="*60 + "\n")
    
    logging.info("Starting TCHP encounter data extraction process")
    
    # Load credentials - try current directory first, then default path
    credentials = load_credentials("credentials.json")
    if credentials is None:
        credentials = load_credentials()
        if credentials is None:
            logging.error("Failed to load credentials. Exiting.")
            return
    
    # Extract Redshift connection parameters - handle both flat and nested structure
    if "redshift" in credentials:
        # Nested structure (like your working script)
        redshift_params = credentials.get("redshift", {})
    else:
        # Flat structure
        redshift_params = credentials
    
    # Validate required parameters
    required_params = ['host', 'port', 'database', 'user', 'password']
    missing_params = [param for param in required_params if not redshift_params.get(param)]
    
    if missing_params:
        print(f"Error: Missing required Redshift parameters: {missing_params}")
        print(f"Found parameters: {list(redshift_params.keys())}")
        return
    
    # Ensure port is an integer
    if isinstance(redshift_params.get('port'), str):
        redshift_params['port'] = int(redshift_params['port'])
    
    # Test connection first
    if not test_connection(redshift_params):
        logging.error("Connection test failed. Please check your credentials.")
        return
    
    try:
        # Read all encounter tables
        dataframes = read_all_encounter_tables(redshift_params)
        
        # Print summary of data read
        print("\n" + "-"*50)
        print("DATA READ SUMMARY")
        print("-"*50)
        for key, df in dataframes.items():
            status = "✓" if not df.empty else "✗"
            print(f"{status} Table {key:10s}: {len(df):,} records")
        print("-"*50 + "\n")
        
        # Write to output file
        output_path = write_encounter_file(dataframes)
        
        if output_path:
            print("\n" + "="*60)
            print("✓ EXTRACTION COMPLETED SUCCESSFULLY")
            print("="*60)
            print(f"Output file: {output_path}")
            print("="*60 + "\n")
        else:
            logging.error("Failed to create output file")
            print("\n✗ Extraction failed - check logs for details\n")
    
    except Exception as e:
        logging.error(f"An error occurred during processing: {str(e)}")
        print(f"\n✗ Process failed: {str(e)}\n")

if __name__ == "__main__":
    main()
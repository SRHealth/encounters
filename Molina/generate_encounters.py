import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
import redshift_connector
import time
import boto3
from botocore.exceptions import NoCredentialsError
import os

# Start timer
start_time = time.time()

# Load environment variables from .env file
load_dotenv()

# Database connection
conn = redshift_connector.connect(
    host=os.getenv('REDSHIFT_HOST'),
    database=os.getenv('REDSHIFT_DATABASE'),
    user=os.getenv('REDSHIFT_USER'),
    password=os.getenv('REDSHIFT_PASSWORD')
)
cursor = conn.cursor()

# SQL query
query = '''
select 
    *
from srh_encounters.v_837_molina_encounters e
where to_date(e.pickup_date,'YYYYMMDD') >= '2025-01-01'
    and to_date(e.pickup_date,'YYYYMMDD') < '2025-08-01'
;
'''

# Output configuration - using relative path
# This assumes the script is run from within the srh_encounters directory
output_folder = './Molina/output'  # Relative path from script location
# Alternative: If script is in srh_encounters/scripts/, use '../Molina/output'

claims_per_file = 1000
base_filename = 'MHCA_SafeRide_MC_837P'

# Create output folder if it doesn't exist
os.makedirs(output_folder, exist_ok=True)

# Print the absolute path for confirmation
print(f"Output directory: {os.path.abspath(output_folder)}")

try:
    # Execute query
    print("Executing query...")
    cursor.execute(query)
    
    # Fetch column names
    column_names = [desc[0] for desc in cursor.description]
    
    # Initialize variables for pagination
    file_counter = 0
    row_counter = 0
    current_batch = []
    
    # Get initial timestamp
    base_timestamp = datetime.now()
    
    print("Processing results...")
    
    # Fetch and process rows in batches
    while True:
        # Fetch a batch of rows
        rows = cursor.fetchmany(1000)
        
        if not rows:
            # No more rows to fetch
            break
        
        for row in rows:
            current_batch.append(row)
            row_counter += 1
            
            # Check if we've reached the limit for current file
            if len(current_batch) >= claims_per_file:
                # Create DataFrame from current batch
                df = pd.DataFrame(current_batch, columns=column_names)
                
                # Generate timestamp for this file (base + file_counter seconds)
                file_timestamp = base_timestamp + timedelta(seconds=file_counter)
                timestamp_str = file_timestamp.strftime('%Y%m%d%H%M%S')
                
                # Generate filename
                filename = f"{base_filename}_{timestamp_str}.txt"
                filepath = os.path.join(output_folder, filename)
                
                # Save to file with TAB delimiter
                df.to_csv(filepath, 
                         sep='\t',           # Tab delimiter
                         index=False, 
                         header=True,
                         na_rep='',          # Replace NaN values with empty string
                         lineterminator='\n')
                
                print(f"Created file: {filename} with {len(current_batch)} claims")
                
                # Wait 1 second before processing next file (optional - remove if not needed)
                time.sleep(1)
                
                # Reset for next batch
                current_batch = []
                file_counter += 1
    
    # Handle any remaining records
    if current_batch:
        # Create DataFrame from remaining batch
        df = pd.DataFrame(current_batch, columns=column_names)
        
        # Generate timestamp for this file
        file_timestamp = base_timestamp + timedelta(seconds=file_counter)
        timestamp_str = file_timestamp.strftime('%Y%m%d%H%M%S')
        
        # Generate filename
        filename = f"{base_filename}_{timestamp_str}.txt"
        filepath = os.path.join(output_folder, filename)
        
        # Save to file with TAB delimiter
        df.to_csv(filepath, 
                 sep='\t',           # Tab delimiter
                 index=False, 
                 header=True,
                 na_rep='',          # Replace NaN values with empty string
                 lineterminator='\n')
        
        print(f"Created file: {filename} with {len(current_batch)} claims")
    
    # Print summary
    print(f"\nProcessing complete!")
    print(f"Total claims processed: {row_counter}")
    print(f"Total files created: {file_counter + (1 if current_batch else 0)}")
    print(f"Files saved to: {os.path.abspath(output_folder)}")
    
except Exception as e:
    print(f"Error occurred: {str(e)}")
    
finally:
    # Close cursor and connection
    cursor.close()
    conn.close()

# End timer
end_time = time.time()
execution_time = end_time - start_time
print(f"\nTotal execution time: {execution_time:.2f} seconds")
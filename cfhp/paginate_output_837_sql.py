import psycopg2
import json
import os
import math
from datetime import datetime

def load_credentials(credentials_path="/Users/stephanielaface/Documents/GitHub/encounters/txmcd_encounters/credentials.json"):
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

def convert_nulls_to_strings(obj):
    """Recursively convert all values to strings and None/null to 'None'"""
    if obj is None:
        return "None"
    elif isinstance(obj, dict):
        return {k: convert_nulls_to_strings(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_nulls_to_strings(item) for item in obj]
    else:
        # Convert everything else to string
        return str(obj)

def fetch_and_convert_to_json(connection_params, output_base_path, rows_per_file=1500):
    """
    Fetch data from Redshift and convert to JSON files with specified row limits
    
    Args:
        connection_params: Dictionary with Redshift connection parameters
        output_base_path: Base path for output files
        rows_per_file: Maximum rows per file (default 1500)
    """
    
    base_query = """

    SELECT *

    FROM srh_encounters.v_srh_humana_medicaid_awsb2b
       
    limit 5 
    """
    
    try:
        # Connect to Redshift
        conn = psycopg2.connect(**connection_params)
        cursor = conn.cursor()
        
        print("Fetching data from Redshift...")
        
        # Execute the query
        cursor.execute(base_query)
        
        # Get column names
        column_names = [desc[0] for desc in cursor.description]
        print(f"Found {len(column_names)} columns")
        
        # Fetch all rows
        rows = cursor.fetchall()
        total_rows = len(rows)
        
        print(f"Retrieved {total_rows} rows from Redshift")
        
        # Calculate number of files needed
        num_files = math.ceil(total_rows / rows_per_file)
        print(f"Will create {num_files} JSON files with {rows_per_file} rows each")
        
        # Create extracted_jsons directory
        extracted_dir = os.path.join(output_base_path, "extracted_jsons")
        os.makedirs(extracted_dir, exist_ok=True)
        print(f"Created directory: {extracted_dir}")
        
        # Process data in chunks
        for i in range(num_files):
            start_idx = i * rows_per_file
            end_idx = min((i + 1) * rows_per_file, total_rows)
            
            # Get the chunk of rows
            chunk_rows = rows[start_idx:end_idx]
            
            # Convert rows to list of dictionaries
            json_objects = []
            for row in chunk_rows:
                # Create dictionary from row data
                row_dict = dict(zip(column_names, row))
                
                # Convert nulls to "None" and all values to strings
                converted_obj = convert_nulls_to_strings(row_dict)
                json_objects.append(converted_obj)
            
            # Generate filename with letter suffix (a, b, c, etc.)
            file_suffix = chr(ord('a') + i)
            output_filename = f"humana_encounters_input_11_12_25_{file_suffix}.json"
            output_path = os.path.join(extracted_dir, output_filename)
            
            # Write JSON array to file
            with open(output_path, 'w') as f:
                json.dump(json_objects, f, indent=2)
            
            print(f"✓ Created file {i+1}/{num_files}: {output_filename} ({len(json_objects)} records)")
        
        print(f"\n✅ All JSON files created successfully in: {extracted_dir}")
        return True
        
    except Exception as e:
        print(f"Error processing data: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()



if __name__ == "__main__":
    # Load credentials from JSON file
    credentials = load_credentials("credentials.json")
    
    if credentials is None:
        print("Failed to load credentials. Exiting.")
        exit(1)
    
    # Extract Redshift connection parameters
    redshift_params = credentials.get("redshift", {})
    
    if not redshift_params:
        print("Error: 'redshift' section not found in credentials.json")
        exit(1)
    
    # Validate required parameters
    required_params = ['host', 'port', 'database', 'user', 'password']
    missing_params = [param for param in required_params if not redshift_params.get(param)]
    
    if missing_params:
        print(f"Error: Missing required Redshift parameters: {missing_params}")
        exit(1)
    
    # Set output path
    output_path = '/Users/stephanielaface/Documents/GitHub/encounters/txmcd_encounters/output_837s'
    
    print("TXMCD Encounters JSON Splitter")
    print("=" * 40)
    print(f"Output directory: {output_path}/extracted_jsons/")
    
    # Create split JSON files
    print("\nCreating split JSON files...")
    success = fetch_and_convert_to_json(redshift_params, output_path, 1500)
    
    if success:
        print(f"\n🎉 Split JSON files created successfully!")
        print(f"📁 Files location: {output_path}/extracted_jsons/")
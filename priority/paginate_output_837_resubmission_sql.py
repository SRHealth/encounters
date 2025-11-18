import psycopg2
import json
import math
import boto3
from datetime import datetime
from io import StringIO

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

def fetch_and_upload_to_s3(connection_params, s3_bucket, s3_prefix="extracted_jsons", rows_per_file=1500, aws_credentials=None):
    """
    Fetch data from Redshift and upload to S3 as JSON files with specified row limits
    
    Args:
        connection_params: Dictionary with Redshift connection parameters
        s3_bucket: S3 bucket name
        s3_prefix: S3 prefix/folder path (default "extracted_jsons")
        rows_per_file: Maximum rows per file (default 2500)
        aws_credentials: AWS credentials dict with 'access_key_id' and 'secret_access_key'
    """
    
    base_query = """

    select * 

    from srh_encounters.v_priority_medicaid_encounters_awsb2b_resubmission e 

    where 
    e.fromZipcode is not null
    and e.toZipcode is not null
    and e.fromState is not null
    and e.toState is not null
    and e.insuredZip is not null
    and e.insuredAddress is not null
    and e.insuredzip <> 'State'

    ;

    """
    
    conn = None
    cursor = None
    
    try:
        # Initialize S3 client
        if aws_credentials:
            s3_client = boto3.client(
                's3',
                aws_access_key_id=aws_credentials.get('access_key_id'),
                aws_secret_access_key=aws_credentials.get('secret_access_key'),
                region_name=aws_credentials.get('region', 'us-west-2')
            )
        else:
            # Use default credentials (IAM role, environment variables, or AWS CLI config)
            s3_client = boto3.client('s3')
        
        print(f"Initialized S3 client for bucket: {s3_bucket}")
        
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
        
        # Get current date in YYYYMMDD format
        date_str = datetime.now().strftime("%Y%m%d")
        
        # Process data in chunks and upload to S3
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
            
            # Generate filename with numeric counter (1, 2, 3, etc.)
            file_counter = i + 1
            output_filename = f"priority_encounters_input_{date_str}_{file_counter}.json"
            
            # Create S3 key
            s3_key = f"{s3_prefix}/{output_filename}" if s3_prefix else output_filename
            
            # Convert JSON to string for upload
            json_string = json.dumps(json_objects, indent=2)
            
            # Upload to S3
            s3_client.put_object(
                Bucket=s3_bucket,
                Key=s3_key,
                Body=json_string,
                ContentType='application/json'
            )
            
            print(f"✓ Uploaded file {file_counter}/{num_files}: s3://{s3_bucket}/{s3_key} ({len(json_objects)} records)")
        
        print(f"\n✅ All JSON files uploaded successfully to S3 bucket: {s3_bucket}")
        print(f"📁 Files location: s3://{s3_bucket}/{s3_prefix}/")
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
    
    # Validate required Redshift parameters
    required_params = ['host', 'port', 'database', 'user', 'password']
    missing_params = [param for param in required_params if not redshift_params.get(param)]
    
    if missing_params:
        print(f"Error: Missing required Redshift parameters: {missing_params}")
        exit(1)
    
    # S3 configuration
    S3_BUCKET = "aws-b2b-edi"  # Replace with your S3 bucket name
    S3_PREFIX = "production/priority-input-json/ca-bb4007205c4d48cb8/tp-f1eb2352582e481b8"    # Folder path in S3
    
    # Optional: Load AWS credentials from credentials.json
    # If not provided, will use default AWS credentials (IAM role, environment variables, etc.)
    aws_creds = credentials.get("aws", None)
    
    print("Encounters JSON Uploader to S3")
    print("=" * 40)
    print(f"Target S3 location: s3://{S3_BUCKET}/{S3_PREFIX}/")
    
    # Fetch from Redshift and upload to S3
    print("\nFetching data and uploading to S3...")
    success = fetch_and_upload_to_s3(
        redshift_params, 
        S3_BUCKET, 
        S3_PREFIX, 
        rows_per_file=1500,
        aws_credentials=aws_creds
    )
    
    if success:
        print(f"\n🎉 All files uploaded to S3 successfully!")
        print(f"📁 S3 Bucket: {S3_BUCKET}")
        print(f"📁 S3 Prefix: {S3_PREFIX}")
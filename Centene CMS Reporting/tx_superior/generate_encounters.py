#!/usr/bin/env python3
# File: paginate_data.py

import os
import pandas as pd
import psycopg2
import psycopg2.extras
import time
import csv
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime

# Load environment variables from .env file if it exists
load_dotenv()

# Database connection parameters
# Update these with your actual connection details or use environment variables
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "database_name")
DB_USER = os.getenv("DB_USER", "username")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")

# Pagination settings
PAGE_SIZE = 5000  # Process 5000 claims per page
OUTPUT_DIR = "output"  # Directory to save tab-delimited text files

def create_output_dir():
    """Create output directory if it doesn't exist."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)

def get_query():
    # Use the provided SQL query directly
    query = "SELECT * FROM srh_encounters.v_837_shp_cms_encounters;"
    return query

def paginate_query(base_query, page_size, page_number):
    """Add pagination to SQL query for PostgreSQL."""
    # First remove any trailing semicolon from the base query
    clean_query = base_query.strip()
    if clean_query.endswith(';'):
        clean_query = clean_query[:-1]
    
    # PostgreSQL-specific pagination with LIMIT and OFFSET
    paginated_query = f"""
    {clean_query}
    ORDER BY "authorizationNumber1"
    LIMIT {page_size} OFFSET {page_size * page_number}
    """
    return paginated_query

def connect_to_db():
    """Connect to the database."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        raise

def execute_query(conn, query):
    """Execute query and return results."""
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cursor.execute(query)
    results = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]
    cursor.close()
    return results, column_names

def save_to_csv(data, column_names, page_number):
    """Save data to tab-delimited text file with the new naming convention."""
    # Get current date in YYYYMMDD format
    current_date = datetime.now().strftime("%Y%m%d")
    
    # Format sequence number with leading zeros
    sequence_number = str(page_number).zfill(3)
    
    # Create filename using the requested pattern: DMIT09.INP837D.UPLOAD.YYYYMMDD_SR_XXX.TXT
    filename = f"DTXT03.INP837D.UPLOAD.{current_date}_SR_{sequence_number}.TXT"
    file_path = os.path.join(OUTPUT_DIR, filename)
    
    with open(file_path, 'w', newline='') as txtfile:
        writer = csv.writer(txtfile, delimiter='\t')
        writer.writerow(column_names)  # Write header
        for row in data:
            writer.writerow(row)
    
    print(f"Saved page {page_number} to {file_path}")

def process_data():
    """Main function to process data with pagination."""
    create_output_dir()
    base_query = get_sql_query()
    
    try:
        conn = connect_to_db()
        
        page = 0
        more_data = True
        total_rows_processed = 0
        
        # Process pages until no more data is found
        while more_data:
            start_time = time.time()
            print(f"Processing page {page+1}...")
            
            query = paginate_query(base_query, PAGE_SIZE, page)
            data, column_names = execute_query(conn, query)
            
            if not data:
                print(f"No more data found after page {page}")
                more_data = False
                break
                
            # We use page+1 for the sequence number to start at 001 instead of 000
            save_to_csv(data, column_names, page+1)
            
            # Update counters
            total_rows_processed += len(data)
            page += 1
            
            elapsed_time = time.time() - start_time
            print(f"Page {page} processed with {len(data)} rows in {elapsed_time:.2f} seconds")
            print(f"Total rows processed so far: {total_rows_processed}")
            
            # If we got fewer rows than PAGE_SIZE, we've reached the end
            if len(data) < PAGE_SIZE:
                more_data = False
            
            # Optional: add a small delay between queries to reduce database load
            time.sleep(0.5)
        
        conn.close()
        print(f"Data processing complete! Total rows processed: {total_rows_processed}")
        
    except Exception as e:
        print(f"Error processing data: {e}")

if __name__ == "__main__":
    process_data()
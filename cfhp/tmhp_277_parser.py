import csv
import re
import io
from pathlib import Path
from typing import List, Dict, Optional

def parse_edi_277_mx_errors(content: str, source_file: str = "") -> List[Dict[str, str]]:
    """
    Parse EDI 277 content to extract TRN values and associated MX errors.
    
    Args:
        content (str): EDI 277 file content
        source_file (str): Source file name for tracking
        
    Returns:
        list: List of dictionaries containing TRN and MX error data
    """
    # Split by segment terminator (~ in this case)
    segments = content.strip().split('~')
    
    # Initialize variables
    current_trn = None
    results = []
    current_record = {}
    mx_errors = []
    
    for segment in segments:
        # Clean the segment
        segment = segment.strip()
        if not segment:
            continue
            
        # Split by element separator (* in this case)
        elements = segment.split('*')
        
        # Check for TRN segment
        if elements[0] == 'TRN' and len(elements) >= 3:
            # If we have a previous record with MX errors, save it
            if current_trn and mx_errors:
                current_record['TRN'] = current_trn
                current_record['source_file'] = source_file
                # Add MX errors to the record
                for i, mx_error in enumerate(mx_errors):
                    current_record[f'MX_Error_{i+1}'] = mx_error
                results.append(current_record)
            
            # Start new record
            current_trn = elements[2]  # TRN value is in the third element
            current_record = {}
            mx_errors = []
        
        # Check for STC segment with MX errors
        elif elements[0] == 'STC' and len(elements) >= 2:
            # Look for MX error codes in the first element after STC
            if 'MX' in elements[1]:
                # Extract just the MX error code using regex
                match = re.search(r'MX\d+', elements[1])
                if match:
                    mx_error_code = match.group()
                    mx_errors.append(mx_error_code)
    
    # Don't forget the last record
    if current_trn and mx_errors:
        current_record['TRN'] = current_trn
        current_record['source_file'] = source_file
        for i, mx_error in enumerate(mx_errors):
            current_record[f'MX_Error_{i+1}'] = mx_error
        results.append(current_record)
    
    return results

def parse_edi_277_from_file(file_path: str) -> List[Dict[str, str]]:
    """
    Parse EDI 277 file to extract TRN values and associated MX errors.
    
    Args:
        file_path (str): Path to the EDI 277 file
        
    Returns:
        list: List of dictionaries containing TRN and MX error data
    """
    with open(file_path, 'r') as file:
        content = file.read()
    
    return parse_edi_277_mx_errors(content, Path(file_path).name)

def get_max_mx_errors(data: List[Dict[str, str]]) -> int:
    """
    Determine the maximum number of MX errors across all records.
    
    Args:
        data (list): List of dictionaries containing TRN and MX error data
        
    Returns:
        int: Maximum number of MX errors found
    """
    if not data:
        return 0
    
    return max(len([k for k in record.keys() if k.startswith('MX_Error_')]) 
               for record in data)

def normalize_mx_error_records(data: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """
    Normalize records to have consistent MX error fields.
    
    Args:
        data (list): List of dictionaries containing TRN and MX error data
        
    Returns:
        list: Normalized list with consistent field structure
    """
    if not data:
        return []
    
    max_errors = get_max_mx_errors(data)
    normalized_data = []
    
    for record in data:
        normalized_record = {
            'TRN': record.get('TRN', ''),
            'source_file': record.get('source_file', '')
        }
        
        # Add all MX error fields, even if empty
        for i in range(1, max_errors + 1):
            normalized_record[f'MX_Error_{i}'] = record.get(f'MX_Error_{i}', '')
        
        normalized_data.append(normalized_record)
    
    return normalized_data

def get_mx_error_fieldnames(max_errors: int) -> List[str]:
    """
    Generate fieldnames for MX error CSV export.
    
    Args:
        max_errors (int): Maximum number of MX errors
        
    Returns:
        list: List of field names for CSV
    """
    fieldnames = ['TRN', 'source_file']
    for i in range(1, max_errors + 1):
        fieldnames.append(f'MX_Error_{i}')
    return fieldnames

def write_mx_errors_to_csv(data: List[Dict[str, str]], output_path: str) -> None:
    """
    Write the extracted MX error data to a CSV file.
    
    Args:
        data (list): List of dictionaries containing TRN and MX error data
        output_path (str): Path for the output CSV file
    """
    if not data:
        print("No MX errors found in the file.")
        return
    
    # Normalize data and get fieldnames
    normalized_data = normalize_mx_error_records(data)
    max_errors = get_max_mx_errors(data)
    fieldnames = get_mx_error_fieldnames(max_errors)
    
    # Write to CSV
    with open(output_path, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(normalized_data)
    
    print(f"CSV file created successfully: {output_path}")
    print(f"Total records with MX errors: {len(data)}")

def mx_errors_to_csv_string(data: List[Dict[str, str]]) -> str:
    """
    Convert MX error data to CSV string format.
    
    Args:
        data (list): List of dictionaries containing TRN and MX error data
        
    Returns:
        str: CSV formatted string
    """
    if not data:
        return ""
    
    # Normalize data and get fieldnames
    normalized_data = normalize_mx_error_records(data)
    max_errors = get_max_mx_errors(data)
    fieldnames = get_mx_error_fieldnames(max_errors)
    
    # Create CSV string
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(normalized_data)
    
    return output.getvalue()

def main():
    """Main function for standalone execution."""
    # Input file path
    input_file = "/Users/stephanielaface/Downloads/645990162.J505RYQ1.277CA"
    
    # Output file path (same directory as input, with .csv extension)
    output_file = Path(input_file).parent / "mx_errors_output.csv"
    
    try:
        # Parse the EDI file
        print(f"Processing file: {input_file}")
        mx_error_data = parse_edi_277_from_file(input_file)
        
        # Write to CSV
        write_mx_errors_to_csv(mx_error_data, str(output_file))
        
        # Print summary
        if mx_error_data:
            print("\nSample of extracted data:")
            for i, record in enumerate(mx_error_data[:3]):  # Show first 3 records
                print(f"\nRecord {i+1}:")
                print(f"  TRN: {record.get('TRN', 'N/A')}")
                print(f"  Source: {record.get('source_file', 'N/A')}")
                for key, value in record.items():
                    if key.startswith('MX_Error_'):
                        print(f"  {key}: {value}")
        
    except FileNotFoundError:
        print(f"Error: File not found - {input_file}")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()
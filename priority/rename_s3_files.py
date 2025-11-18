#!/usr/bin/env python3

import os
import re
import sys
from pathlib import Path
from typing import List, Tuple

# Directory path
DIR_PATH = "/Users/stephanielaface/Downloads/aws-b2b-edi/production/priority-output-edi/ca-bb4007205c4d48cb8/tp-f1eb2352582e481b8"

def parse_filename(filename: str) -> Tuple[str, str, str]:
    """
    Parse the filename to extract date and sequence number.
    Returns: (original_filename, date_part, sequence_number) or (original_filename, None, None) if no match
    """
    # Pattern to match: priority_encounters_input_YYYYMMDD_N.json.TIMESTAMP.x12
    # where N can be any number (1, 2, 128, etc.)
    pattern = r'priority_encounters_input_(\d{8})_(\d+)\.json\..*\.x12$'
    
    match = re.search(pattern, filename)
    if match:
        date_part = match.group(1)
        seq_num = match.group(2)
        return filename, date_part, seq_num
    
    return filename, None, None

def create_new_filename(date_part: str, seq_num: str) -> str:
    """Create the new filename format."""
    return f"5476_Priority_Saferide_{date_part}_{seq_num}.837"

def analyze_files(directory: str) -> dict:
    """Analyze all files in the directory and categorize them."""
    results = {
        'to_rename': [],
        'already_renamed': [],
        'no_match': [],
        'would_conflict': [],
        'not_found': []
    }
    
    # Check if directory exists
    if not os.path.exists(directory):
        print(f"❌ Error: Directory does not exist: {directory}")
        return results
    
    # Get all files in directory
    all_files = os.listdir(directory)
    
    # Filter for files matching our pattern
    priority_files = [f for f in all_files if f.startswith('priority_encounters_input_') and f.endswith('.x12')]
    already_renamed = [f for f in all_files if f.startswith('5476_Priority_Saferide_')]
    
    print(f"📁 Found {len(priority_files)} files to potentially rename")
    print(f"✅ Found {len(already_renamed)} files already in target format")
    print("-" * 60)
    
    # Analyze each file
    for filename in priority_files:
        original, date_part, seq_num = parse_filename(filename)
        
        if date_part and seq_num:
            new_name = create_new_filename(date_part, seq_num)
            full_old_path = os.path.join(directory, filename)
            full_new_path = os.path.join(directory, new_name)
            
            if os.path.exists(full_new_path):
                results['would_conflict'].append((filename, new_name))
            else:
                results['to_rename'].append((filename, new_name, date_part, seq_num))
        else:
            results['no_match'].append(filename)
    
    results['already_renamed'] = already_renamed
    
    return results

def display_results(results: dict, dry_run: bool = True):
    """Display the analysis results."""
    print("\n" + "="*60)
    print("ANALYSIS RESULTS" + (" (DRY RUN)" if dry_run else ""))
    print("="*60)
    
    # Files to rename
    if results['to_rename']:
        print(f"\n✅ Files to be renamed: {len(results['to_rename'])}")
        print("-" * 40)
        
        # Group by sequence number to see distribution
        seq_nums = {}
        for old, new, date, seq in results['to_rename']:
            seq_int = int(seq)
            if seq_int not in seq_nums:
                seq_nums[seq_int] = []
            seq_nums[seq_int].append((old, new))
        
        # Show range of sequence numbers
        if seq_nums:
            min_seq = min(seq_nums.keys())
            max_seq = max(seq_nums.keys())
            print(f"📊 Sequence number range: {min_seq} to {max_seq}")
            print(f"📈 Unique sequence numbers: {len(seq_nums)}")
            
            # Show sample of files
            print("\n📝 Sample of files to rename:")
            samples = results['to_rename'][:3] + results['to_rename'][-3:] if len(results['to_rename']) > 6 else results['to_rename']
            for old, new, date, seq in samples[:3]:
                print(f"  {old}")
                print(f"  → {new} (seq: {seq})")
                print()
            
            if len(results['to_rename']) > 6:
                print("  ...")
                for old, new, date, seq in samples[-3:]:
                    print(f"  {old}")
                    print(f"  → {new} (seq: {seq})")
                    print()
    
    # Files that would conflict
    if results['would_conflict']:
        print(f"\n⚠️ Files that would conflict (target already exists): {len(results['would_conflict'])}")
        for old, new in results['would_conflict'][:5]:
            print(f"  {old} → {new} (EXISTS)")
    
    # Files that don't match pattern
    if results['no_match']:
        print(f"\n❌ Files not matching expected pattern: {len(results['no_match'])}")
        for filename in results['no_match'][:5]:
            print(f"  {filename}")
    
    # Already renamed files
    if results['already_renamed']:
        print(f"\n✅ Files already in target format: {len(results['already_renamed'])}")
        # Show sequence number range for already renamed files
        renamed_seqs = []
        for filename in results['already_renamed']:
            match = re.search(r'5476_Priority_Saferide_\d{8}_(\d+)\.837', filename)
            if match:
                renamed_seqs.append(int(match.group(1)))
        if renamed_seqs:
            print(f"   Sequence range in renamed files: {min(renamed_seqs)} to {max(renamed_seqs)}")

def rename_files(results: dict, directory: str):
    """Actually rename the files."""
    renamed_count = 0
    failed_count = 0
    
    print("\n" + "="*60)
    print("RENAMING FILES")
    print("="*60)
    
    for old_name, new_name, date_part, seq_num in results['to_rename']:
        old_path = os.path.join(directory, old_name)
        new_path = os.path.join(directory, new_name)
        
        try:
            os.rename(old_path, new_path)
            print(f"✅ Renamed: {old_name} → {new_name}")
            renamed_count += 1
        except Exception as e:
            print(f"❌ Failed to rename {old_name}: {e}")
            failed_count += 1
    
    print("\n" + "-"*60)
    print(f"Summary: {renamed_count} files renamed, {failed_count} failed")
    
    return renamed_count, failed_count

def main():
    """Main function."""
    # Check for command line arguments
    dry_run = True
    if len(sys.argv) > 1:
        if sys.argv[1] in ['--run', '-r']:
            dry_run = False
        elif sys.argv[1] in ['--help', '-h']:
            print("Usage: python rename_files.py [--run|-r]")
            print("  Default: Dry run (no changes)")
            print("  --run, -r: Actually rename files")
            print("  --help, -h: Show this help")
            sys.exit(0)
    
    print("🔍 Priority EDI File Renamer")
    print(f"📂 Directory: {DIR_PATH}")
    print(f"🔧 Mode: {'DRY RUN (no changes)' if dry_run else 'LIVE (will rename files)'}")
    print("="*60)
    
    # Analyze files
    results = analyze_files(DIR_PATH)
    
    # Display results
    display_results(results, dry_run)
    
    # If not dry run and there are files to rename, proceed
    if not dry_run and results['to_rename']:
        print("\n" + "="*60)
        response = input("Proceed with renaming? (yes/no): ").strip().lower()
        if response == 'yes':
            rename_files(results, DIR_PATH)
        else:
            print("Cancelled.")
    elif dry_run and results['to_rename']:
        print("\n" + "="*60)
        print("💡 To actually rename files, run: python rename_files.py --run")
    
    # Final check to see if any priority files remain
    print("\n" + "="*60)
    print("FINAL CHECK")
    remaining = len([f for f in os.listdir(DIR_PATH) 
                     if f.startswith('priority_encounters_input_') and f.endswith('.x12')])
    if remaining > 0:
        print(f"⚠️ {remaining} priority_encounters_input files still remain")
    else:
        print("✅ All priority_encounters_input files have been processed")

if __name__ == "__main__":
    main()
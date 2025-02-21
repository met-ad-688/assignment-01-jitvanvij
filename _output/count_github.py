import os
import pandas as pd

# Absolute paths to the CSV files
files = ["/home/ubuntu/assignment-01-jitvanvij/question_tags.csv", "/home/ubuntu/assignment-01-jitvanvij/questions.csv"]

# Print absolute paths for debugging
for file in files:
    print(f"Looking for file: {file}")

count = 0
chunk_size = 100000  # Size of each chunk to read from the CSV file

for file in files:
    try:
        # Check if the file exists before attempting to read it
        if not os.path.exists(file):
            print(f"File does not exist: {file}")
            continue
        
        # Process the CSV file in chunks
        for chunk in pd.read_csv(file, dtype=str, on_bad_lines="skip", chunksize=chunk_size):
            print(f"Processing a chunk from {file}")
            
            # Count occurrences of "GitHub" (case-insensitive) in any column
            count += chunk.apply(lambda row: row.astype(str).str.contains("GitHub", case=False, na=False).any(), axis=1).sum()

    except FileNotFoundError:
        print(f"Warning: {file} not found.")
    except Exception as e:
        print(f"Error processing {file}: {e}")

# Print the total count
print(f"Total lines containing 'GitHub': {count}")



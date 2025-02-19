import pandas as pd

# Load the CSV files
files = ["../question_tags.csv", "../questions.csv"]  # Replace with actual file names
count = 0

for file in files:
    try:
        # Read CSV file
        df = pd.read_csv(file, dtype=str, on_bad_lines="skip")

        # Count occurrences of "GitHub" (case-insensitive) in any column
        count += df.apply(lambda row: row.astype(str).str.contains("GitHub", case=False, na=False).any(), axis=1).sum()

    except FileNotFoundError:
        print(f"Warning: {file} not found.")
    except Exception as e:
        print(f"Error processing {file}: {e}")

# Print the total count
print(f"Total lines containing 'GitHub': {count}")

# Save the result to a text file
with open("_output/github_count.txt", "w") as f:
    f.write(f"Total lines containing 'GitHub': {count}\n")


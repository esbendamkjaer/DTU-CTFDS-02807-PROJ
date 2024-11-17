import csv
import sys
from tqdm import tqdm

# Increase field size limit
csv.field_size_limit(sys.maxsize)

in_file: str = "data/games_may2024_cleaned.csv"
out_file: str = "data/steam_games_flattened.csv"

with open(in_file, "r", encoding="utf-8") as infile, open(out_file, "w", newline='', encoding="utf-8") as outfile:
    # Optional: get the total number of lines for tqdm
    total_lines = sum(1 for line in open(in_file, "r", encoding="utf-8"))
    
    reader = csv.reader(infile)
    writer = csv.writer(outfile)

    for row in tqdm(reader, total=total_lines, desc="Processing rows"):
        # Replace actual newlines in each field with literal \n
        processed_row = [field.replace('\n', '\\n') for field in row]
        writer.writerow(processed_row)

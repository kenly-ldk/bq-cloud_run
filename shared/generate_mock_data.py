import csv
import random
import time
from datetime import datetime, timedelta

def generate_mock_csv(filename, total_rows):
    start_time = time.time()
    print(f"Generating {total_rows} rows to {filename}...")
    
    # Pre-generate some domains and names to speed up random selections
    domains = ["example.com", "test.org", "demo.net", "corporate.co", "sparkcorners.com"]
    first_names = ["John", "Jane", "Alex", "Emily", "Michael", "Sarah", "David", "Lisa", "Chris", "Emma"]
    last_names = ["Smith", "Doe", "Johnson", "Williams", "Brown", "Jones", "Miller", "Davis", "Garcia", "Rodriguez"]
    
    with open(filename, 'w', newline='') as csvfile:
        fieldnames = ['id', 'name', 'email', 'ssn', 'dob']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        # Batch writing for speed
        batch_size = 100000
        batch = []
        
        base_date = datetime(1950, 1, 1)
        
        for i in range(1, total_rows + 1):
             first = random.choice(first_names)
             last = random.choice(last_names)
             name = f"{first} {last}"
             email = f"{first.lower()}.{last.lower()}.{i}@{random.choice(domains)}"
             ssn = f"{random.randint(100, 999)}-{random.randint(10, 99)}-{random.randint(1000, 9999)}"
             
             # Random DOB between 1950 and 2005
             days_to_add = random.randint(0, 55 * 365)
             dob = (base_date + timedelta(days=days_to_add)).strftime('%Y-%m-%d')
             
             batch.append({
                  'id': i,
                  'name': name,
                  'email': email,
                  'ssn': ssn,
                  'dob': dob
             })
             
             if i % batch_size == 0:
                  writer.writerows(batch)
                  batch = []
                  elapsed = time.time() - start_time
                  print(f"Generated {i}/{total_rows} rows... ({elapsed:.2f}s elapsed)")
        
        if batch:
             writer.writerows(batch)
             
    total_time = time.time() - start_time
    print(f"Finished! Total time: {total_time:.2f} seconds.")

if __name__ == "__main__":
    import sys
    total_rows = 1000000 # Default 1M for new repo
    if len(sys.argv) > 1:
         total_rows = int(sys.argv[1])
    generate_mock_csv("pii_data.csv", total_rows)

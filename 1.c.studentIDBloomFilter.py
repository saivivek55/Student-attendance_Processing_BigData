from redis import Redis
redis_client = Redis(host='localhost', port=6379, db=0)

# prefixes/departments for the student IDs
prefixes = ['CS', 'EE', 'ME', 'CE', 'BI']
# bloom filter
bloom_filter_name = 'studentID_bloomfilter'
error_rate = 0.01
capacity = 4500  

try:
    redis_client.execute_command('BF.RESERVE', bloom_filter_name, error_rate, capacity)
except Exception as e:
    print(f"Error creating Bloom filter (it might already exist): {e}")

# Generate student IDs based on given criteria and add them to Bloom filter
for prefix in prefixes:
    for num in range(1, 600):  
        student_id = f"{prefix}{num:03d}"  
        redis_client.execute_command('BF.ADD', bloom_filter_name, student_id)
print("All generated student IDs have been added to the Bloom filter.")
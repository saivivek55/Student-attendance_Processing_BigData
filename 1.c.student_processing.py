from pulsar import Client
import redis
from datetime import datetime
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
#redis setup
redis_host = 'localhost'
redis_port = 6379
r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)

bloom_filter_key = 'studentID_bloomfilter'
valid_departments = ['CS', 'EE', 'ME', 'CE', 'BI']

def is_valid_student_id(student_id):
    department = student_id[:2]
    if department not in valid_departments:
        return False
    try:
        number = int(student_id[2:])
        return 100 <= number <= 600
    except ValueError:
        return False

# Cassandra setup
auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
cluster = Cluster(['localhost'], auth_provider=auth_provider)
session = cluster.connect('student_attendance')

# statement for inserting data into Cassandra
insert_stmt = session.prepare("""
    INSERT INTO records (student_id, student_name, lecture_date, lecture_time, class)
    VALUES (?, ?, ?, ?, ?)
""")

# Pulsar setup
pulsar_client = Client('pulsar://localhost:6650')
subscription_name = 'attendance_subscription'
topic_name = 'persistent://public/default/attendance'

# Subscribe to the topic
consumer = pulsar_client.subscribe(topic=topic_name, subscription_name=subscription_name)

def process_message(msg):
    try:
        data = msg.data().decode('utf-8')
        print(f"Received: {data}")
        parts = data.split(', ')
        student_id = parts[0].split(': ')[1].strip()
        student_name = parts[1].split(': ')[1].strip()
        class_num = int(parts[2].split(': ')[1].strip())
        time_str = parts[3].split(': ')[1].strip()
        lecture_datetime = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
        lecture_date = lecture_datetime.date()
        lecture_time = lecture_datetime.strftime('%H:%M:%S')

        if is_valid_student_id(student_id):
            session.execute(insert_stmt, [student_id, student_name, lecture_date, lecture_time, class_num])
            print(f"Record saved for student ID: {student_id} to Cassandra.")

            # Redis Bloom filter and HyperLogLog logic
            if not r.execute_command('BF.EXISTS', bloom_filter_key, student_id):
                r.execute_command('BF.ADD', bloom_filter_key, student_id)
            r.execute_command('PFADD', f"attendance:{lecture_date}", student_id)
            unique_count = r.execute_command('PFCOUNT', f"attendance:{lecture_date}")
            print(f"Valid student ID: {student_id}. Current unique attendance count for {lecture_date}: {unique_count}")

        else:
            print(f"Invalid student ID: {student_id}, not saved to Cassandra.")

        consumer.acknowledge(msg)
    except Exception as e:
        print(f"Failed to process message: {e}")
        consumer.negative_acknowledge(msg)

try:
    while True:
        msg = consumer.receive(timeout_millis=5000)
        process_message(msg)
except Exception as e:
    print(f"Encountered an exception: {e}")
finally:
    consumer.close()
    pulsar_client.close()

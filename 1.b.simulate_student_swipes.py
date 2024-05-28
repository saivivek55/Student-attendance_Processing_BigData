import time
import random
from pulsar import Client
from faker import Faker
from datetime import datetime, timedelta

client = Client("pulsar://localhost:6650")
producer = client.create_producer("persistent://public/default/attendance")
faker = Faker()
departments = ['CS', 'EE', 'ME', 'CE', 'BI']

def generate_timestamp():
    #Generating a random timestamp for same day between 8 AM and 5 PM.
    today_date = datetime.now().date()
    start_time = datetime.combine(today_date, datetime.min.time()) + timedelta(hours=8)  # 8 AM
    end_time = start_time + timedelta(hours=9)  # 5 PM
    random_time = faker.date_time_between_dates(datetime_start=start_time, datetime_end=end_time)
    return random_time
try:
    while True:
        department = random.choice(departments)
        student_number = random.randint(100, 999)
        student_id = f"{department}{student_number}"
        student_name = faker.name()
        class_number = random.randint(1, 3)
        
        swipe_time = generate_timestamp()
        message = f"ID: {student_id}, Name: {student_name}, Class: {class_number}, Time: {swipe_time}".encode()
        producer.send(message)
        print(f"Sent: {message.decode()}")
        time.sleep(random.uniform(0.5, 1.5))
finally:
    client.close()

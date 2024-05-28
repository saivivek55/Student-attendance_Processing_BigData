"""
import redis

# Configuration for Redis connection
redis_host = 'localhost'
redis_port = 6379
r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)

def estimate_unique_attendees(lecture_date):
    hll_key = f"attendance:{lecture_date}"
    # Estimate the number of unique attendees
    unique_attendees_count = r.pfcount(hll_key)
    return unique_attendees_count

lecture_date = "2024-03-28"
unique_attendees = estimate_unique_attendees(lecture_date)
print(f"Estimated unique attendees for {lecture_date}: {unique_attendees}")
# Calculating based on entire day
"""

import redis
from datetime import datetime
# Configuration for Redis connection
redis_host = 'localhost'
redis_port = 6379
# Connect to Redis
r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
def estimate_unique_attendees_within_time_window(lecture_date, start_time, end_time):
    sorted_set_key = f"attendance_times:{lecture_date}"
    # Convert start and end times to timestamps
    start_timestamp = datetime.strptime(f"{lecture_date} {start_time}", '%Y-%m-%d %H:%M').timestamp()
    end_timestamp = datetime.strptime(f"{lecture_date} {end_time}", '%Y-%m-%d %H:%M').timestamp()
    attendees_in_window = r.zrangebyscore(sorted_set_key, min=start_timestamp, max=end_timestamp)
    return len(set(attendees_in_window))  # Simplistic approach for demonstration
# Specify the lecture date and time window you're interested in
lecture_date = "2024-03-28"
start_time = "11:30"
end_time = "19:00"
unique_attendees = estimate_unique_attendees_within_time_window(lecture_date, start_time, end_time)
print(f"Estimated unique attendees for {lecture_date} between {start_time} and {end_time}: {unique_attendees}")

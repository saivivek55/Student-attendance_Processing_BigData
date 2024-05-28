from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from collections import Counter
from collections import defaultdict
from cassandra.query import dict_factory
from datetime import datetime, timedelta

# Cassandra connection
auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
cluster = Cluster(['localhost'], auth_provider=auth_provider)
session = cluster.connect('student_attendance')


##4. most attended classes
def most_attended_classes():
    query = "SELECT class FROM records"
    try:
        rows = session.execute(query)
        class_counts = Counter(row[0] for row in rows)  
        return class_counts.most_common(5)
    except Exception as e:
        print(f"An error occurred: {e}")
        return []
print(most_attended_classes())


##5. Habitual Latecomers
auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
cluster = Cluster(['localhost'], auth_provider=auth_provider)
session = cluster.connect('student_attendance')
def find_habitual_latecomers_and_total():
    time_ranges = [
        ('10:00:00', '10:30:00'),
        ('13:00:00', '13:30:00'),
        ('16:00:00', '16:30:00')
    ]
    latecomers = Counter()
    for start_time, end_time in time_ranges:
        query = f"""
            SELECT student_id
            FROM records
            WHERE lecture_time > '{start_time}' AND lecture_time < '{end_time}'
            ALLOW FILTERING
        """
        try:
            rows = session.execute(query)
            latecomers.update(row.student_id for row in rows)
        except Exception as e:
            print(f"An error occurred while querying the time range {start_time} to {end_time}: {e}")
            continue
    # Total count of unique latecomers
    total_unique_latecomers = len(latecomers)
    # Get the top 5 latecomers
    top_latecomers = latecomers.most_common(5)
    return top_latecomers, total_unique_latecomers
top_latecomers, total_unique_latecomers = find_habitual_latecomers_and_total()
print(f"Top 5 habitual latecomers: {top_latecomers}")
print(f"Total unique latecomers: {total_unique_latecomers}")


##6. Number of students in each department:
def student_count_by_department():
    query = "SELECT student_id FROM records"
    rows = session.execute(query)
    department_counts = Counter(row.student_id[:2] for row in rows)
    return department_counts
department_counts = student_count_by_department()
for department, count in department_counts.items():
    print(f"Department {department}: {count} students")


###7. Most popular time students coming in(15-min interval):
def round_time_to_nearest_quarter_hour(time_str):
    time_obj = datetime.strptime(time_str, '%H:%M:%S')
    rounding = (time_obj.minute // 15) * 15
    rounded_time = time_obj.replace(minute=rounding, second=0)
    if time_obj.minute - rounding >= 8:
        rounded_time += timedelta(minutes=15)
        rounded_time = rounded_time.replace(second=0) 
    return rounded_time.strftime('%H:%M:%S')
def popular_lecture_times():
    query = "SELECT lecture_time FROM records"
    rows = session.execute(query)
    # Round times to the nearest 15-minute mark and count frequency
    time_counter = Counter(round_time_to_nearest_quarter_hour(row.lecture_time) for row in rows)
    most_popular_times = time_counter.most_common(5)
    return most_popular_times
popular_times = popular_lecture_times()
print("Most Popular Time students coming to Lecture in 15-minute Timeframes:")
for time, count in popular_times:
    print(f"Time: {time}, Count: {count}")




###8. Number of students in each department for each class:
session.row_factory = dict_factory
def student_count_by_department_and_class():
    query = "SELECT student_id, class FROM records"
    rows = session.execute(query)
    department_class_counts = defaultdict(lambda: defaultdict(int))
    
    for row in rows:
        department = row['student_id'][:2] 
        class_ = row['class']
        department_class_counts[department][class_] += 1
    return department_class_counts
# Example 
department_class_counts = student_count_by_department_and_class()
for department, classes in department_class_counts.items():
    print(f"Department {department}:")
    for class_, count in classes.items():
        print(f"  Class {class_}: {count} students")


##9. Distribution of Class Sizes within Each Department
session.row_factory = dict_factory
def average_class_size_by_department():
    query = "SELECT student_id, class FROM records"
    rows = session.execute(query)
    department_class_sizes = defaultdict(lambda: defaultdict(list))
    
    for row in rows:
        department = row['student_id'][:2]
        class_ = row['class']  # Corrected access to 'class'
        department_class_sizes[department][class_].append(row['student_id'])
    average_sizes = {
        dept: {
            class_: len(students) / len(department_class_sizes[dept])
            for class_, students in classes.items()
        }
        for dept, classes in department_class_sizes.items()
    }
    return average_sizes
average_sizes = average_class_size_by_department()
for dept, classes in average_sizes.items():
    print(f"Department {dept}:")
    for class_, size in classes.items():
        print(f"  Class {class_}: Average Size: {size}")



###10. Most Frequent Attendee per each Department
def frequent_attendees_by_department():
    query = "SELECT student_id FROM records"
    rows = session.execute(query)
    attendance = defaultdict(Counter)
    for row in rows:
        department = row.student_id[:2]
        attendance[department][row.student_id] += 1
    most_frequent = {dept: attendees.most_common(1)[0] if attendees else ('None', 0) 
                     for dept, attendees in attendance.items()}
    
    return most_frequent
frequent_attendees = frequent_attendees_by_department()
for dept, (student_id, count) in frequent_attendees.items():
    print(f"Department {dept}: Most Frequent Attendee: {student_id} with {count} attendances")
# Paola Calle -- MongoDB Atlas setup & demo (students + classes)
# Requires: pymongo
# Usage: replace MONGO_URI below with your connection string
# Then: python mongo_school_with_classes.py


from pymongo import MongoClient, errors
import sys

MONGO_URI = ""

try:
    client =MongoClient(MONGO_URI)
except errors.ConfigurationError:
    print("An Invalid URI host error was received. Is your Atlas host name correct in your connection string?")
    sys.exit(1)
    
    
db = client["school"]      # database
students = db["students"]  # collection
classes = db["classes"]    # new collection for classes

students.delete_many({}) # delete all existing documents
classes.delete_many({})  # delete all existing documents in classes collection


students.insert_many([
    { "_id": 1, "name": "Alice", "courses": [2433, 404] },
    { "_id": 2, "name": "Bob",   "courses": [2433] },
    { "_id": 3, "name": "Cara",  "courses": [404] }
])

# Classes collection with metadata you can join on
classes.insert_many([
    {
        "_id": 2433,
        "title": "Database",
        "credits": 3,
        "instructor": "Dr.Jean-Claude",
        "term": "Fall 2025",
        "meeting": {"days": ["Thursday"], "time": "5:55–7:30"}
    },
    {
        "_id": 404,
        "title": "Data Science",
        "credits": 3,
        "instructor": "Dr. Herodotus",
        "term": "Fall 2025",
        "meeting": {"days": ["Tue", "Thu"], "time": "13:00–14:15"}
    }
])

# _id is already indexed. these help common queries:
students.create_index("name")
students.create_index("courses")   # array index is useful for $in / equality
classes.create_index("title")


# Find all students enrolled in course Data Science
print("\nStudents enrolled in Data Science:")
course = classes.find_one({"title": "Data Science"}, {"_id": 1})
if not course:
    print("No course titled 'Data Science' found.")
else:
    for doc in students.find({"courses": course["_id"]}, {"_id": 0, "name": 1}):
        print("-", doc["name"])

# Find students who share >=1 course with Alice
print("\nStudents who share classes with Alice:")
alice = students.find_one({"name": "Alice"})
if alice:
    pipeline = [
        {"$match": {"_id": {"$ne": alice["_id"]}}},
        {"$addFields": {"overlap_ids": {"$setIntersection": ["$courses", alice["courses"]]}}},
        {"$match": {"$expr": {"$gt": [{"$size": "$overlap_ids"}, 0]}}},
        {
            "$lookup": {
                "from": "classes",
                "let": {"overlap_ids": "$overlap_ids"},
                "pipeline": [
                    {"$match": {"$expr": {"$in": ["$_id", "$$overlap_ids"]}}},
                    {"$project": {"_id": 0, "title": 1}}
                ],
                "as": "overlap"
            }
        },
        {"$project": {"_id": 0, "name": 1, "overlap_titles": "$overlap.title"}}
    ]
    
    for student in students.aggregate(pipeline):
        print(f"{student['name']} is also enrolled in {', '.join(student['overlap_titles'])}")
else:
    print("Alice not found in the database.")
print()


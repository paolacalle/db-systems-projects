# Paola Calle - BigQuery setup & demo (students + classes)
# Requires: google-cloud-bigquery
# Usage: place 'bigquery-key.json' (your service account key) in the same folder.
# Then: python bigquery_school_with_classes.py

import os
from google.cloud import bigquery

# ---------- CONFIG ----------
PROJECT = "school-with-classes"         # <-- change this
DATASET = "school"
LOCAL_KEY = "bigquery-demo.json"      # <-- key file in the same folder as this script
# -----------------------------

if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ:
    if os.path.exists(LOCAL_KEY):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.abspath(LOCAL_KEY)
    else:
        raise SystemExit(
            f"Could not find {LOCAL_KEY}. Either put your service-account JSON key "
            "next to this script or set GOOGLE_APPLICATION_CREDENTIALS."
        )

client = bigquery.Client(project=PROJECT)

def run(sql: str):
    """Run a single SQL statement and return rows as a list of dicts."""
    job = client.query(sql)
    return [dict(row) for row in job.result()]

# Step 1: Create tables and insert data
client.create_dataset(bigquery.Dataset(f"{PROJECT}.{DATASET}"), exists_ok=True)

# clear existing tables (if any)
run(f"DROP TABLE IF EXISTS `{PROJECT}.{DATASET}.students`")
run(f"DROP TABLE IF EXISTS `{PROJECT}.{DATASET}.classes`")
run(f"DROP TABLE IF EXISTS `{PROJECT}.{DATASET}.enrollments`")

# Create students table
run(f"""
CREATE TABLE IF NOT EXISTS `{PROJECT}.{DATASET}.students` (
  student_id INT64,
  name STRING
)
""")

# Create classes table
run(f"""
CREATE TABLE IF NOT EXISTS `{PROJECT}.{DATASET}.classes` (
  class_id INT64,
  title STRING,
  credits INT64,
  instructor STRING
)
""")

# Create enrollment table (many-to-many relationship)
run(f"""
CREATE TABLE IF NOT EXISTS `{PROJECT}.{DATASET}.enrollments` (
  student_id INT64,
  class_id INT64
)
""")

# Insert sample data
run(f"""
INSERT INTO `{PROJECT}.{DATASET}.students` (student_id, name) VALUES
  (1, 'Alice'),
  (2, 'Bob'),
  (3, 'Cara')
""")

run(f"""
INSERT INTO `{PROJECT}.{DATASET}.classes` (class_id, title, credits, instructor) VALUES
  (2433, 'Database Systems', 3, 'Dr. Jean-Claude'),
  (404, 'Data Science', 3, 'Prof. Herodotus')
""")

run(f"""
INSERT INTO `{PROJECT}.{DATASET}.enrollments` (student_id, class_id) VALUES
  (1, 2433),
  (1, 404),
  (2, 2433),
  (3, 404)
""")


# Find all students enrolled in Data Science Class
print("\nStudents enrolled in Data Science:")
rows = run(f"""
SELECT s.name
FROM `{PROJECT}.{DATASET}.students` s
JOIN `{PROJECT}.{DATASET}.enrollments` e ON s.student_id = e.student_id
JOIN `{PROJECT}.{DATASET}.classes` c ON e.class_id = c.class_id
WHERE c.title = 'Data Science'
""")

for row in rows:
    print(" - ", row["name"])
    
print()

# Find students who share >=1 course with Alice
print("\nStudents who share classes with Alice:")
rows = run(f"""
WITH AliceClasses AS (
  SELECT e.class_id
  FROM `{PROJECT}.{DATASET}.students` s
  JOIN `{PROJECT}.{DATASET}.enrollments` e ON s.student_id = e.student_id
  WHERE s.name = 'Alice'
)
SELECT DISTINCT s.name AS classmate_name, c.title AS class_title
FROM `{PROJECT}.{DATASET}.students` s
JOIN `{PROJECT}.{DATASET}.enrollments` e ON s.student_id = e.student_id
JOIN `{PROJECT}.{DATASET}.classes` c ON e.class_id = c.class_id
JOIN AliceClasses ac ON e.class_id = ac.class_id
WHERE s.name != 'Alice'
""")
for row in rows:
    print(f"{row['classmate_name']} is also enrolled in {row['class_title']}")
print()

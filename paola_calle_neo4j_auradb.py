# Paola Calle -- Neo4j AuraDB setup & demo (students + classes)
# Requires: neo4j
# Usage: replace <your_neo4j_uri>, <your_neo4j_username
#        and <your_neo4j_password> below with your AuraDB credentials
# Then: python neo4j_auradb_school_with_classes.py
# references:
# - https://neo4j.com/developer/neo4j-auradb/

from neo4j import GraphDatabase



URI = "<your_neo4j_uri>"
AUTH = ("<your_neo4j_username>", "<your_neo4j_password>")

with GraphDatabase.driver(URI, auth=AUTH) as driver:
    driver.verify_connectivity()
    
    
    # Step 0: Clear existing data (if any)
    driver.session().run("MATCH (n) DETACH DELETE n")


    # Step 1: Create the nodes (students + classes)

    # Create student nodes
    students = [
        {"id": 1, "name": "Alice", "age": 20},
        {"id": 2, "name": "Bob", "age": 22},
        {"id": 3, "name": "Cara", "age": 100}
    ]
    for student in students:
        driver.session().run(
            """
            CREATE (s:Student {id: $id, name: $name, age: $age})
            """,
            student,
        )

    # Create class nodes
    classes = [
        {"id": 2433, "title": "Database Systems", "instructor": "Dr. Jean-Claude"},
        {"id": 404, "title": "Data Science", "instructor": "Prof. Herodotus"},
    ]
    for cls in classes:
        driver.session().run(
            """
            CREATE (c:Class {id: $id, title: $title, instructor: $instructor})
            """,
            cls,
        )
        
    # Step 2: Create relationships (enrollments)
    enrollments = [
        (1, 2433),
        (1, 404),
        (2, 2433),
        (3, 404)
    ]
    for student_id, class_id in enrollments:
        driver.session().run(
            """
            MATCH (s:Student {id: $student_id}), (c:Class {id: $class_id})
            CREATE (s)-[:ENROLLED_IN]->(c)
            """,
            {"student_id": student_id, "class_id": class_id},
        )
        
    # Step 3: Sample queries
    # Find all students enrolled in "Data Science"
    result = driver.session().run(
        """
        MATCH (s:Student)-[:ENROLLED_IN]->(c:Class {title: $class_title})
        RETURN s.name AS student_name
        """,
        {"class_title": 'Data Science'},
    )
    print("Students enrolled in Data Science:")
    for record in result:
        print(" - ", record["student_name"])

    print()

    # Find students who share >=1 course with Alice
    result = driver.session().run(
        """
        MATCH (alice:Student {name: 'Alice'})-[:ENROLLED_IN]->(c:Class)<-[:ENROLLED_IN]-(classmate:Student)
        RETURN classmate.name AS classmate_name, c.title AS class_title
        """,
    )
    print("\nStudents who share classes with Alice:")
    for record in result:
        print(f"{record['classmate_name']} is also enrolled in {record['class_title']}")
    print()
    
    driver.close()
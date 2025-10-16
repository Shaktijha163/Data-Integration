import sqlite3
import pandas as pd

DB_PATH = "db1_student.db"

def create_tables():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS Attendance")
    cursor.execute("DROP TABLE IF EXISTS Enrollment")
    cursor.execute("DROP TABLE IF EXISTS Students")
    cursor.execute("""
        CREATE TABLE Students (
            student_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT,
            program TEXT,
            year INTEGER
        )
    """)
    cursor.execute("""
        CREATE TABLE Enrollment (
            enrollment_id INTEGER PRIMARY KEY AUTOINCREMENT,
            student_id TEXT NOT NULL,
            course_id TEXT NOT NULL,
            semester TEXT,
            enrollment_date DATE,
            FOREIGN KEY (student_id) REFERENCES Students(student_id)
        )
    """)
    cursor.execute("""
        CREATE TABLE Attendance (
            attendance_id INTEGER PRIMARY KEY AUTOINCREMENT,
            student_id TEXT NOT NULL,
            course_id TEXT NOT NULL,
            date DATE,
            status TEXT CHECK(status IN ('Present', 'Absent', 'Late', 'present', 'absent', 'late')),
            FOREIGN KEY (student_id) REFERENCES Students(student_id)
        )
    """)
    
    conn.commit()
    conn.close()
    print(" Tables created successfully")


def import_data():
    conn = sqlite3.connect(DB_PATH)
    
    try:
        # Import Students
        print(" Importing students.csv...")
        students_df = pd.read_csv("data/students.csv")
        
        # Clean and validate data
        students_df['student_id'] = students_df['student_id'].astype(str).str.upper()
        students_df['email'] = students_df['email'].str.lower()
        
        # Use append instead of replace to keep schema
        students_df.to_sql('Students', conn, if_exists='append', index=False)
        print(f" Imported {len(students_df)} students")
        
        # Import Enrollment
        print(" Importing Enrollment.csv...")
        enrollment_df = pd.read_csv("data/Enrollment.csv")
        
        # Clean data
        enrollment_df['student_id'] = enrollment_df['student_id'].astype(str).str.upper()
        enrollment_df['course_id'] = enrollment_df['course_id'].astype(str).str.upper()
        
        enrollment_df.to_sql('Enrollment', conn, if_exists='append', index=False)
        print(f" Imported {len(enrollment_df)} enrollments")
        
        # Import Attendance
        print(" Importing Attendance.csv...")
        attendance_df = pd.read_csv("data/Attendance.csv")
        
        # Clean data - normalize status to title case
        attendance_df['student_id'] = attendance_df['student_id'].astype(str).str.upper()
        attendance_df['course_id'] = attendance_df['course_id'].astype(str).str.upper()
        attendance_df['status'] = attendance_df['status'].str.capitalize()  # present -> Present
        
        attendance_df.to_sql('Attendance', conn, if_exists='append', index=False)
        print(f" Imported {len(attendance_df)} attendance records")
        
    except FileNotFoundError as e:
        print(f" Error: {e}")
        print("Make sure CSV files are in the 'data' directory")
    except Exception as e:
        print(f" Error importing data: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()


def verify_data():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    print("\nDatabase Statistics:")
    cursor.execute("SELECT COUNT(*) FROM Students")
    print(f"   Students: {cursor.fetchone()[0]}")
    
    cursor.execute("SELECT COUNT(*) FROM Enrollment")
    print(f"   Enrollments: {cursor.fetchone()[0]}")
    
    cursor.execute("SELECT COUNT(*) FROM Attendance")
    print(f"   Attendance records: {cursor.fetchone()[0]}")
    
    # Show sample data
    print("\n Sample Student Data:")
    cursor.execute("SELECT * FROM Students LIMIT 3")
    for row in cursor.fetchall():
        print(f"   {row}")
    
    print("\n Sample Enrollment Data:")
    cursor.execute("SELECT * FROM Enrollment LIMIT 3")
    for row in cursor.fetchall():
        print(f"   {row}")
    
    print("\n Sample Attendance Data:")
    cursor.execute("SELECT * FROM Attendance LIMIT 3")
    for row in cursor.fetchall():
        print(f"   {row}")
    
    conn.close()


if __name__ == "__main__":
    print("="*60)
    print(" Setting up PC1 Student Database...")
    print("="*60)
    create_tables()
    import_data()
    verify_data()
    print("\n PC1 Database setup complete!")
    print("="*60)
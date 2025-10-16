import sqlite3

def create_cache_db():
    conn = sqlite3.connect("cache.db")
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS query_cache (
            query_hash TEXT PRIMARY KEY,
            query_text TEXT,
            query_type TEXT,
            result TEXT,
            created_at TIMESTAMP
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS joined_cache (
            cache_id INTEGER PRIMARY KEY AUTOINCREMENT,
            student_id INTEGER,
            course_id INTEGER,
            student_name TEXT,
            course_name TEXT,
            faculty_name TEXT,
            created_at TIMESTAMP
        )
    """)
    
    conn.commit()
    conn.close()
    print(" Cache database created successfully!")

if __name__ == "__main__":
    create_cache_db()
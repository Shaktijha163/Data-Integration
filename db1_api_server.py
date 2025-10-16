from flask import Flask, jsonify, request
import sqlite3

app = Flask(__name__)
DB_PATH = "db1_student.db"

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "healthy", "database": "db1_student"})

@app.route('/api/students', methods=['GET'])
def get_students():
    """Get all students or filter by ID"""
    student_id = request.args.get('student_id')
    
    conn = get_db()
    cursor = conn.cursor()
    
    if student_id:
        cursor.execute("SELECT * FROM Students WHERE student_id = ?", (student_id,))
    else:
        cursor.execute("SELECT * FROM Students")
    
    students = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return jsonify({"success": True, "data": students})

@app.route('/api/enrollment', methods=['GET'])
def get_enrollment():
    """Get enrollment data with optional filters"""
    course_id = request.args.get('course_id')
    student_id = request.args.get('student_id')
    
    conn = get_db()
    cursor = conn.cursor()
    
    query = "SELECT * FROM Enrollment WHERE 1=1"
    params = []
    
    if course_id:
        query += " AND course_id = ?"
        params.append(course_id)
    if student_id:
        query += " AND student_id = ?"
        params.append(student_id)
    
    cursor.execute(query, params)
    enrollments = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return jsonify({"success": True, "data": enrollments})

@app.route('/api/attendance', methods=['GET'])
def get_attendance():
    """Get attendance records with optional filters"""
    student_id = request.args.get('student_id')
    course_id = request.args.get('course_id')
    status = request.args.get('status')
    
    conn = get_db()
    cursor = conn.cursor()
    
    query = "SELECT * FROM Attendance WHERE 1=1"
    params = []
    
    if student_id:
        query += " AND student_id = ?"
        params.append(student_id)
    if course_id:
        query += " AND course_id = ?"
        params.append(course_id)
    if status:
        query += " AND status = ?"
        params.append(status)
    
    cursor.execute(query, params)
    attendance = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return jsonify({"success": True, "data": attendance})

@app.route('/api/attendance/summary', methods=['GET'])
def get_attendance_summary():
    """Get attendance summary per student per course"""
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            student_id,
            course_id,
            COUNT(*) as total_classes,
            SUM(CASE WHEN status = 'Present' THEN 1 ELSE 0 END) as present_count,
            ROUND(100.0 * SUM(CASE WHEN status = 'Present' THEN 1 ELSE 0 END) / COUNT(*), 2) as attendance_percentage
        FROM Attendance
        GROUP BY student_id, course_id
    """)
    
    summary = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return jsonify({"success": True, "data": summary})

@app.route('/api/query', methods=['POST'])
def execute_custom_query():
    """Execute custom SQL query (SELECT only)"""
    data = request.json
    sql = data.get('sql', '').strip()
    
    # Security: Only allow SELECT
    if not sql.upper().startswith('SELECT'):
        return jsonify({"success": False, "error": "Only SELECT queries allowed"}), 403
    
    if any(word in sql.upper() for word in ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER']):
        return jsonify({"success": False, "error": "Destructive queries not allowed"}), 403
    
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute(sql)
        
        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        conn.close()
        return jsonify({"success": True, "data": results, "columns": columns})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 400

if __name__ == '__main__':
    print("="*60)
    print(" PC1 Student API Server")
    print("="*60)
    print("Database: db1_student.db (SQLite)")
    print("Tables: Students, Enrollment, Attendance")
    print("Port: 5001")
    print("="*60)
    app.run(host='0.0.0.0', port=5001, debug=True)

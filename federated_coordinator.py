import sqlite3
import requests
import hashlib
import json
import sys
import re
from datetime import datetime, timedelta
try:
    from google import genai
    # import google.generativeai as genai
    from auth import GEMINI_API_KEY
    HAS_GENAI = True
except Exception:
    HAS_GENAI = False

PC2_URL = "http://192.168.42.7:5002"

client = None
if HAS_GENAI:
    try:
        client = genai.Client(api_key=GEMINI_API_KEY)
    except Exception:
        client = None

CACHE_DB = "cache.db"
CACHE_TTL = 300  # seconds

def init_cache():
    """Initialize cache database (only query_cache used)"""
    conn = sqlite3.connect(CACHE_DB)
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
    conn.commit()
    conn.close()

def get_from_cache(query_hash):
    """Retrieve from cache if not expired"""
    conn = sqlite3.connect(CACHE_DB)
    cursor = conn.cursor()
    cursor.execute("SELECT result, created_at FROM query_cache WHERE query_hash = ?", (query_hash,))
    row = cursor.fetchone()
    conn.close()
    if row:
        result_json, created_at = row
        try:
            created_time = datetime.fromisoformat(created_at)
            if datetime.now() - created_time < timedelta(seconds=CACHE_TTL):
                return json.loads(result_json)
        except Exception:
            # if created_at stored in another format, attempt best-effort parse / return cached
            try:
                return json.loads(result_json)
            except Exception:
                return None
    return None

def save_to_cache(query_hash, query_text, query_type, result):
    """Save result to cache"""
    conn = sqlite3.connect(CACHE_DB)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT OR REPLACE INTO query_cache
        (query_hash, query_text, query_type, result, created_at)
        VALUES (?, ?, ?, ?, ?)
    """, (query_hash, query_text, query_type, json.dumps(result), datetime.now().isoformat()))
    conn.commit()
    conn.close()

def call_llm(prompt, max_tokens=250):
    """Call Google Gemini API using new SDK (if available), else return helpful error."""
    if not client:
        return "LLM not available (genai client not configured)."
    try:
        response = client.models.generate_content(
            model="gemini-2.0-flash-exp",
            contents=prompt,
            config={
                "temperature": 0.1,
                "max_output_tokens": max_tokens,
            }
        )
        if hasattr(response, 'text') and response.text:
            return response.text.strip()
        else:
            return "No response generated"
    except Exception as e:
        # Provide clearer messages for common issues
        if "quota" in str(e).lower():
            return "API quota exceeded. Please wait and retry."
        elif "not found" in str(e).lower() or "not available" in str(e).lower():
            return "Model not available"
        else:
            return f"LLM Error: {str(e)}"


def analyze_query(nl_query):
    """
    Determine query type and target databases
    Returns: ('sql'|'llm'|'federated', [db_sources])
    """
    q = nl_query.lower()

    # LLM keywords - primarily explanatory questions
    llm_kw = ["explain", "why", "how", "summarize", "suggest", "recommend",
              "describe", "importance", "tell me about", "what is", "define"]

    # DB1 (Student) keywords
    db1_kw = ["student", "enrollment", "enroll", "attendance", "attend", "students", "enrolled"]

    # DB2 (Academic) keywords
    db2_kw = ["faculty", "professor", "teacher", "course", "course name", "exam", "remedial", "resource", "courses"]

    # Data retrieval keywords (should be SQL)
    sql_kw = ["show", "list", "get", "select", "find", "display", "count", "how many", "give me", "all"]

    # Check if it's clearly an LLM explanatory question (and not a SQL retrieval)
    is_llm = any(kw in q for kw in llm_kw) and not any(kw in q for kw in sql_kw)
    if is_llm:
        return "llm", []

    # Attendance special-case: DB1
    if "attendance" in q and not any(kw in q for kw in ["course name", "taught by", "professor", "faculty"]):
        return "sql", ["db1"]

    needs_db1 = any(kw in q for kw in db1_kw)
    needs_db2 = any(kw in q for kw in db2_kw)

    # Federated special-case: "students in courses taught by X" or both present
    if "student" in q and "course" in q and ("taught by" in q or "teaching" in q or "taught" in q):
        return "federated", ["db1", "db2"]

    if needs_db1 and needs_db2:
        return "federated", ["db1", "db2"]
    elif needs_db1:
        return "sql", ["db1"]
    elif needs_db2:
        return "sql", ["db2"]
    else:
        return "sql", ["db1"]

def pattern_match_query(nl_query, target_db):
    """
    Pattern matching fallback for common query types.
    Returns SQL string or None.
    """
    q = nl_query.lower().strip()

    if target_db == "db1":
        # Students with attendance > X%
        if "student" in q and "attendance" in q and ("greater" in q or "more than" in q or ">" in q):
            percent_match = re.search(r'(\d+)\s*(%|percent)?', q)
            if percent_match:
                threshold = float(percent_match.group(1)) / 100.0
                return f"""
SELECT DISTINCT s.student_id, s.name, s.email, s.program, s.year,
    ROUND(100.0 * SUM(CASE WHEN LOWER(a.status) = 'present' THEN 1 ELSE 0 END) / COUNT(*), 2) as attendance_percentage
FROM Students s
JOIN Attendance a ON s.student_id = a.student_id
GROUP BY s.student_id, s.name, s.email, s.program, s.year
HAVING (CAST(SUM(CASE WHEN LOWER(a.status) = 'present' THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*)) > {threshold};
"""

        # Students by ID
        student_id_match = re.search(r'\bS(\d{3})\b', q)
        if student_id_match and "student" in q:
            student_id = 'S' + student_id_match.group(1)
            return f"""
SELECT a.course_id,
       COUNT(*) as total_classes,
       SUM(CASE WHEN LOWER(a.status) = 'present' THEN 1 ELSE 0 END) as present_count,
       ROUND(100.0 * SUM(CASE WHEN LOWER(a.status) = 'present' THEN 1 ELSE 0 END) / COUNT(*), 2) as percentage
FROM Attendance a
WHERE a.student_id = '{student_id}'
GROUP BY a.course_id;
"""

        # Students taking a specific course
        course_match = re.search(r'taking\s+([a-zA-Z\s]+)', q)
        if course_match and "student" in q:
            course_name = course_match.group(1).strip()
            return f"""
SELECT s.student_id, s.name, s.email, s.program, s.year, c.course_id, c.course_name
FROM Students s
JOIN Enrollment e ON s.student_id = e.student_id
JOIN Courses c ON e.course_id = c.course_id
WHERE c.course_name LIKE '%{course_name}%';
"""

        # Default: show all students
        if re.search(r"\bstudent'?s?\b", q) and any(kw in q for kw in ["show", "list", "get", "display", "all", "data", "info"]):
            return "SELECT * FROM Students;"

    elif target_db == "db2":
        # Courses by faculty or department
        if "course" in q and ("taught by" in q or "faculty" in q or "professor" in q or "by" in q):
            faculty_match = re.search(r'(?:taught by|by|faculty|professor)\s+([a-zA-Z\s]+)(?:\s+from\s+([a-zA-Z\s]+))?', q, re.IGNORECASE)
            if faculty_match:
                faculty_name = faculty_match.group(1).strip()
                department = faculty_match.group(2).strip() if faculty_match.group(2) else None
                sql = f"""
SELECT c.*, f.name as faculty_name
FROM Courses c
JOIN Faculty f ON c.faculty_id = f.faculty_id
WHERE 1=1
"""
                if faculty_name:
                    sql += f" AND f.name LIKE '%{faculty_name}%'"
                if department:
                    sql += f" AND f.department LIKE '%{department}%'"
                sql += ";"
                return sql

        # Show all faculty
        if q in ["all faculty", "show all faculty", "list all faculty"]:
            return "SELECT * FROM Faculty;"

        # Show all courses
        if q in ["all courses", "show all courses", "list all courses"]:
            return "SELECT * FROM Courses;"

    return None



def generate_sql(nl_query, target_db):
    """Generate SQL using pattern matching first, then LLM as fallback"""
    print("  Attempting pattern matching...")
    pattern_sql = pattern_match_query(nl_query, target_db)

    if pattern_sql:
        sql = pattern_sql.strip()
        if not sql.endswith(';'):
            sql += ';'
        print("  Pattern matched.")
        return sql

    print("  Pattern not found. Using LLM for SQL generation...")
    if target_db == "db1":
        schema = """
Database: SQLite (db1_student.db)
Tables:
Students(student_id TEXT PRIMARY KEY, name TEXT, email TEXT, program TEXT, year INTEGER)
Enrollment(enrollment_id INTEGER PRIMARY KEY, student_id TEXT, course_id TEXT, semester TEXT, enrollment_date DATE)
Attendance(attendance_id INTEGER PRIMARY KEY, student_id TEXT, course_id TEXT, date DATE, status TEXT)
"""
        examples = """
Q: Get all students
A: SELECT * FROM Students;

Q: Students with more than 75% attendance
A: SELECT DISTINCT s.student_id, s.name, s.email
   FROM Students s
   JOIN Attendance a ON s.student_id = a.student_id
   GROUP BY s.student_id
   HAVING (CAST(SUM(CASE WHEN LOWER(a.status) = 'present' THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*)) > 0.75;
"""
    else:
        schema = """
Database: MySQL (smart_campus_db2)
Tables:
Faculty(faculty_id INT PRIMARY KEY, name VARCHAR(100), department VARCHAR(50), email VARCHAR(100))
Courses(course_id INT PRIMARY KEY, course_name VARCHAR(100), faculty_id INT, credits INT)
Exams(exam_id INT PRIMARY KEY, course_id INT, exam_date DATE, eligibility_criteria TEXT)
Remedial_Resources(resource_id INT PRIMARY KEY, course_id INT, type VARCHAR(50), description TEXT)
"""
        examples = """
Q: All courses taught by Dr. Smith
A: SELECT c.* FROM Courses c JOIN Faculty f ON c.faculty_id = f.faculty_id WHERE f.name LIKE '%Smith%';

Q: Get all faculty in Computer Science
A: SELECT * FROM Faculty WHERE department = 'Computer Science';
"""

    prompt = f"""Convert this question to a VALID SQL query. You MUST return a complete, executable SQL query.

{schema}

{examples}

Question: {nl_query}

CRITICAL REQUIREMENTS:
1. Return ONLY a complete SQL query - NO explanations, NO markdown
2. Query must start with SELECT (or WITH ...) and include a FROM clause
3. Use proper table names from the schema above
4. For percentages, use: CAST(numerator AS FLOAT) / denominator
5. End with a semicolon

SQL Query (complete and ready to execute):"""

    sql = call_llm(prompt, max_tokens=300)


    if not sql or not isinstance(sql, str):
        return "SELECT 'LLM failed to generate valid SQL. LLM unavailable or returned nothing.' as error_message;"

    if "```" in sql:
        sql = sql.replace("```", "")

    sql = sql.strip()
    if ';' in sql:
        sql = sql.split(';')[0].strip() + ';'
    elif not sql.endswith(';'):
        sql += ';'

    lines = [l.strip() for l in sql.splitlines() if l.strip()]
    for i, line in enumerate(lines):
        upper = line.upper()
        if upper.startswith('SELECT') or upper.startswith('WITH'):
            sql = '\n'.join(lines[i:])
            break

    sql = sql.strip()
    sql_upper = ' '.join(sql.upper().split())
    if not (sql_upper.startswith('SELECT') or sql_upper.startswith('WITH')) or ' FROM ' not in sql_upper:
        return "SELECT 'LLM failed to generate valid SQL. Please rephrase your question.' as error_message;"

    return sql

def query_db1(sql):
    """Query local SQLite (DB1)"""
    try:
        conn = sqlite3.connect("db1_student.db")
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(sql)
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        rows = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return {"success": True, "columns": columns, "rows": rows}
    except Exception as e:
        return {"success": False, "error": str(e), "sql": sql}

def query_db2(sql):
    """Query remote MySQL (DB2) via API"""
    try:
        response = requests.post(f"{PC2_URL}/api/query", json={"sql": sql}, timeout=10)
        if response.status_code == 200:
            return response.json()
        else:
            return {"success": False, "error": f"API Error {response.status_code}"}
    except requests.exceptions.ConnectionError:
        return {"success": False, "error": "Cannot connect to PC2. Is the server running?"}
    except Exception as e:
        return {"success": False, "error": str(e)}

def process_federated_query(nl_query):
    """Handle queries spanning both databases"""
    print("\n Processing federated query...")
    q = nl_query.lower()

    # Extract faculty name and/or department
    faculty_match = re.search(r'(?:taught by|by|faculty|professor)\s+([a-zA-Z\s]+)(?:\s+from\s+([a-zA-Z\s]+))?', q)
    if faculty_match:
        faculty_name = faculty_match.group(1).strip()
        department = faculty_match.group(2).strip() if faculty_match.group(2) else None
        db2_sql = f"""
SELECT c.course_id, c.course_name, f.name as faculty_name
FROM Courses c
JOIN Faculty f ON c.faculty_id = f.faculty_id
WHERE 1=1
"""
        if faculty_name:
            db2_sql += f" AND f.name LIKE '%{faculty_name}%'"
        if department:
            db2_sql += f" AND f.department LIKE '%{department}%'"
        db2_sql += ";"
    else:
        db2_sql = generate_sql(nl_query, "db2")

    print("  DB2 SQL:", db2_sql)
    db2_result = query_db2(db2_sql)
    if not db2_result.get("success"):
        return db2_result

    # Extract course ids
    course_ids = [str(row["course_id"]) for row in db2_result.get("rows", []) if "course_id" in row]
    if not course_ids:
        return {"success": True, "message": "No matching courses found in DB2", "rows": []}

    # DB1: students in courses
    course_list = "'" + "','".join(course_ids) + "'"
    db1_sql = f"""
SELECT s.student_id, s.name, s.email, s.program, e.course_id
FROM Students s
JOIN Enrollment e ON s.student_id = e.student_id
WHERE e.course_id IN ({course_list})
ORDER BY s.student_id;
"""
    db1_result = query_db1(db1_sql)
    if not db1_result.get("success"):
        return db1_result

    # Combine DB1 and DB2 results
    final_rows = []
    for student in db1_result.get("rows", []):
        for course in db2_result.get("rows", []):
            if str(student.get("course_id")) == str(course.get("course_id")):
                combined = {**student, **course}
                final_rows.append(combined)

    return {
        "success": True,
        "columns": list(final_rows[0].keys()) if final_rows else [],
        "rows": final_rows,
        "federated": True
    }


def execute_query(nl_query):
    """Main entry point for query execution"""
    query_hash = hashlib.md5(nl_query.encode()).hexdigest()
    cached = get_from_cache(query_hash)
    if cached:
        print("⚡ Retrieved from cache")
        return cached, True

    qtype, sources = analyze_query(nl_query)
    print(f"\n Query Type: {qtype.upper()}")
    print(f" Data Sources: {sources if sources else ['LLM']}")

    result = None
    if qtype == "llm":
        context_prompt = f"""You are a helpful assistant for a Smart Campus Query System.
The system manages student data, course information, faculty details, and academic resources.

Please provide a clear, concise, and informative answer to this question:
{nl_query}

Keep your response focused and practical for an academic environment."""
        answer = call_llm(context_prompt, max_tokens=300)
        result = {"type": "llm", "answer": answer}

    elif qtype == "federated":
        result = process_federated_query(nl_query)

    else:
        target_db = sources[0]
        print(f"\n Generating SQL for {target_db.upper()}...")
        sql = generate_sql(nl_query, target_db)
        print(f"   SQL: {sql}")

        print(f"\n Executing on {target_db.upper()}...")
        if target_db == "db1":
            result = query_db1(sql)
        else:
            result = query_db2(sql)

    save_to_cache(query_hash, nl_query, qtype, result)
    return result, False

def clear_cache_for_api_switch():
    """Clear cache when switching APIs"""
    try:
        conn = sqlite3.connect(CACHE_DB)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM query_cache")
        conn.commit()
        conn.close()
        print(" Cache cleared for API switch")
    except Exception as e:
        print(f" Cache clear warning: {e}")

def display_results(result, from_cache=False):
    """Pretty print results"""
    cache_indicator = "⚡ CACHED" if from_cache else ""
    print("\n" + "="*80)
    print(f" RESULTS {cache_indicator}")
    print("="*80)

    if result.get("type") == "llm":
        print(result.get("answer"))

    elif result.get("success"):
        rows = result.get("rows", [])
        columns = result.get("columns", [])
        if rows:
            if columns:
                header = "  |  ".join(str(col)[:20] for col in columns)
                print(header)
                print("-"*len(header))
            for row in rows[:20]:
                if isinstance(row, dict):
                    print("  |  ".join(str(row.get(col, ""))[:20] for col in columns))
                else:
                    print("  |  ".join(str(val)[:20] for val in row))
            if len(rows) > 20:
                print(f"\n... and {len(rows) - 20} more rows")
            print(f"\nTotal: {len(rows)} rows")
        else:
            print(result.get("message", "No results found"))
    else:
        print(f" Error: {result.get('error')}")
        if result.get('sql'):
            print(f"   SQL: {result.get('sql')}")
    print("="*80 + "\n")

def main():
    print("="*80)
    print("FEDERATED SMART CAMPUS QUERY SYSTEM")
    print("="*80)
    print("Data Sources:")
    print("DB1 (SQLite - Local): Students, Enrollment, Attendance")
    print(f" DB2 (MySQL - {PC2_URL}): Faculty, Courses, Exams, Resources")
    print(" LLM (Gemini API): Natural language explanations (if configured)")
    print("="*80)
    print("\nType 'exit' to quit\n")

    init_cache()

    # Check PC2 health (non-fatal)
    print(" Testing connection to PC2...")
    try:
        resp = requests.get(f"{PC2_URL}/health", timeout=5)
        if resp.status_code == 200:
            print(" PC2 connected successfully\n")
        else:
            print(" PC2 responded but with error\n")
    except Exception:
        print(" Cannot connect to PC2. Make sure the PC2 server is running at", PC2_URL)

    while True:
        try:
            query = input(" Enter query: ").strip()
            if not query:
                continue
            if query.lower() in ['exit', 'quit', 'q']:
                break
            result, from_cache = execute_query(query)
            display_results(result, from_cache)
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"\n Error: {e}\n")

if __name__ == "__main__":
    clear_cache_for_api_switch()
    main()

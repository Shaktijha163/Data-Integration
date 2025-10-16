# Federated Smart Campus Query System

##  Project Overview

A distributed database query system that enables natural language querying across heterogeneous databases located on separate physical machines. The system intelligently decomposes complex queries, executes them on appropriate data sources, and seamlessly integrates results.

### Key Features

-  **Natural Language Interface**: Query using plain English
-  **Federated Architecture**: Queries data from SQLite (PC1) and MySQL (PC2)
-  **AI-Powered SQL Generation**: Uses Google Gemini API with pattern matching fallback
-  **Smart Caching**: MD5-based result caching with 5-minute TTL
-  **Semijoin Optimization**: Minimizes cross-database data transfer
-  **Intelligent Query Routing**: Automatically classifies and routes queries

---

##  System Architecture

### Architecture Type
**Hybrid: Virtualized with Temporary Materialization**

- **Primary Method**: Virtualization (on-demand query integration)
- **Optimization**: Temporary result caching (5-minute TTL)
- **Join Strategy**: Application-level join with semijoin reduction

### Data Sources

#### PC1 (Local - SQLite)
- **Database**: `db1_student.db`
- **Tables**: 
  - `Students` (student demographics)
  - `Enrollment` (course enrollments)
  - `Attendance` (attendance records)
- **Connection**: Direct SQLite connection

#### PC2 (Remote - MySQL)
- **Database**: `smart_campus_db2`
- **Tables**:
  - `Faculty` (faculty information)
  - `Courses` (course catalog)
  - `Exams` (examination schedules)
  - `Remedial_Resources` (academic support)
- **Connection**: REST API endpoint (`http://192.168.42.7:5002`)

---

##  Database Schemas

###


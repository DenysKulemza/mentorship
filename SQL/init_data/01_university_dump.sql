-- Connect to the correct database (just in case)
\c university_db;

-- 1. Create Tables
CREATE TABLE students (
    student_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100) UNIQUE,
    enrollment_year INT
);

CREATE TABLE courses (
    course_id SERIAL PRIMARY KEY,
    course_name VARCHAR(100),
    credits INT
);

CREATE TABLE enrollments (
    enrollment_id SERIAL PRIMARY KEY,
    student_id INT REFERENCES students(student_id),
    course_id INT REFERENCES courses(course_id),
    grade VARCHAR(2),
    UNIQUE(student_id, course_id) -- A student can't take the same course twice
);

-- 2. Insert Mock Data

-- Students
INSERT INTO students (first_name, last_name, email, enrollment_year) VALUES
('Alice', 'Smith', 'alice@uni.edu', 2023),
('Bob', 'Johnson', 'bob@uni.edu', 2023),
('Charlie', 'Brown', 'charlie@uni.edu', 2024),
('Diana', 'Prince', 'diana@uni.edu', 2022);

-- Courses
INSERT INTO courses (course_name, credits) VALUES
('Intro to SQL', 3),
('Advanced Python', 4),
('Web Development', 3),
('Data Science 101', 4);

-- Enrollments (Linking Table)
INSERT INTO enrollments (student_id, course_id, grade) VALUES
(1, 1, 'A'), -- Alice in SQL
(1, 2, 'B'), -- Alice in Python
(2, 1, 'A-'), -- Bob in SQL
(3, 3, 'B+'), -- Charlie in Web Dev
(4, 1, 'A'), -- Diana in SQL
(4, 4, 'A'); -- Diana in Data Science
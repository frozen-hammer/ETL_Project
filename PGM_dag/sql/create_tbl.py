create_tables_sql = """
CREATE TABLE IF NOT EXISTS employee_data (
    name VARCHAR(100) NOT NULL,
    age INT NOT NULL,
    emp_id BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS employee_leave_data (
    emp_id BIGINT NOT NULL,
    date DATE NOT NULL,
    status VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS employee_leave_quota_data (
    emp_id BIGINT NOT NULL,
    leave_quota INT NOT NULL,
    year INT NOT NULL
    );

CREATE TABLE IF NOT EXISTS employee_leave_calender_data (
    reason VARCHAR(10) NOT NULL,
    date DATE NOT NULL
    );

CREATE TABLE IF NOT EXISTS staging_employee_leave_data (
    emp_id BIGINT NOT NULL,
    date DATE NOT NULL,
    status VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS employee_ts_table (
    emp_id bigint,
    designation text,
    start_date DATE,
    end_date DATE,
    salary INT,
    status text
    );

CREATE TABLE IF NOT EXISTS staging_employee_ts_table (
    emp_id bigint,
    designation text,
    start_date DATE,
    end_date DATE,
    salary INT,
    status text
    );

CREATE TABLE IF NOT EXISTS ac_employee_ts_table (
    emp_id bigint,
    designation text,
    active_count INT
    );

CREATE TABLE IF NOT EXISTS employee_upcoming_leaves_table (
    emp_id BIGINT,
    upcoming_leaves INT
    );

CREATE TABLE IF NOT EXISTS employee_leaves_spend_table (
    emp_id BIGINT,
    year INT,
    leave_quota INT,
    total_leaves_taken INT,
    percentage_used DOUBLE PRECISION
);

"""
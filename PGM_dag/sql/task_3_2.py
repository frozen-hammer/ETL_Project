employee_leave_data_transformation_sql = """
-- Update existing records in employee_leave_data with matching records from staging_employee_leave_data
UPDATE employee_leave_data AS e
SET status = s.status
FROM staging_employee_leave_data AS s
WHERE e.emp_id = s.emp_id AND e.date = s.date;

-- Insert new records into employee_leave_data from staging_employee_leave_data
INSERT INTO employee_leave_data (emp_id, status, date)
SELECT emp_id, status, date
FROM staging_employee_leave_data
EXCEPT
SELECT emp_id, status, date
FROM employee_leave_data;

-- Truncate staging_employee_leave_data table after processing
TRUNCATE TABLE staging_employee_leave_data;
"""
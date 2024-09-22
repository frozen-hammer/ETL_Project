compare_staging_employee_ts_table_query = '''
   -- Update end_date and status for existing records
UPDATE employee_ts_table AS h
SET end_date = i.min_start_date, status = 'INACTIVE'
FROM (
    SELECT emp_id, MIN(start_date) AS min_start_date
    FROM staging_employee_ts_table
    WHERE end_date IS NULL
    GROUP BY emp_id
) AS i
WHERE h.emp_id = i.emp_id AND h.end_date IS NULL;

-- Insert new records
INSERT INTO employee_ts_table
SELECT * 
FROM staging_employee_ts_table st
WHERE NOT EXISTS (
    SELECT 1 
    FROM employee_ts_table et 
    WHERE et.emp_id = st.emp_id AND et.end_date IS NULL
);

-- Truncate staging table
TRUNCATE TABLE staging_employee_ts_table;

    '''
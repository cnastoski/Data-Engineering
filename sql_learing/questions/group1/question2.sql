1.SQL Query to print the number of employees per department in the organization

    select count(*) as emp_count, deptno from cards_ingest.emp
    group by deptno

2.SQL Query to find the employee details who got second maximum salary

        select * from cards_ingest.emp
        order by sal desc
        limit 1 offset 1

3.SQL Query to find the employee details who got second maximum salary in each department
4.SQL Query to find the employee who got minimum salary in 2019
5.SQL query to select the employees getting salary greater than the average salary of the department that are working in
6.SQL query to compute the group salary of all the employees.
7.SQL query to list the employees and name of employees reporting to each person.
8.SQL query to find the department with highest number of employees.
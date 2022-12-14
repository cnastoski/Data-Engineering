--- Basic sql complexity is Easy. This needs to be solved first before going to question2


1. Display all the information of the Employee table.

    select * from cards_ingest.emp

2. Display unique Department names from Employee table.

    select distinct dname from cards_ingest.dept

3. List the details of the employees in ascending order of their salaries.

    select * FROM cards_ingest.emp
	ORDER BY sal ASC

4. List the employees who joined before 1981.

    select * FROM cards_ingest.emp
    where hiredate < '1-1-1981'

5. List the employees who are joined in the year 1981

    select * FROM cards_ingest.emp
    where hiredate >= '1-1-1981' and hiredate <= '12-31-1981'

6. List the Empno, Ename, Sal, Daily Sal of all Employees in the ASC order of AnnSal. (Note devide sal/30 as annsal)

    select empno, ename, sal, sal/30 as annsal FROM cards_ingest.emp
    order by annsal asc

7. List the employees who are working for the department name ACCOUNTING

    select * from cards_ingest.emp
    where deptno = 10

8. List the employees who does not belong to department name ACCOUNTING

    select * from cards_ingest.emp
    where deptno != 10
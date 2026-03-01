import pandas as pd
import sqlite3

conn = sqlite3.connect('library.db')
# ─── Template for Task 1.1 ────────────────────────────────────────────────────
query1_1 = """
    SELECT  b.title,
    a.name AS author_name,
    b.publication_year
    FROM books b
    INNER JOIN authors a ON b.author_id=a.author_id
    WHERE b.genre='Fiction' AND b.publication_year>1960
"""
df1_1 = pd.read_sql_query(query1_1, conn)
df1_1.to_csv('task1_1.csv', index=False)

# ─── Template for Task 1.2 ────────────────────────────────────────────────────
query1_2 = """
SELECT m.name AS member_name,m.email,
COUNT(br.borrow_id) AS total_borrowings
FROM members m
INNER JOIN borrowings br ON m.member_id=br.member_id
WHERE m.membership_type='student'
GROUP BY m.member_id,m.name,m.email
ORDER BY total_borrowings DESC
"""
df1_2 = pd.read_sql_query(query1_2, conn)
df1_2.to_csv('task1_2.csv', index=False)

# ─── Template for Task 1.3 ────────────────────────────────────────────────────
query1_3 = """
SELECT m.membership_type,
COUNT(DISTINCT m.member_id) AS total_members,
SUM(br.fine_amount) AS total_fines,
SUM(br.fine_amount)/COUNT(DISTINCT m.member_id) AS avg_fine_per_member
FROM members m
INNER JOIN borrowings br ON m.member_id=br.member_id
GROUP BY m.membership_type
HAVING SUM(br.fine_amount) >0
"""

df1_3 = pd.read_sql_query(query1_3, conn)
df1_3.to_csv('task1_3.csv', index=False)

# ─── Template for Task 2.1 ────────────────────────────────────────────────────
query2_1 = """
SELECT b.title,a.name AS author_name,
CASE 
WHEN 
COUNT(br.borrow_id)=0 
THEN 'Never Borrowed'
ELSE CAST(COUNT(br.borrow_id) AS TEXT)
END AS times_borrowed,
b.copies_available
AS currently_available 
FROM books b
LEFT JOIN borrowings br ON b.book_id=br.book_id
INNER JOIN authors a ON a.author_id=b.author_id
GROUP BY b.book_id,b.title,a.name,b.copies_available
ORDER BY COUNT(br.borrow_id) DESC
LIMIT 3
"""
# currently_available this can be (available book - books that still borrowed )
# but the database dose not consider the available count when generate borrowed rows it dose not decrement and increment them
# so I made it only copies_available
df2_1 = pd.read_sql_query(query2_1, conn)
df2_1.to_csv('task2_1.csv', index=False)

# ─── Template for Task 2.2 ────────────────────────────────────────────────────
query2_2 = """
SELECT b.title AS book_title,
m.name AS borrower_name,
br.borrow_date,br.due_date,
CAST (JULIANDAY(CURRENT_DATE) - JULIANDAY(br.due_date) AS INTEGER) AS days_overdue,
CAST (JULIANDAY(CURRENT_DATE) - JULIANDAY(br.due_date) AS INTEGER)*2 AS estimated_fine
FROM borrowings br
JOIN  books b ON b.book_id=br.book_id
JOIN members m ON m.member_id=br.member_id
WHERE br.return_date IS NULL AND br.due_date < CURRENT_DATE
ORDER BY days_overdue DESC
"""
df2_2 = pd.read_sql_query(query2_2, conn)
df2_2.to_csv('task2_2.csv', index=False)

# ─── Template for Task 3.1 ────────────────────────────────────────────────────
# on_time_return_rate  I considered it (Count of returned on time)/(total count of returned)
# Use the left outer join to include all memeber even if they didnot borrow and book
query3_1 = """
WITH member_borrowing_stats AS (
    SELECT m.member_id,m.name AS member_name,m.membership_type,
    COUNT(br.borrow_id) AS total_borrowings,
    COUNT(br.return_date) AS books_returned,
    COUNT(br.borrow_id)-COUNT(br.return_date) AS books_still_borrowed,
    COALESCE(SUM(br.fine_amount),0) AS total_fines_paid
    FROM  members m 
    LEFT JOIN borrowings br ON m.member_id=br.member_id
    GROUP BY m.member_id,m.name,m.membership_type
),
return_performance AS (
    SELECT m.member_id,
    COUNT(
        CASE
        WHEN br.return_date IS NOT NULL AND br.return_date<= br.due_date THEN 1 
        END) AS on_time_return_count
    FROM  members m 
    LEFT JOIN borrowings br ON m.member_id=br.member_id
    GROUP BY m.member_id
)
SELECT
    mbs.member_name,
    mbs.membership_type,
    mbs.total_borrowings,
    mbs.books_returned,
    mbs.books_still_borrowed,
    mbs.total_fines_paid,
    CASE
    WHEN mbs.books_returned=0 THEN 0
    ELSE ROUND(
        (rp.on_time_return_count*100.0)/mbs.books_returned,2
    )
    END AS on_time_return_rate,
    
    CASE 
    WHEN mbs.total_borrowings=0 THEN 'Inactive'
    WHEN mbs.total_borrowings BETWEEN 1 AND 5  THEN 'Active'
    ELSE 'Very Active'
    END AS member_category
    FROM member_borrowing_stats mbs 
    JOIN return_performance rp ON mbs.member_id=rp.member_id
"""
df3_1 = pd.read_sql_query(query3_1, conn)
df3_1.to_csv('task3_1.csv', index=False)
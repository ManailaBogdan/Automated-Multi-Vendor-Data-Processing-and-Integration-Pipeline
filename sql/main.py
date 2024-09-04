import mysql.connector

# Database connection details
db_config = {
    'host': '172.17.0.1',
    'user': 'root',
    'password': 'secret',
    'database': 'library_db'
}

# Connect to MySQL server
conn = mysql.connector.connect(**db_config)



cursor = conn.cursor()
conn.start_transaction()


# Exercise 1: Top 5 Most Borrowed Books
# Write a query to find the top 5 most borrowed books based on 
# the number of times they were borrowed. Display the book title 
# and the number of times it was borrowed.
print("EXERCISE 1")
cursor.execute("""
               SELECT title, count(*)
               FROM Books A
               JOIN Borrowing B ON
               A.book_id = B.book_id
               GROUP BY title
               ORDER BY count(*) DESC, title
               LIMIT 5;
               """)

for x in cursor:
    print(x)
print()
print("EXERCISE 2")
# Exercise 2: Books Borrowed by Members Who Joined This Year
# Write a query to find the titles of books borrowed by members 
# who joined the library in the current year. 
# Display the book title and the member's name.    
cursor.execute("""
               SELECT title, member_name
               FROM Books A
               JOIN Borrowing B ON
               A.book_id = B.book_id
               JOIN Members C ON
               C.member_id = B.member_id
               WHERE
               YEAR(join_date) = YEAR(CURDATE());  
               """)

for x in cursor:
    print(x)
print()


# Exercise 3: Authors with No Books Borrowed
# Write a query to find authors who have written books, 
# but none of those books have been borrowed. 
# Display the author's name.   
print("EXERCISE 3")
cursor.execute("""
               SELECT A.author_name
               FROM Authors A
               JOIN Books B ON
               A.author_id = B.author_id
               LEFT JOIN Borrowing C ON
               B.book_id = C.book_id
               WHERE
               C.borrowing_id is NULL
               GROUP BY A.author_name
               HAVING count(*) = (SELECT count(*)
                                  FROM Authors D 
                                  JOIN Books E ON
                                  D.author_id = E.author_id
                                  WHERE
                                  D.author_name = A.author_name);
               """)

for x in cursor:
    print(x)
print()


# Exercise 4: Average Borrowing Duration
# Write a query to calculate the average duration (in days)
# that books are borrowed. Display the average duration.
print("EXERCISE 4")
cursor.execute("""
               SELECT B.title, AVG(DATEDIFF(A.borrow_date, A.return_date))
               FROM Borrowing A
               JOIN Books B ON
               A.book_id = B.book_id
               GROUP BY A.book_id;
               """)

for x in cursor:
    print(x)
print()


# Exercise 5: Members Who Have Borrowed the Same Book More Than Once
# Write a query to find members who have borrowed the same book more 
# than once. Display the member's name and the title of the book.
print("EXERCISE 5")
cursor.execute("""
               SELECT member_name, title
               FROM Members M
               JOIN Borrowing Br ON
               M.member_id = Br.member_id
               JOIN Books B ON
               B.book_id = Br.book_id
               GROUP BY Br.member_id, Br.book_id
               HAVING count(*) > 1; 
               """)

for x in cursor:
    print(x)
print()


# Exercise 6: Books That Have Not Been Borrowed
# Write a query to find books that have never been 
# borrowed. Display the book title and the author's name.
print("EXERCISE 6")
cursor.execute("""
                SELECT B.title, A.author_name
                FROM Authors A
                JOIN Books B ON
                A.author_id = B.author_id
                LEFT JOIN Borrowing C ON
                B.book_id = C.book_id
                WHERE
                C.borrowing_id is NULL;
                """)

for x in cursor:
    print(x)
print()


# Exercise 7: Books with Most Recent Borrow Date
# Write a query to find the book with the most recent 
# borrow date. Display the book title and the most recent borrow date.
print("EXERCISE 7")
cursor.execute("""
                SELECT B.title, Br.borrow_date
                FROM Books B
                JOIN Borrowing Br ON
                B.book_id = Br.book_id
                WHERE
                Br.borrow_date = (SELECT MAX(borrow_date) FROM Borrowing);
                """)

for x in cursor:
    print(x)
print()


# Exercise 8: Number of Books Borrowed Per Genre
# Write a query to count the number of books borrowed 
# per genre. Display the genre and the number of books borrowed.
print("EXERCISE 8")
cursor.execute("""
                SELECT B.genre, count(*)
                FROM Books B
                JOIN Borrowing Br ON
                B.book_id = Br.book_id
                GROUP BY B.genre;
                """)

for x in cursor:
    print(x)
print()


# Exercise 9: Members Who Have Borrowed More Than 5 Books
# Write a query to find members who have borrowed more than 
# 5 books. Display the member's name and the number of books borrowed.
print("EXERCISE 9")
cursor.execute("""
                SELECT M.member_name, count(*)
                FROM Members M
                JOIN Borrowing Br ON
                M.member_id = Br.member_id
                GROUP BY M.member_id
                HAVING count(*) > 5;
                """)

for x in cursor:
    print(x)
print()


# Exercise 10: Authors Who Have Written More Than 3 Books
# Write a query to find authors who have written more than 3 
# books. Display the author's name and the number of books they have written.
print("EXERCISE 10")
cursor.execute("""
                SELECT A.author_name, count(*)
                FROM Authors A
                JOIN Books B ON
                A.author_id = B.author_id
                GROUP BY A.author_id
                HAVING count(*) > 3;
                """)

for x in cursor:
    print(x)
print()

conn.commit()
cursor.close()
conn.close()

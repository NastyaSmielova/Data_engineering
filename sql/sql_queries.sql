-- Task 1: Identify Authors that Published More than 3 Books
WITH cte_authors_books_num AS (
   SELECT Authors.author_id , COUNT(Books.book_id) as books_num
   FROM Books
   LEFT JOIN Authors ON Books.author_id = Authors.author_id
   GROUP BY Authors.author_id
   )
SELECT *
FROM Authors
INNER JOIN cte_authors_books_num USING (author_id)
WHERE books_num > 3;


-- Task 2: Identify Books with Titles Containing 'The' Using Regular Expressions

WITH books_with_the AS (
                     SELECT book_id
                     FROM Books
                     WHERE title ~* 'the'
                   )
                   SELECT Books.title , Authors.name, Genres.genre_name , Books.published_date
                   FROM books_with_the
                   INNER JOIN Books USING (book_id)
                   INNER JOIN Authors ON Books.author_id = Authors.author_id
                   INNER JOIN Genres ON Genres.genre_id = Books.genre_id;


-- Task 3: Rank Books by Price within Each Genre Using the RANK() Window Function

SELECT title, price,genre_id,
  RANK() OVER(PARTITION BY genre_id ORDER BY price DESC) AS ranking
FROM Books;


-- Task 4: Bulk Update Book Prices by Genre

SELECT* from Books;
CREATE OR REPLACE PROCEDURE sp_bulk_update_book_prices_by_genre
  (p_genre_id INTEGER,
  p_percentage_change NUMERIC(5, 2))
  language plpgsql
  AS $$
  declare
     updated_count integer;
  begin
    SELECT count(*)
         into updated_count
    FROM Books
    WHERE genre_id = p_genre_id;
    UPDATE Books
      set price = price*( 1 + p_percentage_change/100)
    WHERE genre_id = p_genre_id;
    RAISE NOTICE 'updated books: %', updated_count;
  end;$$;
  call sp_bulk_update_book_prices_by_genre(1, 10.0);

SELECT * FROM Books;


-- Task 5: Update Customer Join Date Based on First Purchase

CREATE OR REPLACE PROCEDURE sp_update_customer_join_date ()

  language plpgsql
  as $$
  begin
      WITH earliest_purchase AS (
         SELECT Customers.customer_id as customer_id, MIN(Sales.sale_date) as earliest_date
         FROM Customers
         LEFT JOIN Sales ON Sales.customer_id = Customers.customer_id
         GROUP BY Customers.customer_id
         )
      update Customers
        SET join_date = (SELECT MIN(x) FROM (VALUES (Customers.join_date),(earliest_purchase.earliest_date)) AS value(x))
      FROM earliest_purchase
      WHERE Customers.customer_id = earliest_purchase.customer_id;
  end;$$;
  call sp_update_customer_join_date();


-- Task 6: Calculate Average Book Price by Genre

create function fn_avg_price_by_genre(p_genre_id INTEGER)
returns NUMERIC(10, 2)
language plpgsql
as
$$
declare
   avarage_price integer;
begin
   SELECT AVG(price)
     into avarage_price
   FROM Books
   WHERE genre_id = p_genre_id;

   return avarage_price;
end;
$$;

SELECT fn_avg_price_by_genre(1);


-- Task 7: Get Top N Best-Selling Books by Genre


create function fn_get_top_n_books_by_genre(p_genre_id INTEGER,
p_top_n INTEGER)
returns TABLE ( title VARCHAR(255), total NUMERIC)
language plpgsql
as
$$

begin
  return QUEry
    WITH ravenue AS (
         SELECT Sales.book_id, SUM(Sales.quantity * Books.price) as total
         FROM Sales
         LEFT JOIN Books USING(book_id)
         GROUP BY Sales.book_id
         )

   SELECT  Books.title, ravenue.total
   FROM Books
   INNER JOIN ravenue USING(book_id)
   WHERE genre_id = p_genre_id
   ORDER BY total DESC
   LIMIT p_top_n;

end;
$$;


select * From fn_get_top_n_books_by_genre(5, 2);

--Task 8: Log Changes to Sensitive Data


CREATE TABLE CustomersLog (
 log_id SERIAL PRIMARY KEY,
 column_name VARCHAR(50),
 old_value TEXT,
 new_value TEXT,
 changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
-- changed_by VARCHAR(50) -- This assumes you can track the user making the change
);


CREATE OR REPLACE FUNCTION log_customer_change()
RETURNS TRIGGER
AS
$$
BEGIN
  IF OLD.first_name != NEW.first_name then

    INSERT INTO CustomersLog(column_name, old_value, new_value)
    VALUES ('first_name', OLD.first_name, NEW.first_name);
    END IF;
  IF OLD.last_name != NEW.last_name then
  INSERT INTO CustomersLog(column_name, old_value, new_value)
    VALUES ('last_name', OLD.last_name, NEW.last_name);
     END IF;
  IF OLD.email != NEW.email then
  INSERT INTO CustomersLog(column_name, old_value, new_value)
    VALUES ('email', OLD.email, NEW.email);
     END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER tr_log_sensitive_data_changes
AFTER UPDATE OF first_name, last_name, email  ON Customers
FOR EACH ROW
EXECUTE FUNCTION log_customer_change();


UPDATE Customers
SET last_name = 'piupiur', first_name = 'jdnvkdk'

WHERE customer_id = 1;
UPDATE Customers
SET email = 'fv'
WHERE customer_id = 3;
select * From CustomersLog;


-- Task 9: Automatically Adjust Book Prices Based on Sales Volume

-- select * From Books WHERE book_id = 7;

CREATE OR REPLACE PROCEDURE sp_increase_price
  (p_book_id INTEGER,
  p_percentage_change NUMERIC(5, 2))
  language plpgsql
  AS $$
  begin
    UPDATE Books
      set price = price*( 1 + p_percentage_change/100)
    WHERE book_id = p_book_id;
  end;$$;
-- call sp_increase_price(7, 10);

-- select * From Books WHERE book_id = 7;


create function fn_get_book_sold_num(p_book_id INTEGER)
returns INTEGER
language plpgsql
as
$$
declare
   purchased integer;
begin
     SELECT SUM(quantity)
     into purchased
     FROM Sales

     WHERE book_id = p_book_id;
     return purchased;

end;
$$;

-- SELECT fn_get_book_sold_num(7)


CREATE OR REPLACE FUNCTION update_price_if_needed()
RETURNS TRIGGER AS $$
BEGIN
    IF fn_get_book_sold_num(NEW.book_id) > 5 then

    call sp_increase_price(NEW.book_id, 10);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


CREATE TRIGGER tr_adjust_book_price
AFTER INSERT ON Sales
FOR EACH ROW
EXECUTE FUNCTION update_price_if_needed();


SELECT * from Books Where book_id = 1;
INSERT INTO Sales (book_id, customer_id, quantity, sale_date) VALUES
(1, 3, 10, '2022-01-15');
SELECT * from Books Where book_id = 1;


SELECT * from Books Where book_id = 7;
INSERT INTO Sales (book_id, customer_id, quantity, sale_date) VALUES
(7, 3, 1, '2022-01-15');
SELECT * from Books Where book_id = 7;
SELECT fn_get_book_sold_num(7);
SELECT * from Books Where book_id = 7;
INSERT INTO Sales (book_id, customer_id, quantity, sale_date) VALUES
(7, 3, 2, '2022-01-15');
SELECT * from Books Where book_id = 7;



-- Task 10: Archive Old Sales Records


CREATE TABLE SalesArchive (
    sale_id SERIAL PRIMARY KEY,
    book_id INTEGER NOT NULL,
    customer_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    sale_date DATE NOT NULL,
    FOREIGN KEY (book_id) REFERENCES Books(book_id),
    FOREIGN KEY (customer_id) REFERENCES Customers(customer_id)
);

CREATE OR REPLACE PROCEDURE sp_archive_old_sales(p_year DATE)
AS
$$
DECLARE
    sales_cursor CURSOR FOR
        SELECT sale_id, book_id, customer_id, quantity, sale_date
        FROM Sales
        WHERE Sales.sale_date < p_year;
    sale_record RECORD;
BEGIN
    OPEN sales_cursor;
    LOOP
        FETCH NEXT FROM sales_cursor INTO sale_record;
        EXIT WHEN NOT FOUND;

        INSERT INTO SalesArchive(book_id, customer_id, quantity, sale_date) VALUES
          (sale_record.book_id, sale_record.customer_id,
          sale_record.quantity, sale_record.sale_date);
        DELETE FROM Sales WHERE Sales.sale_id = sale_record.sale_id;

        -- RETURN NEXT;
    END LOOP;

    CLOSE sales_cursor;
END;
$$
LANGUAGE PLPGSQL;


call sp_archive_old_sales('2022-12-29');
-- SELECT * FROM Sales;
SELECT * FROM SalesArchive;

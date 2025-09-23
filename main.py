import psycopg

conn = psycopg.connect(host="localhost", dbname="politikk", user="postgres", port=5432, password="password")


cur = conn.cursor()
cur.execute("""CREATE TABLE IF NOT EXISTS person (
id SERIAL PRIMARY KEY,
name VARCHAR(255), 
age INT,
gender CHAR 
);
""")

cur.execute("""INSERT INTO person (id, name, age, gender) VALUES
(1, 'Alice', 30, 'F'), 
(2, 'Bob', 25, 'M'), 
(3, 'Charlie', 35, 'M');
""")

cur.execute("""SELECT * FROM person WHERE name ='Alice';""")

for row in cur.fethcall():
    print(row)

conn.commit()

cur.close()
conn.close()


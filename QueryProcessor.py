import psycopg2

connection_params = {
    'dbname': 'carmengvargas',
    'host': 'localhost',
    'port': '5432'
}

try: 
    connection = psycopg2.connect(**connection_params)
    print("Connection successful!")
except Exception as e:
    print("error connecting to the database: ", e)

try:
    cursor = connection.cursor()
    query = "select * from sales where cust = 'Boo' and quant < 10;"
    cursor.execute(query)
    sales_rows = cursor.fetchall()
    for row in sales_rows:
        print(row)

except Exception as e:
    print("Error retrieving data: ", e)

finally:
    cursor.close()
    connection.close()
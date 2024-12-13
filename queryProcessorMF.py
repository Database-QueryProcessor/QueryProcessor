"""
    Program: MF Query Processor
    Author: Carmen Vargas and Aradhana Ramamoorthy aka Girl Power
    Date: Fall Semester 2024
    Description:
        This program is a simple solution to ad-hoc OLAP complex queries using the newly introduced Phi operator. Instead of conducting multiple joins which leads to multiple scans of the underlying table, this programs aims to reduce the number of joins and number of scans by computing aggregates functions of subsets of the group by attributes using just a few table scans.
"""

import psycopg2
from tabulate import tabulate
from queries import esqlQuery

#Global Variables
connection_params = {
    'dbname': 'carmengvargas',
    'host': 'localhost',
    'port': '5432'
    
}
sales_table_schema = {
    "cust": ["char", 20],
    "prod": ["char", 20],
    "day": "int",
    "month": "int",
    "year": "int",
    "state": ["char", 2],
    "quant": "int",
    "date": "date"
}
sales_table_columns = ["cust", "prod", "day", "month", "year", "state", "quant", "date"]
mf_struct_header = []
mf_table = [] #Store the Output of the result

# The set of below operations attempt to connect to Database and retrieve Sales Table
try: 
    connection = psycopg2.connect(**connection_params)
    print("Connection successful!")
except Exception as e:
    print("Error connecting to the database: ", e)

try:
    cursor = connection.cursor()
    # table rows
    cursor.execute("select * from sales;")
    global table_rows
    table_rows = cursor.fetchall()

except Exception as e:
    print("Error retrieving data: ", e)

finally:
    cursor.close()
    connection.close()

# ************************* Print Table Rows Test Function **************************
def print_table_rows():
    """
    This function prints out the rows of the Sales tables. This function is purely for testing purposes.
    """
    # Using Tabulate to output result table
    print("\n\n******************** \n    MF Table \n********************")
    
    data = []
    
    # Iterate through each row in mf_table
    for row in mf_table:
        new_row = []
        
        # For each column in the header, get the corresponding value from the row
        for header in mf_struct_header:
            if header in row:
                new_row.append(row[header])  # Append the value under the correct column
            else:
                new_row.append('NULL')  # If a column doesn't exist in the row, append an empty string
        
        data.append(new_row)
    
    # Print the table using tabulate
    print(tabulate(data, headers=mf_struct_header, tablefmt="grid"))



# ************************* Get MF Structure  **************************
def get_mf_structure(phi):
    """
        This function prints out the MF Structure of the input query
        Parameters:
            The single parameters is the Phi Arguments written as a nested dictionary containing all of the information of the phi arguments.
        Output:
            The output is the MF structure of the resulting table, as well as the headers of the table.
    """
    print("\n\n******************** \n    MF Structure \n********************")
    print("struct {")
    for column, obj in phi["V"].items():
        col_name = obj["name"]
        col_type = obj["type"]
        col_size = obj["size"]
        print(f"       {col_type}    {col_name} [{col_size}];")
        mf_struct_header.append(col_name)
    for agg in phi["F-VECT"]:
        col_name = agg["name"]
        col_type = "int"
        print(f"       {col_type}    {col_name};")
        mf_struct_header.append(col_name)
    print("} mf_struct [500]\n\n")

    print("Output Table Headers:")
    col_widths = [max(len(str(row[i])) for row in [mf_struct_header]) for i in range(len(mf_struct_header))]
    print(f" | ".join(f"{str(item).ljust(col_widths[i])}" for i, item in enumerate(mf_struct_header)))
    print("\n")

# ************************* Get Indeces of Grouping Attributes **************************
def get_indeces(attribute):
    """
        This function retrieves the group by attributes from the Sales table as indeces. Because the database returns the table rows as python lists with indexed values, this function gets the index of the group-by attributes using the hard coded sales table headers list.
        Parameters:
            group by attribute
        Output:
            the index of the group by attribute
    """
    index = 0
    for column in sales_table_columns:
        if(attribute == column):
            return index
        else:
            index += 1

# ************************* Lookup Current Row against MF Struct **************************
def lookup(row, indeces):
    """
        This function compares the current row with the rows in the mf_table (ultimately the output table).
        Parameters:
            current row as "row"
            indeces of the group by attributes as a list of indeces
        Output:
            A positive value if a match is found
            A negative value is no match was found
    """
    #Find the index attribute from the grouping attribute(s) in the table_schema
    if(len(mf_table) == 0):
        return [-1, -1]
    else:
        match = 0
        mf_index = 0
        for mf_row in range(len(mf_table)):
            for index in indeces:
                if(mf_table[mf_row][mf_struct_header[mf_index]] == row[index]):
                    match += 1
                    mf_index += 1
            if(match == len(indeces)):
                return [match, mf_row]
            else:
                mf_index = 0
                match = 0
        return [-1, -1]

# ************************* Add Current Row **************************
def add_row(row, indeces):
    """
        This function adds a row to the mf_table (output table)
        Parameters:
            Current row of the underlying table as "row"
            List of indeces of the group by attributes
    """
    mf_index = 0
    new_row = {}
    for index in indeces:
        new_row[mf_struct_header[mf_index]] = row[index]
        mf_index += 1
    mf_table.append(new_row)

# ************************* Main Function **************************
def main():
    """
        Main function that runs all of the functions and loops to output the result of the submitted (input) query. 
    """
    query = esqlQuery()
    
    # Get MF structure and print
    get_mf_structure(query)
    
    # Get indices of grouping attributes (cust, prod)
    indeces = [get_indeces(obj["name"]) for column, obj in query["V"].items()]

    # First Table Scan: Populate the mf_table with distinct values of grouping attributes
    for row in table_rows:
        match = lookup(row, indeces)
        if match[0] == -1:
            add_row(row, indeces)

    # Second Table Scan: Calculate aggregates within one table scan
    for row in table_rows:
        for n in range(len(query["F-VECT"])):
            agg = query["F-VECT"][n]
            agg_name = agg["name"]
            group_var = agg["group_var"]
            agg_type = agg["agg"]

            predicate = query["PRED-LIST"][f"var{n + 1}"]
            match = lookup(row, indeces)
            if match[0] > -1:
                pos = match[1]
                attr_index = get_indeces(predicate["attribute"])
                
                if row[attr_index] == predicate["value"]:
                    quant_index = get_indeces("quant")
                    current_value = mf_table[pos].get(agg_name, None)
                    if agg_type == "sum":
                        mf_table[pos][agg_name] = current_value + row[quant_index] if current_value else row[quant_index]
                    elif agg_type == "max":
                        mf_table[pos][agg_name] = max(current_value, row[quant_index]) if current_value else row[quant_index]
                    elif agg_type == "min":
                        mf_table[pos][agg_name] = min(current_value, row[quant_index]) if current_value else row[quant_index]
                    elif agg_type == "count":
                        mf_table[pos][agg_name] = current_value + 1 if current_value else 1
                    elif agg_type == "avg":
                        sum_key = f"{agg_name}_sum"
                        count_key = f"{agg_name}_count"
                        mf_table[pos][sum_key] = mf_table[pos].get(sum_key, 0) + row[quant_index]
                        mf_table[pos][count_key] = mf_table[pos].get(count_key, 0) + 1
                        mf_table[pos][agg_name] = mf_table[pos][sum_key] / mf_table[pos][count_key]


    # Print the final result (MF table)
    print_table_rows()


if __name__ == "__main__":
    main()

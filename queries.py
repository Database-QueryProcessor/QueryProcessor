"""
Description:
    The queries file is where the sql and esql queries are stored and fed into the program. 
    Variables with sql_ prefix are the sql equivalent of the esql queries that we can test against our custom query processor
    Variables with esql_ prefix are the esql queries we feed into our custom query processor.
    Each variable can be returned in the respective function: sqlQuery or esqlQuery
"""

sql_a = """
    select cust, prod, sum(quant) as sum_NY_quant
    from sales 
    where state = 'NY'
    group by cust, prod;
"""

sql_b = """
    with q0 as(
    select cust, prod, state, max(quant) as max_q
    from sales
    group by cust, prod, state
    ),
    nj as(
    select q0.cust, q0.prod, q0.state, q0.max_q as nj_max
    from q0, sales
    where sales.quant = q0.max_q and q0.state = 'NJ'
    ),
    ny as(
    select q0.cust, q0.prod, q0.state, q0.max_q as ny_max
    from q0, sales
    where sales.quant = q0.max_q and q0.state = 'NY'
    ),
    ct as(
    select q0.cust, q0.prod, q0.state, q0.max_q as ct_max
    from q0, sales
    where sales.quant = q0.max_q and q0.state = 'CT'
    )
    select ny.cust, ny.prod, nj.nj_max, ny.ny_max, ct.ct_max
    from ny
    join nj on ny.cust = nj.cust and ny.prod = nj.prod
    join ct on ny.cust = ct.cust and ny.prod = ct.prod;
"""

esql_c = {
    "S": ["cust", "prod", "sum_NY_quant", "sum_NJ_quant", "max_CT_quant", "count_CT_quant", "min_NY_quant", "avg_NY_quant"],
    "n": 3,
    "V": {
        "col1": {"name": "cust", "type": "char", "size": 20},
        "col2": {"name": "prod", "type": "char", "size": 20}
    },
    "F-VECT": [
        {"name": "sum_NY_quant", "group_var": "NY", "agg": "sum"},
        {"name": "sum_NJ_quant", "group_var": "NJ", "agg": "sum"},
        {"name": "max_CT_quant", "group_var": "CT", "agg": "max"},
        {"name": "count_CT_quant", "group_var": "CT", "agg": "count"},
        {"name": "min_NY_quant", "group_var": "NY", "agg": "min"},
        {"name": "avg_NJ_quant", "group_var": "NJ", "agg": "avg"},
    ],
    "PRED-LIST": {
        "var1": {"name": "sum_NY_quant", "group_var": "NY", "attribute": "state", "value": "NY"},
        "var2": {"name": "sum_NJ_quant", "group_var": "NJ", "attribute": "state", "value": "NJ"},
        "var3": {"name": "max_CT_quant", "group_var": "CT", "attribute": "state", "value": "CT"},
        "var4": {"name": "count_CT_quant", "group_var": "CT", "attribute": "state", "value": "CT"},
        "var5": {"name": "min_NY_quant", "group_var": "NY", "attribute": "state", "value": "NY"},
        "var6": {"name": "avg_NY_quant", "group_var": "NJ", "attribute": "state", "value": "NJ"}
    }
}


def sqlQuery():
    return sql_a

def esqlQuery():
    return esql_c
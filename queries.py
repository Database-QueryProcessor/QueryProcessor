"""
Description:
    The queries file is where the sql and esql queries are stored and fed into the program. 
    Variables with sql_ prefix are the sql equivalent of the esql queries that we can test against our custom query processor
    Variables with esql_ prefix are the esql queries we feed into our custom query processor.
    Each variable can be returned in the respective function: sqlQuery or esqlQuery
"""

sql_a = """
    with q1 as (
        select cust, prod, sum(quant) as sum_quant
        from sales
        group by cust, prod
    ),
    q2 as (
        select q1.cust, q1.prod, sum(quant) as sum_NY_quant
        from q1 join sales on q1.cust = sales.cust and q1.prod = sales.prod
        where sales.state = 'NY'
        group by q1.cust, q1.prod
    ),
    q3 as (
        select q1.cust, q1.prod, sum(quant) as sum_NJ_quant
        from q1 join sales on q1.cust = sales.cust and q1.prod = sales.prod
        where sales.state = 'NJ'
        group by q1.cust, q1.prod
    )
    select q1.cust, q1.prod, q2.sum_ny_quant, q3.sum_NJ_quant
    from q1 
    join q2 on q1.cust = q2.cust and q1.prod = q2.prod
    join q3 on q1.cust = q3.cust and q1.prod = q3.prod
"""
esql_a = {
    "S": ["cust", "prod", "sum_NY_quant", "sum_NJ_quant"],
    "n": 2,
    "V": {
        "col1": {"name": "cust", "type": "char", "size": 20},
        "col2": {"name": "prod", "type": "char", "size": 20}
    },
    "F-VECT": [
        {"name": "sum_NY_quant", "group_var": "NY", "agg": "sum"},
        {"name": "sum_NJ_quant", "group_var": "NJ", "agg": "sum"}
    ],
    "PRED-LIST": {
        "var1": {"name": "sum_NY_quant", "group_var": "NY", "attribute": "state", "value": "NY"},
        "var2": {"name": "sum_NJ_quant", "group_var": "NJ", "attribute": "state", "value": "NJ"}
    }
}
phi_a = """
select cust, prod, Y.sum(quant), J.sum(quant)
from sales
group by cust, prod : Y, J
such that Y.state = 'NY' and J.state = 'NJ
"""

sql_b = """
    with q0 as(
        select cust, prod, state, max(quant) as max_q
        from sales
        group by cust, prod, state
    ),
    nj as(
        select q0.cust, q0.prod, q0.max_q as max_nj_quant
        from q0
        where q0.state = 'NJ'
    ),
    ny as(
        select q0.cust, q0.prod, q0.max_q as max_ny_quant
        from q0
        where q0.state = 'NY'
    ),
    ct as(
        select q0.cust, q0.prod, q0.max_q as max_ct_quant
        from q0
        where q0.state = 'CT'
    )
    select ny.cust, ny.prod, nj.max_nj_quant, ny.max_ny_quant, ct.max_ct_quant
    from ny
    join nj on ny.cust = nj.cust and ny.prod = nj.prod
    join ct on ny.cust = ct.cust and ny.prod = ct.prod;
"""

phi_b = """
select cust, prod, J.max(quant), Y.max(quant), T.max(quant)
from sales
group by cust, prod, state : J, Y, T
such that J.state = 'NJ' and
          Y.state = 'NY' and
          T.state = 'CT'
"""

esql_b = {
    "S": ["cust", "prod", "max_nj_quant", "max_ny_quant", "max_ct_quant"],
    "n": 3,
    "V": {
        "col1" : {"name": "cust", "type": "char", "size": 20},
        "col2" : {"name": "prod", "type": "char", "size": 20}
    },
    "F-VECT": [
        {"name": "max_nj_quant", "group_var": "NJ", "agg": "max"},
        {"name": "max_ny_quant", "group_var": "NY", "agg": "max"},
        {"name": "max_ct_quant", "group_var": "CT", "agg": "max"}
    ],
    "PRED-LIST": {
        "var1": {"name": "max_nj_quant", "group_var": "NJ", "attribute": "state", "value": "NJ"},
        "var2": {"name": "max_ny_quant", "group_var": "NY", "attribute": "state", "value": "NY"},
        "var3": {"name": "max_ct_quant", "group_var": "CT", "attribute": "state", "value": "CT"}
    }

}

sql_c = """
    with new_sales as (
        select cust,prod,state,MAX(quant) as MAX_Q, MIN(quant) as MIN_Q, AVG(quant) as AVG_Q, count(quant) as COUNT_Q, SUM(quant) as SUM_Q
        from sales
        group by cust,prod,state
    ),
    sales_data as (
        select cust,prod,MIN_Q as NY_MIN, SUM_Q as NY_SUM
        from new_sales natural join sales s
        where new_sales.state = 'NY' and (s.quant = new_sales.MIN_Q or s.quant = new_sales.SUM_Q)
    ),
    sales_data1 as (
        select cust,prod, MAX_Q as NJ_MAX, AVG_Q as NJ_AVG
        from new_sales natural join sales s
        where new_sales.state = 'NJ' and (s.quant = new_sales.MAX_Q or s.quant = new_sales.AVG_Q)
    ),
    sales_data2 as (
        select cust,prod,MAX_Q as CT_MAX,COUNT_Q as CT_COUNT
        from new_sales natural join sales s
        where new_sales.state = 'CT' and s.quant = new_sales.MAX_Q
    ),
    sales_data3 as (
        select sales_data.cust,sales_data.prod, sales_data1.NJ_MAX, sales_data.NY_MIN, sales_data.NY_SUM,
        sales_data1.NJ_AVG, sales_data2.CT_MAX, sales_data2.CT_COUNT
        from sales_data LEFT JOIN sales_data1 ON sales_data.cust = sales_data1.cust and sales_data.prod = sales_data1.prod
    LEFT JOIN sales_data2 ON sales_data.cust = sales_data2.cust and sales_data.prod = sales_data2.prod
    )
    select * from sales_data3;
"""

esql_c = {
    "S": ["cust", "prod", "sum_NY_quant", "max_NJ_quant", "max_CT_quant", "count_CT_quant", "min_NY_quant", "avg_NY_quant"],
    "n": 3,
    "V": {
        "col1": {"name": "cust", "type": "char", "size": 20},
        "col2": {"name": "prod", "type": "char", "size": 20}
    },
    "F-VECT": [
        {"name": "sum_NY_quant", "group_var": "NY", "agg": "sum"},
        {"name": "max_NJ_quant", "group_var": "NJ", "agg": "max"},
        {"name": "max_CT_quant", "group_var": "CT", "agg": "max"},
        {"name": "count_CT_quant", "group_var": "CT", "agg": "count"},
        {"name": "min_NY_quant", "group_var": "NY", "agg": "min"},
        {"name": "avg_NJ_quant", "group_var": "NJ", "agg": "avg"},
    ],
    "PRED-LIST": {
        "var1": {"name": "sum_NY_quant", "group_var": "NY", "attribute": "state", "value": "NY"},
        "var2": {"name": "max_NJ_quant", "group_var": "NJ", "attribute": "state", "value": "NJ"},
        "var3": {"name": "max_CT_quant", "group_var": "CT", "attribute": "state", "value": "CT"},
        "var4": {"name": "count_CT_quant", "group_var": "CT", "attribute": "state", "value": "CT"},
        "var5": {"name": "min_NY_quant", "group_var": "NY", "attribute": "state", "value": "NY"},
        "var6": {"name": "avg_NY_quant", "group_var": "NJ", "attribute": "state", "value": "NJ"}
    }
}


sql_d = """
with new_sales as
(select prod, state, MAX(quant) as MAX_Q , AVG(quant) as AVG_Q, COUNT(quant) as COUNT_Q
from sales
     group by prod, state),
     sales_data as
     (select prod, MAX_Q as NY_MAX, AVG_Q as NY_AVG, COUNT_Q as NY_COUNT
      from new_sales natural join sales s
      where new_sales.state = 'NY' and (s.quant = new_sales.MAX_Q or s.quant = new_sales.AVG_Q)),
      sales_data1 as
      (select prod, MAX_Q as NJ_MAX, AVG_Q as NJ_AVG
      from new_sales natural join sales s
      where new_sales.state = 'NJ' and (s.quant = new_sales.MAX_Q or s.quant = new_sales.AVG_Q)),
      sales_data2 as 
      (select sales_data.prod, sales_data.NY_MAX, sales_data.NY_AVG, sales_data.NY_COUNT, sales_data1.NJ_MAX, sales_data1.NJ_AVG
       from sales_data LEFT JOIN sales_data1 ON sales_data.prod = sales_data1.prod)
       select * from sales_data2;

"""

esql_z = {
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
    return sql_b

def esqlQuery():
    return esql_b
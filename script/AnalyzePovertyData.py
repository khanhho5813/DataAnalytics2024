import psycopg2
import os 
import glob
import matplotlib.pyplot as plt
from matplotlib.ticker import PercentFormatter
import numpy as np 

import time as tlib
time_start_absolute = tlib.time()

def hist2moments(histogram_data, num_terms, generate_figs, algebra_estimates):
    # Normalize histogram data to create PDF
    counts, edges = np.histogram(histogram_data, bins='auto', density=True)
    x = edges[:-1] + np.diff(edges) / 2
    y = counts
    h = (max(x) - min(x)) / (len(x) - 1)
    f_approx = representation_theory(num_terms, x, y, h)

    # Graphic output if needed
    if generate_figs:
        # Create the first plot
        fig, ax1 = plt.subplots()

        color = 'tab:red'
        plt.xlabel('Percentage of Texan Children 5-17 in Poverty')
        ax1.set_ylabel('PDF')
        ax1.hist(histogram_data, bins='auto', density=True, color='r', alpha=0.25, label='Histogram')
        ax1.plot(x, f_approx, 'r--', label='Fitted Curve')
        ax1.tick_params(axis='y', labelcolor=color)

        ax2 = ax1.twinx()

        color = 'tab:blue'
        ax2.set_ylabel('Proportion of Texan Children Enrolled in Algebra II', color=color)  # we already handled the x-label with ax1
        ax2.plot(histogram_data, algebra_estimates, '.', color=color)
        ax2.tick_params(axis='y', labelcolor=color)

        # Show the plot
        fig.tight_layout()  # To ensure the right y-label is not slightly clipped
        
        plt.gca().xaxis.set_major_formatter(PercentFormatter(1))
        plt.savefig('Poverty_vs_Alg2.png', dpi=400)
        plt.close()

    # Calculate the first four statistical moments
    e_x = inner_product(x * f_approx, np.ones_like(f_approx), h)
    std = np.sqrt(inner_product((x - e_x) ** 2 * f_approx, np.ones_like(f_approx), h))
    skw = inner_product((x - e_x) ** 3 * f_approx, np.ones_like(f_approx), h) / (std**2) ** (3/2)
    krt = inner_product((x - e_x) ** 4 * f_approx, np.ones_like(f_approx), h) / (std**2) ** 2 - 3

    # Return statistical moments
    return e_x, std, skw, krt

def representation_theory(num_terms, t, f, h):
    # Basis array
    u = np.vstack([t ** m for m in range(num_terms)])
    # Coeff. Matrix and forcing vector A and b
    A = np.zeros((num_terms, num_terms))
    b = np.zeros(num_terms)
    for row in range(num_terms):
        b[row] = inner_product(u[row, :], f, h)
        for col in range(num_terms):
            A[row, col] = inner_product(u[row, :], u[col, :], h)
    # Solve for c
    c = np.linalg.solve(A, b)
    f_approx = np.dot(c, u)
    return f_approx

def inner_product(f, g, h):
    product = f * g
    integral = h * (0.5 * product[0] + np.sum(product[1:-1]) + 0.5 * product[-1])
    return integral

# Retrieve the PostgreSQL_PWD environment variable
time_start_connectSQL = tlib.time()
postgresql_pwd = os.getenv('PostgreSQL_PWD')

# Connect to your PostgreSQL server
conn = psycopg2.connect(
    dbname='postgres', 
    user='postgres', 
    password=postgresql_pwd, 
    host='localhost', 
    port='5432'
)

# Create a cursor object
cursor = conn.cursor()

# SQL query
sql_query = 'SELECT leaid from "DataAnalytics".ussd17 where state_postal_code like \'TX\';'

# Execute the query
cursor.execute(sql_query)

# Fetch the data
data = cursor.fetchall()
time_end_connectSQL = tlib.time()

# Save LEAID from TX to another list
TX_LEAID = []
for row in data:
    TX_LEAID.append(row[0])

    
query = """
SELECT 
    SUM(estimated_children_in_poverty) AS total_estimated_children_in_poverty,
    SUM(estimated_population_5_17) AS total_estimated_population_5_17, 
    leaid
FROM 
    "DataAnalytics".ussd17 
WHERE 
    leaid = ANY(%s)
GROUP BY 
    leaid;
"""

query_enrollment = """
SELECT 
    SUM(CASE WHEN CAST(TOT_ENR_M AS INTEGER) < 0 THEN 0 ELSE CAST(TOT_ENR_M AS INTEGER) END) AS sum_enrollment_m,
    SUM(CASE WHEN CAST(TOT_ENR_F AS INTEGER) < 0 THEN 0 ELSE CAST(TOT_ENR_F AS INTEGER) END) AS sum_enrollment_f, 
    LEAID
FROM 
    "DataAnalytics".Enrollment
WHERE 
    LEAID = ANY(%s)
GROUP BY 
    LEAID;
"""

query_algebra = """
SELECT 
    SUM(CASE WHEN CAST(TOT_MATHENR_ALG2_M AS INTEGER) < 0 THEN 0 ELSE CAST(TOT_MATHENR_ALG2_M AS INTEGER) END) AS sum_tot_alge2_enroll_m,
    SUM(CASE WHEN CAST(TOT_MATHENR_ALG2_F AS INTEGER) < 0 THEN 0 ELSE CAST(TOT_MATHENR_ALG2_F AS INTEGER) END) AS sum_tot_alge2_enroll_f, 
    LEAID
FROM 
    "DataAnalytics".algebraII
WHERE 
    LEAID = ANY(%s)
GROUP BY 
    LEAID;
"""

# Execute the query for each table
time_start_executequery1 = tlib.time()
with conn.cursor() as cur:
    cur.execute(query, (TX_LEAID,))
    rows = cur.fetchall()

estimated_childrenPoverty = []
estimated_population_5_17 = []

leaid_poverty = []

for row in rows:
    if row[2]=='4846110':
        # skip - a unmatched LEAID (a discrepency between datasets) TODO: review
        continue
    else:
        estimated_childrenPoverty.append(row[0])
        estimated_population_5_17.append(row[1])
        leaid_poverty.append(row[2])

normalized_estimated_childrenPoverty = (np.array(estimated_childrenPoverty)/np.array(estimated_population_5_17))
time_end_executequery1 = tlib.time()

#Algebra II - enrolled
time_start_executequery2 = tlib.time()
with conn.cursor() as cur_2:
    cur_2.execute(query_algebra, (TX_LEAID,))
    rows_algebra = cur_2.fetchall()

algebraII_estimated_Male = []
algebraII_estimated_Female = []
leaid_algebra = []

for row in rows_algebra:
    #to change the -9 to 0 
    #updated_list = ['0' if item == '-9' else item for item in row]

    algebraII_estimated_Male.append(int(row[0]))
    algebraII_estimated_Female.append(int(row[1]))

    leaid_algebra.append(row[2])
time_end_executequery2 = tlib.time()

#Total enrollment 
time_start_executequery3 = tlib.time()
with conn.cursor() as cur_3:
    cur_3.execute(query_enrollment, (TX_LEAID,))
    rows_enrollment = cur_3.fetchall()

total_enrollment_Male = []
total_enrollment_Female = []
leaid_enrollment = []

for row in rows_enrollment:
    #to change the -9 to 0 
    #updated_list = ['0' if item == '-9' else item for item in row]

    total_enrollment_Male.append(int(row[0]))
    total_enrollment_Female.append(int(row[1]))

    leaid_enrollment.append(row[2])

time_end_executequery3 = tlib.time()

algebraII_estimated = (np.array(algebraII_estimated_Male) + np.array(algebraII_estimated_Female))/(np.array(total_enrollment_Male) + np.array(total_enrollment_Female))

# algebra vs poverty
not_in_algebra = [item for item in leaid_poverty if item not in leaid_algebra]
print(not_in_algebra)

num_terms = 7
generate_figs = True

# We have more than 1022 in both files, we suspected that each file contains the information of all schools in one district. We aggregate them by LEAID. 
# print(len(leaid_poverty))
# print(len(leaid_algebra))
# print(len(normalized_estimated_childrenPoverty))
# print(len(algebraII_estimated))

#Create dictionaries for easy lookup
poverty_dict = dict(zip(leaid_poverty, normalized_estimated_childrenPoverty))
algebra_dict = dict(zip(leaid_algebra, algebraII_estimated))
standard_order_leaid = sorted(set(leaid_poverty + leaid_algebra))
sorted_normalized_estimated_childrenPoverty = [poverty_dict.get(leaid, 0) for leaid in standard_order_leaid]
sorted_algebraII_estimated = [algebra_dict.get(leaid, 0) for leaid in standard_order_leaid]

#There are in the same order 
print(np.all(standard_order_leaid == np.sort(leaid_algebra)))
print(np.all(standard_order_leaid == np.sort(leaid_poverty)))

#To manage NaN values 
# sorted_algebraII_estimated = [x if x <= 1 else np.nan for x in sorted_algebraII_estimated]
# sorted_algebraII_estimated = [x if x > 0 else np.nan for x in sorted_algebraII_estimated]
# print(np.max(sorted_algebraII_estimated))


time_start_graphics = tlib.time()
sm = hist2moments(sorted_normalized_estimated_childrenPoverty, num_terms, generate_figs, sorted_algebraII_estimated)
print(f"Statistical Moments: {sm}")
print(np.mean(sorted_normalized_estimated_childrenPoverty))
time_end_graphics = tlib.time()

print(max(normalized_estimated_childrenPoverty))
print(min(normalized_estimated_childrenPoverty))

# # Close the connection
conn.close()

time_end_absolute = tlib.time()

cpu_times=[
    time_end_connectSQL-time_start_connectSQL,
    time_end_executequery1-time_start_executequery1,
    time_end_executequery2-time_start_executequery2,
    time_end_executequery3-time_start_executequery3,
    time_end_graphics-time_start_graphics,
    time_end_absolute-time_start_absolute]

print(' ')
print('Wall clock times')
print(cpu_times)
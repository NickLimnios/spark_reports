import sys
import os
import glob
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

plt.close("all")

mode = int(sys.argv[1])

# Read CSV into pandas
inputFilePath = "/home/lab/repos/spark_reports/export/totalOrdersPerWeekdayReport/"
csv_files = glob.glob(os.path.join(inputFilePath, "*.csv"))

for f in csv_files:
    df = pd.read_csv(f)

dow = df['Day_Of_Week']
orders_cnt = df['Orders_Count']

# Creating explode data
explode = (0.1, 0.0, 0.2, 0.3, 0.1, 0.0, 0.0)
 
# Creating color parameters
colors = ( "orange", "cyan", "brown",
          "grey", "indigo", "beige", "purple")

## Wedge properties
wp = { 'linewidth' : 1, 'edgecolor' : "green" }

# Creating autocpt arguments
def func(pct, allvalues):
    absolute = int(pct / 100.*np.sum(allvalues))
    return "{:.1f}%\n({:d} )".format(pct, absolute)

# Creating plot
fig, ax = plt.subplots(figsize =(10, 7))
wedges, texts, autotexts = ax.pie(orders_cnt,
                                  autopct = lambda pct: func(pct, orders_cnt),
                                  explode = explode,
                                  labels = dow,
                                  shadow = True,
                                  colors = colors,
                                  startangle = 90,
                                  wedgeprops = wp,
                                  textprops = dict(color ="magenta"))

# Adding legend
ax.legend(wedges, dow,
          title ="Day Of Week",
          loc ="center left",
          bbox_to_anchor =(1, 0, 0.5, 1))
 
plt.setp(autotexts, size = 8, weight ="bold")
ax.set_title("Total orders per day of week")

if (mode == 1):
    plt.savefig("/home/lab/repos/spark_reports/export/graphs/report_2_pie.png")
else:
    plt.show()

import sys
import os
import glob
import pandas as pd
import matplotlib.pyplot as plt

plt.close("all")

mode = int(sys.argv[1])

# Read CSV into pandas
inputFilePath = "/home/lab/repos/spark_reports/export/totalProductsPerDepartment/"
csv_files = glob.glob(os.path.join(inputFilePath, "*.csv"))

for f in csv_files:
    df = pd.read_csv(f)

department = df['Department_Name']
products_cnt = df['Products_Count']

# Figure Size
fig, ax = plt.subplots(figsize =(16, 9))

# Horizontal Bar Plot
ax.barh(department, products_cnt)

# Remove axes splines
for s in ['top', 'bottom', 'left', 'right']:
    ax.spines[s].set_visible(False)

# Remove x, y Ticks
ax.xaxis.set_ticks_position('none')
ax.yaxis.set_ticks_position('none')

# Add padding between axes and labels
ax.xaxis.set_tick_params(pad = 5)
ax.yaxis.set_tick_params(pad = 10)

# Add x, y gridlines
ax.grid(b = True, color ='grey',
        linestyle ='-.', linewidth = 0.5,
        alpha = 0.2)

# Show top values
ax.invert_yaxis()

# Add annotation to bars
for i in ax.patches:
    plt.text(i.get_width()+0.2, i.get_y()+0.5,
             str(round((i.get_width()), 2)),
             fontsize = 10, fontweight ='bold',
             color ='grey')

# Add Plot Title
ax.set_title('Total products count per department', loc ='left', )

if (mode == 1):
    plt.savefig("/home/lab/repos/spark_reports/export/graphs/report_1_bar.png")
else:
    plt.show()

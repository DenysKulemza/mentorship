import matplotlib.pyplot as plt
import csv
from collections import defaultdict

x = []
y = []

dct = defaultdict(int)

with open('transformed_orders_summary.csv', 'r') as csvfile:
    plots = csv.reader(csvfile, delimiter=',')

    header = next(plots)

    for row in plots:
        dct[row[1]] += int(row[2])


for product_category, value in dct.items():
    x.append(product_category)
    y.append(value)

plt.bar(x, y, color='g', width=0.72)
plt.xlabel('Product_Category')
plt.ylabel('Sell amount')
plt.title('Product selling')
plt.legend()
plt.show()
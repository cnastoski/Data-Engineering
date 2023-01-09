import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta, date
import timeit
import sys

brand_name = ['Apple', 'Samsung', 'Nokia']
product_name = {
    'Apple': ['iphone11', 'iphone12', 'iphone13', 'iphoneSE', 'IpadMax', 'IpadMini', 'laptop256', 'Macbook512'],
    'Samsung': ['galaxy10', 'galaxy11', 'galaxy12', 'galaxy13', 'watch320', 'watch340'],
    'Nokia': ['Nk320', 'Nk400', 'Nk500']}


def createData(row_amt, file_name):
    orderid = 1000001
    db_file = open(f"C:/Users/cnast/Desktop/aws_learning-main/redshift/Projects/utils/{file_name}", "w")
    db_file.write("orderid, brand_name, product_name, sales_ammount, sales_date\n")
    for i in range(row_amt):
        rand_brand = np.random.choice(brand_name)  # random brand name from the list
        prod_name = np.random.choice(
            product_name[rand_brand])  # random model number from the dict based on random brand
        price = round(np.random.uniform(5, 1000), 2)
        sales_date = getRandomDates()
        row = f"{orderid}, {rand_brand}, {prod_name}, {price}, {sales_date} \n"

        db_file.write(row)
        orderid += 1

    db_file.close()


def getRandomDates():
    start = date.fromisoformat('2022-01-01')
    end = start + timedelta(days=89)
    random_date = (start + (end - start) * random.random()).strftime('%Y-%m-%d')

    return random_date


def main():
    # timeit library used to gauge how fast the program runs
    starttime = timeit.default_timer()
    print(f"The start time is : {starttime:.3f} Seconds")

    # taking inputs from the command line script
    createData(int(sys.argv[1]), sys.argv[2])


    endtime = timeit.default_timer()
    print(f"The end time is: {endtime:.3f} Seconds")
    print(f"Script Runtime : {(endtime - starttime):.3f} Seconds")


if __name__ == "__main__":
    main()


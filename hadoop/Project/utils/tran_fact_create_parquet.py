import random
import pandas
import fastparquet


def create_data(start_date, end_date):
    input_dir = "C:/Users/cnast/Desktop/aws_learning-main/hadoop/Project/config/class_question1_parquet/"
    id_prefix = ['FX', 'TD']
    start_id = 100000
    country_codes = ['MI', 'CA', 'NY', 'MD', 'WA', 'OH', 'IN', 'FL', 'NJ', 'VA']
    tran_types = ['C', 'D']

    start_day = int(start_date[-2:])
    end_day = int(end_date[-2:])

    for days in range(start_day, end_day + 1):
        if days < 10:
            date = start_date[0:-1] + str(days)
        else:
            date = start_date[0:-2] + str(days)
        file = open(f"{input_dir}{date}.csv", 'w')

        file.write("tran_id,cust_id,tran_date,tran_ammount,tran_type,state_cd\n")

        for i in range(5000):
            tran_id = str(start_id)
            cust_id = 'CA' + str(round(random.uniform(1000, 9999)))
            tran_date = date
            tran_amount = round(random.uniform(1.02, 9999999.02), 2)
            tran_type = random.choice(tran_types)
            state_cd = random.choice(country_codes)

            file.write(f"{tran_id},{cust_id},{tran_date},{tran_amount},{tran_type},{state_cd}\n")

            start_id += 1
        file.close()


#create_data('2022-02-02', '2022-02-08')

print(pandas.read_parquet('C:\\Users\\cnast\\Desktop\\aws_learning-main\\hadoop\\Project\\config\\table.parquet',engine='auto'))


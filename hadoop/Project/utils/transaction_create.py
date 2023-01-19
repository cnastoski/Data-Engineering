import random

# TODO: add in function overloads for different input methods
def create_data(start_date, end_date):
    input_dir = "C:/Users/cnast/Desktop/aws_learning-main/hadoop/Project/config/"
    id_prefix = ['FX', 'TD']
    start_id = 100000
    country_codes = ['USA', 'CAN', 'IND', 'AFG', 'CHN', 'JPN', 'KON', 'PAL']
    tran_types = ['C', 'D']

    start_day = int(start_date[-2:])
    end_day = int(end_date[-2:])

    for days in range(start_day, end_day + 1):
        if days < 10:
            date = start_date[0:-1] + str(days)
        else:
            date = start_date[0:-2] + str(days)
        file = open(f"{input_dir}{date}.csv", 'w')
        file.write("tran_id,cust_id,tran_amount,tran_type,country_cd,tran_date\n")

        for i in range(round(random.uniform(1000, 10000))):
            tran_id = random.choice(id_prefix) + "_" + str(start_id)
            cust_id = 'cust_' + str(round(random.uniform(1000, 9999)))
            tran_amount = round(random.uniform(1.02, 9999999.02), 2)
            tran_type = random.choice(tran_types)
            country_code = random.choice(country_codes)
            tran_date = date

            file.write(f"{tran_id},{cust_id},{tran_amount},{tran_type},{country_code},{tran_date}\n")

            start_id += 1
        file.close()




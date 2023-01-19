from redshift.Projects.utils.db_driver import *

file_path = '/redshift/Questions/data_creation_scripts/order_data_20230401.csv'
table_name = 'manufacturing_info'
schema_name = 'cards_ingest'
starttime = timeit.default_timer()
print(f"Inserting table {table_name} into {schema_name}...")

loadFromCSV(file_path, table_name, schema_name)

endtime = timeit.default_timer()
print(f"Success: operation finished in : {(endtime - starttime):.3f} Seconds")
import random

product_id = ['iphone11', 'iphone12', 'iphone13', 'iphoneSE', 'IpadMax', 'IpadMini', 'laptop256', 'Macbook512',
              'galaxy10', 'galaxy11', 'galaxy12', 'galaxy13', 'watch320', 'watch340', 'Nk320', 'Nk400', 'Nk500']

manufacturing_price = ["{:.2f}".format(round(random.uniform(100.02, 300.09), 2)) for i in range(len(product_id))]


def createManufacturingPrice(file_name: str):
    input_file = open(file_name, 'w')
    header = "product_name, manufacturing_cost\n"
    input_file.write(header)
    for idx in range(len(product_id)):
        row = f"{product_id[idx]},{manufacturing_price[idx]}\n"
        input_file.write(row)

    input_file.close()
    return 1


def main():
    createManufacturingPrice("product_cost.csv")

    return 1


main()

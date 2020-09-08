import csv
import mysql.connector

DATA_CONSTANTS = {
    'sets': {
        'table_params': '(set_num VARCHAR(255), name VARCHAR(255), year VARCHAR(255), theme_id VARCHAR(255), num_parts '
                        'VARCHAR(255))',
        'params': '(set_num, name, year, theme_id, num_parts )',
        'values': '("%s", "%s", "%s", "%s", "%s")'
    },
    'colors': {
        'table_params': '(id VARCHAR(255), name VARCHAR(255), rgb VARCHAR(255), is_trans VARCHAR(255))',
        'params': '(id, name, rgb, is_trans)',
        'values': '("%s", "%s", "%s", "%s")'
    },
    'inventories': {
        'table_params': '(id VARCHAR(255), version VARCHAR(255), set_num VARCHAR(255))',
        'params': '(id, version, set_num)',
        'values': '("%s", "%s", "%s")'
    },
    'inventory_parts': {
        'table_params': '(inventory_id VARCHAR(255), part_num VARCHAR(255), color_id VARCHAR(255), '
                        'quantity VARCHAR(255), is_spare VARCHAR(255))',
        'params': '(inventory_id,part_num,color_id,quantity,is_spare)',
        'values': '("%s", "%s", "%s", "%s", "%s")',

    },
    'part_categories': {
        'table_params': '(id VARCHAR(255), name VARCHAR(255))',
        'params': '(id,name)',
        'values': '("%s", "%s")',
    },
    'parts': {
        'table_params': '(part_num VARCHAR(255), name VARCHAR(255), part_cat_id VARCHAR(255))',
        'params': '(part_num,name,part_cat_id)',
        'values': '("%s", "%s", "%s")',
    },
}


def connect_to_data_base(db_name):
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="admin",
        db=db_name
    )


def prepare_csv_file_data(my_db, csv_file_name):
    file_link = 'files/' + csv_file_name + '.csv'
    check_table_exist_statement = 'SHOW TABLES LIKE \'' + csv_file_name + '\''
    create_table_str = 'CREATE TABLE ' + csv_file_name + ' ' + DATA_CONSTANTS.get(csv_file_name).get('table_params')
    insert_file_str = 'INSERT INTO ' + csv_file_name + ' ' + DATA_CONSTANTS.get(csv_file_name).get(
        'params') + ' VALUES ' + DATA_CONSTANTS.get(csv_file_name).get('values')

    cursor = my_db.cursor()
    file_data = csv.reader(open(file_link))

    cursor.execute(check_table_exist_statement)
    result = cursor.fetchone()

    if result is None:
        cursor.execute(create_table_str)

    for set_row in file_data:
        cursor.execute(insert_file_str, set_row)

    my_db.commit()
    cursor.close()


def init_data():
    database_connection = connect_to_data_base('lego')

    prepare_csv_file_data(database_connection, 'sets')
    prepare_csv_file_data(database_connection, 'colors')
    prepare_csv_file_data(database_connection, 'inventories')
    prepare_csv_file_data(database_connection, 'inventory_parts')
    prepare_csv_file_data(database_connection, 'part_categories')
    prepare_csv_file_data(database_connection, 'parts')

init_data()

print("Done")

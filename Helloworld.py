import csv
import mysql.connector

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="admin",
    db="lego"
)
print(mydb)

cursor = mydb.cursor()

sets_data = csv.reader(open('files/sets.csv'))
# Sets import into lego database
statementForSettsTableExists = "SHOW TABLES LIKE 'sets'"
cursor.execute(statementForSettsTableExists)
result = cursor.fetchone()
if result:
    # there is a table named "sets"
    for set_row in sets_data:
        if set_row[0] == 'set_num':
            continue
        set_row[2] = int(set_row[2])
        set_row[3] = int(set_row[3])
        set_row[4] = int(set_row[4])
        cursor.execute(
            'INSERT INTO sets (set_num, name, year, theme_id, num_parts ) VALUES ("%s", "%s", "%s", "%s", "%s")',
            set_row)
else:
    # there are no tables named "sets"
    cursor.execute(
        'CREATE TABLE sets (set_num VARCHAR(255), name VARCHAR(255), year INT, theme_id INT,'
        'num_parts INT)')
mydb.commit()

colors_data = csv.reader(open('files/colors.csv'))
statementForSettsTableExists = "SHOW TABLES LIKE 'colors'"
cursor.execute(statementForSettsTableExists)
result = cursor.fetchone()
if result:
    for colors_row in colors_data:
        if colors_row[0] == 'id':
            continue
        colors_row[0] = int(colors_row[0])
        cursor.execute(
            'INSERT INTO colors (id, name, rgb, is_trans) VALUES ("%s", "%s", "%s", "%s")', colors_row)
else:
    cursor.execute(
        'CREATE TABLE colors (id INT, name VARCHAR(255), rgb VARCHAR(255), is_trans VARCHAR(255))')
mydb.commit()

inventories_data = csv.reader(open('files/inventories.csv'))
statementForSettsTableExists = "SHOW TABLES LIKE 'inventories'"
cursor.execute(statementForSettsTableExists)
result = cursor.fetchone()
if result:
    for inventories_row in inventories_data:
      if inventories_row[0] == 'id':
        continue
      inventories_row[0] = int(inventories_row[0])
      inventories_row[1] = int(inventories_row[1])
      cursor.execute(
            'INSERT INTO inventories (id, version, set_num) VALUES ("%s", "%s", "%s")',
            inventories_row)
else:
    cursor.execute(
        'CREATE TABLE inventories (id INT, version INT, set_num VARCHAR(255))')
mydb.commit()

inventory_parts = csv.reader(open('files/inventory_parts.csv'))
statementForSettsTableExists = "SHOW TABLES LIKE 'inventory_parts'"
cursor.execute(statementForSettsTableExists)
result = cursor.fetchone()
if result:
    for inventory_parts_row in inventory_parts:
      if inventory_parts_row[0] == 'inventory_id':
        continue
      inventory_parts_row[0] = int(inventory_parts_row[0])
      inventory_parts_row[2] = int(inventory_parts_row[2])
      inventory_parts_row[3] = int(inventory_parts_row[3])
      cursor.execute(
            'INSERT INTO inventory_parts (inventory_id,part_num,color_id,quantity,is_spare) '
            'VALUES ("%s", "%s", "%s", "%s", "%s")', inventory_parts_row)
else:
    cursor.execute(
        'CREATE TABLE inventory_parts (inventory_id INT, part_num VARCHAR(255), color_id INT, '
        'quantity INT, is_spare VARCHAR(255))')
mydb.commit()

part_categories = csv.reader(open('files/part_categories.csv'))
statementForSettsTableExists = "SHOW TABLES LIKE 'part_categories'"
cursor.execute(statementForSettsTableExists)
result = cursor.fetchone()
if result:
    for part_categories_row in part_categories:
      if part_categories_row[0] == 'id':
        continue
      part_categories_row[0] = int(part_categories_row[0])
      cursor.execute(
            'INSERT INTO part_categories (id,name) VALUES ("%s", "%s")', part_categories_row)
else:
    cursor.execute(
        'CREATE TABLE part_categories (id INT, name VARCHAR(255))')
mydb.commit()

parts = csv.reader(open('files/parts.csv', encoding="utf8"))
statementForSettsTableExists = "SHOW TABLES LIKE 'parts'"
cursor.execute(statementForSettsTableExists)
result = cursor.fetchone()
if result:
    for parts_row in parts:
      if parts_row[0] == 'part_num':
        continue
      parts_row[2] = int(parts_row[2])
      cursor.execute(
            'INSERT INTO parts (part_num,name,part_cat_id) VALUES ("%s", "%s", "%s")', parts_row)
else:
    cursor.execute(
        'CREATE TABLE parts (part_num VARCHAR(255), name VARCHAR(255), part_cat_id INT)')
mydb.commit()

themes = csv.reader(open('files/themes.csv'))
statementForSettsTableExists = "SHOW TABLES LIKE 'themes'"
cursor.execute(statementForSettsTableExists)
result = cursor.fetchone()
if result:
    for themes_row in themes:
      if themes_row[0] == 'id':
         continue
      themes_row[0] = int(themes_row[0])
      cursor.execute(
            'INSERT INTO themes (id,name,parent_id) VALUES ("%s", "%s", "%s")', themes_row)
else:
    cursor.execute(
        'CREATE TABLE themes (id INT, name VARCHAR(255), parent_id VARCHAR(255))')
mydb.commit()

cursor.close()
print("Done")

cursor.execute("alter table colors add PRIMARY KEY(id)")
cursor.execute("alter table part_categories add PRIMARY KEY(id)")
cursor.execute("alter table themes add PRIMARY KEY(id)")
vv
cursor.execute("alter table inventory_parts add foreign key (color_id) references colors(id)")
cursor.execute("alter table parts add foreign key (part_cat_id) references part_categories(id)")
cursor.execute("alter table sets add foreign key (theme_id) references themes(id)")

sql = "SELECT * FROM themes  LEFT JOIN sets ON themes.id = theme_id WHERE year LIKE '_2001_'"

cursor.execute(sql)

myresult = cursor.fetchall()

for x in myresult:
    print(x)

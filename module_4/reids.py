import json
import os
import sqlite3

# print(os.getcwd())
with open('reids.json', 'r') as f:
    data = json.load(f)
    

# open file
# with open('Module_4/reids.json', 'r') as f:
#     data = json.load(f)
    
# prints line for each order
# for line in data['orders']:
#     print(line)

# connect to db with sqlite
connection = sqlite3.connect('reids.db')

# cursor
crsr = connection.cursor()
print('Connected to the database')
print(connection.total_changes)

# delete table if exists
crsr.execute('DROP TABLE IF EXISTS items;')
crsr.execute('DROP TABLE IF EXISTS charges;')
crsr.execute('DROP TABLE IF EXISTS payments;')

# commands
crsr.execute('''CREATE TABLE items(
                name TEXT, 
                price REAL);''')
crsr.execute('''CREATE TABLE charges( 
                date TEXT, 
                total REAL,
                taxes REAL,
                subtotal REAL);''')
crsr.execute('''CREATE TABLE payments(
                method TEXT, 
                card_type TEXT,
                last_4_card_number INTEGER,
                zip INTEGER,
                cardholder TEXT
                );''')

# for each order of items/charges/payments
for order in data['orders']:
    
    items = order['items']
    
    for item in items:
        exists = crsr.execute('''SELECT count(*) FROM items WHERE name='coffee'
                                ''')
        crsr.execute('''INSERT INTO items (name, price) VALUES (?, ?)''',
                        (item['name'], item['price']))
        
        # this is the correct way
    # print(order['charges']['date'], '\n')
    charges = order['charges']
    crsr.execute('''INSERT INTO charges (date, subtotal, taxes, total)
                    VALUES (?, ?, ?, ?)''',
                    (charges['date'], charges['subtotal'], 
                        charges['taxes'], charges['total']))

    payments = order['payment']
    # print(payments, len(payments))
    if len(payments) > 1:
        crsr.execute('''INSERT INTO payments (method, card_type, last_4_card_number,
                        zip, cardholder) VALUES (?, ?, ?, ?, ?)''',
                    (payments['method'], payments['card_type'],
                    payments['last_4_card_number'], payments['zip'],
                    payments['cardholder']))
    else:
        crsr.execute('''INSERT INTO payments (method) VALUES (?)''',
                    (payments['method'],))

# delete duplicate values in each table
# items table
crsr.execute('''CREATE TABLE new_items AS SELECT DISTINCT name, price
                FROM items''')
crsr.execute('''DROP TABLE items''')
crsr.execute('''ALTER TABLE new_items RENAME TO items''')

# each charge is unique, so don't need to change that here

#payments table
crsr.execute('''CREATE TABLE new_payments AS
            SELECT DISTINCT method, card_type, last_4_card_number, zip, cardholder
            FROM payments''')
crsr.execute('''DROP TABLE payments''')
crsr.execute('''ALTER TABLE new_payments RENAME TO payments''')


connection.commit()
    


res = crsr.execute('SELECT name FROM sqlite_master')
print(res.fetchall()) #yes this works

print(connection.total_changes)
print('Database is ready')

# close connection
connection.close()

import json
import os
import sqlite3

# open file
with open('reids.json', 'r') as f:
    data = json.load(f)
    
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
crsr.execute('DROP TABLE IF EXISTS orders;')

# commands
crsr.execute('''CREATE TABLE items(
                name TEXT, 
                price REAL,
                charge_id INTEGER FOREIGN KEY);''')
crsr.execute('''CREATE TABLE charges( 
                id INTEGER PRIMARY KEY,
                date DATE,
                time TEXT, 
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
        

    charges = order['charges']

    # format date into correct order
    crsr.execute('''INSERT INTO charges (date, time, subtotal, taxes, total)
                    VALUES (?, ?, ?, ?, ?)''',
('20'+ charges['date'][6:8] + '-'+charges['date'][0:2]+'-'+charges['date'][3:5],
charges['date'][9:], charges['subtotal'],
charges['taxes'], charges['total']))

    payments = order['payment']

    # determine whether cash or credit
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
crsr.execute('''CREATE TABLE single_items AS SELECT DISTINCT name, price
                FROM items''')


# each charge is unique, so don't need to change that here

#payments table
crsr.execute('''CREATE TABLE single_payments AS
            SELECT DISTINCT method, card_type, last_4_card_number, zip, cardholder
            FROM payments''')


# create primary and foreign keys after table creations
crsr.execute('''CREATE TABLE new_items(
                id INTEGER PRIMARY KEY,
                name TEXT,
                price REAL
)''')
crsr.execute('''INSERT INTO new_items (name, price)
                SELECT name, price FROM single_items''')
crsr.execute('''DROP TABLE items''')
crsr.execute('''ALTER TABLE new_items RENAME TO items''')
crsr.execute('''DROP TABLE single_items''')

# payments
crsr.execute('''CREATE TABLE new_payments(
                id INTEGER PRIMARY KEY,
                method TEXT, 
                card_type TEXT,
                last_4_card_number INTEGER,
                zip INTEGER,
                cardholder TEXT
)''')
crsr.execute('''INSERT INTO 
                new_payments (method, card_type, last_4_card_number, zip, cardholder)
                SELECT method, card_type, last_4_card_number, zip, cardholder
                FROM single_payments
''')
crsr.execute('''DROP TABLE payments''')
crsr.execute('''ALTER TABLE new_payments RENAME TO payments''')
crsr.execute('''DROP TABLE single_payments''')


# foreign key giving error?
crsr.execute('''CREATE TABLE orders(
                id INTEGER PRIMARY KEY,
                charges_id INTEGER,
                items_id INTEGER,
                payments_id INTEGER,
                FOREIGN KEY(charges_id) REFERENCES charges(id),
                FOREIGN KEY(items_id) REFERENCES items(id),
                FOREIGN KEY(payments_id) REFERENCES payments(id)
)''')


# doesn't work properly-trying to link orders and charges with item ids
for order in data['orders']:
    for item in order['items']:
        crsr.execute('''INSERT INTO orders (items_id)
                        SELECT id FROM items WHERE name = (?)''',
                        (item['name'],))

    fixed_date = '20'+ order['charges']['date'][6:8] + '-' \
                +order['charges']['date'][0:2]+'-'+order['charges']['date'][3:5]
    time = order['charges']['date'][9:]
    
    crsr.execute('''INSERT INTO orders (charges_id)
                    SELECT id FROM charges WHERE date = (?)
                    AND time = (?)''', (fixed_date, time))
    
    payment = order['payment']
    if len(payment) > 1:
        
        crsr.execute('''INSERT INTO orders (payments_id)
                        SELECT id FROM payments WHERE cardholder = (?)''',
                        (payment['cardholder'],))
    

connection.commit()

res = crsr.execute('SELECT name FROM sqlite_master')
print(res.fetchall()) #yes this works

print(connection.total_changes)
print('Database is ready')

# close connection
connection.close()

# write to file
with open('reids.sql', 'w') as f:
    for line in data:
        f.write(line)
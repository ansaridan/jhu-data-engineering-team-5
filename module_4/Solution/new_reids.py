import json
import os
import sqlite3
from datetime import datetime, timedelta

# open file
with open('reids.json', 'r') as f:
    data = json.load(f)
    
# connect to db with sqlite
connection = sqlite3.connect('reids.db')

# cursor
crsr = connection.cursor()
print('Connected to the database')
print(connection.total_changes)

# # delete table if exists
crsr.execute('DROP TABLE IF EXISTS items;')
crsr.execute('DROP TABLE IF EXISTS charges;')
crsr.execute('DROP TABLE IF EXISTS payments;')
crsr.execute('DROP TABLE IF EXISTS orders;')

# commands
crsr.execute('''CREATE TABLE charges(
                id INTEGER PRIMARY KEY,
                date TEXT,
                subtotal REAL,
                total REAL,
                taxes REAL,
                payment_id INTEGER,
                FOREIGN KEY(payment_id) REFERENCES payments(id)

);''')

crsr.execute('''CREATE TABLE items(
                id INTEGER PRIMARY KEY,
                name TEXT, 
                price REAL,
                charge_id INTEGER,
                FOREIGN KEY(charge_id) REFERENCES charges(id)
);''')

# start from payments, go to charges, from charges go to items
crsr.execute('''CREATE TABLE payments(
                id INTEGER PRIMARY KEY,
                method TEXT, 
                card_type TEXT,
                last_4_card_number INTEGER,
                zip INTEGER,
                cardholder TEXT
                );''')

# for IDS
# crsr.execute('''INSERT INTO orders (items_id)
#                         SELECT id FROM items WHERE name = (?)''',
#                         (item['name'],))



names_list = []
count = 1
for order in data['orders']:
    payments = order['payment']
    # names_dict[name] = 1
    if len(payments) > 1:
        name = payments['cardholder']
        
        if name not in names_list:
            # print(names_list)
            crsr.execute('''INSERT INTO payments (method, card_type, last_4_card_number,
                            zip, cardholder) VALUES (?, ?, ?, ?, ?)''',
                        (payments['method'], payments['card_type'],
                        payments['last_4_card_number'], payments['zip'],
                        payments['cardholder']))
            names_list.append(name)
    else:
        
        crsr.execute('''INSERT INTO payments (method) VALUES (?)''',
                    (payments['method'],))
        
    # # print(order['charges'])
    if len(payments) > 1:
        name = payments['cardholder']
        # crsr.execute('''INSERT INTO charges (date, subtotal, total, taxes)
        #                 VALUES (?,?,?,?)''',
        #                 (order['charges']['date'], order['charges']['subtotal'],
        #                 order['charges']['total'], order['charges']['taxes']))
        # crsr.execute('''INSERT INTO charges (payment_id) VALUES (?)''',
        #              ('SELECT'))
    
    # SEEMS TO WORK< START HERE
    count += 1
# def extract_properties(properties, data, defaults=dict()):
#     result = []
#     for prop in properties:
#         if prop in data:
#             result.append(data[prop])
#         else:
#             result.append(defaults.get(prop, None)) # we do need things to be Nullable
#     return tuple(result)

# def process_payment(payment_data, payments, current_id):
#     key = extract_properties(["cardholder", "last_4_card_number"], payment_data)
#     if key in payments:
#         return current_id, payments[key], None
#     payments[key] = current_id
#     payment_id = payments[key]
#     current_id +=1
#     return current_id, payment_id, extract_properties(["id", "method", "card_type", "cardholder", "last_4_card_number", "zip"], payment_data, {"id": payment_id})

# def process_charge(charge, current_id, payment_id):
#     result = extract_properties(["id", "date", "subtotal", "taxes", "total", "payment_id"], charge, {"id": current_id, "payment_id": payment_id})
#     current_id += 1
#     return current_id, result

# def process_items(items, current_id, charge_id):
#     results = []
#     for item in items:
#         result = extract_properties(["id", "charge_id", "name", "price"], item, {"id": current_id, "charge_id": charge_id})
#         results.append(result)
#         current_id += 1
#     return current_id, results

# payment_id = 1
# payment_lookup = {}
# payments = []
# charge_id = 1
# charges = []
# item_id = 1
# items = []

# for order in data['orders']:
#     payment_id, this_payment_id, payment = process_payment(order["payment"], payment_lookup, payment_id)
#     if payment: # could be None
#         payments.append(payment)
#     charge_id, charge = process_charge(order["charges"], charge_id, this_payment_id)
#     charges.append(charge)
#     item_id, these_items = process_items(order["items"], item_id, charge_id)
#     items += these_items # list concatenation


# connection.executemany('INSERT INTO payments (id, method, card_type, cardholder, last_4_card_number, zip) VALUES (?, ?, ?, ?, ?, ?)', payments)
# connection.commit()


# connection.executemany('INSERT INTO charges (id, date, subtotal, taxes, total, payment_id) VALUES (?, ?, ?, ?, ?, ?)', charges)
# connection.commit()

# connection.executemany('INSERT INTO items (id, charge_id, name, price) VALUES (?, ?, ?, ?)', items)
# connection.commit()

# days_of_the_week = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
# calendar = []

# today = datetime(2021, 3, 31, 0, 0)
# for _ in range(0, 61):
#     today = today + timedelta(days=1)
#     date = today.strftime("%m/%d/%y")
#     day = days_of_the_week[today.weekday()]
#     calendar.append((date, day))

# connection.executemany('INSERT INTO calendar (date, day_of_week) VALUES (?, ?)', calendar)
# connection.commit()


# end
connection.commit()
print(connection.total_changes)
print('Database is ready')

# close connection
connection.close()

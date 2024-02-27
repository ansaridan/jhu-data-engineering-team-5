import xlrd
import json
import os
import sqlite3


book = xlrd.open_workbook('./Top_100_Contractors_Report_Fiscal_Year_2015.xls')
depts = book.sheet_names()[0:22]

def process_global_vendor_names(depts):

    names = dict()
    vendor_id = 1

    for i, dept_name in enumerate(depts):

        sheet_name = book.sheet_by_index(i)

        for index in range(1, sheet_name.nrows):

            if sheet_name.row(index)[0].value not in names:
                names[sheet_name.row(index)[0].value] = vendor_id
                vendor_id += 1

    return names


def process_department_actions(depts, vendor_names):
    action_list = []
    for i, dept_name in enumerate(depts):

        sheet_name = book.sheet_by_index(i)

        for index in range(1, sheet_name.nrows):
            dept = dept_name
            vendor = sheet_name.row(index)[0].value
            action_nums = int(sheet_name.row(index)[1].value)
            dollars = sheet_name.row(index)[2].value
            vendor_id = vendor_names[vendor]

            actions_of_vendors = [dept, action_nums, dollars, vendor_id]
            action_list.append(actions_of_vendors)

    return action_list


# get names to populate contractors table
vendor_names_dict = process_global_vendor_names(depts)

# get actions to populate actions table
actions = process_department_actions(depts, vendor_names_dict)

connection = sqlite3.connect('./contracts.db')

# cursor
crsr = connection.cursor()
print('Connected to the database')
print(connection.total_changes)

# inserting into contractors table
crsr.executemany('''INSERT INTO contractors (global_vendor_name)
                    VALUES (?)''', ((n,) for n in vendor_names_dict.keys()))

for action in actions:
    crsr.execute('''INSERT INTO actions (department, actions, dollars, contractor_id)
                        VALUES (?, ?, ?, ?)''', action)

connection.commit()

print(connection.total_changes)
print('Database is ready')

# close connection
connection.close()


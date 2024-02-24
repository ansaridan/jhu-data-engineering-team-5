import xlrd
import json
import os
import sqlite3


book = xlrd.open_workbook('./Top_100_Contractors_Report_Fiscal_Year_2015.xls')
# book = xlrd.open_workbook('C:/Users/Jhirs/desktop/data_science_sp24/problem_set_2/Top_100_Contractors_Report_Fiscal_Year_2015.xls')
# print(book.sheet_names())
federal = book.sheet_by_index(0)
# print('{0} {1} {2}'.format(sh.name, sh.nrows, sh.ncols))
# print('cell d30 is {0}'.format(sh.cell_value(rowx=29, colx=3)))

global_vendor_names = []
for rx in range(federal.nrows):
    # print(type(federal.row(rx)[0].value))
    global_vendor_names.append(federal.row(rx)[0].value)
global_vendor_names.pop(0)



connection = sqlite3.connect('./contracts.db')
# connection = sqlite3.connect('C:/Users/Jhirs/desktop/data_science_sp24/problem_set_2/contracts.db')


# cursor
crsr = connection.cursor()
print('Connected to the database')
print(connection.total_changes)

# inserting into contractors table
crsr.executemany('''INSERT INTO contractors (global_vendor_name)
                    VALUES (?)''', [(n,) for n in global_vendor_names])




connection.commit()


print(connection.total_changes)
print('Database is ready')

# close connection
connection.close()


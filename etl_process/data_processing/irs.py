# import statements
import pandas as pd
from warnings import simplefilter
import psycopg2
import psycopg2.extras

simplefilter(action="ignore", category=pd.errors.PerformanceWarning)

# read in csv from data file in same directory
irs_data = pd.read_csv("./nyc_irs.csv", header=0)


# PSQL db connection using psycopg2
conn = psycopg2.connect(
    dbname="new_db",
    user="awesome_user",
    password="awesome_password",
    host="postgres",
    port="5432",
)
c = conn.cursor()


# Create new schema in new_db database
c.execute("CREATE SCHEMA IF NOT EXISTS irs_data")
conn.commit()

# convert df to dict for easier insertion into PSQL
data_dict = irs_data.to_dict(orient="records")

# create list of dicts for irs codes - this will be a single table in PSQL
codes = [
    {"Code": "STATEFIPS", "Description": "State Federal Information Processing System"},
    {"Code": "STATE", "Description": "State associated with zip code"},
    {"Code": "ZIPCODE", "Description": "5-digit zip code"},
    {"Code": "AGI_STUB", "Description": "Size of adjusted gross income"},
    {"Code": "N1", "Description": "Number of returns"},
    {"Code": "MARS1", "Description": "Number of single returns"},
    {"Code": "MARS2", "Description": "Number of joint returns"},
    {"Code": "MARS4", "Description": "Number of head of household returns"},
    {"Code": "ELF", "Description": "Number of electronically filed returns"},
    {"Code": "CPREP", "Description": "Number of computer prepared paper returns"},
    {"Code": "PREP", "Description": "Number of returns with paid preparer's signature"},
    {"Code": "DIR_DEP", "Description": "Number of returns with direct deposit"},
    {
        "Code": "VRTCRIND",
        "Description": "Number of returns with virtual currency indicator",
    },
    {"Code": "N2", "Description": "Number of individuals"},
    {"Code": "TOTAL_VITD", "Description": "Total number of volunteer prepared returns"},
    {
        "Code": "VITA",
        "Description": "Number of volunteer income tax assistance prepared returns",
    },
    {
        "Code": "TCE",
        "Description": "Number of tax counseling for the elderly prepared returns",
    },
    {
        "Code": "VITA_EIC",
        "Description": "Number of volunteer prepared returns with Earned Income Credit",
    },
    {"Code": "RAC", "Description": "Number of refund anticipation check returns"},
    {"Code": "ELDERLY", "Description": "Number of elderly returns"},
    {"Code": "A00100", "Description": "Adjust gross income (AGI)"},
    {"Code": "N02650", "Description": "Number of returns with total income"},
    {"Code": "A02650", "Description": "Total income amount"},
    {"Code": "N00200", "Description": "Number of returns with salaries and wages"},
    {"Code": "A00200", "Description": "Salaries and wages amount"},
    {"Code": "N00300", "Description": "Number of returns with taxable interest"},
    {"Code": "A00300", "Description": "Taxable interest amount"},
    {"Code": "N00600", "Description": "Number of returns with ordinary dividends"},
    {"Code": "A00600", "Description": "Ordinary dividends amount"},
    {"Code": "N00650", "Description": "Number of returns with qualified dividends"},
    {"Code": "A00650", "Description": "Qualified dividends amount"},
    {
        "Code": "N00700",
        "Description": "Number of returns with state and local income tax refunds",
    },
    {"Code": "A00700", "Description": "State and local income tax refunds amount"},
    {
        "Code": "N00900",
        "Description": "Number of returns with business or professional net income (less loss)",
    },
    {
        "Code": "A00900",
        "Description": "Business or professional net income (less loss) amount",
    },
    {
        "Code": "N01000",
        "Description": "Number of returns with net capital gain (less loss)",
    },
    {"Code": "A01000", "Description": "Net capital gain (less loss) amount"},
    {
        "Code": "N01400",
        "Description": "Number of returns with taxable individual retirement arrangements distributions",
    },
    {
        "Code": "A01400",
        "Description": "Taxable individual retirement arrangements distributions amount",
    },
    {
        "Code": "N01700",
        "Description": "Number of returns with taxable pensions and annuities",
    },
    {"Code": "A01700", "Description": "Taxable pensions and annuities amount"},
    {"Code": "SCHF", "Description": "Number of farm returns"},
    {
        "Code": "N02300",
        "Description": "Number of returns with unemployment compensation",
    },
    {"Code": "A02300", "Description": "Unemployment compensation amount"},
    {
        "Code": "N02500",
        "Description": "Number of returns with taxable Social Security benefits",
    },
    {"Code": "A02500", "Description": "Taxable Social Security benefits amount"},
    {
        "Code": "N26270",
        "Description": "Number of returns with partnership/S-corp net income (less loss)",
    },
    {
        "Code": "A26270",
        "Description": "Partnership/S-corp ne income (less loss) amount",
    },
    {
        "Code": "N02900",
        "Description": "Number of returns with total statutory adjustments",
    },
    {"Code": "A02900", "Description": "Total statutory adjustments amount"},
    {"Code": "N03220", "Description": "Number of returns with educator expenses"},
    {"Code": "A03220", "Description": "Educator expenses amount"},
    {
        "Code": "N03330",
        "Description": "Number of returns with Self-employed (Keogh) retirement plans",
    },
    {"Code": "A03300", "Description": "Self-employed (Keogh) retirement plans amount"},
    {
        "Code": "N03270",
        "Description": "Number of returns with Self-employed health insurance deduction amount",
    },
    {
        "Code": "A03270",
        "Description": "Self-employed health insurance deduction amount",
    },
    {
        "Code": "N03150",
        "Description": "Number of returns with individual retirement arrangement payments",
    },
    {
        "Code": "A03150",
        "Description": "Individual retirement arrangement payments amount",
    },
    {
        "Code": "N03210",
        "Description": "Number of returns with student loan interest deduction",
    },
    {"Code": "A03210", "Description": "Student loan interest deduction amount"},
    {
        "Code": "N02910",
        "Description": "Number of returns with charitable contributions if took standard deduction",
    },
    {
        "Code": "A02910",
        "Description": "Charitable contributions if took standard deduction",
    },
    {
        "Code": "N04450",
        "Description": "Number of returns with total standard deduction",
    },
    {"Code": "A04450", "Description": "Total standard deduction amount"},
    {
        "Code": "N04100",
        "Description": "Number of returns with basic standard deduction",
    },
    {"Code": "A04100", "Description": "Basic standard deduction amount"},
    {
        "Code": "N04200",
        "Description": "Number of returns with additional standard deduction",
    },
    {"Code": "A04200", "Description": "Additional standard deduction amount"},
    {"Code": "N04470", "Description": "Number of returns with itemized deductions"},
    {"Code": "A04470", "Description": "Total itemized deductions amount"},
    {"Code": "A00101", "Description": "Amount of AGI for itemized returns"},
    {
        "Code": "N17000",
        "Description": "Number of returns with total medical and dental expense deduction",
    },
    {
        "Code": "A17000",
        "Description": "Total medical and dental expense deduction amount",
    },
    {
        "Code": "N18425",
        "Description": "Number of returns with state and local income taxes",
    },
    {"Code": "A18425", "Description": "State and local income taxes amount"},
    {
        "Code": "N18450",
        "Description": "Number of returns with state and local general sales tax",
    },
    {"Code": "A18450", "Description": "State and local general sales tax amount"},
    {"Code": "N18500", "Description": "Number of returns with real estate taxes"},
    {"Code": "A18500", "Description": "Real estate taxes amount"},
    {"Code": "N18800", "Description": "Number of returns with personal property taxes"},
    {"Code": "A18800", "Description": "Personal property taxes amount"},
    {
        "Code": "N18460",
        "Description": "Number of returns with limited state and local taxes",
    },
    {"Code": "A18460", "Description": "Limited and tate local taxes"},
    {"Code": "N18300", "Description": "Number of returns with total taxes paid"},
    {"Code": "A18300", "Description": "Total taxes paid amount"},
    {
        "Code": "N19300",
        "Description": "Number of returns with home mortgage interest paid",
    },
    {"Code": "A19300", "Description": "Home mortgage interest paid amount"},
    {
        "Code": "N19500",
        "Description": "Number of returns with home mortgage from personal seller",
    },
    {"Code": "A19500", "Description": "Home mortgage from personal seller amount"},
    {"Code": "N19530", "Description": "Number of returns with deductible points"},
    {"Code": "A19530", "Description": "Deductible points amount"},
    {
        "Code": "N19550",
        "Description": "Number of returns with qualified mortgage insurance premiums",
    },
    {"Code": "A19550", "Description": "Qualified mortgage insurance premiums amount"},
    {
        "Code": "N19570",
        "Description": "Number of returns with investment interest paid",
    },
    {"Code": "A19570", "Description": "Investment interest paid amount"},
    {
        "Code": "N19700",
        "Description": "Number of returns with total charitable contributions",
    },
    {"Code": "A19700", "Description": "Total charitable contributions amount"},
    {
        "Code": "N20950",
        "Description": "Number of returns with other non-limited miscellanous deductions",
    },
    {
        "Code": "A20950",
        "Description": "Other non-limited miscellaneous deductions amount",
    },
    {
        "Code": "N04475",
        "Description": "Number of returns with qualified business income deduction",
    },
    {"Code": "A04475", "Description": "Qualified business income deduction"},
    {"Code": "N04800", "Description": "Number of returns with taxable income"},
    {"Code": "A04800", "Description": "Taxable income amount"},
    {
        "Code": "N05800",
        "Description": "Number of returns with income tax before credits",
    },
    {"Code": "A05800", "Description": "Income tax before credits amount"},
    {"Code": "N09600", "Description": "Number of returns with alternative minimum tax"},
    {"Code": "A09600", "Description": "Alternative minimum tax amount"},
    {
        "Code": "N05780",
        "Description": "Number of returns with excess advance premium tax credit repayment",
    },
    {
        "Code": "A05780",
        "Description": "Excess advance premium tax credit repayment amount",
    },
    {"Code": "N07100", "Description": "Number of returns with total tax credits"},
    {"Code": "A07100", "Description": "Total tax credits amount"},
    {"Code": "N07300", "Description": "Number of returns with foreign tax credit"},
    {"Code": "A07300", "Description": "Foreign tax credit amount"},
    {
        "Code": "N07180",
        "Description": "Number of returns with child and dependent care credit",
    },
    {"Code": "A07180", "Description": "Child and dependent care credit amount"},
    {
        "Code": "N07230",
        "Description": "Number of returns with nonrefundable education credit",
    },
    {"Code": "A07230", "Description": "Nonrefundable education credit amount"},
    {
        "Code": "N07240",
        "Description": "Number of returns with retirement savings contribution credit",
    },
    {"Code": "A07240", "Description": "Retirement savings contribution credit amount"},
    {
        "Code": "N07225",
        "Description": "Number of returns with nonrefundable child and other dependent credit",
    },
    {
        "Code": "A07225",
        "Description": "Nonrefundable child and other dependent credit amount",
    },
    {
        "Code": "N07260",
        "Description": "Number of returns with residental energy tax credit",
    },
    {"Code": "A07260", "Description": "Residential energy tax credit amount"},
    {"Code": "N09400", "Description": "Number of returns with self-employment tax"},
    {"Code": "A09400", "Description": "Self-employment tax amount"},
    {
        "Code": "N85770",
        "Description": "Number of returns with total premium tax credit",
    },
    {"Code": "A85770", "Description": "Total premium tax credit amount"},
    {
        "Code": "N85775",
        "Description": "Number of returns with advance premium tax credit",
    },
    {"Code": "A85775", "Description": "Advance premium tax credit amount"},
    {"Code": "N10600", "Description": "Number of returns with total tax payments"},
    {"Code": "A10600", "Description": "Total tax payments amount"},
    {"Code": "N59660", "Description": "Number of returns with earned income credit"},
    {"Code": "A59660", "Description": "Earned income credit amount"},
    {
        "Code": "N59720",
        "Description": "Number of returns with excess earned income credit",
    },
    {
        "Code": "A59720",
        "Description": "Excess earned income credit (refundable) amount",
    },
    {
        "Code": "N11070",
        "Description": "Number of returns with refundable child tax credit or additional child tax credit",
    },
    {
        "Code": "A11070",
        "Description": "Refundable child tax credit or additional child tax credit amount",
    },
    {
        "Code": "N10960",
        "Description": "Number of returns with refundable education credit",
    },
    {"Code": "A10960", "Description": "Refundable education credit amount"},
    {"Code": "N11560", "Description": "Number of returns with net premium tax credit"},
    {"Code": "A11560", "Description": "Net premium tax credit amount"},
    {
        "Code": "N11450",
        "Description": "Number of returns with qualified sick and family leave credit for leave taken before April 1, 2021",
    },
    {
        "Code": "A11450",
        "Description": "Qualified sick and family leave credit for leave taken before April 1, 2021",
    },
    {
        "Code": "N11520",
        "Description": "Number of returns with refundable child and dependent care credit",
    },
    {"Code": "A11520", "Description": "Refundable child and dependent care credit"},
    {
        "Code": "N11530",
        "Description": "Number of returns with qualified sicka dn family leave credit for leave taken afte March 31, 2021",
    },
    {
        "Code": "A11530",
        "Description": "Qualified sick and family leave credit for leave taken after March 31, 2021",
    },
    {"Code": "N10970", "Description": "Number of returns with recivery rebate credit"},
    {"Code": "A10970", "Description": "Recovery rebate credit amount"},
    {
        "Code": "N10971",
        "Description": "Number of returns with economic impact payment third round",
    },
    {"Code": "A10971", "Description": "Economic impact payment third round amount"},
    {
        "Code": "N06500",
        "Description": "Number of returns with income tax after credits",
    },
    {"Code": "A06500", "Description": "Income tax after credits amount"},
    {"Code": "N10300", "Description": "Number of returns with tax liability"},
    {"Code": "A10300", "Description": "Total tax liability amount"},
    {"Code": "N85530", "Description": "Number of returns with additional Medicare tax"},
    {"Code": "A85530", "Description": "Additional Medicare tax amount"},
    {
        "Code": "N85300",
        "Description": "Number of returns with net investment income tax",
    },
    {"Code": "A85300", "Description": "Net investment income tax amount"},
    {
        "Code": "N11901",
        "Description": "Number of returns with tax due at time of filing",
    },
    {"Code": "A11901", "Description": "Tax due at time of filing amount"},
    {"Code": "N11900", "Description": "Number of returns with total overpayments"},
    {"Code": "A11900", "Description": "Total overpayments amount"},
    {"Code": "N11902", "Description": "Number of returns with overpayments refunded"},
    {"Code": "A11902", "Description": "Overpayments refunded amount"},
    {
        "Code": "N12000",
        "Description": "Number of returns with credit to next year's estimated tax",
    },
    {"Code": "A12000", "Description": "Credited to next year's estimated tax amount"},
    {"Code": "Borough", "Description": "Borough in NYC"},
    {"Code": "Neighborhood", "Description": "Neighborhood"},
]

# create table with appropriate columns
c.execute("DROP TABLE IF EXISTS irs_data.ny_irs_data")

c.execute(
    """CREATE TABLE IF NOT EXISTS irs_data.ny_irs_data 
            (id SERIAL PRIMARY KEY, STATEFIPS VARCHAR(50), STATE TEXT, zipcode TEXT, agi_stub NUMERIC, N1 NUMERIC, mars1 NUMERIC, MARS2 NUMERIC, MARS4 NUMERIC,              ELF NUMERIC, CPREP NUMERIC, PREP NUMERIC, DIR_DEP NUMERIC, VRTCRIND NUMERIC, N2 NUMERIC, TOTAL_VITA NUMERIC, VITA NUMERIC, TCE NUMERIC, 
            VITA_EIC NUMERIC, RAC NUMERIC, ELDERLY NUMERIC, A00100 NUMERIC, N02650 NUMERIC, A02650 NUMERIC, N00200 NUMERIC, A00200 NUMERIC, N00300 NUMERIC,                  A00300 NUMERIC, N00600 NUMERIC, A00600 NUMERIC, N00650 NUMERIC, A00650 NUMERIC, N00700 NUMERIC, A00700 NUMERIC, N00900 NUMERIC, A00900 NUMERIC,                  N01000 NUMERIC, A01000 NUMERIC, N01400 NUMERIC, A01400 NUMERIC, N01700 NUMERIC, A01700 NUMERIC, SCHF NUMERIC, N02300 NUMERIC, A02300 NUMERIC, 
            N02500 NUMERIC, A02500 NUMERIC, N26270 NUMERIC, A26270 NUMERIC, N02900 NUMERIC, A02900 NUMERIC, N03220 NUMERIC, A03220 NUMERIC, N03300 NUMERIC,                  A03300 NUMERIC, N03270 NUMERIC, A03270 NUMERIC, N03150 NUMERIC, A03150 NUMERIC, N03210 NUMERIC, A03210 NUMERIC, N02910 NUMERIC, A02910 NUMERIC,                  N04450 NUMERIC, A04450 NUMERIC, N04100 NUMERIC, A04100 NUMERIC, N04200 NUMERIC, A04200 NUMERIC, N04470 NUMERIC, A04470 NUMERIC, A00101 NUMERIC,                  N17000 NUMERIC, A17000 NUMERIC, N18425 NUMERIC, A18425 NUMERIC, N18450 NUMERIC, A18450 NUMERIC, N18500 NUMERIC, A18500 NUMERIC, N18800 NUMERIC,                  A18800 NUMERIC, N18460 NUMERIC, A18460 NUMERIC, N18300 NUMERIC, A18300 NUMERIC, N19300 NUMERIC, A19300 NUMERIC, N19500 NUMERIC, A19500 NUMERIC,                  N19530 NUMERIC, A19530 NUMERIC, N19550 NUMERIC, A19550 NUMERIC, N19570 NUMERIC, A19570 NUMERIC, N19700 NUMERIC, A19700 NUMERIC, N20950 NUMERIC,                  A20950 NUMERIC, N04475 NUMERIC, A04475 NUMERIC, N04800 NUMERIC, A04800 NUMERIC, N05800 NUMERIC, A05800 NUMERIC, N09600 NUMERIC, A09600 NUMERIC,                  N05780 NUMERIC, A05780 NUMERIC, N07100 NUMERIC, A07100 NUMERIC, N07300 NUMERIC, A07300 NUMERIC, N07180 NUMERIC, A07180 NUMERIC, N07230 NUMERIC,                  A07230 NUMERIC, N07240 NUMERIC, A07240 NUMERIC, N07225 NUMERIC, A07225 NUMERIC, N07260 NUMERIC, A07260 NUMERIC, N09400 NUMERIC, A09400 NUMERIC,                  N85770 NUMERIC, A85770 NUMERIC, N85775 NUMERIC, A85775 NUMERIC, N10600 NUMERIC, A10600 NUMERIC, N59660 NUMERIC, A59660 NUMERIC, N59720 NUMERIC,                  A59720 NUMERIC, N11070 NUMERIC, A11070 NUMERIC, N10960 NUMERIC, A10960 NUMERIC, N11560 NUMERIC, A11560 NUMERIC, N11450 NUMERIC, A11450 NUMERIC,                  N11520 NUMERIC, A11520 NUMERIC, N11530 NUMERIC, A11530 NUMERIC, N10970 NUMERIC, A10970 NUMERIC, N10971 NUMERIC, A10971 NUMERIC, N06500 NUMERIC,                  A06500 NUMERIC, N10300 NUMERIC, A10300 NUMERIC, N85530 NUMERIC, A85530 NUMERIC, N85300 NUMERIC, A85300 NUMERIC, N11901 NUMERIC, A11901 NUMERIC,                  N11900 NUMERIC, A11900 NUMERIC, N11902 NUMERIC, A11902 NUMERIC, N12000 NUMERIC, A12000 NUMERIC, Borough TEXT, Neighborhood TEXT)"""
)

# insert data into ny_irs_data table in irs_data schema
columns = data_dict[0].keys()

query = "INSERT INTO irs_data.ny_irs_data ({}) VALUES %s".format(",".join(columns))

values = [[value for value in data.values()] for data in data_dict]

psycopg2.extras.execute_values(c, query, values)
conn.commit()

# create table with appropriate columns
c.execute("DROP TABLE IF EXISTS irs_data.irs_codes")

c.execute(
    """CREATE TABLE IF NOT EXISTS irs_data.irs_codes
            (id SERIAL PRIMARY KEY, Code VARCHAR(250), Description TEXT
)"""
)

# insert data into irs_codes table
columns = codes[0].keys()

query = "INSERT INTO irs_data.irs_codes ({}) VALUES %s".format(",".join(columns))

values = [[value for value in code.values()] for code in codes]

psycopg2.extras.execute_values(c, query, values)
conn.commit()

# close connection
conn.close()

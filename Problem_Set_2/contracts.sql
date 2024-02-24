DROP TABLE IF EXISTS contractors;
CREATE TABLE contractors(
    id INTEGER PRIMARY KEY,
    global_vendor_name VARCHAR
);


DROP TABLE IF EXISTS actions;
CREATE TABLE actions(
    id INTEGER PRIMARY KEY,
    department TEXT,
    actions INTEGER,
    dollars NUMERIC,
    contractor_id INTEGER,
    FOREIGN KEY (contractor_id) REFERENCES contractors(id)

);

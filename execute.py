import sys
import json
import xmltodict
import sqlite3
import os


conn = sqlite3.connect('smc.db')
cursor = conn.cursor()


def execute(file_path, file_name):
    if "Sales Detail Report" in file_name:
        sales(file_path)
    elif "Strength Wise Stock Detail" in file_name:
        stock(file_path)

def sales(file_path):
    with open(file_path, 'r') as xml_file:
        xml_content = xml_file.read()

        xml_data = xmltodict.parse(xml_content)

        json_data = json.dumps(xml_data, indent=4)

        with open('data/sales.json', 'w') as json_file:
            json_file.write(json_data)
        


def stock(file_path):
    with open(file_path, 'r') as xml_file:
        xml_content = xml_file.read()
        xml_data = xmltodict.parse(xml_content)
        json_data = json.dumps(xml_data, indent=4)
        json_data = json.loads(json_data)
        
        table_schema = '''
        CREATE TABLE IF NOT EXISTS stock (
            ID TEXT PRIMARY KEY,
            Barcode INTEGER,
            Brand TEXT,
            Category TEXT,
            FKLotID INTEGER,
            Size TEXT,
            LotMRP REAL,
            ManufacturingCo TEXT,
            MarketingCo TEXT,
            NameToDisplay TEXT,
            Product TEXT,
            PurchaseRate REAL,
            RefDate TEXT,
            RefNo TEXT,
            SellLoose TEXT,
            Stock REAL,
            StockValue REAL
        );
        '''
        
        cursor.execute(table_schema)
        
        insert_query = '''
        INSERT OR REPLACE INTO stock (ID, Barcode, Brand, Category, FKLotID, Size, LotMRP, ManufacturingCo, MarketingCo,
                                        NameToDisplay, Product, PurchaseRate, RefDate, RefNo, SellLoose,
                                        Stock, StockValue)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        '''
        
        for product_data in json_data.get('NewDataSet', {}).get('Table', []):
            cursor.execute(insert_query, (
                f"{product_data.get('Barcode')}-{product_data.get('FKLotID')}",
                product_data.get("Barcode"),
                product_data.get("Brand"),
                product_data.get("Category"),
                product_data.get("FKLotID"),
                product_data.get("LotBatch"),
                float(product_data.get("LotMRP", 0.0)),
                product_data.get("ManufacturingCo"),
                product_data.get("MarketingCo"),
                product_data.get("NameToDisplay"),
                product_data.get("Product"),
                float(product_data.get("PurchaseRate", 0.0)),
                product_data.get("RefDate"),
                product_data.get("RefNo"),
                product_data.get("SellLoose"),
                float(product_data.get("Stock", 0.0)),
                float(product_data.get("StockValue", 0.0))
            ))
        conn.commit()

if __name__ == "__main__":
    if len(sys.argv) >= 3:
        file_path = sys.argv[1]
        file_name = sys.argv[2]
        execute(file_path, file_name)
    else:
        print("Insufficient command-line arguments provided.")

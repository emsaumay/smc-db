import sys, os, requests, sqlite3
from datetime import datetime, timedelta
import pypyodbc as odbc
import psycopg2
from psycopg2 import sql
from decimal import Decimal



DRIVER_NAME = 'SQL SERVER'
SERVER_NAME = 'DESKTOP-NJ6D2AK\\SQLEXPRESS'
DATABASE_NAME = 'LunawatData'

connection_string = f"DRIVER={{{DRIVER_NAME}}};SERVER={SERVER_NAME};DATABASE={DATABASE_NAME};Trust_Connection=yes;"


def stock():
    sql_server_conn = odbc.connect(connection_string)
    cursor = sql_server_conn.cursor()

    query = """
    SELECT 
    [tblMktGroup_Mas].PKMktGroupID AS MktID,
    [tblMktGroup_Mas].GroupName AS MktName,
    [tblProd_Mas].Brand AS Brand,
    [tblProd_Mas].Product AS Product,
    [tblProd_Mas].Strength AS Strength,
    [tblProd_Mas].PKProdID AS ProdID,
    [tblProd_Mas].NameToDisplay AS NameToDisplay,
    [tblProd_Mas].FKProdCatgId AS CategoryID,
    [tblProdCatg_Mas].Category AS Category,
    [tblProdLot_Dtl].PKLotID AS LotID,
    [tblProdLot_Dtl].Barcode AS Barcode,
    [tblProdLot_Dtl].Batch AS Size,
    [tblProdLot_Dtl].MRP AS MRP,
    [tblProdLot_Dtl].SaleRate AS SaleRate,
    [tblProdLot_Dtl].CostRate AS CostRate,
    [tblProdLot_Dtl].PurchaseRate AS PurchaseRate,
    [tblProdLot_Dtl].Remarks AS DesignCode,
    [tblProdStock_Dtl].StockDate AS StockDate,
    [tblProdStock_Dtl].InStock AS InStock,
    [tblProdStock_Dtl].OutStock AS OutStock,
    [tblProdStock_Dtl].CurStock AS CurStock,
    [tblPurchaseInv_Trn].FKVendorID AS VendorID,
    [tblPurchaseInv_Trn].GRNo AS PurchaseInvNo,
    [tblPurchaseInv_Trn].GRDate AS PurchaseInvDate,
    [tblVendor_Mas].Vendor AS Vendor
    FROM tblProd_Mas 
    INNER JOIN tblMktGroup_Mas 
    ON tblProd_Mas.FKMktGroupID = tblMktGroup_Mas.PKMktGroupID
    INNER JOIN [tblProdCatg_Mas]
    ON [tblProdCatg_Mas].PKProdCatgID = [tblProd_Mas].FKProdCatgId
    INNER JOIN 
    [tblProdStock_Dtl] ON [tblProdStock_Dtl].FKProdID = [tblProd_Mas].PKProdId
    INNER JOIN 
    [tblProdLot_Dtl] ON [tblProdLot_Dtl].FKProdId = [tblProdStock_Dtl].FKProdID AND [tblProdLot_Dtl].PKLotID = [tblProdStock_Dtl].FKLotID
    LEFT OUTER JOIN 
    [tblPurchaseInv_Dtl] ON [tblPurchaseInv_Dtl].FKProdId = [tblProd_Mas].PKProdID AND [tblPurchaseInv_Dtl].FKLotID = [tblProdLot_Dtl].PKLotID
    LEFT OUTER JOIN [tblPurchaseInv_Trn]
    ON [tblPurchaseInv_Trn].PKID = [tblPurchaseInv_Dtl].FKID
    LEFT OUTER JOIN [tblVendor_Mas]
    ON [tblVendor_Mas].PKVendorID = [tblPurchaseInv_Trn].FKVendorID
    """

    cursor.execute(query)
    data = cursor.fetchall()

    sqlite_conn = sqlite3.connect("C:\\Users\\abhi\\Desktop\\code\\smc-db\\stock_local.db")
    sqlite_cursor = sqlite_conn.cursor()

    create_table_query = """
    CREATE TABLE IF NOT EXISTS stock (
        MktID INTEGER,
        MktName TEXT,
        Brand TEXT,
        Product TEXT,
        Strength TEXT,
        ProdID INTEGER,
        NameToDisplay TEXT,
        CategoryID INTEGER,
        Category TEXT,
        LotID INTEGER,
        Barcode TEXT,
        Size TEXT,
        MRP REAL,
        SaleRate REAL,
        CostRate REAL,
        PurchaseRate REAL,
        DesignCode TEXT,
        StockDate TEXT,
        InStock INTEGER,
        OutStock INTEGER,
        CurStock INTEGER,
        VendorID INTEGER,
        PurchaseInvNo TEXT,
        PurchaseInvDate TEXT,
        Vendor TEXT
    );
    """
    sqlite_cursor.execute(create_table_query)
    sqlite_conn.commit()

    data = [tuple(map(lambda x: float(x) if isinstance(x, Decimal) else x, row)) for row in data]


    insert_query = """
    INSERT INTO stock VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """
    sqlite_cursor.executemany(insert_query, data)
    sqlite_conn.commit()

    cursor.close()
    sql_server_conn.close()
    sqlite_cursor.close()
    sqlite_conn.close()


    with open('C:\\Users\\abhi\\Desktop\\code\\smc-db\\stock_local.db', 'rb') as file:
        files = {'file': (file.name, file)}
        response = requests.post('https://smc.saumay.dev/upload', files=files)

    if response.status_code == 200:
        print('File uploaded successfully.')
    else:
        print(f'Failed to upload file. Status code: {response.status_code}')

    os.remove('C:\\Users\\abhi\\Desktop\\code\\smc-db\\stock_local.db')

def sales(time):
    date = datetime.strftime(datetime.now() - timedelta(time), '%Y-%m-%d')
    sql_server_conn = odbc.connect(connection_string)
    cursor = sql_server_conn.cursor()

    query = f"""
    SELECT 
       [SalesDetail].[ID]
      ,[SalesDetail].[TranName]
      ,[SalesDetail].[Series]
      ,[SalesDetail].[EntryNo]
      ,[SalesDetail].[InvoiceNo]
      ,[SalesDetail].[NameToDisplay]
      ,[SalesDetail].[Brand]
      ,[SalesDetail].[Product]
      ,[SalesDetail].[Category]
      ,[SalesDetail].[MarketingCo]
      ,[SalesDetail].[ManufacturingCo] AS Vendor
      ,[SalesDetail].[Batch] as Size
      ,[SalesDetail].[Color]
      ,[SalesDetail].[MRP]
      ,[SalesDetail].[Qty]
      ,[SalesDetail].[Amount]
      ,[SalesDetail].[InvoiceAmt]
      ,[SalesDetail].[PurchaseRate]
      ,[SalesDetail].[PurchaseRate]*[SalesDetail].Qty AS [PurchaseAmount]
      ,[SalesDetail].[Amount] - ([SalesDetail].[PurchaseRate]*[SalesDetail].Qty) AS Profit
      ,[SalesDetail].[TotalAmt]
      ,[SalesInvoiceTrnDetail].EntryDate AS SalesInvoiceEntryDate
      ,[SalesInvoiceTrnDetail].EntryDateWithTime AS SalesInvoiceEntryTime
      ,[SalesReturnTrnDetail].EntryDate AS SalesReturnEntryDate
      ,[SalesReturnTrnDetail].EntryDateWithTime AS SalesReturnEntryTime
      ,[SalesDetail].[FKMktGroupID]
      ,[SalesDetail].[FKMfgGroupID]
      ,[SalesDetail].[FKProdCatgID]
      ,[SalesDetail].[FKProdID]
      ,[SalesDetail].[FKLotID]
  FROM [LunawatData].[dbo].[SalesDetail] 
  LEFT OUTER JOIN [LunawatData].[dbo].[SalesInvoiceTrnDetail]
  ON [SalesDetail].TranName='sales invoice' AND [SalesDetail].id = [SalesInvoiceTrnDetail].id 
  LEFT OUTER JOIN [LunawatData].[dbo].[SalesReturnTrnDetail]
  ON [SalesDetail].TranName='sales return' AND [SalesDetail].id = [SalesReturnTrnDetail].id
  WHERE [SalesInvoiceTrnDetail].EntryDate >= '{date}' OR [SalesReturnTrnDetail].EntryDate >= '{date}'
    """

    cursor.execute(query)
    data = cursor.fetchall()

    sqlite_conn = sqlite3.connect("C:\\Users\\abhi\\Desktop\\code\\smc-db\\sales_local.db")
    sqlite_cursor = sqlite_conn.cursor()

    create_table_query = """
        CREATE TABLE sales (
        ID INTEGER,
        TranName TEXT,
        Series TEXT,
        EntryNo INTEGER,
        InvoiceNo TEXT,
        NameToDisplay TEXT,
        Brand TEXT,
        Product TEXT,
        Category TEXT,
        MarketingCo TEXT,
        Vendor TEXT,
        Size TEXT,
        Color TEXT,
        MRP REAL,
        Qty INTEGER,
        Amount REAL,
        InvoiceAmt REAL,
        PurchaseRate REAL,
        PurchaseAmount REAL,
        Profit REAL,
        TotalAmt REAL,
        SalesInvoiceEntryDate DATE,
        SalesInvoiceEntryTime DATETIME,
        SalesReturnEntryDate DATE,
        SalesReturnEntryTime DATETIME,
        FKMktGroupID INTEGER,
        FKMfgGroupID INTEGER,
        FKProdCatgID INTEGER,
        FKProdID INTEGER,
        FKLotID INTEGER
    );
    """
    sqlite_cursor.execute(create_table_query)
    sqlite_conn.commit()

    data = [tuple(map(lambda x: float(x) if isinstance(x, Decimal) else x, row)) for row in data]
    def replace_none_with_null(tuple_data):
        return tuple(None if val is None else val for val in tuple_data)

    # Preprocess the data list to replace None values
    data = [replace_none_with_null(row) for row in data]

    insert_query = """
    INSERT INTO sales VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                              ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                              ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """

    sqlite_cursor.executemany(insert_query, data)
    sqlite_conn.commit()

    cursor.close()
    sql_server_conn.close()
    sqlite_cursor.close()
    sqlite_conn.close()

    with open('C:\\Users\\abhi\\Desktop\\code\\smc-db\\sales_local.db', 'rb') as file:
        files = {'file': (file.name, file)}
        response = requests.post('https://smc.saumay.dev/upload', files=files)

    if response.status_code == 200:
        print('File uploaded successfully.')
    else:
        print(f'Failed to upload file. Status code: {response.status_code}')

    os.remove('C:\\Users\\abhi\\Desktop\\code\\smc-db\\sales_local.db')

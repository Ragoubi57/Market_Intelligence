RAW_DATA_SOURCES = {
    "amazon": "Data/Amazon Sale Report.csv",
    "international": "Data/International sale Report.csv",
    "cloud_cost": "Data/Cloud Warehouse Compersion Chart.csv",
    "expense": "Data/Expense IIGF.csv"
}


COLUMN_MAPPINGS = {
    "amazon": {
        "Date": "order_date",
        "SKU": "product_sku",
        "Style": "product_name",
        "Category": "product_category",
        "Qty": "quantity",
        "Amount": "total_amount",
        "ship-country": "country_code",
        "currency": "currency"
    },
    "international": {
        "DATE": "order_date",
        "SKU": "product_sku",
        "Style": "product_name",
        "PCS": "quantity",
        "GROSS AMT": "total_amount",
        "CUSTOMER": "customer_name" 
    }
}

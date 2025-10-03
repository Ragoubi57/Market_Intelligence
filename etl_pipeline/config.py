RAW_DATA_SOURCES = {
    "amazon": "data/Amazon Sale report.csv",
    "international": "data/International sale report.csv",
    "cloud_cost": "Data/Cloud Warehouse Compersion Chart.csv",
    "expense": "data/Expense IIGF.csv",
    "pandl_march": "Data\P  L March 2021.csv",
    "pandl_may": "data/May-2022.csv"
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

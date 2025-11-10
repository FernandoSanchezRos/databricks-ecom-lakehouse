CATALOG = "ecom_lakehouse"
SCHEMA_LANDING = "files"
SCHEMA_BRONZE = "bronze"
SCHEMA_SILVER = "silver"

VOLUME_LANDING_ROOT = "/Volumes/ecom_lakehouse/files/landing/"
CHECKPOINTS_ROOT = "abfss://lakehouse@ecomlakehousesg.dfs.core.windows.net/files/checkpoints/"

SOURCES = {
    "customers": f"{VOLUME_LANDING_ROOT}/customers",
    "orders":    f"{VOLUME_LANDING_ROOT}/orders",
    "payments":  f"{VOLUME_LANDING_ROOT}/payments",
    "products":  f"{VOLUME_LANDING_ROOT}/products",
    "order_items": f"{VOLUME_LANDING_ROOT}/order_items",
}

BRONZE_TABLES = {
    "customers": f"{CATALOG}.{SCHEMA_BRONZE}.customers",
    "orders":    f"{CATALOG}.{SCHEMA_BRONZE}.orders",
    "payments":  f"{CATALOG}.{SCHEMA_BRONZE}.payments",
    "products":  f"{CATALOG}.{SCHEMA_BRONZE}.products",
    "order_items": f"{CATALOG}.{SCHEMA_BRONZE}.order_items",
}

SILVER_TABLES = {
    "customers": f"{CATALOG}.{SCHEMA_SILVER}.customers",
    "orders":    f"{CATALOG}.{SCHEMA_SILVER}.orders",
    "payments":  f"{CATALOG}.{SCHEMA_SILVER}.payments",
    "products":  f"{CATALOG}.{SCHEMA_SILVER}.products",
    "order_items": f"{CATALOG}.{SCHEMA_SILVER}.order_items",   
}
"""
Configurações centralizadas do pipeline Medallion - Projeto Olist E-commerce.
"""

CATALOG = "olist_project"

BRONZE_SCHEMA = f"{CATALOG}.bronze"
SILVER_SCHEMA = f"{CATALOG}.silver"
GOLD_SCHEMA = f"{CATALOG}.gold"

RAW_PATH = "/Volumes/olist_project/bronze/raw_files"

# Tabelas Bronze: nome da tabela -> arquivo CSV de origem
TABELAS_BRONZE = {
    "orders": "olist_orders_dataset.csv",
    "customers": "olist_customers_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "payments": "olist_order_payments_dataset.csv",
    "reviews": "olist_order_reviews_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
}

# Opções extras de leitura por tabela (ex: reviews precisa de parsing multiline)
OPCOES_LEITURA_BRONZE = {
    "reviews": {"multiLine": "true", "quote": '"', "escape": '"'},
}


# ==========================================
# Tipagem - Silver
# ==========================================

# Datas / Timestamps
Orders_Date = {
    "order_purchase_timestamp": "timestamp",
    "order_approved_at": "timestamp",
    "order_delivered_carrier_date": "timestamp",
    "order_delivered_customer_date": "timestamp",
    "order_estimated_delivery_date": "timestamp",
}

Items_Date = {
    "shipping_limit_date": "timestamp",
}

Reviews_Date = {
    "review_creation_date": "timestamp",
    "review_answer_timestamp": "timestamp",
}

# Números (inteiros, decimais, valores monetários)
Items_Number = {
    "price": "decimal(10,2)",
    "freight_value": "decimal(10,2)",
}

Reviews_Number = {
    "review_score": "int",
}

Payments_Number = {
    "payment_value": "decimal(10,2)",
    "payment_installments": "int",
}

Products_Number = {
    "product_weight_g": "double",
    "product_length_cm": "double",
    "product_height_cm": "double",
    "product_width_cm": "double",
    "product_name_lenght": "int",          # nome mantido igual ao dataset original (erro de digitação no Kaggle)
    "product_description_lenght": "int",
    "product_photos_qty": "int",
}

Geolocation_Number = {
    "geolocation_lat": "double",
    "geolocation_lng": "double",
}

# Strings / Texto
Products_String = {
    "product_category_name": "string",
}

CategoryTranslation_String = {
    "product_category_name": "string",
    "product_category_name_english": "string",
}

Geolocation_String = {
    "geolocation_city": "string",
    "geolocation_state": "string",
}

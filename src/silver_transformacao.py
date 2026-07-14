"""
02 - Silver: Tratamento e modelagem.

Pipeline em etapas sobre a camada Bronze:
1. Tipagem de colunas
2. Deduplicação
3. Tratamento de nulos/inválidos
4. Padronização de texto
5. Regras de negócio
6. Gravação
7. Validação (chave única + integridade referencial)
"""

from pyspark.sql import functions as F

from config import (
    BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA,
    Orders_Date, Items_Date, Reviews_Date,
    Items_Number, Reviews_Number, Payments_Number, Products_Number, Geolocation_Number,
    Products_String, Geolocation_String,
)
from utils import (
    get_spark, aplicar_tipos, deduplicar,
    checar_chave_unica, checar_integridade_referencial,
)


def tratar_tabelas(spark):
    """Etapas 1-5: tipagem, deduplicação, nulos, padronização e regras de negócio."""

    # --- Etapa 1: Tipagem ---
    orders = aplicar_tipos(spark.table(f"{BRONZE_SCHEMA}.orders"), "orders", Orders_Date)
    order_items = aplicar_tipos(
        spark.table(f"{BRONZE_SCHEMA}.order_items"), "order_items",
        {**Items_Date, **Items_Number}
    )
    reviews = aplicar_tipos(
        spark.table(f"{BRONZE_SCHEMA}.reviews"), "reviews",
        {**Reviews_Date, **Reviews_Number}
    )
    payments = aplicar_tipos(spark.table(f"{BRONZE_SCHEMA}.payments"), "payments", Payments_Number)
    products = aplicar_tipos(
        spark.table(f"{BRONZE_SCHEMA}.products"), "products",
        {**Products_Number, **Products_String}
    )

    customers = spark.table(f"{BRONZE_SCHEMA}.customers")
    sellers = spark.table(f"{BRONZE_SCHEMA}.sellers")

    geolocation_tipada = aplicar_tipos(
        spark.table(f"{BRONZE_SCHEMA}.geolocation"), "geolocation",
        {**Geolocation_Number, **Geolocation_String}
    )
    geolocation = (geolocation_tipada
        .groupBy("geolocation_zip_code_prefix")
        .agg(
            F.avg("geolocation_lat").alias("latitude"),
            F.avg("geolocation_lng").alias("longitude"),
            F.first("geolocation_city").alias("cidade"),
            F.first("geolocation_state").alias("estado"),
        )
        .withColumn("cidade", F.lower(F.trim(F.col("cidade"))))
        .withColumn("estado", F.upper(F.trim(F.col("estado"))))
    )

    category_translation = (spark.table(f"{BRONZE_SCHEMA}.category_translation")
        .withColumn("product_category_name", F.lower(F.trim(F.col("product_category_name"))))
        .withColumn("product_category_name_english", F.lower(F.trim(F.col("product_category_name_english"))))
        .dropDuplicates(["product_category_name"])
    )

    # --- Etapa 2: Deduplicação ---
    orders = deduplicar(orders, "orders", "order_id")
    customers = deduplicar(customers, "customers", "customer_id")
    products = deduplicar(products, "products", "product_id")
    sellers = deduplicar(sellers, "sellers", "seller_id")
    reviews = deduplicar(reviews, "reviews", "review_id")
    order_items = order_items.dropDuplicates(["order_id", "order_item_id"])

    # --- Etapa 3: Nulos e valores inválidos ---
    orders = orders.filter(F.col("order_id").isNotNull())
    products = products.withColumn(
        "product_category_name",
        F.coalesce(F.col("product_category_name"), F.lit("nao_informado"))
    )

    # --- Etapa 4: Padronização de texto ---
    customers = (customers
        .withColumn("customer_state", F.upper(F.trim(F.col("customer_state"))))
        .withColumn("customer_city", F.lower(F.trim(F.col("customer_city"))))
    )
    sellers = (sellers
        .withColumn("seller_state", F.upper(F.trim(F.col("seller_state"))))
        .withColumn("seller_city", F.lower(F.trim(F.col("seller_city"))))
    )
    products = products.withColumn(
        "product_category_name", F.lower(F.trim(F.col("product_category_name")))
    )

    # --- Etapa 5: Regras de negócio ---
    orders = orders.withColumn(
        "flag_data_inconsistente",
        F.when(
            F.col("order_delivered_customer_date") < F.col("order_purchase_timestamp"),
            True
        ).otherwise(False)
    )

    return {
        "orders": orders, "customers": customers, "order_items": order_items,
        "payments": payments, "reviews": reviews, "products": products,
        "sellers": sellers, "geolocation": geolocation,
        "category_translation": category_translation,
    }


def gravar_tabelas(tabelas_tratadas):
    """Etapa 6: Gravação."""
    for nome, df in tabelas_tratadas.items():
        df.write.format("delta").mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(f"{SILVER_SCHEMA}.{nome}")
        print(f"✅ silver.{nome}: {df.count()} linhas")


def validar(spark):
    """Etapa 7: Validação — chave única e integridade referencial."""

    # Chaves únicas
    checar_chave_unica(spark, GOLD_SCHEMA, SILVER_SCHEMA, "orders", "order_id")
    checar_chave_unica(spark, GOLD_SCHEMA, SILVER_SCHEMA, "customers", "customer_id")
    checar_chave_unica(spark, GOLD_SCHEMA, SILVER_SCHEMA, "products", "product_id")
    checar_chave_unica(spark, GOLD_SCHEMA, SILVER_SCHEMA, "reviews", "review_id")
    checar_chave_unica(spark, GOLD_SCHEMA, SILVER_SCHEMA, "sellers", "seller_id")
    checar_chave_unica(spark, GOLD_SCHEMA, SILVER_SCHEMA, "geolocation", "geolocation_zip_code_prefix")
    checar_chave_unica(spark, GOLD_SCHEMA, SILVER_SCHEMA, "category_translation", "product_category_name")

    # Integridade referencial
    checar_integridade_referencial(spark, GOLD_SCHEMA, SILVER_SCHEMA, "order_items", "order_id", "orders", "order_id")
    checar_integridade_referencial(spark, GOLD_SCHEMA, SILVER_SCHEMA, "orders", "customer_id", "customers", "customer_id")
    checar_integridade_referencial(spark, GOLD_SCHEMA, SILVER_SCHEMA, "order_items", "product_id", "products", "product_id")
    checar_integridade_referencial(spark, GOLD_SCHEMA, SILVER_SCHEMA, "order_items", "seller_id", "sellers", "seller_id")
    checar_integridade_referencial(spark, GOLD_SCHEMA, SILVER_SCHEMA, "payments", "order_id", "orders", "order_id")
    checar_integridade_referencial(spark, GOLD_SCHEMA, SILVER_SCHEMA, "reviews", "order_id", "orders", "order_id")


def main():
    spark = get_spark()

    tabelas_tratadas = tratar_tabelas(spark)
    gravar_tabelas(tabelas_tratadas)
    validar(spark)

    print("✅ Camada Silver concluída")


if __name__ == "__main__":
    main()

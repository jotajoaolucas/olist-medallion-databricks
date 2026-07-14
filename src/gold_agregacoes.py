"""
03 - Gold: Tabelas de negócio para o dashboard "Visão Geral do Negócio".

1. receita_mensal              -> Evolução de receita
2. receita_por_estado          -> Onde estão nossos clientes
3. performance_entrega_mensal  -> Performance de entrega
4. satisfacao_entrega          -> Satisfação do cliente x atraso
5. ranking_vendedores          -> Top vendedores
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from config import SILVER_SCHEMA, GOLD_SCHEMA
from utils import get_spark, registrar_checagem, falhar_se_checagem_critica


def carregar_silver(spark):
    return {
        "orders": spark.table(f"{SILVER_SCHEMA}.orders"),
        "customers": spark.table(f"{SILVER_SCHEMA}.customers"),
        "order_items": spark.table(f"{SILVER_SCHEMA}.order_items"),
        "payments": spark.table(f"{SILVER_SCHEMA}.payments"),
        "reviews": spark.table(f"{SILVER_SCHEMA}.reviews"),
        "products": spark.table(f"{SILVER_SCHEMA}.products"),
        "sellers": spark.table(f"{SILVER_SCHEMA}.sellers"),
    }


def gravar(df, nome_tabela):
    df.write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"{GOLD_SCHEMA}.{nome_tabela}")
    print(f"✅ gold.{nome_tabela}: {df.count()} linhas")


# ==========================================
# 1. Receita Mensal
# ==========================================

def gerar_receita_mensal(spark, orders, order_items):
    receita_mensal = (orders.join(order_items, "order_id")
        .withColumn("mes", F.date_format("order_purchase_timestamp", "yyyy-MM"))
        .groupBy("mes")
        .agg(
            F.sum("price").alias("receita_produtos"),
            F.sum("freight_value").alias("receita_frete"),
            F.countDistinct("order_id").alias("qtd_pedidos")
        )
        .orderBy("mes")
    )

    janela_mensal = Window.orderBy("mes")
    receita_mensal = receita_mensal.withColumn(
        "receita_mes_anterior", F.lag("receita_produtos").over(janela_mensal)
    ).withColumn(
        "variacao_pct",
        F.round(
            (F.col("receita_produtos") - F.col("receita_mes_anterior")) / F.col("receita_mes_anterior") * 100, 2
        )
    ).drop("receita_mes_anterior")

    gravar(receita_mensal, "receita_mensal")

    receita_gold = spark.table(f"{GOLD_SCHEMA}.receita_mensal").agg(F.sum("receita_produtos")).collect()[0][0]
    receita_silver = order_items.agg(F.sum("price")).collect()[0][0]
    diferenca = abs(float(receita_gold) - float(receita_silver))
    status = "PASSOU" if diferenca < 0.01 else "FALHOU"
    registrar_checagem(spark, GOLD_SCHEMA, "gold", "receita_mensal", "reconciliacao_com_silver", status,
                        f"gold={receita_gold}, silver={receita_silver}")

    negativos = spark.table(f"{GOLD_SCHEMA}.receita_mensal").filter(F.col("receita_produtos") < 0).count()
    status = "PASSOU" if negativos == 0 else "FALHOU"
    registrar_checagem(spark, GOLD_SCHEMA, "gold", "receita_mensal", "sem_valores_negativos", status,
                        f"{negativos} negativos")

    return receita_silver  # reaproveitado na validação da próxima tabela


# ==========================================
# 2. Receita por Estado
# ==========================================

def gerar_receita_por_estado(spark, orders, order_items, customers, receita_silver):
    receita_por_estado = (orders.join(order_items, "order_id")
        .join(customers, "customer_id")
        .groupBy(F.col("customer_state").alias("estado"))
        .agg(
            F.sum("price").alias("receita_total"),
            F.countDistinct("order_id").alias("qtd_pedidos"),
            F.round(F.avg("price"), 2).alias("ticket_medio")
        )
        .orderBy(F.desc("receita_total"))
    )

    gravar(receita_por_estado, "receita_por_estado")

    total_gold = spark.table(f"{GOLD_SCHEMA}.receita_por_estado").agg(F.sum("receita_total")).collect()[0][0]
    diferenca = abs(float(total_gold) - float(receita_silver))
    status = "PASSOU" if diferenca < 0.01 else "FALHOU"
    registrar_checagem(spark, GOLD_SCHEMA, "gold", "receita_por_estado", "reconciliacao_com_silver", status,
                        f"gold={total_gold}, silver={receita_silver}")

    estados_nulos = spark.table(f"{GOLD_SCHEMA}.receita_por_estado").filter(F.col("estado").isNull()).count()
    status = "PASSOU" if estados_nulos == 0 else "FALHOU"
    registrar_checagem(spark, GOLD_SCHEMA, "gold", "receita_por_estado", "sem_estado_nulo", status,
                        f"{estados_nulos} nulos")


# ==========================================
# 3. Performance de Entrega Mensal
# ==========================================

def gerar_performance_entrega(spark, orders):
    pedidos_entregues = (orders
        .filter((F.col("order_status") == "delivered") &
                F.col("order_delivered_customer_date").isNotNull() &
                (F.col("flag_data_inconsistente") == False))
        .withColumn("mes", F.date_format("order_purchase_timestamp", "yyyy-MM"))
        .withColumn("dias_entrega", F.datediff("order_delivered_customer_date", "order_purchase_timestamp"))
        .withColumn("no_prazo", F.col("order_delivered_customer_date") <= F.col("order_estimated_delivery_date"))
    )

    performance_entrega_mensal = (pedidos_entregues
        .groupBy("mes")
        .agg(
            F.round(F.avg("dias_entrega"), 1).alias("tempo_medio_entrega_dias"),
            F.round(F.avg(F.col("no_prazo").cast("int")) * 100, 2).alias("pct_entregas_no_prazo"),
            F.countDistinct("order_id").alias("qtd_pedidos_entregues")
        )
        .withColumn("pct_entregas_atrasadas", F.round(100 - F.col("pct_entregas_no_prazo"), 2))
        .orderBy("mes")
    )

    gravar(performance_entrega_mensal, "performance_entrega_mensal")

    df_check = spark.table(f"{GOLD_SCHEMA}.performance_entrega_mensal")
    fora_intervalo = df_check.filter(
        (F.col("pct_entregas_no_prazo") < 0) | (F.col("pct_entregas_no_prazo") > 100)
    ).count()
    status = "PASSOU" if fora_intervalo == 0 else "FALHOU"
    registrar_checagem(spark, GOLD_SCHEMA, "gold", "performance_entrega_mensal",
                        "percentual_no_intervalo_0_100", status, f"{fora_intervalo} fora do intervalo")

    negativos = df_check.filter(F.col("tempo_medio_entrega_dias") < 0).count()
    status = "PASSOU" if negativos == 0 else "FALHOU"
    registrar_checagem(spark, GOLD_SCHEMA, "gold", "performance_entrega_mensal",
                        "tempo_entrega_nao_negativo", status, f"{negativos} negativos")

    return pedidos_entregues  # reaproveitado pela tabela de satisfação


# ==========================================
# 4. Satisfação x Entrega
# ==========================================

def gerar_satisfacao_entrega(spark, pedidos_entregues, reviews):
    satisfacao_entrega = (pedidos_entregues.join(reviews, "order_id")
        .withColumn("status_entrega", F.when(F.col("no_prazo"), "no_prazo").otherwise("atrasado"))
        .groupBy("status_entrega")
        .agg(
            F.round(F.avg("review_score"), 2).alias("nota_media"),
            F.count("review_id").alias("qtd_avaliacoes")
        )
        .orderBy("status_entrega")
    )

    gravar(satisfacao_entrega, "satisfacao_entrega")

    df_check = spark.table(f"{GOLD_SCHEMA}.satisfacao_entrega")
    fora_intervalo = df_check.filter((F.col("nota_media") < 1) | (F.col("nota_media") > 5)).count()
    status = "PASSOU" if fora_intervalo == 0 else "FALHOU"
    registrar_checagem(spark, GOLD_SCHEMA, "gold", "satisfacao_entrega",
                        "nota_media_no_intervalo_1_5", status, f"{fora_intervalo} fora do intervalo")

    categorias = [row["status_entrega"] for row in df_check.select("status_entrega").collect()]
    status = "PASSOU" if set(categorias) == {"no_prazo", "atrasado"} else "FALHOU"
    registrar_checagem(spark, GOLD_SCHEMA, "gold", "satisfacao_entrega",
                        "categorias_esperadas", status, f"{categorias}")


# ==========================================
# 5. Ranking de Vendedores
# ==========================================

def gerar_ranking_vendedores(spark, order_items, orders, sellers, reviews):
    ranking_vendedores = (order_items.join(orders, "order_id")
        .join(sellers, "seller_id")
        .join(reviews, "order_id", "left")
        .groupBy("seller_id", "seller_state")
        .agg(
            F.sum("price").alias("receita_total"),
            F.countDistinct(F.col("order_id")).alias("qtd_pedidos"),
            F.round(F.avg("review_score"), 2).alias("nota_media")
        )
        .filter(F.col("qtd_pedidos") >= 5)
        .orderBy(F.desc("receita_total"))
    )

    gravar(ranking_vendedores, "ranking_vendedores")

    df_check = spark.table(f"{GOLD_SCHEMA}.ranking_vendedores")
    total = df_check.count()
    distintos = df_check.select("seller_id").distinct().count()
    status = "PASSOU" if total == distintos else "FALHOU"
    registrar_checagem(spark, GOLD_SCHEMA, "gold", "ranking_vendedores",
                        "chave_unica_seller_id", status, f"{total} vs {distintos}")

    abaixo_minimo = df_check.filter(F.col("qtd_pedidos") < 5).count()
    status = "PASSOU" if abaixo_minimo == 0 else "FALHOU"
    registrar_checagem(spark, GOLD_SCHEMA, "gold", "ranking_vendedores",
                        "filtro_minimo_pedidos_respeitado", status, f"{abaixo_minimo} abaixo do mínimo")


def main():
    spark = get_spark()
    silver = carregar_silver(spark)

    receita_silver = gerar_receita_mensal(spark, silver["orders"], silver["order_items"])
    gerar_receita_por_estado(spark, silver["orders"], silver["order_items"], silver["customers"], receita_silver)
    pedidos_entregues = gerar_performance_entrega(spark, silver["orders"])
    gerar_satisfacao_entrega(spark, pedidos_entregues, silver["reviews"])
    gerar_ranking_vendedores(spark, silver["order_items"], silver["orders"], silver["sellers"], silver["reviews"])

    falhar_se_checagem_critica(spark, GOLD_SCHEMA, camada="gold")

    print("✅ Camada Gold concluída")


if __name__ == "__main__":
    main()

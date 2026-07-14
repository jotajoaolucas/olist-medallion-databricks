"""
Funções utilitárias compartilhadas entre bronze_ingestao.py, silver_transformacao.py
e gold_agregacoes.py.
"""

from pyspark.sql import functions as F
from pyspark.sql import Row, SparkSession
from datetime import datetime


def get_spark():
    return SparkSession.builder.getOrCreate()


# ==========================================
# Log de qualidade
# ==========================================

def registrar_checagem(spark, gold_schema, camada, tabela, checagem, status, detalhe=""):
    """Grava o resultado de uma checagem de qualidade na tabela {gold_schema}.quality_log."""
    row = Row(
        camada=camada, tabela=tabela, checagem=checagem,
        status=status, detalhe=detalhe, timestamp=datetime.now()
    )
    spark.createDataFrame([row]).write.format("delta").mode("append") \
        .saveAsTable(f"{gold_schema}.quality_log")

    simbolo = "✅" if status == "PASSOU" else "❌"
    print(f"{simbolo} [{camada}.{tabela}] {checagem} — {detalhe}")


def falhar_se_checagem_critica(spark, gold_schema, camada=None):
    """Lança exceção se alguma checagem da execução atual falhou — usado pra travar o Job."""
    df = spark.table(f"{gold_schema}.quality_log").filter("status = 'FALHOU'")
    if camada:
        df = df.filter(f"camada = '{camada}'")
    falhas = df.count()
    if falhas > 0:
        raise Exception(f"Pipeline abortado: {falhas} checagem(ns) de qualidade falharam na camada '{camada or 'todas'}'.")


# ==========================================
# Bronze
# ==========================================

def ingerir_tabela(spark, bronze_schema, raw_path, nome_tabela, arquivo, opcoes_extra=None):
    """Lê um CSV bruto do Volume e grava como tabela Delta na Bronze, com metadados de auditoria."""
    caminho = f"{raw_path}/{arquivo}"
    reader = spark.read.option("header", True).option("inferSchema", False)
    if opcoes_extra:
        for chave, valor in opcoes_extra.items():
            reader = reader.option(chave, valor)

    df = (reader.csv(caminho)
          .withColumn("_ingest_timestamp", F.current_timestamp())
          .withColumn("_source_file", F.lit(arquivo)))

    df.write.format("delta").mode("overwrite").saveAsTable(f"{bronze_schema}.{nome_tabela}")
    return df


# ==========================================
# Silver
# ==========================================

def aplicar_tipos(df, tabela, mapa_tipos):
    """Converte colunas para os tipos corretos via try_cast, avisando sobre falhas de conversão."""
    for coluna, tipo in mapa_tipos.items():
        antes_nulos = df.filter(F.col(coluna).isNull()).count()
        df = df.withColumn(coluna, F.expr(f"try_cast({coluna} as {tipo})"))
        depois_nulos = df.filter(F.col(coluna).isNull()).count()
        novos_nulos = depois_nulos - antes_nulos
        if novos_nulos > 0:
            print(f"⚠️  {tabela}.{coluna}: {novos_nulos} valores não puderam ser convertidos pra {tipo}")
    return df


def deduplicar(df, tabela, chave):
    """Remove duplicatas por chave (string ou lista de colunas), avisando quantas foram removidas."""
    chaves = [chave] if isinstance(chave, str) else chave
    antes = df.count()
    df = df.dropDuplicates(chaves)
    depois = df.count()
    removidas = antes - depois
    if removidas > 0:
        print(f"🔁 {tabela}: {removidas} duplicatas removidas (chave={chaves})")
    return df


def checar_chave_unica(spark, gold_schema, silver_schema, tabela, coluna):
    df = spark.table(f"{silver_schema}.{tabela}")
    total, distintos = df.count(), df.select(coluna).distinct().count()
    status = "PASSOU" if total == distintos else "FALHOU"
    registrar_checagem(spark, gold_schema, "silver", tabela, f"chave_unica_{coluna}", status,
                        f"{total} vs {distintos}")


def checar_integridade_referencial(spark, gold_schema, silver_schema, filho, fk, pai, pk):
    df_filho = spark.table(f"{silver_schema}.{filho}")
    df_pai = spark.table(f"{silver_schema}.{pai}")
    orfaos = df_filho.join(df_pai, df_filho[fk] == df_pai[pk], "left_anti").count()
    status = "PASSOU" if orfaos == 0 else "FALHOU"
    registrar_checagem(spark, gold_schema, "silver", filho, f"integridade_{fk}_para_{pai}", status,
                        f"{orfaos} órfãos")

"""
01 - Bronze: Ingestão bruta em Delta.

Lê os CSVs brutos do Kaggle (já baixados no Volume) e grava como tabelas Delta,
sem transformação de negócio, apenas com metadados de auditoria.
"""

from pyspark.sql import functions as F

from config import BRONZE_SCHEMA, GOLD_SCHEMA, RAW_PATH, TABELAS_BRONZE, OPCOES_LEITURA_BRONZE
from utils import get_spark, ingerir_tabela, registrar_checagem


def validar_tabela(spark, nome_tabela, arquivo, linhas_gravadas):
    linhas_origem = spark.read.option("header", True).csv(f"{RAW_PATH}/{arquivo}").count()
    status = "PASSOU" if linhas_origem == linhas_gravadas else "FALHOU"
    registrar_checagem(spark, GOLD_SCHEMA, "bronze", nome_tabela,
                        "contagem_linhas_vs_origem", status,
                        f"origem={linhas_origem}, gravado={linhas_gravadas}")


def main():
    spark = get_spark()

    for nome_tabela, arquivo in TABELAS_BRONZE.items():
        opcoes = OPCOES_LEITURA_BRONZE.get(nome_tabela)
        df = ingerir_tabela(spark, BRONZE_SCHEMA, RAW_PATH, nome_tabela, arquivo, opcoes)
        linhas = df.count()
        validar_tabela(spark, nome_tabela, arquivo, linhas)
        print(f"✅ bronze.{nome_tabela}: {linhas} linhas")

    print("✅ Camada Bronze concluída")


if __name__ == "__main__":
    main()

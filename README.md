# 📊 Olist E-commerce — Pipeline Medallion no Databricks

Pipeline de engenharia de dados end-to-end construído no **Databricks Free Edition**, usando o
dataset público [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
(Kaggle). O projeto implementa a arquitetura **Medallion (Bronze → Silver → Gold)**, com testes
de qualidade de dados em cada camada, orquestração via Databricks Jobs e um dashboard executivo
final.

---

## 🏗️ Arquitetura

```
Kaggle API (CSV)
   │
   ▼
Unity Catalog Volume (raw files)
   │
   ▼
🥉 BRONZE   — ingestão bruta, schema como string, metadados de auditoria
   │
   ▼
🥈 SILVER   — tipagem, deduplicação, tratamento de nulos, padronização,
              regras de negócio, validação (chave única + integridade referencial)
   │
   ▼
🥇 GOLD     — métricas de negócio agregadas, prontas para consumo em dashboard
   │
   ▼
📊 Dashboard (Databricks AI/BI) — Visão Geral do Negócio
```

Cada camada tem seu próprio notebook exploratório (documentando o processo de investigação e
decisão) e um script Python enxuto correspondente, pronto para rodar como Job em produção.

---

## 🧱 Stack

- **Databricks Free Edition** — compute serverless + Unity Catalog
- **PySpark / Delta Lake** — processamento e armazenamento
- **Unity Catalog Volumes** — armazenamento governado dos arquivos brutos
- **Databricks Jobs** — orquestração do pipeline (Bronze → Silver → Gold)
- **Databricks AI/BI Dashboards** — visualização final
- **Kaggle API** — ingestão automatizada do dataset

---

## 📁 Estrutura do repositório

```
.
├── notebooks/                          # Notebooks exploratórios (processo documentado)
│   ├── 1__API_Kaggle
│   ├── 2__Camada_Bronze
│   ├── 3__Camada_Silver
│   ├── 4__Camada_Gold_-_Dashboard
│   └── 4__Camada_Gold_-_Dicionario_de_Dados
├── src/                                 # Scripts Python enxutos (produção / Jobs)
│   ├── config.py                        # Configurações centralizadas
│   ├── utils.py                         # Funções compartilhadas (cast, dedup, quality log)
│   ├── bronze_ingestao.py
│   ├── silver_transformacao.py
│   └── gold_agregacoes.py
└── README.md
```

---

## 🗂️ Modelo de dados

### Camada Silver (9 tabelas)

| Tabela | Grão | Descrição |
|---|---|---|
| `orders` | 1 pedido | Tabela central — status, datas de compra/entrega |
| `customers` | 1 cliente por pedido | Localização e identificador único do cliente |
| `order_items` | 1 item de pedido | Produto, vendedor, preço, frete |
| `payments` | 1 transação de pagamento | Forma de pagamento, parcelas, valor |
| `reviews` | 1 avaliação | Nota (1-5) e comentário do cliente |
| `products` | 1 produto | Categoria, dimensões, peso |
| `sellers` | 1 vendedor | Localização do vendedor |
| `geolocation` | 1 CEP (agregado) | Coordenadas médias por prefixo de CEP |
| `category_translation` | 1 categoria | Tradução PT → EN |

### Camada Gold (5 tabelas — uma por painel do dashboard)

| Tabela | Pergunta de negócio que responde |
|---|---|
| `receita_mensal` | Estamos crescendo mês a mês? |
| `receita_por_estado` | Quais estados compram mais / têm maior ticket médio? |
| `performance_entrega_mensal` | Estamos entregando no prazo prometido? |
| `satisfacao_entrega` | Atraso na entrega afeta a nota do cliente? |
| `ranking_vendedores` | Quem são os melhores vendedores (volume + qualidade)? |

---

## 🔍 Decisões técnicas e problemas encontrados

**Bug de parsing no CSV de reviews.** O arquivo `olist_order_reviews_dataset.csv` tem campos de
texto livre (comentários) com quebras de linha e aspas internas, que corrompiam o parsing padrão
do Spark — colunas de data acabavam caindo na coluna `review_score`. Resolvido com
`multiLine=True`, `quote='"'` e `escape='"'` na leitura desse arquivo específico.

**`try_cast` em vez de `cast`.** Toda tipagem na Silver usa `try_cast`, que retorna `NULL` em vez
de lançar erro para valores malformados — o pipeline nunca quebra por um valor inesperado, e a
função `aplicar_tipos()` registra quantos valores foram afetados por conversão, para
rastreabilidade.

**Log de qualidade centralizado.** Toda checagem (chave única, integridade referencial,
reconciliação de valores entre camadas, intervalos válidos) é gravada em
`gold.quality_log`, criando um histórico auditável de execução, em vez de apenas prints
perdidos no console.

**Camada Gold no grão do consumo final.** Cada tabela Gold já está no grão exato que o dashboard
precisa (ex: uma linha por mês, uma linha por estado) — nenhuma agregação adicional é necessária
no lado do BI.

**Scripts enxutos separados dos notebooks.** Os notebooks documentam o processo de exploração,
diagnóstico e decisão (útil para leitura/aprendizado). Os scripts em `src/` contêm apenas a
lógica final validada, sem código exploratório, prontos para rodar como Job de produção.

---

## ⚙️ Como reproduzir

1. Criar workspace no [Databricks Free Edition](https://www.databricks.com/product/databricks-free-edition)
2. Gerar um token de API no Kaggle (não-legacy) e salvá-lo em um Volume do Unity Catalog
3. Rodar `notebooks/1__API_Kaggle` para baixar o dataset
4. Rodar os notebooks 2, 3 e 4 em sequência (ou usar os scripts de `src/` via Databricks Job)
5. Criar um Job com 3 tasks Python script (`bronze_ingestao.py` → `silver_transformacao.py` →
   `gold_agregacoes.py`), com dependência sequencial entre elas
6. Criar um AI/BI Dashboard sobre as tabelas de `gold.*`

---

## 📈 Dashboard

O dashboard "Visão Geral do Negócio" reúne os 5 painéis Gold em uma única página: evolução de
receita, distribuição geográfica, performance de entrega, satisfação do cliente e ranking de
vendedores.

*(adicionar print do dashboard aqui)*

---

## 📚 Fonte dos dados

[Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) — dataset público disponibilizado no Kaggle, com dados reais (anonimizados) de
e-commerce brasileiro entre 2016 e 2018.

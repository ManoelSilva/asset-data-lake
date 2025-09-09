[Read in English](README.md)

# Asset Data Lake

Asset Data Lake é um projeto Python para analisar, transformar e criar um data lake rico em features a partir de dados históricos da B3 (Bolsa de Valores do Brasil). Utiliza DuckDB, pandas e outras ferramentas modernas de dados para permitir análises eficientes e engenharia de atributos para ativos financeiros.

## Funcionalidades
- Faz o parsing de arquivos históricos da B3 (COTAHIST)
- Transforma dados brutos em features para ML/analytics
- Armazena e consulta dados usando DuckDB (MotherDuck)
- Fácil de estender para novas fontes de dados e transformações

## Estrutura do Projeto
```
asset-data-lake/
├── src/
│   ├── b3/
│   │   ├── parser.py         # Faz o parsing dos arquivos históricos da B3
│   │   ├── transformer.py    # Engenharia de features para dados da B3
│   ├── md_lake.py            # MotherDuckLakeService: gerencia o lake DuckDB
│   ├── lake_creator_app.py   # CLI para criar os lakes
├── requirements.txt          # Dependências Python
```

## Início Rápido
1. **Instale as dependências**
   ```bash
   pip install -r requirements.txt
   ```
2. **Prepare os dados da B3**
   - Coloque os arquivos COTAHIST em `src/b3/assets/` (ex: `COTAHIST_A2025.txt`)
3. **Execute o criador de lake**
   ```bash
   python src/lake_creator_app.py
   ```
   Isso irá analisar e transformar os dados da B3, criando tabelas DuckDB para dados brutos e com features.

## Componentes Principais
- `B3HistFileParser` (`src/b3/parser.py`): Faz o parsing dos arquivos históricos da B3 para DataFrames pandas.
- `B3Transformer` (`src/b3/transformer.py`): Cria features (retornos, volatilidade, momentum, etc.) a partir dos dados brutos.
- `MotherDuckLakeService` (`src/md_lake.py`): Gerencia a conexão DuckDB e criação de tabelas.
- `LakeCreatorApp` (`src/lake_creator_app.py`): App CLI que orquestra o processo.

## Dados de Exemplo
Arquivos de exemplo da B3 estão em `src/b3/assets/` para testes e desenvolvimento.

## Requisitos
- Python 3.8+
- pandas, numpy, pyarrow, requests, boto3, duckdb, python-dotenv

## Extensões
- Adicione novos parsers/transformers para outras classes de ativos em `src/`
- Modifique o `MotherDuckLakeService` para suportar novas tabelas ou análises

## Licença
MIT License

---
[Leia em português](README.pt-br.md)

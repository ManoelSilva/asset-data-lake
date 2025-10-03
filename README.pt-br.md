[Read in English](README.md)

# Asset Data Lake

Asset Data Lake é um projeto Python para analisar, transformar e criar um data lake rico em features a partir de dados
históricos da B3 (Bolsa de Valores do Brasil). Utiliza DuckDB, pandas e outras ferramentas modernas de dados para
permitir análises eficientes e engenharia de atributos para ativos financeiros.

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
- `B3Transformer` (`src/b3/transformer.py`): Cria features (retornos, volatilidade, momentum, etc.) a partir dos dados
  brutos.
- `MotherDuckLakeService` (`src/md_lake.py`): Gerencia a conexão DuckDB e criação de tabelas.
- `LakeCreatorApp` (`src/lake_creator_app.py`): App CLI que orquestra o processo.

## Dados de Exemplo

Arquivos de exemplo da B3 estão em `src/b3/assets/` para testes e desenvolvimento.

## Requisitos

- Python 3.8+
- pandas, numpy, pyarrow, requests, boto3, duckdb, python-dotenv

## API REST

O projeto inclui uma API REST baseada em Flask para acessar o data lake:

### Principais Endpoints

- `GET /asset/<ticker>` - Obter informações do ativo por símbolo
- `GET /assets` - Listar ativos disponíveis com busca e paginação
  - Parâmetros: `search`, `page`, `page_size`
- `POST /scheduled/b3-data-update` - Atualizar dados da B3 da fonte
- `GET /health` - Endpoint de verificação de saúde
- `GET /swagger` - Documentação interativa da API (Swagger UI)
- `GET /swagger.yaml` - Especificação OpenAPI

### Exemplos de Uso da API

```python
import requests

# Obter informações do ativo
response = requests.get("http://localhost:5002/asset/PETR4")
asset_data = response.json()

# Buscar ativos
response = requests.get("http://localhost:5002/assets?search=PETR&page=1&page_size=10")
assets = response.json()

# Atualizar dados da B3
response = requests.post("http://localhost:5002/scheduled/b3-data-update")
update_status = response.json()
```

## Configuração de Ambiente

### Variáveis de Ambiente Necessárias

```bash
export MOTHERDUCK_TOKEN="seu_token_motherduck"
export environment="AWS"  # ou "LOCAL" para desenvolvimento local
```

### Configuração de Desenvolvimento Local

1. **Instalar dependências**
   ```bash
   pip install -r requirements.txt
   ```

2. **Definir variáveis de ambiente**
   ```bash
   export MOTHERDUCK_TOKEN="seu_token"
   export environment="LOCAL"
   ```

3. **Executar o servidor da API**
   ```bash
   python src/web_api.py
   ```

## Fontes de Dados e Formatos

### Dados Históricos da B3 (COTAHIST)
- **Formato**: Arquivos de texto de largura fixa
- **Fonte**: B3 (Bolsa de Valores do Brasil)
- **Frequência de Atualização**: Diária
- **Nomenclatura**: `COTAHIST_AYYYY.txt` (anual), `COTAHIST_MMMYYYY.TXT` (mensal)

### Pipeline de Processamento de Dados
1. **Parsing**: Arquivos de largura fixa → DataFrames pandas
2. **Transformação**: Engenharia de features (retornos, volatilidade, momentum)
3. **Armazenamento**: Tabelas DuckDB (dados brutos e com features)
4. **Acesso via API**: Endpoints REST para recuperação de dados

## Deploy do Serviço

### Deploy de Produção (AWS EC2)

O serviço é projetado para rodar como um serviço systemd no AWS EC2:

```bash
# Deploy usando o script fornecido
sudo MOTHERDUCK_TOKEN=seu_token EC2_HOST=seu_ip bash deploy_asset_data_lake.sh
```

### Gerenciamento do Serviço

```bash
# Verificar status do serviço
sudo systemctl status asset-data-lake

# Ver logs
sudo journalctl -u asset-data-lake -f

# Reiniciar serviço
sudo systemctl restart asset-data-lake
```

## Schema do Data Lake

### Tabelas

- **b3_hist**: Dados históricos brutos da B3
- **b3_featured**: Dados processados com features engenheiradas
- **Asset metadata**: Informações da empresa e mapeamentos de ticker

### Features Engenheiradas

- **Baseadas em preço**: Retornos, volatilidade, médias móveis
- **Baseadas em volume**: Tendências de volume, métricas de liquidez
- **Indicadores técnicos**: RSI, MACD, Bandas de Bollinger
- **Indicadores de mercado**: Performance do setor, capitalização de mercado

## Extensões

- Adicione novos parsers/transformers para outras classes de ativos em `src/`
- Modifique o `MotherDuckLakeService` para suportar novas tabelas ou análises
- Estenda a API com novos endpoints para análises customizadas
- Adicione validação de dados e verificações de qualidade

## Licença

MIT License

---
[Leia em português](README.pt-br.md)

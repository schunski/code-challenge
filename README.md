# CODE CHALLENGE

## Pipeline de Extração, Transformação e Carregamento (ETL)

Neste projeto, enfrentamos o desafio de criar uma pipeline eficiente para extrair dados de um banco de dados inicial, transformá-los de maneira apropriada e carregá-los em um banco de dados final, culminando na impressão dos dados consolidados.

## Para implementar e usar a solução, siga os seguintes passos:

Configuração Inicial:

Certifique-se de ter o Docker instalado em sua máquina.

Abra o terminal e navegue até a pasta do projeto.

Execute `docker-compose build` para construir as imagens necessárias.

Em seguida, execute `docker-compose up` para iniciar os serviços conforme definido no **docker-compose.yml.**

Execução dos Scripts:

Os scripts Python foram desenvolvidos para serem executados manualmente.

Abra o terminal ou uma IDE de sua escolha.

Execute o script export.py para extrair os dados das tabelas do banco de dados inicial. Os dados serão salvos em arquivos CSV dentro do diretório:
```
"/data/postgres/data_atual/nome_tabela.csv"
```
Após a extração, execute o script import.py para importar os dados dos arquivos CSV para o banco de dados final. Isso resultará na criação de um arquivo CSV consolidado contendo todas as tabelas e linhas importadas em:
```
"/result_csv/result.csv"
```
Resultado Final:

Após a execução bem-sucedida dos scripts, um arquivo CSV final com todos os dados das tabelas estará disponível para análise.
O projeto demonstra a habilidade de transferir dados entre bancos de dados, mantendo a organização e a integridade dos dados.

PySpark-Docker: Ambientes de Desenvolvimento com PySpark e Jupyter

Este repositório contém um ambiente pré-configurado para executar PySpark com Jupyter Notebook usando Docker. O objetivo é fornecer uma plataforma prática e replicável para testes e desenvolvimento de projetos de engenharia de dados.

Instruções para Configuração do Ambiente:


Passo 1: Clonar o Repositório

Primeiro, clone este repositório em sua máquina local:


    git clone https://github.com/Esdras-Emerson/pyspark-docker.git

    cd pyspark-docker

Passo 2: Instalar Docker Desktop

    Certifique-se de ter o Docker instalado em sua máquina. Se ainda não o fez, baixe e instale o Docker Desktop.

Passo 3: Preparar o Projeto

    Criar um Diretório: Crie uma nova pasta em seu sistema para armazenar os arquivos do projeto.

    mkdir pyspark-docker
    cd pyspark-docker

    Adicionar o Arquivo docker-compose.yaml: Coloque o arquivo docker-compose.yaml no diretório criado. Esse arquivo define os serviços necessários para rodar PySpark e Jupyter Notebook em contêineres Docker.

Passo 4: Configurar o Volume

    Edite o caminho do volume no arquivo docker-compose.yaml para apontar para o diretório onde seus notebooks e dados estão armazenados:

    volumes:
    - /caminho/para/seu/diretorio:/home/

Passo 5: Iniciar o Ambiente

    Com o arquivo docker-compose.yaml configurado, você pode iniciar os contêineres com o seguinte comando:

    docker compose up

Passo 6: Acessar o Jupyter Notebook

    Depois que os contêineres estiverem rodando, abra o navegador e acesse a URL:


    http://127.0.0.1:8888/?token=SEU_TOKEN

    Dica: O token é exibido no terminal onde você executou o docker compose up.

Passo 7: Executar o Notebook

Abra o notebook "teste de conhecimento.ipynb" na interface do Jupyter em seu navegador. O arquivo com extensão .ipynb está dentro da pasta "jovyan" e execute as células conforme as instruções.


Descrição do Caso:

Parte 1: Manipulação de Dados

    - Criação de DataFrame: Crie um DataFrame PySpark com um conjunto de dados fictício.

        from pyspark.sql import SparkSession

        # Criando uma sessão do Spark
        spark = SparkSession.builder.appName("PySpark Interview Test").getOrCreate()

        # Dados fornecidos
        data = [
            ("Alice", 34, "Data Scientist"),
            ("Bob", 45, "Data Engineer"),
            ("Cathy", 29, "Data Analyst"),
            ("David", 35, "Data Scientist")
        ]
        columns = ["Name", "Age", "Occupation"]

    # Criando o DataFrame
    df = spark.createDataFrame(data, schema=columns)

    # Mostrando o DataFrame
    df.show()

    - Filtragem e Seleção: Selecione as colunas "Name" e "Age" e filtre as linhas onde "Age" é maior que 30.

        # Seleção das colunas "Name" e "Age"
    selected_df = df.select("Name", "Age")

    # Filtragem onde "Age" é maior que 30
    filtered_df = selected_df.filter(selected_df.Age > 30)

    # Mostrando o DataFrame filtrado
    filtered_df.show()

    - Agrupamento e Agregação: Agrupe os dados por "Occupation" e calcule a média de idade para cada grupo.

        from pyspark.sql.functions import avg

    # Agrupamento por "Occupation" e cálculo da média de "Age"
    grouped_df = df.groupBy("Occupation").agg(avg("Age").alias("Average_Age"))

    # Mostrando o DataFrame agrupado
    grouped_df.show()

    - Ordenação: Ordene os resultados pela média de idade em ordem decrescente.
# Ordenando em ordem decrescente pela média de "Age"
sorted_df = grouped_df.orderBy(grouped_df.Average_Age.desc())

# Mostrando o DataFrame ordenado
sorted_df.show()



Parte 2: Funções Avançadas

    - UDFs (User Defined Functions): Crie uma função em Python que classifica as idades em categorias ("Jovem", "Adulto", "Senior") e aplique essa função ao DataFrame usando uma UDF.
    - Funções de Janela: Adicione uma coluna ao DataFrame que mostre a diferença de idade entre cada indivíduo e a média de idade do seu grupo de ocupação.

Parte 3: Performance e Otimização

    - Particionamento: Explicação sobre como o particionamento pode melhorar a performance de operações de leitura/escrita e exemplo prático de particionamento de um DataFrame.
    - Broadcast Join: Descrição e exemplo de como usar Broadcast Join para otimizar operações de join em PySpark.

Parte 4: Integração com Outras Tecnologias

    L- eitura e Escrita de Dados: Demonstração de como ler dados de um arquivo CSV e salvá-los em formato Parquet.
    - Integração com Hadoop: Exemplos de leitura e escrita de dados no Hadoop HDFS usando PySpark.

Parte 5: Problema de Caso - Processamento de Logs

    - Carregar Arquivo de Log: Importe um arquivo de log contendo "timestamp", "user_id" e "action" em um DataFrame.
    - Análise de Logs: Conte o número de ações por usuário e identifique os 10 usuários mais ativos.
    - Exportar Resultado: Salve o resultado final em um arquivo CSV.




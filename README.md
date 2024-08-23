
# PySpark-Docker: Ambiente de Desenvolvimento com PySpark e Jupyter

Este repositório contém um ambiente pré-configurado para executar PySpark com Jupyter Notebook usando Docker. O objetivo é fornecer uma plataforma prática e replicável para testes e desenvolvimento de projetos de engenharia de dados.

## Instruções para Configuração do Ambiente

### Passo 1: Clonar o Repositório

Primeiro, clone este repositório em sua máquina local:

```bash
git clone https://github.com/Esdras-Emerson/pyspark-docker.git
cd pyspark-docker
```

### Passo 2: Instalar Docker Desktop

Certifique-se de ter o Docker instalado em sua máquina. Se ainda não o fez, [baixe e instale o Docker Desktop](https://www.docker.com/products/docker-desktop).

### Passo 3: Preparar o Projeto

- **Criar um Diretório:** Crie uma nova pasta em seu sistema para armazenar os arquivos do projeto.

    ```bash
    mkdir pyspark-docker
    cd pyspark-docker
    ```

- **Adicionar o Arquivo `docker-compose.yaml`:** Coloque o arquivo `docker-compose.yaml` no diretório criado. Esse arquivo define os serviços necessários para rodar PySpark e Jupyter Notebook em contêineres Docker.

### Passo 4: Configurar o Volume

Edite o caminho do volume no arquivo `docker-compose.yaml` para apontar para o diretório onde seus notebooks e dados estão armazenados:

```yaml
volumes:
  - /caminho/para/seu/diretorio:/home/jovyan/work
```

### Passo 5: Iniciar o Ambiente

Com o arquivo `docker-compose.yaml` configurado, inicie os contêineres com o seguinte comando:

```bash
docker compose up
```

### Passo 6: Acessar o Jupyter Notebook

Depois que os contêineres estiverem rodando, abra o navegador e acesse a URL:

```
http://127.0.0.1:8888/?token=SEU_TOKEN
```

> **Dica:** O token é exibido no terminal onde você executou o comando `docker compose up`.

### Passo 7: Executar o Notebook

Abra o notebook **"teste de conhecimento.ipynb"** na interface do Jupyter em seu navegador. O arquivo com extensão `.ipynb` está dentro da pasta `jovyan/work`. Execute as células conforme as instruções.

---

## Descrição do Caso

### Parte 1: Manipulação de Dados

- **Criação de DataFrame:** Crie um DataFrame PySpark com o seguinte conjunto de dados fictício:

    ```python
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
    ```

- **Filtragem e Seleção:** Selecione as colunas "Name" e "Age" e filtre as linhas onde "Age" é maior que 30:

    ```python
    # Seleção das colunas "Name" e "Age"
    selected_df = df.select("Name", "Age")

    # Filtragem onde "Age" é maior que 30
    filtered_df = selected_df.filter(selected_df.Age > 30)

    # Mostrando o DataFrame filtrado
    filtered_df.show()
    ```

- **Agrupamento e Agregação:** Agrupe os dados por "Occupation" e calcule a média de idade para cada grupo:

    ```python
    from pyspark.sql.functions import avg

    # Agrupamento por "Occupation" e cálculo da média de "Age"
    grouped_df = df.groupBy("Occupation").agg(avg("Age").alias("Average_Age"))

    # Mostrando o DataFrame agrupado
    grouped_df.show()
    ```

- **Ordenação:** Ordene os resultados pela média de idade em ordem decrescente:

    ```python
    # Ordenando em ordem decrescente pela média de "Age"
    sorted_df = grouped_df.orderBy(grouped_df.Average_Age.desc())

    # Mostrando o DataFrame ordenado
    sorted_df.show()
    ```

### Parte 2: Funções Avançadas

- **UDFs (User Defined Functions):** Crie uma função em Python que classifica as idades em categorias ("Jovem", "Adulto", "Senior") e aplique essa função ao DataFrame usando uma UDF:

    ```python
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType

    # Definindo a função em Python
    def classify_age(age):
        if age < 30:
            return "Jovem"
        elif 30 <= age <= 40:
            return "Adulto"
        else:
            return "Senior"

    # Registrando a UDF no PySpark
    classify_age_udf = udf(classify_age, StringType())

    # Aplicando a UDF ao DataFrame
    df_with_age_category = df.withColumn("Age_Category", classify_age_udf(df.Age))

    # Mostrando o DataFrame com a nova coluna
    df_with_age_category.show()
    ```

- **Funções de Janela:** Adicione uma coluna ao DataFrame que mostre a diferença de idade entre cada indivíduo e a média de idade do seu grupo de ocupação:

    ```python
    from pyspark.sql.window import Window
    from pyspark.sql.functions import col, avg

    # Definindo a janela
    window_spec = Window.partitionBy("Occupation")

    # Calculando a média de idade e a diferença para cada indivíduo
    df_with_age_diff = df.withColumn("Age_Diff", col("Age") - avg("Age").over(window_spec))

    # Mostrando o DataFrame com a nova coluna
    df_with_age_diff.show()
    ```

### Parte 3: Performance e Otimização

- **Particionamento:** Explicação sobre como o particionamento pode melhorar a performance de operações de leitura/escrita:
    
    **O particionamento de dados é uma técnica que divide um conjunto de dados grande em partes menores e gerenciáveis, chamadas de "partições"**
    **Traz melhoria na leitura por conseguir acessar diretamente a partição em questão, na escrita o sistema pode escrever diretamente na partição correta**

    
    Exemplo prático de particionamento de um DataFrame:

    ```python
    # Particionando o DataFrame pela coluna "Occupation"
    partitioned_df = df.repartition(4, "Occupation")

    # Salvando o DataFrame particionado em disco (apenas exemplo, não executa no Docker local)
    partitioned_df.write.partitionBy("Occupation").parquet("../saida")
    ```

- **Broadcast Join:** Descrição:

    **Processo de otimização na execução de operações de junção (join) em grandes volumes de dados**
    **Ajuda na melhor performace na redução de movimentação de dados, junção local do nó e escalabilidade**

    Exemplo de como usar Broadcast Join para otimizar operações de join em PySpark:

    ```python
    from pyspark.sql.functions import broadcast

    # Criando outro DataFrame para exemplo de join
    job_data = [("Data Scientist", "DS"), ("Data Engineer", "DE"), ("Data Analyst", "DA")]
    job_df = spark.createDataFrame(job_data, ["Occupation", "Job_Code"])

    # Executando o Broadcast Join
    joined_df = df.join(broadcast(job_df), "Occupation")

    # Mostrando o DataFrame resultante
    joined_df.show()
    ```

### Parte 4: Integração com Outras Tecnologias

- **Leitura e Escrita de Dados:** Demonstração de como ler dados de um arquivo CSV e salvá-los em formato Parquet:

    ```python
    # Leitura de um arquivo CSV
    csv_df = spark.read.csv("../arquivo.csv", header=True, inferSchema=True)

    # Salvando o DataFrame em formato Parquet
    csv_df.write.parquet("../saida_parquet")
    ```

- **Integração com Hadoop:** Exemplos de leitura e escrita de dados no Hadoop HDFS usando PySpark (genérico):

    ```python
    # Leitura de um arquivo do HDFS
    hdfs_df = spark.read.csv("hdfs:///arquivo.csv", header=True, inferSchema=True)

    # Salvando o DataFrame de volta no HDFS em formato Parquet
    hdfs_df.write.parquet("hdfs:////saida_parquet")
    ```

### Parte 5: Problema de Caso - Processamento de Logs

- **Carregar Arquivo de Log:** Importe um arquivo de log contendo "timestamp", "user_id" e "action" em um DataFrame:

    ```python
    # Leitura do arquivo de log
    log_df = spark.read.csv("/logs.csv", header=True, inferSchema=True)
    ```

- **Análise de Logs:** Conte o número de ações por usuário e identifique os 10 usuários mais ativos:

    ```python
    # Contagem de ações por usuário
    user_action_count = log_df.groupBy("user_id").count()

    # Identificação dos 10 usuários mais ativos
    top_users =

    user_action_count.orderBy("count", ascending=False).limit(10)

    # Mostrando os resultados
    top_users.show()
    ```

- **Exportar Resultado:** Salve o resultado final em um arquivo CSV:

    ```python
    # Salvando o resultado em um arquivo CSV
    top_users.write.csv("../saida_usuarios_ativos")
    ```





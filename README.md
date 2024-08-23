PySpark-Docker: Ambientes de Desenvolvimento com PySpark e Jupyter

Este repositório contém um ambiente pré-configurado para executar PySpark com Jupyter Notebook usando Docker. O objetivo é fornecer uma plataforma prática e replicável para testes e desenvolvimento de projetos de engenharia de dados.
Instruções para Configuração do Ambiente

Passo 1: Instalar Docker Desktop

    Certifique-se de ter o Docker instalado em sua máquina. Se ainda não o fez, baixe e instale o Docker Desktop.

Passo 2: Preparar o Projeto

    Criar um Diretório: Crie uma nova pasta em seu sistema para armazenar os arquivos do projeto.

    mkdir pyspark-docker
    cd pyspark-docker

    Adicionar o Arquivo docker-compose.yaml: Coloque o arquivo docker-compose.yaml no diretório criado. Esse arquivo define os serviços necessários para rodar PySpark e Jupyter Notebook em contêineres Docker.

Passo 3: Configurar o Volume

    Edite o caminho do volume no arquivo docker-compose.yaml para apontar para o diretório onde seus notebooks e dados estão armazenados:

    volumes:
    - /caminho/para/seu/diretorio:/home/jovyan/work

Passo 4: Iniciar o Ambiente

    Com o arquivo docker-compose.yaml configurado, você pode iniciar os contêineres com o seguinte comando:

    docker compose up

Passo 5: Acessar o Jupyter Notebook

    Depois que os contêineres estiverem rodando, abra o navegador e acesse a URL:


    http://127.0.0.1:8888/?token=SEU_TOKEN

    Dica: O token é exibido no terminal onde você executou o docker compose up.

Passo 6: Executar o Notebook

Abra o notebook "teste de conhecimento.ipynb" na interface do Jupyter em seu navegador. O arquivo com extensão .ipynb está dentro da pasta "jovyan" e execute as células conforme as instruções.


Descrição do Caso de Estudo
Parte 1: Manipulação de Dados

    - Criação de DataFrame: Crie um DataFrame PySpark com um conjunto de dados fictício.
    - Filtragem e Seleção: Selecione as colunas "Name" e "Age" e filtre as linhas onde "Age" é maior que 30.
    - Agrupamento e Agregação: Agrupe os dados por "Occupation" e calcule a média de idade para cada grupo.
    - Ordenação: Ordene os resultados pela média de idade em ordem decrescente.

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




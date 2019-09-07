# AWS Glue + Amazon Comprehend

Escalando análise de sentimento com AWS Glue e Amazon Comprehend.

## Pré-requisitos

-   Criar um Bucket no `S3` que será utilizado como output
-   Criar um `IAM Role` que da premissão ao AWS Glue de escrever no seu bucket no S3 e acesso ao Amazon Comprehend.

## Parte I - Testando o código

Para testar o código que realizará o Job no AWS Glue utilizaremos o Jupyter Notebook do próprio AWS Glue.

Para criar o Notebook é necessário primeiro criar um `Dev endpoint`. Para isso:

1.  Entre na página no AWS Glue
2.  No menu da esquerda clique em `Dev endpoints`
3.  Na tela de Dev endpoints, clique no botão azul 'Add endpoint'
4.  Insira um nome no endopoit e associe a IAM role criada nos pré-requisitos.
5.  Abra a guia de `Security configuration, script libraries, and job parameters (optional)` e altere os `Data processing units (DPUs)` de 5 para **6**. Esse problema não executa corretamente com menos de `12 DPUs`
6.  Mantenha as demais opções com o padrão e vá dando `Next` até criar o endpoint
7.  Espere cerca de 5 minutos para a criação do endpoint (até o **Provisioning status** do endpoint mudar para **READY**)

Com o endpoint criado, já podemos criar o Notebook.

8.  No menu da esquerda do AWS Glue clique em `Notebooks`
9.  Na tela do Notebook, seleciona 'SageMaker notebooks' e clique no botão azul 'Create notebook'
10. Insira um nome para o notebook
11. Associe o endpoint criado anteriormente
12. Selecione `Create an IAM role`
    -   Coloque um nome para a nova role
    -   A role completa ficará como AWSGlueServiceSageMakerNotebookRole-<NOME_DA_ROLE>
13. Mantenha as demais opções com o padrão e clique em `Create notebook`
14.  Espere cerca de 5 minutos para a criação do notebook
15. Com o notebook criado, selecione-o e clique em `Open notebook`
16. Já com o notebook aberto, selecione `new > Sparkmagic (PySpark)` para criar um novo notebook com kernel de **PySpark**
17. Agora você pode copiar o código do arquivo `glue_compreend_amazon_reviews_job.py` e executá-lo em blocos nas células do notebook.
    ```
    A ideia aqui é executar o código aos poucos, parte por parte, para entender o funcionamento e executar degub. Então separe tudo em pequenas células.
    ```
    -   Lembre de alterar a linha 22 do arquivo, inserindo o nome do Bucket criado nos pré-requisitos
    ```
    S3_BUCKET = '<<BUCKET_NAME>>'  # Insert your Bucket Name
    ````
18. Ao terminar o programa criará uma pasta chamada `notebook` dentro do seu Bucket, contendo o resultado a execução feita pelo SageMaker Notebook


Com o programa devidamente validado, podemos iniciar o Job do Glue.

## Parte II - Executando o Job do Glue

1.  Entre na página no AWS Glue
2.  No menu da esquerda clique em `Jobs`
3.  Na tela de Jobs, clique no botão azul 'Add Job'
4.  Insira o nome do Job
5.  Selecione a IAM role criada nos pré-requisitos
6.  Em `Type`, selecione **Spark**
7.  Em `Glue version`selecione **Spark 2.4, Python 3 (glue version 1.0)**
8.  Em `This job runs`, selecione a opção **A new script to be authored by you**
9. Insira o nome do script
10. Abra a guia de `Monitoring options` e troque a opção `Continuous logging` para ***Enable*
11.  Abra a guia de `Security configuration, script libraries, and job parameters (optional)` e altere `Maximum capacity` para 12 e clique em `Next`
12. Na Parte de connections, simplesmente clique em `Save job and edit script`
13. O AWS Glue abrirá uma página onde poderemos inserir o script. Copie e cole o conteúdo do script `glue_compreend_amazon_reviews_job.py` na parte de edição do Job
14. Salve e Clique em `Run job`
15. Aguarda alguns minutos para a execução do Job. Demora em média 10 minutos com 6 DPUs
16. Ao terminar o programa criará uma pasta chamada `glue` dentro do seu Bucket, contendo o resultado a execução feita pelo job do glue

##  Encerrando o ambiente [IMPORTANTE]

Lembre-se de deletar os recursos criados para não ter gastos inesperados na sua conta. Delete:

- Dev endpoint
- notebook
- job
- buckets s3

##  Erros Comuns:

-   `DPU` menor que 12 usando o notebook
-   `Maximum capacity` manor que 12 usando o job
-   `Comprehend ThrottlingException` - isso é uma limitação da AWS, então ou você tem que rodar em outra conta/região ou abrir um ticket pedindo para aumentar a capacidade da API do Comprehend.


## Referência

O link the referência mostra como configurar o `Amazon Athena` para realizar Queries em cima do resultado gerado pelo job e usa o `Amazon QuickSight` para gerar dashboards (visualização de dados).

-   https://aws.amazon.com/blogs/machine-learning/how-to-scale-sentiment-analysis-using-amazon-comprehend-aws-glue-and-amazon-athena/
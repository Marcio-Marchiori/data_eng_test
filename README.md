# Teste - Engenharia de Dados

## Comentário Sobre a Arquitetura

A ideia da arquitetura apresentada é montar toda a esteira de deploy da infra numa repo do GitHub utilizando Terraform para subir os recursos a serem utilizados. O Airflow foi considerado por ser uma ferramenta versátil de orquestração, onde podemos consumir os dados da API, inserir e atualizar os dados no Redshift numa estrutura lakehouse, enviar notificações e acompanhar o andamento das execuções dos DAGs.

Pensando em boas práticas na construção da pipeline foi considerado o uso de 3 zonas para os dados: Raw, Staging e Consume. Para processar os dados entre as zonas foi considerado o uso de 2 serviços da AWS: EMR e Glue Jobs, pensando na escalabilidade e no custo do serviço o EMR acabou sendo o escolhido, caso o volume de dados e número de utilizações permaneça baixo o Glue Jobs poderia ser uma alternativa interessante.

O uso das Lambdas para triggar o EMR foi apenas um exemplo de funções dentro da AWS que podem ser utilizadas, porém o próprio Airflow poderia servir como trigger para os scripts rodando no EMR. A opção por subir o Airflow num ambiente EKS ao invés de utilizar MWAA ou então uma EC2 se dá por conta da diferença de custo entre as plataformas, onde o EKS custa em torno 10% apenas do que utilizar MWAA e metade do custo de uma EC2 considerando uso 24 horas por dia.

E todos esses serviços rodando dentro de uma VPC com controle de acessos e tráfego bem definidos para evitar qualquer acesso indesejado.

A ideia com mais informações sobre volumetria e escalabilidade da infra seria escolher definitivamente entre EMR e Glue, eliminar as Lambdas ou caso o volume seja muito baixo consumir diretamente pelas Lambdas e eliminar o Airflow da infra.

## Comentário sobre exercícios 1 a 3

Existem diversas oportunidades de melhoria, porém a ideia foi criar código legível e reaproveitável, uma oportunidade para melhorar consideravelmente a velocidade de execução é no exercício 01, onde alterar o uso de groupby para rdd demonstraria ganhos consideráveis de performance.

Como não foi explícito o modo de entrega dos dados foi inserido a opção de salvar um parquet no primeiro exercício, parquet que poderia ser consumido por outros recursos.

Para o terceiro exercício não sei se foi interpretado da maneira correta a expansão da coluna, se deveria quebrar apenas em itens ou se já poderia ter partido para um explode de todos os subitens do array, para separar em dois DFs distintos existiam duas opções de chave, optei por utilizar as duas, mas num use case real apenas chave já atenderia.

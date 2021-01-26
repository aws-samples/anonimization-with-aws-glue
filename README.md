## Anonimização com AWS Glue

Este código fonte se refere a este [artigo](https://aws.amazon.com/pt/blogs/aws-brasil/como-anonimizar-seus-dados-usando-o-aws-glue/)

1. Notebook Jupyter com todo o script para testes:

    - [Notebook Jupyter](/portuguese/Anonimizacao.ipynb)

    - Para executar o notebook, você precisará de um Glue Development Endpoint. Mais informações em: [Glue Development Endpoint](https://docs.aws.amazon.com/pt_br/glue/latest/dg/dev-endpoint.html)

    - Development Endpoints do Glue tem custo. Para mais informações: [Preço do development endpoint do Glue](https://aws.amazon.com/pt/glue/pricing/)

2. Script a ser importado no job do AWS Glue:

    - [Script em Pyspark](/portuguese/script-glue.py)

    - Para importar um script ao criar um job no AWS Glue, visite a [documentação](https://docs.aws.amazon.com/pt_br/glue/latest/dg/add-job.html)

3. Arquivo .CSV de exemplo

    - Um arquivo .CSV de exemplo para uso simulando a origem dos dados por ser obtido [aqui](/portuguese/pacientes-raw.csv) 


## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.


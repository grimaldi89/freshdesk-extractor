# bigquery.py

"""
Arquivo bigquery.py
Propósito: Este módulo interage com o BigQuery usando diferentes estratégias de carregamento de dados.
Autor: Rodolfo Grimaldi
Data de criação: 16/05/2024
Última atualização: 16/05/2024
"""

import json
import logging
from google.cloud import bigquery
from abc import ABC, abstractmethod
from utils import Utils

class Bigquery:
    """
    Classe principal para interagir com o BigQuery.
    
    Parâmetros:
    - job (BigqueryPattern): Instância de uma estratégia de carregamento de dados específica.
    """
    def __init__(self, job) -> None:
        self.client = bigquery.Client()
        self._job = job

    @property
    def job(self):
        """
        Propriedade para obter a estratégia de carregamento de dados atual.
        """
        return self._job

    @job.setter
    def job(self, job) -> None:
        """
        Define uma nova estratégia de carregamento de dados.
        
        Parâmetros:
        - job (BigqueryPattern): Nova estratégia de carregamento de dados a ser utilizada.
        """
        self._job = job

    def run_job(self, params) -> None:
        """
        Executa um trabalho de carregamento de dados usando a estratégia configurada.
        
        Parâmetros:
        - params (dict): Parâmetros do trabalho de carregamento de dados.
        """
        self._job.load_job(self.client, params)

class BigqueryPattern(ABC):
    """
    Classe abstrata base para diferentes estratégias de carregamento de dados no BigQuery.
    """
    @abstractmethod
    def load_job(self, client, params):
        """
        Método abstrato que deve ser implementado pelas estratégias concretas.
        
        Parâmetros:
        - client (bigquery.Client): Cliente do BigQuery.
        - params (dict): Parâmetros do trabalho de carregamento de dados.
        """
        pass

class BigqueryUtils:
    """
    Classe utilitária para auxiliar nas operações relacionadas ao BigQuery.
    """
    def schema_extractor(self, json_data):
        """
        Constrói o esquema do BigQuery a partir de um JSON de esquema.
        
        Parâmetros:
        - json_data (dict): Dados JSON contendo o esquema.
        
        Retorno:
        - list: Lista de objetos SchemaField do BigQuery.
        """
        bigquery_schema = []
        for field in json_data:
            field_name = field["name"]
            field_type = field["type"]
            field_mode = field.get("mode", "NULLABLE")

            if field_type == "RECORD":
                subfields = self.schema_extractor(field["fields"])
                bigquery_schema.append(bigquery.SchemaField(field_name, field_type, field_mode, fields=subfields))
            else:
                bigquery_schema.append(bigquery.SchemaField(field_name, field_type, field_mode))

        return bigquery_schema

    def schema(self, schema_name):
        """
        Carrega o esquema do BigQuery a partir de um arquivo JSON.
        
        Parâmetros:
        - schema_name (str): Nome do esquema a ser carregado.
        
        Retorno:
        - list: Lista de objetos SchemaField do BigQuery ou None se o esquema não for encontrado.
        """
        json_file_path = 'schema.json'
        with open(json_file_path, 'r') as json_file:
            data = json.load(json_file)
        if schema_name in data:
            return self.schema_extractor(data[schema_name])
        else:
            return None

    def write_disposition(self, disposition_option):
        """
        Obtém a configuração de disposição de escrita do BigQuery.
        
        Parâmetros:
        - disposition_option (str): Opção de disposição de escrita.
        
        Retorno:
        - WriteDisposition: Objeto de disposição de escrita do BigQuery.
        """
        options = {
            "WRITE_APPEND": bigquery.WriteDisposition.WRITE_APPEND,
            "WRITE_TRUNCATE": bigquery.WriteDisposition.WRITE_TRUNCATE,
            "WRITE_EMPTY": bigquery.WriteDisposition.WRITE_EMPTY
        }
        return options[disposition_option]

    def create_disposition(self, create_option):
        """
        Obtém a configuração de disposição de criação do BigQuery.
        
        Parâmetros:
        - create_option (str): Opção de disposição de criação.
        
        Retorno:
        - CreateDisposition: Objeto de disposição de criação do BigQuery.
        """
        options = {
            "CREATE_IF_NEEDED": bigquery.CreateDisposition.CREATE_IF_NEEDED,
            "CREATE_NEVER": bigquery.CreateDisposition.CREATE_NEVER
        }
        return options[create_option]

class InsertJson(BigqueryPattern, BigqueryUtils):
    """
    Estratégia de carregamento de dados JSON no BigQuery.
    """
    def load_job(self, client, params):
        """
        Carrega dados JSON no BigQuery usando a configuração fornecida.
        
        Parâmetros:
        - client (bigquery.Client): Cliente do BigQuery.
        - params (dict): Parâmetros do trabalho de carregamento de dados.
        
        Retorno:
        - bool: True se o carregamento foi bem-sucedido, caso contrário False.
        """
        try:
            params = params["function_parameters"]
            schema_name = params["schema_name"]
            json_data = params["data"]
            project_name = params["project_name"]
            dataset_name = params["dataset_name"]
            table_name = Utils.get_table_name(params["table_name"])

            if not json_data:
                print("Sem dados para inserir")
                return True

            write_disposition = params.get("write_disposition", "WRITE_APPEND")
            create_disposition = params.get("create_disposition", "CREATE_IF_NEEDED")

            autodetect = False
            schema = self.schema(schema_name)
            if not schema:
                autodetect = True

            job_config = bigquery.LoadJobConfig(
                schema=schema,
                autodetect=autodetect,
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=self.write_disposition(write_disposition),
                create_disposition=self.create_disposition(create_disposition)
            )

            table_ref = client.dataset(dataset_name).table(table_name)
            load_job = client.load_table_from_json(json_data, table_ref, job_config=job_config)
            load_job.result()
            print("Dados JSON carregados com sucesso na tabela", table_name)
            return True
        except Exception as erro:
            logging.error(erro)
            return False

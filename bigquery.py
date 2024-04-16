import json
import logging
from google.cloud import bigquery
from abc import ABC, abstractmethod
from utils import Utils

class Bigquery:

    def __init__(self,job) -> None:

      self.client = bigquery.Client()
      self._job = job

    @property
    def job(self):
      return self._job

    @job.setter
    def job(self, job) -> None:
      self._job = job

    def run_job(self,params) -> None:
      self._job.load_job(self.client,params)

class BigqueryPattern(ABC):

    @abstractmethod
    def load_job(self,params):
      pass
class BigqueryUtils():
    def schema_extractor(self,json_data):
        """
        Constrói o esquema do BigQuery a partir de um JSON de esquema.
        """
        bigquery_schema = []
        for field in json_data:
            field_name = field["name"]
            field_type = field["type"]
            field_mode = field.get("mode", "NULLABLE")

            if field_type == "RECORD":
                print(field)
                subfields = self.schema_extractor(field["fields"])
                bigquery_schema.append(bigquery.SchemaField(field_name,field_type,field_mode,fields=subfields))
            else:
                print(field)
                bigquery_schema.append(bigquery.SchemaField(field_name,field_type,field_mode,))

        return bigquery_schema

    def schema(self, schema_name):
        json_file_path = 'schema.json'
        with open(json_file_path, 'r') as json_file:
            data = json.load(json_file)
        if schema_name in data:
            return self.schema_extractor(data[schema_name])
        else:
            return None

    def write_disposition(self,disposition_option="WRITE_APPEND"):
        options = {
            "WRITE_APPEND":bigquery.WriteDisposition.WRITE_APPEND,
            "WRITE_TRUNCATE":bigquery.WriteDisposition.WRITE_TRUNCATE,
            "WRITE_EMPTY":bigquery.WriteDisposition.WRITE_EMPTY
        }
        return options[disposition_option]

    def create_disposition(self,create_option="CREATE_IF_NEEDED"):
        options = {
            "CREATE_IF_NEEDED":bigquery.CreateDisposition.CREATE_IF_NEEDED,
            "CREATE_NEVER":bigquery.CreateDisposition.CREATE_NEVER
        }
        return options[create_option]

class InsertJson(BigqueryPattern,BigqueryUtils):

    def load_job(self,client,params):
        try:


            params = params["function_parameters"]
            schema_name = params["schema_name"]
            json_data = params["data"]
            project_name = params["project_name"]
            dataset_name = params["dataset_name"]
            table_name = Utils.get_table_name(params["table_name"])
            write_disposition = "WRITE_APPEND"

            if "write_disposition" in params:
                write_disposition = params["write_disposition"]

            create_disposition = "CREATE_IF_NEEDED"

            if "create_disposition" in params:
                create_disposition = params["create_disposition"]
            autodetect = False
            schema = self.schema(schema_name)
            if not schema:
                autodetect = True

            job_config = bigquery.LoadJobConfig(

                schema=schema,
                autodetect=autodetect,
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition= self.write_disposition(write_disposition),
                create_disposition= self.create_disposition(create_disposition)


            )
            table_ref = client.dataset(dataset_name).table(table_name)
            load_job = client.load_table_from_json(json_data, table_ref, job_config=job_config)
            load_job.result()
            print("Dados JSON carregados com sucesso na tabela", table_name)
            return True
        except Exception as erro:
            logging.error(erro)

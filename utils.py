
import requests
import time
import logging
import base64
import re
from datetime import datetime, timedelta
import json
from google.api_core.exceptions import NotFound


class Freshdesk:
    def __init__(self,bigquery,api_key,project_name,dataset_name,update_last_month=False):
        self.client = bigquery.Client()
        self.bigquery = bigquery
        self.api_key = api_key
        self.project_name=project_name
        self.dataset_name = dataset_name
        self.update_last_month = update_last_month
        if update_last_month:
            last_day_of_last_month = datetime.now().replace(day=1) - timedelta(days=1)
            table_suffix = last_day_of_last_month.strftime("%Y%m")
        else:
            yesterday = datetime.now() - timedelta(days=1)
            table_suffix = yesterday.strftime("%Y%m")
        self.empty_table_name = f"freshdesk_time_entries_{table_suffix}"

        
 
    
    def get_time_entries(self,executed_after=None, executed_before=None, page=1, per_page=100):
        base_url = 'https://dp6.freshdesk.com/api/v2/time_entries'
        
        today = datetime.now()
        last_day_of_last_month = today.replace(day=1) - timedelta(days=1)
        first_day_of_last_month = last_day_of_last_month.replace(day=1,hour=0, minute=0, second=0, microsecond=0).isoformat() + 'Z'
        last_day_of_last_month = last_day_of_last_month.replace(hour=0, minute=0, second=0, microsecond=0).isoformat() + 'Z'
        yesterday = (datetime.now() - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0).isoformat() + 'Z'

        if executed_after == "yesterday":
            executed_after = yesterday
        elif executed_after == "last_month":
            executed_after = first_day_of_last_month

        if executed_before == "yesterday":
            executed_before = yesterday
        elif executed_before == "last_month":
            executed_before = last_day_of_last_month


        if not executed_after:
            executed_after = yesterday
        if not executed_before:
            executed_before = yesterday

        headers = {
            'Authorization': 'Basic ' + base64.b64encode((self.api_key + ':X').encode()).decode(),
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        }
        all_time_entries = []
        has_more_pages = True

        while has_more_pages:
            params = {
                'executed_after': executed_after,
                'executed_before': executed_before,
                'page': page,
                'per_page': per_page
            }
            response = requests.get(base_url, headers=headers, params=params)
            if response.status_code == 200:
                time_entries = response.json()
                all_time_entries.extend(time_entries)
                # Incrementa a página para a próxima iteração
                page += 1
                # Verifica se existem mais páginas
                # A API do Freshdesk indica isso pelo tamanho da resposta
                if len(time_entries) < per_page:
                    has_more_pages = False
            else:
                raise Exception(f'Error making request to Freshdesk API: {response.status_code} - {response.text}')
        return all_time_entries

    def insert_json_to_bigquery(self, json_data,table_name):
        """
        Insere dados de um JSON em uma tabela do BigQuery.

        Args:
        - table_id (str): ID da tabela no formato 'seu-projeto.seu-dataset.sua-tabela'.
        - json_data (str or list): String JSON representando uma lista de objetos ou uma lista de dicionários.

        Returns:
        - None
        """
       

        if table_name is None:
            table_name = self.empty_table_name
        

        table_id = f"{self.project_name}.{self.dataset_name}.{table_name}"
        if isinstance(json_data, str):
            # Converte a string JSON para uma lista de dicionários, se necessário
            json_data = json.loads(json_data)
            

        if not json_data:
            print("Nenhum dado para inserir.")
            return

        if self.update_last_month is True:
            self.client.delete_table(table_id)
            time.sleep(15)

        
        try:
        # Define o ID da tabela
            table = self.client.get_table(table_id)  # Garante que a tabela existe e as colunas estão corretas
            self.client.insert_rows_json(table, json_data)
        except NotFound:
            
            # Defina seus campos e tipos aqui
            schema = [
                self.bigquery.SchemaField("billable", "BOOLEAN", mode="NULLABLE"),
                self.bigquery.SchemaField("note", "STRING", mode="NULLABLE"),
                self.bigquery.SchemaField("id", "STRING", mode="NULLABLE"),
                self.bigquery.SchemaField("timer_running", "BOOLEAN", mode="NULLABLE"),
                self.bigquery.SchemaField("agent_id", "STRING", mode="NULLABLE"),
                self.bigquery.SchemaField("ticket_id", "STRING", mode="NULLABLE"),
                self.bigquery.SchemaField("company_id", "STRING", mode="NULLABLE"),
                self.bigquery.SchemaField("time_spent", "STRING", mode="NULLABLE"),
                self.bigquery.SchemaField("executed_at", "TIMESTAMP", mode="NULLABLE"),
                self.bigquery.SchemaField("start_time", "TIMESTAMP", mode="NULLABLE"),
                self.bigquery.SchemaField("created_at", "TIMESTAMP", mode="NULLABLE"),
                self.bigquery.SchemaField("updated_at", "TIMESTAMP", mode="NULLABLE"),
                self.bigquery.SchemaField("time_spent_in_seconds", "INTEGER", mode="NULLABLE")

            ]
            time_partitioning = self.bigquery.TimePartitioning(
                type_=self.bigquery.TimePartitioningType.DAY,
                field=None  # Usar `field=None` particiona por tempo de ingestão
            )

            table_ref = self.client.dataset(self.dataset_name).table(table_name)
            table = self.bigquery.Table(table_ref, schema=schema)
            table.time_partitioning = time_partitioning

            table = self.client.create_table(table)  # Cria a tabela se não existir
            
            table_exists = False
            retries = 100  
            retry_delay = 5

            while not table_exists and retries > 0:
                try:
                    self.client.insert_rows_json(table, json_data)
                    table_exists = True
                    
                    

                except Exception as e:  # Log more details
                    logging.error(f"Error details: {e}")  # Log error and any response info
                    retries -= 1
                    time.sleep(retry_delay)
            

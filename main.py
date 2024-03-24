import functions_framework
import requests
import logging
import base64
import json
import os
import re
import json
from google.cloud import bigquery
from utils import Freshdesk


def run_function(request):
    
    try:
        if request.method == "OPTIONS":
            # Responde às solicitações OPTIONS para CORS
            headers = {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET",
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Max-Age": "3600",
            }
            return ("", 204, headers)
        request_json = request.get_json(silent=True)
        api_key = os.environ.get('api_key')
        project_name = request_json["project_name"]
        dataset_name = request_json["dataset_name"]
        executed_after = request_json["executed_after"]
        executed_before = request_json["executed_before"]
        update_last_month = False
        table_name = None
        if "update_last_month" in request_json:
            update_last_month = request_json["update_last_month"]
        if "table_name" in request_json:
            table_name = request_json["table_name"]
        

        # Seta os cabeçalhos CORS para a solicitação principal
        headers = {"Access-Control-Allow-Origin": "*"}
        freshdesk = Freshdesk(bigquery,api_key,project_name,dataset_name,update_last_month=update_last_month)
        time_entries = freshdesk.get_time_entries(executed_after=executed_after,executed_before=executed_before)
      
        freshdesk.insert_json_to_bigquery(time_entries,table_name)
        return json.dumps(request_json), 200
    except Exception as erro:
      logging.error(erro)
      return erro,400



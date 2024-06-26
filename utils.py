import json
import logging
import requests
import time
import base64
from datetime import datetime, timedelta
from abc import ABC, abstractmethod

class Utils:
    @staticmethod
    def get_table_name(table_name):
          last_day_of_last_month = datetime.now().replace(day=1) - timedelta(days=1)
          last_month_table_suffix = last_day_of_last_month.strftime("%Y%m")
          last_month_table_name = f"freshdesk_time_entries_{last_month_table_suffix}"
          yesterday = datetime.now() - timedelta(days=1)
          yesterday_table_suffix = yesterday.strftime("%Y%m")
          yesterday_table_name = f"freshdesk_time_entries_{yesterday_table_suffix}"

          if not table_name:
            return yesterday_table_name
          elif table_name == "yesterday":
            return yesterday_table_name

          elif table_name == "last_month":

            return last_month_table_name
          else:
            return table_name



    def is_iso_format(date_string):
        """
        Verifica se a string de data está no formato ISO.

        Args:
            date_string (str): String de data a ser verificada.

        Returns:
            bool: True se a string estiver no formato ISO, False caso contrário.
        """
        try:
            datetime.strptime(date_string, "%Y-%m-%d")
            return True
        except ValueError:
            return False

    def date_util(date_value):
        """
        Manipula a data com base no valor fornecido.

        Args:
            date_value (str): Valor da data.

        Returns:
            str: Data manipulada no formato ISO.
        """
        date_value = date_value.lower().strip()
        if date_value == "yesterday":
            yesterday = (datetime.now() - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%d")
            return yesterday
        elif date_value == "last_month_start":
            last_month_start = (datetime.now().replace(day=1) - timedelta(days=1)).replace(day=1).replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%d")
            return last_month_start
        elif date_value == "last_month_end":
            last_month_end = (datetime.now().replace(day=1) - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%d")
            return last_month_end
        elif Utils.is_iso_format(date_value):
            return datetime.strptime(date_value, "%Y-%m-%d").replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%d")
        else:
            return "Formato indevido"
    @staticmethod
    def make_request(api_key, domain, path, params=None):
        """
        Realiza uma solicitação à API do Freshdesk, lidando com a paginação automaticamente.
        """
        if params is None:
            params = {}
        if "page" not in params:
            params["page"] = 1  # Inicia a paginação na primeira página
        if "per_page" not in params:
            params["per_page"] = 100  # Define um padrão de 100 itens por página
        if "executed_after" in params:
            params["executed_after"] = Utils.date_util(params["executed_after"])
        if "executed_before" in params:
            params["executed_before"] = Utils.date_util(params["executed_before"])

        all_results = []
        has_more_pages = True

        while has_more_pages:
            url = f'{domain}{path}'
            headers = {
                'Authorization': 'Basic ' + base64.b64encode((api_key + ':X').encode()).decode(),
                'Content-Type': 'application/json',
                'Accept': 'application/json',
            }
            response = requests.get(url, headers=headers, params=params)
            if response.status_code == 200:
                results = response.json()
                all_results.extend(results)
                # Incrementa a página para a próxima iteração
                params["page"] += 1
                # Verifica se existem mais páginas
                if len(results) < params["per_page"]:
                    has_more_pages = False
            else:
                raise requests.HTTPError(f'Error making request to Freshdesk API: {response.status_code} - {response.text}')
        return all_results

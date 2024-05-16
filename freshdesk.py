# freshdesk.py

"""
Arquivo freshdesk.py
Propósito: Este módulo interage com a API do Freshdesk usando diferentes estratégias de requisição.
Autor: Seu Nome
Data de criação: 16/05/2024
Última atualização: 16/05/2024
"""

import logging
from abc import ABC, abstractmethod
from utils import Utils

class Freshdesk:
    """
    Classe principal para interagir com a API do Freshdesk.
    
    Parâmetros:
    - api_key (str): Chave de API para autenticação no Freshdesk.
    - company (str): Nome da empresa no domínio Freshdesk.
    - api_request (Strategy): Instância de uma estratégia de requisição específica.
    """
    def __init__(self, api_key, company, api_request):
        self.api_key = api_key
        self.domain = f'https://{company}.freshdesk.com'
        self._api_request = api_request

    @property
    def api_request(self):
        """
        Propriedade para obter a estratégia de requisição atual.
        """
        return self._api_request

    @api_request.setter
    def api_request(self, api_request) -> None:
        """
        Define uma nova estratégia de requisição.
        
        Parâmetros:
        - api_request (Strategy): Nova estratégia de requisição a ser utilizada.
        """
        self._api_request = api_request

    def run_request(self, params):
        """
        Executa uma requisição usando a estratégia de requisição configurada.
        
        Parâmetros:
        - params (dict): Parâmetros da requisição.
        
        Retorno:
        - dict: Resposta da API do Freshdesk.
        """
        return self._api_request.call(params, self.domain, self.api_key)

class Strategy(ABC):
    """
    Classe abstrata base para diferentes estratégias de requisição à API do Freshdesk.
    """
    @abstractmethod
    def call(self, params, domain, api_key):
        """
        Método abstrato que deve ser implementado pelas estratégias concretas.
        
        Parâmetros:
        - params (dict): Parâmetros da requisição.
        - domain (str): Domínio do Freshdesk.
        - api_key (str): Chave de API para autenticação.
        
        Retorno:
        - dict: Resposta da API do Freshdesk.
        """
        pass

class GetAgents(Strategy):
    """
    Estratégia de requisição para obter os agentes do Freshdesk.
    """
    def call(self, params, domain, api_key):
        path = "/api/v2/agents"
        return Utils.make_request(api_key, domain, path, params)

class TicketFields(Strategy):
    """
    Estratégia de requisição para obter os campos de ticket do Freshdesk.
    """
    def call(self, params, domain, api_key):
        path = "/api/v2/ticket_fields"
        return Utils.make_request(api_key, domain, path, params)

class Tickets(Strategy):
    """
    Estratégia de requisição para obter os tickets do Freshdesk.
    """
    def call(self, params, domain, api_key):
        path = "/api/v2/tickets"
        return Utils.make_request(api_key, domain, path, params)

class TimeEntries(Strategy):
    """
    Estratégia de requisição para obter as entradas de tempo do Freshdesk.
    """
    def call(self, params, domain, api_key):
        path = "/api/v2/time_entries"
        return Utils.make_request(api_key, domain, path, params)

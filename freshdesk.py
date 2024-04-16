import logging
from abc import ABC, abstractmethod
from utils import Utils

class Freshdesk:
    def __init__(self, api_key, company, api_request):
        self.api_key = api_key
        self.domain = f'https://{company}.freshdesk.com'
        self._api_request = api_request

    @property
    def api_request(self):
        return self._api_request

    @api_request.setter
    def api_request(self, api_request) -> None:
        self._api_request = api_request

    def run_request(self, params):
        return self._api_request.call(params, self.domain, self.api_key)

class Strategy(ABC):
    @abstractmethod
    def call(self, params, domain, api_key):
        pass

class GetAgents(Strategy):
    def call(self, params, domain, api_key):
        path = "/api/v2/agents"
        return Utils.make_request(api_key, domain, path, params)
class TicketFields(Strategy):
    def call(self, params, domain, api_key):
        path = "/api/v2/ticket_fields"
        return Utils.make_request(api_key, domain, path, params)
class Tickets(Strategy):
    def call(self, params, domain, api_key):
        path = "/api/v2/tickets"
        return Utils.make_request(api_key, domain, path, params)
class TimeEntries(Strategy):
    def call(self, params, domain, api_key):
        path = "/api/v2/time_entries"
        return Utils.make_request(api_key, domain, path, params)

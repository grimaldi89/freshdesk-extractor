import functions_framework
import logging
import os
from freshdesk import Freshdesk,GetAgents,TicketFields,Tickets,TimeEntries
from bigquery import Bigquery,InsertJson


def freshdesk_class(name):
    classes= {

        "Tickets":Tickets,
        "GetAgents":GetAgents,
        "TicketFields":TicketFields,
        "TimeEntries":TimeEntries

    }
    return classes[name]
    
def bigquery_class(name):
    classes= {

        "InsertJson":InsertJson

    }
    return classes[name]


def run_function(request):
    
    try:
        request_json = request.get_json(silent=True)
        freshdesk_request = request_json["freshdesk"]
        bigquery_request = request_json["bigquery"]
        api_key = os.environ.get('api_key')
        company = "dp6"
        
       
        headers = {"Access-Control-Allow-Origin": "*"}
        freshdesk_function = freshdesk_class(freshdesk_request["function_name"])
        freshdesk = Freshdesk(api_key,company,freshdesk_function())
        freshdesk_data = freshdesk.run_request(freshdesk_request["function_parameters"])
       
        bigquery_request["function_parameters"]["data"] = freshdesk_data
        bigquery_request["function_parameters"]["schema_name"] = freshdesk_request["function_name"]
        bigquery_function = bigquery_class(bigquery_request["function_name"])
        bigquery = Bigquery(bigquery_function())
        bigquery.run_job(bigquery_request)
       

       
        return "Sucesso", 200
    except Exception as erro:
        logging.error(erro)
        return erro,400

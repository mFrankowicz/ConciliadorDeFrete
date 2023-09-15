import pandas as pd
import psycopg2
import requests
import concurrent.futures
import json
from datetime import datetime

def login(email, password, saveEmail=True, persistData=True):
    # Define the GraphQL query for login
    query = """
    query ($email: String!, $password: String!) {
      login(email: $email, password: $password) {
        authf2
        client {
          id
          sandbox_id
        }
        user {
          migrated
          id
          company_id
          first_name
          last_name
          email
          position
          token
          enabled
          blocked
          is_production
          created
          created_iso
          modified
          modified_iso
          last_login
          last_login_iso
          user_group_id
          external_id
          time_zone_id
          allow_password_change_from_api
          warehouse_ids
          access_token
          hide_environment
          profiles
          security {
            token_timeout_inactivity
            expired_password_in_days
            time
            hash_password
          }
        }
      }
    }
    """
    
    # Variables used for the query
    variables = {
      "email": email,
      "password": password,
      "saveEmail": saveEmail,
      "persistData": persistData
    }

    # Send the request to the server
    response = requests.post('https://graphql.intelipost.com.br', json={'query': query, 'variables': variables})
    print(response)
    # Parse the response and return the result
    return response.json()

def execute_graphql_query(s: str, e: str, headers=None):
    # Define the GraphQL query
    query = """
    query (
      $warehouses: [Int]
      $delivery_methods: [Int]
      $logistic_providers: [Int]
      $status: [String]
      $margin_status: String
      $difference: String
      $date_range: DateRangeInput
      $search_by: String!
      $search_values: [String]
      $page: Int
      $limit: Int
    ) {
      preInvoicesV2(
        warehouses: $warehouses
        delivery_methods: $delivery_methods
        logistic_providers: $logistic_providers
        status: $status
        margin_status: $margin_status
        difference: $difference
        date_range: $date_range
        search_by: $search_by
        search_values: $search_values
        page: $page
        limit: $limit
      ) {
        total
        hasNextPage
        items {
          id
          order_number
          cte {
            number
            serie
            key
          }
          invoice {
            number
            serie
            key
            value
          }
          status
          margin_status
          tms_value
          cte_value
          payment_type
          payment_custom_value
          payment_custom_description
        }
      }
    }
    """
    
    # Variables used for the query
    variables = {
      "logistic_providers": [
        2734, 127, 969, 365, 591, 978, 3548, 1463, 3429, 20, 3468, 12, 723, 737, 125, 13, 126, 94
      ],
      "delivery_methods": [],
      "status": [
        "WAITING_SHIPMENT_ORDER", "WAITING_CTE", "WAITING_CTE_VALIDATION",
        "WAITING_SERVICE_FINALIZATION", "WAITING_FOR_MARGIN_VALIDATION", "WAITING_FOR_CONCILIATION",
        "WAITING_APPROVAL", "RECONCILIATION_DECLINED", "RELEASED_FOR_PRE_INVOICE", "WAITING_LOGISTIC_PROVIDER_VALIDATION",
        "WAITING_RELEASE_FOR_PAYMENT", "AUTOMATICALLY_RELEASED", "GENERATING_PRE_INVOICE", "REQUEST_DECLINED",
        "REQUEST_APPROVED", "PAYMENT_APPROVED", "PAYMENT_DECLINED", "PAID"
      ],
      "warehouses": [32804, 30741],
      "margin_status": "",
      "difference": "",
      "date_range": {
        "start": s,
        "end": e
      },
      "search_by": "order_number",
      "search_values": [],
      "invoices": {},
      "page": 1,
      "limit": 20000
    }

    # Send the request to the server
    response = requests.post("https://graphql.intelipost.com.br", json={'query': query, 'variables': variables}, headers=headers)
    print("conciliacoes response: ", response)
    # Parse the response and return the result
    return response.json()

def main():
    email = "gabriel.lucina@carolinababy.com.br"
    password = "Gab@050688"  # Consider securing this appropriately
    token = login(email, password)
    token = token['data']['login']['user']['access_token']

    print('TOKEN----------------', token)

    # Usage example
    base_url = "YOUR_GRAPHQL_ENDPOINT_URL"
    headers = {
        "Authorization": token,
        "Content-Type": "application/json"
    }
    response_data = execute_graphql_query("2023-06-01", "2023-09-13", headers)
    print('retorno api, tamanho: ', response_data['data']['preInvoicesV2']['total'])
    intelipost = []
    for data in response_data['data']['preInvoicesV2']['items']:
        info = {}
        info['cte'] = data['cte']['number']
        info['nf'] = data['invoice']['number']
        info['nf_value'] = data['invoice']['value']
        info['status'] = data['status']['label']
        info['tms_value'] = data['tms_value']
        info['cte_value'] = data['cte_value']
        intelipost.append(info)

    intelipost_df = pd.DataFrame(intelipost)
    intelipost_df['nf'] = intelipost_df['nf'].map(int)
    intelipost_df.to_parquet('Conciliacoes.parquet')

if __name__ == '__main__':
    main()
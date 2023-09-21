import requests

proxies = {
    "http": "http://franko:12345@192.168.1.253:3128",
    "https": "http://franko:12345@192.168.1.253:3128"
}

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
    response = requests.post('https://graphql.intelipost.com.br', json={'query': query, 'variables': variables}, proxies=proxies)
    print(response)
    # Parse the response and return the result
    return response.json()

def get_headers():
    email = "gabriel.lucina@carolinababy.com.br"
    password = "Gab@050688"  # Consider securing this appropriately
    token = login(email, password)
    token = token['data']['login']['user']['access_token']

    print('TOKEN----------------', token)

    headers = {
        "Authorization": token,
        "Content-Type": "application/json"
    }
    return headers

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
    response = requests.post("https://graphql.intelipost.com.br", json={'query': query, 'variables': variables}, headers=headers, proxies=proxies)
    print("conciliacoes response: ", response)
    # Parse the response and return the result
    return response.json()

def get_quote_freight_volume(zip_origin, zip_destination, weight, nf_value, transportadora, headers):

    query = """
    mutation (
      $skip_quote_rules: Boolean
      $skip_return_modes: Boolean
      $origin_zip_code: String!
      $destination_zip_code: String!
      $logistic_contract_mode: Boolean
      $shipment_type: Boolean
      $volumes: [VolumeInput]!
      $contract_ids: [Int]
      $additional_information: AdditionalInformationInput
      $debug: Boolean
    ) {
      quoteFreightVolume(
        skip_quote_rules: $skip_quote_rules
        skip_return_modes: $skip_return_modes
        logistic_contract_mode: $logistic_contract_mode
        shipment_type: $shipment_type
        origin_zip_code: $origin_zip_code
        destination_zip_code: $destination_zip_code
        volumes: $volumes
        contract_ids: $contract_ids
        additional_information: $additional_information
        debug: $debug
      ) {
        id
        excluded_methods {
          delivery_restrictions
          name
          logo_url
        }
        logistic_provider {
          delivery_methods {
            id
            name
            description
            final_shipping_cost
            delivery_estimate_minutes
            delivery_estimate_minutes_to_day
            delivery_estimate_date_minutes
            delivery_estimate_date_minutes_iso
            cost
            delivery_estimate_business_days
            type
            note
            logo_url
            logistic_contract_id
          }
        }
        order {
          qty_volumes
        }
      }
    }
    """

    variables = {
      "origin_zip_code": zip_origin,
      "destination_zip_code": zip_destination,
      "volumes": [
        {
          "weight": weight,
          "height": 1,
          "cost_of_goods": nf_value,
          "length": 1,
          "width": 1
        }
      ],
      "additional_information": {
        "federal_tax_payer_type": "cpf",
        "delivery_method_ids": [transportadora],
        "exempt_from_icms": True,
        "additional_business_days": 0,
        "extra_cost_percentage": 0,
        "extra_cost_absolute": 0
      },
      "logistic_contract_mode": False,
      "shipment_type": False,
      "contract_ids": [],
      "skip_quote_rules": False,
      "skip_return_modes": False,
      "debug": True
    }
    response = requests.post("https://graphql.intelipost.com.br", headers=headers, json={'query': query, 'variables': variables}, proxies=proxies)
    
    return response.json()
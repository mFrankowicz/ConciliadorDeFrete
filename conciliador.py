# IMPORTS
import pandas as pd
import psycopg2
import requests
import concurrent.futures
import json
from datetime import datetime
import os
import yaml
from tqdm import tqdm

# ANYMARKET
proxies = {
    "http": "http://franko:12345@192.168.1.253:3128",
    "https": "http://franko:12345@192.168.1.253:3128"
}

ORDER_URL = "http://api.anymarket.com.br/v2/orders"

any_headers = {
    "gumgaToken": "259031280L259048196E1615321580689C161531978454100O259048196.I",
    "Content-Type": "application/json"
}


def generate_list_of_slices(e, size):
    slice_list = []
    for i in range(0, len(e), size):
        slice_list.append(e[i:i+size])
    return slice_list


def get_order_num_pages(date: str):
    querystring = {"limit": 100,
                   "createdAfter": date, "offset": 0}

    response = requests.request(
        "GET", ORDER_URL, headers=any_headers, params=querystring, proxies=proxies).json()
    return response["page"]["totalPages"]


def get_orders(offset, data: list, date: str, num_products: int):
    querystring = {"limit": 100,
                   "createdAfter": date, "offset": offset*100}

    response = requests.request(
        "GET", ORDER_URL, headers=any_headers, params=querystring, proxies=proxies)

    if response.status_code != 200:
        print(response.status_code)
        return
    print(f"coletando pedidos: {len(data)}/{num_products*100}")
    response = response.json()
    # for item in response['values']:
    #     collect_single_order(item, orders)
    if "content" in response:
        data.extend(response['content'])
    else:
        print(response)


def update_pedidos(pedidos_data: list, date: str):
    # client = MongoClient(CONNECTION_STRING)
    # db = client['DataOpsCB']
    # pedidos_collection = db['pedidos']

    num_pages = get_order_num_pages(date)
    print('atualizando pedidos a partir de:', date)
    print('numero de pedidos para atualizar:', num_pages)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        args_list = [(i, pedidos_data, date, num_pages)
                     for i in range(0, num_pages)]
        results = [executor.submit(get_orders, *args)
                   for args in args_list]
        concurrent.futures.wait(results)


# INTELIPOST
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


def gerar_cotacoes(prefaturas_pedidos: pd.DataFrame, transportadora):
    cotacoes = []
    headers = get_headers()
    for index, i in prefaturas_pedidos.iterrows():
        cotacao_get = get_quote_freight_volume(i['CEP REMETENTE'], i['CEP DESTINATARIO'], i['PESO CALC'], i['VAL MERCADORIA'], transportadora, headers)
        cotacao = {}
        if 'errors' in cotacao_get:
            # print(i['CEP REMETENTE'], i['CEP DESTINATARIO'], i['PESO CALC'], i['VAL MERCADORIA'])
            cotacao['NF'] = i['NOTA FISCAL']
            cotacao['valor_cotado_intelipost'] = 0
            cotacao['status'] = cotacao_get['errors']
            cotacoes.append(cotacao)
        else:
            cotacao_get = cotacao_get['data']['quoteFreightVolume']['logistic_provider']['delivery_methods'][0]
            
            cotacao['NF'] = i['NOTA FISCAL']
            cotacao['valor_cotado_intelipost'] = cotacao_get['final_shipping_cost']
            cotacao['status'] = 'ok'
            cotacoes.append(cotacao)
    return pd.DataFrame(cotacoes)


# -----------------------------------------
# PRE FATURA
def gerar_prefatura(inicio: str, fim: str):
    email = "gabriel.lucina@carolinababy.com.br"
    password = "Gab@050688"  # Consider securing this appropriately
    token = login(email, password)
    token = token['data']['login']['user']['access_token']

    print('TOKEN----------------', token)

    headers = {
        "Authorization": token,
        "Content-Type": "application/json"
    }

    response_data = execute_graphql_query(inicio, fim, headers)
    # print('retorno api, tamanho: ', response_data['data']['preInvoicesV2']['total'])
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
    return intelipost_df


# PEDIDOS
def pedidos(date_str: str): 
    date = f"{date_str}T03:00:00.000Z"
    pedidos = []
    update_pedidos(pedidos_data=pedidos, date=date)

    for p in pedidos:
        p['nf'] = 0
        if "id" in p:
            p['_id'] = p.pop('id')
        p["createdAt"] = datetime.strptime(
            p["createdAt"], "%Y-%m-%dT%H:%M:%SZ")
        if "paymentDate" in p:
            p['paymentDate'] = datetime.strptime(
                p["paymentDate"], "%Y-%m-%dT%H:%M:%SZ")
        if "cancelDate" in p:
            p['cancelDate'] = datetime.strptime(
                p["cancelDate"], "%Y-%m-%dT%H:%M:%SZ")
        if "invoice" in p:
            if "date" in p["invoice"]:
                p["invoice"]["date"] = datetime.strptime(
                    p["invoice"]["date"], "%Y-%m-%dT%H:%M:%SZ")
            # atualizar do focco também
            p['nf'] = p['invoice']['number']

        if "promisedShippingTime" in p["shipping"]:
            p["shipping"]["promisedShippingTime"] = datetime.strptime(
                p["shipping"]["promisedShippingTime"], "%Y-%m-%dT%H:%M:%SZ")
        if "promisedShippingTime" in p["anymarketAddress"]:
            p["anymarketAddress"]["promisedShippingTime"] = datetime.strptime(
                p["anymarketAddress"]["promisedShippingTime"], "%Y-%m-%dT%H:%M:%SZ")
        if "tracking" in p:
            if "date" in p["tracking"]:
                p["tracking"]["date"] = datetime.strptime(
                    p["tracking"]["date"], "%Y-%m-%dT%H:%M:%SZ")
            if "deliveredDate" in p["tracking"]:
                p["tracking"]["deliveredDate"] = datetime.strptime(
                    p["tracking"]["deliveredDate"], "%Y-%m-%dT%H:%M:%SZ")
            if "estimateDate" in p["tracking"]:
                p["tracking"]["estimateDate"] = datetime.strptime(
                    p["tracking"]["estimateDate"], "%Y-%m-%dT%H:%M:%SZ")
            if "shippedDate" in p["tracking"]:
                p["tracking"]["shippedDate"] = datetime.strptime(
                    p["tracking"]["shippedDate"], "%Y-%m-%dT%H:%M:%SZ")

    pedidos_df = pd.DataFrame(pedidos)

    pedidos_df = pedidos_df.dropna(subset=['nf'])
    pedidos_df['nf'] = pedidos_df['nf'].map(int)
    
    
    return pedidos_df


# TABELA TRANSPORTADORAS
def add_hyphen(s):
    s = s.replace(".0", "")
    return s[:-3] + '-' + s[-3:]


def carregar_tabela_padrao(diretorio: str):
    arquivos_excel =[f for f in os.listdir(diretorio) if f.endswith('.csv')]

    dfs = []

    for arquivo in arquivos_excel:
        df = pd.read_csv(os.path.join(diretorio, arquivo), sep=';', skiprows=1, encoding='cp1252', decimal=',')
        df['FATURA'] = arquivo
        dfs.append(df)

    tabelas_padrao = pd.concat(dfs, ignore_index=True)

    tabelas_padrao = tabelas_padrao[['FATURA', 'NUMERO CT-E', 'DATA EMISSAO', 'CLIENTE REMETENTE', 'CNPJ REMETENTE', 'CEP REMETENTE', 'CLIENTE DESTINATARIO', 'CNPJ DESTINATARIO', 'CEP DESTINATARIO', 'ENTREGA DIFICIL', 'NOTA FISCAL', 'VAL MERCADORIA', 'PESO CALC', 'VAL RECEBER']]
    tabelas_padrao['VAL MERCADORIA'] = tabelas_padrao['VAL MERCADORIA'].str.replace('.', '').str.replace(',', '.').str.replace(' ', '').astype(float)

    tabelas_padrao['CEP REMETENTE'] = tabelas_padrao['CEP REMETENTE'].astype(str)
    tabelas_padrao['CEP DESTINATARIO'] = tabelas_padrao['CEP DESTINATARIO'].astype(str)

    tabelas_padrao['CEP REMETENTE'] = tabelas_padrao['CEP REMETENTE'].replace(".0", '')
    tabelas_padrao['CEP DESTINATARIO'] = tabelas_padrao['CEP DESTINATARIO'].replace(".0", '')

    tabelas_padrao['CEP REMETENTE'] = tabelas_padrao['CEP REMETENTE'].replace('36500000.0', '36509100')
    tabelas_padrao['CEP DESTINATARIO'] = tabelas_padrao['CEP DESTINATARIO'].replace('36500000.0', '36509100')
    
    tabelas_padrao['CEP REMETENTE'] = tabelas_padrao['CEP REMETENTE'].replace('36500001.0', '36509100')
    tabelas_padrao['CEP DESTINATARIO'] = tabelas_padrao['CEP DESTINATARIO'].replace('36500001.0', '36509100')

    tabelas_padrao['CEP REMETENTE'] = tabelas_padrao['CEP REMETENTE'].apply(add_hyphen)
    tabelas_padrao['CEP DESTINATARIO'] = tabelas_padrao['CEP DESTINATARIO'].apply(add_hyphen)
    
    return tabelas_padrao


def carregar_tabela_jamef(diretorio: str):
    arquivos_excel = [f for f in os.listdir(diretorio) if f.endswith('.xlsx')]
    
    dfs = []
    
    for arquivo in arquivos_excel:
        df = pd.read_excel(os.path.join(diretorio, arquivo))
        df['FATURA'] = arquivo

        jamef_df = []
        for index, item in df.iterrows():
            jamef = {}
            jamef['FATURA'] = item['FATURA']
            jamef['NUMERO CT-E'] = item['CTe']
            jamef['DATA EMISSAO'] = item['DATA EMISSAO']
            jamef['CLIENTE REMETENTE'] = item['NOME REMETENTE']
            jamef['CNPJ REMETENTE'] = item['REMETENTE']
            jamef['CEP REMETENTE'] = ''
            jamef['CLIENTE DESTINATARIO'] = item['NOME DESTINATARIO']
            jamef['CNPJ DESTINATARIO'] = item['DESTINATARIO']
            jamef['CEP DESTINATARIO'] = ''
            jamef['ENTREGA DIFICIL'] = ''
            jamef['NOTA FISCAL'] = item["NF'S"]
            jamef['VAL MERCADORIA'] = item['VALOR MERCADORIA']
            jamef['PESO CALC'] = item['PESO']
            jamef['VAL RECEBER'] = item['VALOR FRETE']
            jamef_df.append(jamef)
        
        final_jamef = pd.DataFrame(jamef_df)
        dfs.append(final_jamef)
    
    tabelas_padrao = pd.concat(dfs, ignore_index=True)
    tabelas_padrao = tabelas_padrao[['FATURA', 'NUMERO CT-E', 'DATA EMISSAO', 'CLIENTE REMETENTE', 'CNPJ REMETENTE', 'CEP REMETENTE', 'CLIENTE DESTINATARIO', 'CNPJ DESTINATARIO', 'CEP DESTINATARIO', 'ENTREGA DIFICIL', 'NOTA FISCAL', 'VAL MERCADORIA', 'PESO CALC', 'VAL RECEBER']]
    tabelas_padrao['VAL MERCADORIA'] = tabelas_padrao['VAL MERCADORIA']

    tabelas_padrao['CEP REMETENTE'] = tabelas_padrao['CEP REMETENTE'].astype(str)
    tabelas_padrao['CEP DESTINATARIO'] = tabelas_padrao['CEP DESTINATARIO'].astype(str)

    tabelas_padrao['CEP REMETENTE'] = tabelas_padrao['CEP REMETENTE'].replace(".0", '')
    tabelas_padrao['CEP DESTINATARIO'] = tabelas_padrao['CEP DESTINATARIO'].replace(".0", '')

    tabelas_padrao['CEP REMETENTE'] = tabelas_padrao['CEP REMETENTE'].replace('36500000.0', '36509100')
    tabelas_padrao['CEP DESTINATARIO'] = tabelas_padrao['CEP DESTINATARIO'].replace('36500000.0', '36509100')
    
    tabelas_padrao['CEP REMETENTE'] = tabelas_padrao['CEP REMETENTE'].replace('36500001.0', '36509100')
    tabelas_padrao['CEP DESTINATARIO'] = tabelas_padrao['CEP DESTINATARIO'].replace('36500001.0', '36509100')

    tabelas_padrao['CEP REMETENTE'] = tabelas_padrao['CEP REMETENTE'].apply(add_hyphen)
    tabelas_padrao['CEP DESTINATARIO'] = tabelas_padrao['CEP DESTINATARIO'].apply(add_hyphen)

    return tabelas_padrao


def carregar_tabela_moovelog(diretorio: str):
    arquivos_excel = [f for f in os.listdir(diretorio) if f.endswith('xlsx')]
    dfs = []
    for arquivo in arquivos_excel:
        df = pd.read_excel(os.path.join(diretorio, arquivo))
        df['FATURA'] = arquivo
        
        # mooveelog['Número CTe'] = mooveelog['Número CTe'].str.replace("'", '')
        # mooveelog['Número Nota Fiscal'] = mooveelog['Número Nota Fiscal'].str.replace("'", '')

        moovelog_df = []
        for index, item in df.iterrows():
            moovelog = {}
            moovelog['FATURA'] = item['FATURA']
            moovelog['NUMERO CT-E'] = item['Número CTe'].replace("'", '')
            moovelog['DATA EMISSAO'] = item['Data de Emissão do CTe']
            moovelog['CLIENTE REMETENTE'] = ''
            moovelog['CNPJ REMETENTE'] = ''
            moovelog['CEP REMETENTE'] = ''
            moovelog['CLIENTE DESTINATARIO'] = item['Nome do Cliente'].replace("'", '')
            moovelog['CNPJ DESTINATARIO'] = ''
            moovelog['CEP DESTINATARIO'] = ''
            moovelog['ENTREGA DIFICIL'] = ''
            moovelog['NOTA FISCAL'] = int(item["Número Nota Fiscal"].replace("'", ''))
            moovelog['VAL MERCADORIA'] = item['Valor Nota Fscal']
            moovelog['PESO CALC'] = item['Peso Cobrado']
            moovelog['VAL RECEBER'] = item['Frete Total']
            moovelog_df.append(moovelog)
        
        final_moovelog = pd.DataFrame(moovelog_df)
        dfs.append(final_moovelog)
    
    tabelas_padrao = pd.concat(dfs, ignore_index=True)
    tabelas_padrao = tabelas_padrao[['FATURA', 'NUMERO CT-E', 'DATA EMISSAO', 'CLIENTE REMETENTE', 'CNPJ REMETENTE', 'CEP REMETENTE', 'CLIENTE DESTINATARIO', 'CNPJ DESTINATARIO', 'CEP DESTINATARIO', 'ENTREGA DIFICIL', 'NOTA FISCAL', 'VAL MERCADORIA', 'PESO CALC', 'VAL RECEBER']]
    # tabelas_padrao['VAL MERCADORIA'] = tabelas_padrao['VAL MERCADORIA'].str.replace('.', '').str.replace(',', '.').str.replace(' ', '').astype(float)

    tabelas_padrao['CEP REMETENTE'] = tabelas_padrao['CEP REMETENTE'].astype(str)
    tabelas_padrao['CEP DESTINATARIO'] = tabelas_padrao['CEP DESTINATARIO'].astype(str)

    tabelas_padrao['CEP REMETENTE'] = tabelas_padrao['CEP REMETENTE'].replace(".0", '')
    tabelas_padrao['CEP DESTINATARIO'] = tabelas_padrao['CEP DESTINATARIO'].replace(".0", '')

    tabelas_padrao['CEP REMETENTE'] = tabelas_padrao['CEP REMETENTE'].replace('36500000.0', '36509100')
    tabelas_padrao['CEP DESTINATARIO'] = tabelas_padrao['CEP DESTINATARIO'].replace('36500000.0', '36509100')
    
    tabelas_padrao['CEP REMETENTE'] = tabelas_padrao['CEP REMETENTE'].replace('36500001.0', '36509100')
    tabelas_padrao['CEP DESTINATARIO'] = tabelas_padrao['CEP DESTINATARIO'].replace('36500001.0', '36509100')

    tabelas_padrao['CEP REMETENTE'] = tabelas_padrao['CEP REMETENTE'].apply(add_hyphen)
    tabelas_padrao['CEP DESTINATARIO'] = tabelas_padrao['CEP DESTINATARIO'].apply(add_hyphen)

    return tabelas_padrao


# -----------------------------------------
# EXECUCAO
def unir_tabelas(prefaturas: pd.DataFrame, pedidos: pd.DataFrame, transportadoras: pd.DataFrame, transportadora):
    transportadoras_prefaturas = pd.merge(transportadoras, prefaturas, how='left', left_on='NOTA FISCAL', right_on='nf')
    prefaturas_pedidos = pd.merge(transportadoras_prefaturas, pedidos, how='left', left_on='NOTA FISCAL', right_on='nf')
    prefaturas_pedidos[['FATURA','NUMERO CT-E','CEP REMETENTE','CEP DESTINATARIO','PESO CALC','VAL MERCADORIA','NOTA FISCAL','VAL RECEBER','nf_value','tms_value','cte_value','marketPlace','status_y','freight', '_id', 'marketPlaceId']]
    prefaturas_pedidos = prefaturas_pedidos.dropna(subset=['NUMERO CT-E'])
    cotacoes = gerar_cotacoes(prefaturas_pedidos, transportadora)
    pedidos_cotados = pd.merge(left=prefaturas_pedidos, right=cotacoes, left_on="NOTA FISCAL", right_on="NF")
    tabela_filtrada = pedidos_cotados[['FATURA','_id', 'marketPlaceId','CEP REMETENTE', 'CEP DESTINATARIO' ,'NUMERO CT-E', 'DATA EMISSAO', 'NOTA FISCAL', 'VAL MERCADORIA', 'PESO CALC', 'VAL RECEBER', 'tms_value', 'freight', 'valor_cotado_intelipost', 'marketPlace']]
    tabela_filtrada.fillna(0, inplace=True)
    tabela_filtrada.reset_index(inplace=True)
    return tabela_filtrada


def conciliar(tabela_unida: pd.DataFrame):
    lista = []
    for index, value in tabela_unida.iterrows():
        info = {}
        info['fatura'] = value['FATURA']
        # info['id any'] = value['_id']
        # info['pedido Any'] = value['marketPlaceId']
        # info['cep origem'] = value['CEP REMETENTE']
        # info['cep destino'] = value['CEP DESTINATARIO']
        info['cte'] = value['NUMERO CT-E']
        info['data emissão'] = value['DATA EMISSAO']
        info['nf'] = value['NOTA FISCAL']
        info['valor mercadoria'] = value['VAL MERCADORIA']
        info['peso'] = value['PESO CALC']
        info['valor receber'] = value['VAL RECEBER']
        if value['tms_value'] > 0:
            info['valor cotado'] = value['tms_value']
            info['observacao'] = "Pré Fatura"
        elif value['freight'] > 0:
            info['valor cotado'] = value['freight']
            info['observacao'] = "Anymarket"
        elif value['valor_cotado_intelipost'] > 0:
            info['valor cotado'] = value['valor_cotado_intelipost']
            info['observacao'] = "Cotacao Pela Intelipost"
        else:
            info['valor cotado'] = 0
            info['observacao'] = "Não foi possível fazer cotação ou encontrar o pedido"
        if value['marketPlace'] == 0:
            info['observacao 2'] = "NF não subiu no pedido"
        lista.append(info)

    return pd.DataFrame(lista)


def salvar_conciliacao(conciliada: pd.DataFrame, pasta: str):
    groups = conciliada.groupby('fatura')
    for name, group in groups:
        group.drop(columns=['fatura'])
        group.to_excel(f"conciliadas/CONCILIADA - {name}.xlsx", index=False)


# def main():
#     with open('configuracoes.yaml', 'r') as file:
#         config = yaml.safe_load(file)

#     date_from = config['config']['de']
#     date_to = config['config']['ate']
#     dir = config['config']['caminho_da_pasta']

#     pedidos_df = pedidos(date_from)
#     prefaturas_df = gerar_prefatura(date_from, date_to)

#     jamef_df = carregar_tabela_jamef("JAMEF")
#     moovelog_df = carregar_tabela_moovelog("MOOVELOG")
#     dominalog_df = carregar_tabela_padrao("DOMINALOG")
#     nova_era_df = carregar_tabela_padrao("NOVA ERA")
#     pajucara_df = carregar_tabela_padrao("PAJUCARA")
#     stl_df = carregar_tabela_padrao("STL")
#     transbarbosa_df = carregar_tabela_padrao("TRANSBARBOSA")
#     vhz_df = carregar_tabela_padrao("VHZ")

#     jamef_unidas = unir_tabelas(prefaturas_df, pedidos_df, jamef_df, 23)
#     moovelog_unidas = unir_tabelas(prefaturas_df, pedidos_df, moovelog_df, 7524)
#     dominalog_unidas = unir_tabelas(prefaturas_df, pedidos_df, dominalog_df, 196)
#     nova_era_unidas = unir_tabelas(prefaturas_df, pedidos_df, nova_era_df, 17387)
#     pajucara_unidas = unir_tabelas(prefaturas_df, pedidos_df, pajucara_df, 30)
#     stl_unidas = unir_tabelas(prefaturas_df, pedidos_df, stl_df, 1297)
#     transbarbosa_unidas = unir_tabelas(prefaturas_df, pedidos_df, transbarbosa_df, 17681)
#     vhz_unidas = unir_tabelas(prefaturas_df, pedidos_df, vhz_df, 14567)

#     conciliacao_jamef = conciliar(jamef_unidas)
#     conciliacao_moovelog = conciliar(moovelog_unidas)
#     conciliacao_dominalog = conciliar(dominalog_unidas)
#     conciliacao_nova_era = conciliar(nova_era_unidas)
#     conciliacao_pajucara = conciliar(pajucara_unidas)
#     conciliacao_stl = conciliar(stl_unidas)
#     conciliacao_transbarbosa = conciliar(transbarbosa_unidas)
#     conciliacao_vhz = conciliar(vhz_unidas)

#     salvar_conciliacao(conciliacao_jamef, dir)
#     salvar_conciliacao(conciliacao_moovelog, dir)
#     salvar_conciliacao(conciliacao_dominalog, dir)
#     salvar_conciliacao(conciliacao_nova_era, dir)
#     salvar_conciliacao(conciliacao_pajucara, dir)
#     salvar_conciliacao(conciliacao_stl, dir)
#     salvar_conciliacao(conciliacao_transbarbosa, dir)
#     salvar_conciliacao(conciliacao_vhz, dir)

def main():
    with open('configuracoes.yaml', 'r') as file:
        config = yaml.safe_load(file)

    date_from = config['config']['de']
    date_to = config['config']['ate']
    dir = config['config']['caminho_da_pasta']

    print("1. GERANDO TABELA DE PEDIDOS")
    pedidos_df = pedidos(date_from)
    print("2. GERANDO TABELA DE PRÉ FATURAS")
    prefaturas_df = gerar_prefatura(date_from, date_to)

    transportadoras = ["JAMEF", "MOOVELOG", "DOMINALOG", "NOVA ERA", "PAJUCARA", "STL", "TRANSBARBOSA", "VHZ"]
    ids = [23, 7524, 196, 17387, 30, 1297, 17681, 14567]
    
    tabelas = []
    for trans, id in tqdm(zip(transportadoras, ids), total=len(transportadoras), desc="Carregando tabelas"):
        if trans in ["JAMEF", "MOOVELOG"]:
            print("3. CARREGANDO TABELA: ", trans)
            df = carregar_tabela_jamef(trans) if trans == "JAMEF" else carregar_tabela_moovelog(trans)
        else:
            print("3. CARREGANDO TABELA: ", trans)
            df = carregar_tabela_padrao(trans)
        print("4. PROCESSANDO TABELA: ", trans)
        unidas = unir_tabelas(prefaturas_df, pedidos_df, df, id)
        print("4. CONCILIANDO TABELA: ", trans)
        conciliacao = conciliar(unidas)
        print("4. SALVANDO TABELA: ", trans)
        salvar_conciliacao(conciliacao, dir)


if __name__ == "__main__":
    main()
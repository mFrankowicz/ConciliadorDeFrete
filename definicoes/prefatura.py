from intelipost import *
import pandas as pd

def gerar_prefatura():
    email = "gabriel.lucina@carolinababy.com.br"
    password = "Gab@050688"  # Consider securing this appropriately
    token = login(email, password)
    token = token['data']['login']['user']['access_token']

    print('TOKEN----------------', token)

    headers = {
        "Authorization": token,
        "Content-Type": "application/json"
    }

    response_data = execute_graphql_query("2023-06-01", "2023-09-20", headers)
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
    return intelipost_df
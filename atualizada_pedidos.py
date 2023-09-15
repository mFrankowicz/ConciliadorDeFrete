import pandas as pd
import psycopg2
import requests
import concurrent.futures
import json
from datetime import datetime


ORDER_URL = "http://api.anymarket.com.br/v2/orders"
headers = {
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
        "GET", ORDER_URL, headers=headers, params=querystring).json()

    return response["page"]["totalPages"]


def get_orders(offset, data: list, date: str, num_products: int):
    querystring = {"limit": 100,
                   "createdAfter": date, "offset": offset*100}

    response = requests.request(
        "GET", ORDER_URL, headers=headers, params=querystring)

    if response.status_code != 200:
        print(response.status_code)
        return
    print(f"coletando pedidos: {len(data)}/{num_products*100}")
    response = response.json()
    # for item in response['values']:
    #     collect_single_order(item, orders)
    if "content" in response:
        data.extend(response['content'])

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

    for p in pedidos_data:
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
    print('pedidos recebidos')

def get_pedidos(date: str) -> pd.DataFrame:
    date = f"{date}T00:00:00-03:00"
    pedidos = []
    update_pedidos(pedidos_data=pedidos, date=date)
    df = pd.DataFrame(pedidos)
    pedidos_df = pd.DataFrame(pedidos)
    pedidos_df = pedidos_df.dropna(subset=['nf'])
    pedidos_df['nf'] = pedidos_df['nf'].map(int)
    pedidos_df.to_parquet(f"Pedidos Gerados.parquet", engine='pyarrow')


if __name__ == '__main__':
    print('gerando pedidos')
    get_pedidos('2023-09-12')
    print('pedidos salvos no arquivo')
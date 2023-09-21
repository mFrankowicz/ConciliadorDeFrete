import requests
import concurrent.futures

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
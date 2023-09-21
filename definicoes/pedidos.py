import pandas as pd
from datetime import datetime
from anymarket import *

def pedidos(date_str: str):
    
    date = f"{date_str}T03:00:00.000Z"
    pedidos = []
    update_pedidos(pedidos_data=pedidos, date=date)

    for p in pedidos:
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
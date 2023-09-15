import pandas as pd
import psycopg2
import requests
import concurrent.futures
import json
from datetime import datetime

def main():
    intelipost_concili = pd.read_parquet('Conciliacoes.parquet')
    pedidos_any = pd.read_parquet('Pedidos Gerados.parquet')
    

    
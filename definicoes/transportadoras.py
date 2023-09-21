import pandas as pd
import os

def add_hyphen(s):
    s = s.replace(".0", "")
    return s[:-3] + '-' + s[-3:]

def carregar_tabela_padrao(diretorio: str):
    arquivos_excel =[f for f in os.listdir(diretorio) if f.endswith('.csv',  sep=';', skiprows=1, encoding='cp1252', decimal=',')]

    dfs = []

    for arquivo in arquivos_excel:
        df = pd.read_csv(os.path.join(diretorio, arquivo))
        df['arquivo fatura'] = arquivo
        dfs.append(df)

    tabelas_padrao = pd.concat(dfs, ignore_index=True)

    tabelas_padrao = tabelas_padrao[['NUMERO CT-E', 'DATA EMISSAO', 'CLIENTE REMETENTE', 'CNPJ REMETENTE', 'CEP REMETENTE', 'CLIENTE DESTINATARIO', 'CNPJ DESTINATARIO', 'CEP DESTINATARIO', 'ENTREGA DIFICIL', 'NOTA FISCAL', 'VAL MERCADORIA', 'PESO CALC', 'VAL RECEBER']]
    tabelas_padrao['VAL MERCADORIA'] = tabelas_padrao['VAL MERCADORIA'].str.replace('.', '').str.replace(',', '.').str.replace(' ', '').astype(float)

    tabelas_padrao['CEP REMETENTE'] = tabelas_padrao['CEP REMETENTE'].astype(str)
    tabelas_padrao['CEP DESTINATARIO'] = tabelas_padrao['CEP DESTINATARIO'].astype(str)

    tabelas_padrao['CEP REMETENTE'] = tabelas_padrao['CEP REMETENTE'].replace(".0", '')
    tabelas_padrao['CEP DESTINATARIO'] = tabelas_padrao['CEP DESTINATARIO'].replace(".0", '')

    tabelas_padrao['CEP REMETENTE'] = tabelas_padrao['CEP REMETENTE'].replace('36500000.0', '36509100')
    tabelas_padrao['CEP DESTINATARIO'] = tabelas_padrao['CEP DESTINATARIO'].replace('36500000.0', '36509100')

    tabelas_padrao['CEP REMETENTE'] = tabelas_padrao['CEP REMETENTE'].apply(add_hyphen)
    tabelas_padrao['CEP DESTINATARIO'] = tabelas_padrao['CEP DESTINATARIO'].apply(add_hyphen)
    
    return tabelas_padrao
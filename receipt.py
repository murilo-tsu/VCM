print('\n')
print('╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗')
print('║                                           ATUALIZACAO DE DADOS - VCM                                           ║')
print('║                                               >>  receipt.py  <<                                               ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ Criado por:    Isabela Nunes dos Santos        Data: 24/03/2025                                                ║')
print('║ Editado por:   Isabela Nunes dos Santos        Data: 24/03/2025                                                ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ CHANGELOG:                                                                                                     ║')
print('║ - v1.0.0 (24/03/2025): Criação da primeira versão do script unificado com edições estruturais nos arquivos     ║')
print('║                        de depara e dado primário.                                                              ║')
print('║                                                                                                                ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ Este script é responsável pela atualização:                                                                    ║')
print('║ >> Receitas de Movimentação                                                                                    ║')
print('╚════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝')
print('\n')

# =======================================================================================================================
# IMPORTAR BIBLIOTECAS
# =======================================================================================================================

import os
import sys
import pandas as pd
import numpy as np
import warnings
import time
import datetime
import logging
from pandas.tseries.offsets import MonthEnd
from pandas.tseries.offsets import MonthBegin
from tkinter import messagebox
from unidecode import unidecode
warnings.filterwarnings('ignore')
start_time = time.time()

# =======================================================================================================================
# CONFIGURAÇÕES INICIAIS
# =======================================================================================================================

cwd = os.getcwd()

# Caminhos dos arquivos
structure_path = 'Structural Data/'            # Dados estruturais (topologia)
path = 'Input Data/'                           # Dados de entrada (ciclo de planejamento)
output_path = 'Output Data/'                   # Dados de saída (input para o VCM)
exec_log_path = 'Error Logs/'                  # Logs de erros durante a execução

# Configuração do logger
logging.basicConfig(
    filename=os.path.join(exec_log_path, 'execution_log.log'),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logging.info("Iniciando execução do script.")

# =======================================================================================================================
# FUNÇÕES
# =======================================================================================================================

# CHECAGEM DE ARQUIVOS
# >> Valida a data 
def validar_data_arquivo(arquivo):
    try:
        
        timestamp = os.path.getmtime(arquivo)
        # Obter data e hora do momento da atualização
        curr_date = time.localtime()
        comp_timestamp = time.localtime(timestamp)

        # Converter em um objeto do tipo datetime
        data_edicao = datetime.datetime.fromtimestamp(timestamp)        

        # Exibe a data em um pop-up
        if curr_date.tm_mon > comp_timestamp.tm_mon and curr_date.tm_year >= comp_timestamp.tm_year:
            messagebox.showinfo("Script Encerrado!!!", f'O arquivo {arquivo} está desatualizado.\nÚltima atualização em: {data_edicao}')
            sys.exit()
    
    except FileNotFoundError: 
        messagebox.showerror("Erro", "Arquivo não encontrado.")

def left_outer_join(df_left, df_right, left_on, right_on):
    print('\n')
    print(f'══════════════════════════════════════════════ LEFT JOIN ═══════════════════════════════════════════════════')
    name_left = [name for name, obj in globals().items() if obj is df_left]
    name_right = [name for name, obj in globals().items() if obj is df_right]
    print(f'Mesclando {name_left} x {name_right}')
    x1 = df_left.shape[0]
    print(f'A quantidade de linhas antes do join é {x1}')
    merged_df = df_left.merge(df_right, how = 'left', left_on = left_on, right_on = right_on)
    # Limpar o DataFrame original e aplicar as novas colunas
    df_left.drop(df_left.columns, axis=1, inplace=True) 
    for col in merged_df.columns:
        df_left[col] = merged_df[col]  # Copiar colunas do merged_df

    x2 = df_left.shape[0]
    print(f'A quantidade de linhas após o join é {x2}')
    if x1 == x2:
        y = '√'
    else:
        y = 'X'
        print(f'Checar por duplicidades em {name_right}')
    print(f'═══════════════════════════════════════ FIM DO JOIN :: Resultado = {y} ═══════════════════════════════════════')

# PADRONIZAR STRINGS
def padronizar(value):
    if isinstance(value, str):
        value = value.upper().strip()
        value = unidecode(value)
    return value

# =======================================================================================================================
# DEFINIR ARQUIVOS
# =======================================================================================================================

arquivos_primarios = {
     'localizacao': 'depGeolocalizacao.xlsx',
     'localizacao_sn': 'depGeolocalizacao',
     'periodos': 'iptPeriodos.xlsx',
     'periodos_sn': 'Períodos de Otimização',
     'cadastro_produtos': 'depSKU.xlsx',
     'cadastro_produtos_sn': 'CADASTRO',     
     'agrupamento_sn':'AGRUPAMENTO',
     'up_correntes':'iptUpdateCorrentes.xlsx',
     'up_correntes_sn': 'iptUpdateCorrentes',
     'lista_preco' : 'iptListaPreco.xlsx',
     'lista_preco_sn' : 'iptListaPreco',
     'unidades_rec_mov' : 'depAtvcBalancosFin.xlsx',
     'unidades_rec_mov_sn' : 'depAtvcBalancosFin',
     'template_rec_mov': 'tmpReceitaMovimentacao.csv',
}

tp_dado_arquivos = {
     'localizacao': {'Unidade':str, 'Estado':str, 'Município':str},
     'periodos':{'NUMERO':str,'PERIODO':'datetime64[ns]', 'NOME_PERIODO':str},
     'cadastro_produtos': {'PRD-VCM':str,'CODIGO_ITEM':str,'DESCRICAO':str, 'TIPO_MATERIAL':str, 'CATEGORIA':str},
     'agrupamento': {'COD_ESPECIFICO':str, 'CODIGO_AGRUPADO':str},
     'up_correntes': {'ConjuntoCorrentes':str, 'Unidade-Origem':str, 'Unidade-Destino':str},
     'lista_preco': {'DATA':'datetime64[ns]', 'DH_INICIAL':'datetime64[ns]', 'DH_FINAL':'datetime64[ns]', 'LISTA':str, 
                     'FILIAL':str, 'ITEM':str, 'DESCRICAO':str, 'MOEDA':str, 'PTAX':np.float64, 'PRECO':np.float64},
     'unidades_rec_mov': {'UNIDADE':str, 'DESC_UNIDADE':str},
     'template_rec_mov_sn': {'Origem':str, 'Destino':str, 'Corrente':str, 'Produto':str, 'Periodo':str, 'Valor':np.float32},
     'template_rec_mov_corrente' : {'Corrente':str, 'Produto':str},
}

rename_dataframes = {
    'df_valor_venda':{'ITEM':'Código do Produto','PRECO':'Preço','PTAX':'Ptax USD','DH_INICIAL':'Data Inicio',
                   'DH_FINAL':'Data fim','MOEDA':'Moeda','LISTA':'Nome da Lista'},
    'df_pontos_venda':{'UNIDADE':'Origem','DESC_UNIDADE':'Origem ECFTO'},
}

# =======================================================================================================================
# CARREGAR DATAFRAMES
# =======================================================================================================================
print('Carregando arquivos necessários... \n')

# DataFrame :: Geolocalizacao
df_localizacao = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['localizacao']),
                         sheet_name= arquivos_primarios['localizacao_sn'], 
                       usecols=list(tp_dado_arquivos['localizacao'].keys()),
                       dtype=tp_dado_arquivos['localizacao'])

# DataFrame :: Horizonte (Período) de Otimização
df_periodos = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['periodos']),
                         usecols=list(tp_dado_arquivos['periodos'].keys()),
                         dtype=tp_dado_arquivos['periodos'])

# Dataframe :: Cadastro Produtos
df_produtos = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['cadastro_produtos']),
                                  sheet_name = arquivos_primarios['cadastro_produtos_sn'],
                                  usecols = list(tp_dado_arquivos['cadastro_produtos'].keys()),
                                  dtype = tp_dado_arquivos['cadastro_produtos'])

# Dataframe :: Agrupamento
agrupamento_pf = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['cadastro_produtos']),
                                  sheet_name = arquivos_primarios['agrupamento_sn'],
                                  usecols = list(tp_dado_arquivos['agrupamento'].keys()),
                                  dtype = tp_dado_arquivos['agrupamento'])

# DataFrame :: Update de Correntes
df_correntes = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['up_correntes']),
                         sheet_name= arquivos_primarios['up_correntes_sn'], 
                       usecols=list(tp_dado_arquivos['up_correntes'].keys()),
                       dtype=tp_dado_arquivos['up_correntes']).rename(columns = {'ConjuntoCorrentes':'Corrente',\
                                            'Unidade-Origem':'Origem', 'Unidade-Destino':'Destino'})

# DataFrame :: Lista Preço
df_valor_venda = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['lista_preco']),
                         sheet_name= arquivos_primarios['lista_preco_sn'], 
                       usecols=list(tp_dado_arquivos['lista_preco'].keys()),
                       dtype=tp_dado_arquivos['lista_preco']).applymap(padronizar)
df_valor_venda = df_valor_venda.rename(columns=rename_dataframes['df_valor_venda'])

# DataFrame :: Unidades Receita Movimentação
df_pontos_venda = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['unidades_rec_mov']),
                         sheet_name= arquivos_primarios['unidades_rec_mov_sn'], 
                       usecols=list(tp_dado_arquivos['unidades_rec_mov'].keys()),
                       dtype=tp_dado_arquivos['unidades_rec_mov']).applymap(padronizar)
df_pontos_venda = df_pontos_venda.rename(columns=rename_dataframes['df_pontos_venda'])

# DataFrame :: Template Receita Movimentação
#validar_data_arquivo(os.path.join(cwd, path + arquivos_primarios['unidades_rec_mov']))
df_template_rmov = pd.read_csv(os.path.join(cwd, path + arquivos_primarios['template_rec_mov']), delimiter = ';', encoding = 'utf-8', 
                               usecols=list(tp_dado_arquivos['template_rec_mov_sn'].keys()), dtype=tp_dado_arquivos['template_rec_mov_sn'])

df_corrente_produto = pd.read_csv(os.path.join(cwd, path + arquivos_primarios['template_rec_mov']), delimiter = ';',
                       encoding = 'utf-8', usecols=list(tp_dado_arquivos['template_rec_mov_corrente'].keys()),
                       dtype=tp_dado_arquivos['template_rec_mov_corrente'])

df_corrente_produto = df_corrente_produto.rename(columns = {'Produto':'PRD-VCM'}).drop_duplicates()

# =======================================================================================================================
# EXECUÇÃO DE SCRIPTS
# =======================================================================================================================

# df_valor_venda["Preço"] = df_valor_venda["Preço"].str.replace("'","")
# df_valor_venda["Preço"] = df_valor_venda["Preço"].str.replace(",",".")
# df_valor_venda["Ptax USD"] = df_valor_venda["Ptax USD"].str.replace("'","")
# df_valor_venda["Ptax USD"] = df_valor_venda["Ptax USD"].str.replace(",",".")
df_valor_venda = df_valor_venda.merge(df_periodos[['PERIODO', 'NOME_PERIODO']], how = 'cross')
df_valor_venda['Validar'] = (df_valor_venda['PERIODO'] >= df_valor_venda['Data Inicio']) & (df_valor_venda['PERIODO'] <= df_valor_venda['Data fim'])
df_valor_venda = df_valor_venda.loc[df_valor_venda.Validar == True]
df_valor_venda = df_valor_venda.reset_index().drop(columns=['index','Validar','Data Inicio','Data fim'])

agrupamento_pf = agrupamento_pf.drop_duplicates(subset = 'COD_ESPECIFICO')
df_valor_venda = df_valor_venda.merge(agrupamento_pf, how = 'left', left_on = 'Código do Produto', right_on = 'COD_ESPECIFICO')
df_valor_venda.rename(columns = {"CODIGO_AGRUPADO": "CODIGO_ITEM"}, inplace=True)
df_valor_venda = df_valor_venda.astype({'Ptax USD':np.float32,'Preço':np.float32})
df_valor_venda = df_valor_venda.merge(df_produtos[["CODIGO_ITEM", "PRD-VCM"]], on = "CODIGO_ITEM", how="inner")

# Convertendo de dólares para reais quando necessário
df_valor_venda["Preço Venda"] = np.where(df_valor_venda["Moeda"] == "BRL",
                                    df_valor_venda["Preço"],
                                    df_valor_venda["Preço"] *                                           
                                    df_valor_venda["Ptax USD"])

df_valor_venda= df_valor_venda.rename(columns={"Nome da Lista": "Desc. Empresa"})
df_valor_venda = df_valor_venda[["Desc. Empresa", "PRD-VCM", "Preço Venda","NOME_PERIODO"]]
df_valor_venda.drop_duplicates(subset = ["Desc. Empresa", "PRD-VCM"], inplace = True)

# Tirando duplicatas em casos de produtos que tinham preços em real e dólar originalmente
valor_venda_medio = df_valor_venda.groupby(by = ["PRD-VCM"])['Preço Venda'].mean()
valor_venda_medio = valor_venda_medio.to_frame()
valor_venda_medio = valor_venda_medio.reset_index()
#valor_venda_medio.reset_index(inplace = True)
valor_venda_medio.rename(columns = {"Preço Venda": "Preço Venda Médio"}, inplace=True)

df_pontos_venda = df_pontos_venda.merge(df_correntes, on = "Origem", how = "left")
# 14/05/2024 - Incluindo uma etapa de exclusão de NaN
df_pontos_venda = df_pontos_venda.dropna()
# Pegando apenas as correntes que vão para mercados consumidores
print('Selecionando correntes para mercados consumidores...')
df_pontos_venda[df_pontos_venda["Destino"].str.contains("MC")]

# Trazendo os produtos de cada corrente
print('Inserindo produtos nas correntes...')
df_pontos_venda = df_pontos_venda.merge(df_corrente_produto, on = "Corrente",
                                        how = "inner")
df_pontos_venda = df_pontos_venda.merge(df_periodos[['NOME_PERIODO','PERIODO']], how = 'cross')

df_pontos_venda["Desc. Empresa"] = df_pontos_venda["Origem ECFTO"]
df_pontos_venda.drop(labels = ["Origem ECFTO"], axis = 1, inplace = True)

df_receita_movimentacao = df_pontos_venda.merge(df_valor_venda, on = ["Desc. Empresa", "PRD-VCM","NOME_PERIODO"], how = "left")
df_receita_movimentacao["Preço Venda"] = (df_receita_movimentacao["Preço Venda"].fillna(0))

# Trazendo preços médios para quando não houver preços específicos
df_receita_movimentacao = df_receita_movimentacao.merge(valor_venda_medio, on = ["PRD-VCM"], how = "left")
df_receita_movimentacao["Preço Venda Médio"] = (df_receita_movimentacao["Preço Venda Médio"].fillna(0))

# Classificação de produtos para eliminar o que não é produto final
df_receita_movimentacao = df_receita_movimentacao.merge(df_produtos[["PRD-VCM", "TIPO_MATERIAL"]],on = "PRD-VCM", how = "left")

# O que não é produto final fica com preço zerado
# O que tem preço específico é usado
print('Preenchendo valores de venda...')
print('Exceções: 1) Quando não há preço específico para unidade, usar o média')
print('          2) Quando SKU não diz respeito a produto acabado, desconsiderar')
print('          3) Preencher com valor >> 0 << quando não for possível estimar valor de venda')
df_receita_movimentacao.loc[df_receita_movimentacao["Preço Venda"] >= 0,
                            "Preço Final"]  = df_receita_movimentacao["Preço Venda"]
df_receita_movimentacao.loc[df_receita_movimentacao["Preço Venda"] == 0,
                            "Preço Final"]  = df_receita_movimentacao["Preço Venda Médio"]
df_receita_movimentacao.loc[(df_receita_movimentacao["TIPO_MATERIAL"] != "PF - FERTILIZANTE"),
                            "Preço Final"]  = 0

# Retirando colunas que não vão mais ser necessárias
df_receita_movimentacao.drop(labels = ["Desc. Empresa", "Preço Venda",
                                       "Preço Venda Médio", "TIPO_MATERIAL"], axis = 1, inplace = True)
df_receita_movimentacao_periodos = df_receita_movimentacao.copy()

df_receita_movimentacao_periodos.sort_values(["Corrente", "PRD-VCM"], inplace = True)

# Deixando na mesma forma que o template vindo do VCM
df_receita_movimentacao_periodos.rename(columns = {"PRD-VCM" : "Produto", 
                                                   "Periodo_VCM" : "Periodo",
                                                   "Preço Final" : "Valor"},
                                        inplace=True)

# Alterando a ordem das colunas e dropando o que nao é necessário
df_receita_movimentacao_periodos = df_receita_movimentacao_periodos[
                                   ["Origem", "Destino", "Corrente", "Produto",
                                    "NOME_PERIODO", "Valor"]].rename(columns={'NOME_PERIODO':'Periodo'})

# Comparação com o template gerado pelo VCM
# Ideia: pegar todas as linhas que apareçam no template e usar apenas o valor
# novo calculado
df_template_rmov.drop(labels="Valor", axis=1, inplace=True) 
df_receita_movimentacao_periodos = df_receita_movimentacao_periodos.merge(
                                   df_template_rmov, 
                                   on = ["Origem", "Destino",
                                         "Corrente", "Produto", "Periodo"], 
                                   how = "right")
df_receita_movimentacao_periodos.fillna(0, inplace = True)

df_receita_movimentacao_periodos['Valor'] = df_receita_movimentacao_periodos['Valor'].round(2)
# (03/07/2025) Zerando receita movimentação pois já usamos preço fixo para isso.
df_receita_movimentacao_periodos['Valor'] = 0.0
# 12/04/2024: Alterando enconding para utf-8 como alinhado com o time da OP2B
df_receita_movimentacao_periodos.to_csv(os.path.join(cwd,output_path + "Receita Movimentacao Para VCM.csv"),
                                          sep = ';', encoding = 'utf-8-sig', index = False)

print('Atualização de Receitas de Movimentação finalizada!')
end_time = time.time()
print(f'Tempo de Execução: {round(end_time - start_time,2)} segundos')
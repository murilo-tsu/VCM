print('\n')
print('╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗')
print('║                                           ATUALIZACAO DE DADOS - VCM                                           ║')
print('║                                                 >>  tax.py  <<                                                 ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ Criado por:    Isabela Nunes dos Santos        Data: 26/03/2025                                                ║')
print('║ Editado por:   Isabela Nunes dos Santos        Data: 07/07/2025                                                ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ CHANGELOG:                                                                                                     ║')
print('║ - v1.0.0 (27/03/2025): Criação da primeira versão do script unificado com edições estruturais nos arquivos     ║')
print('║                        de depara e dado primário.                                                              ║')
print('║                                                                                                                ║')
print('║ - v1.0.1 (07/07/2025): Criação de orientação a objeto para execução de scripts integrados                      ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ Este script é responsável pela atualização:                                                                    ║')
print('║ >> ICMS de Entrada                                                                                             ║')
print('║ >> ICMS de Saída                                                                                               ║')
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
import inspect
from tqdm import tqdm
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

logging.info("Iniciando execução do script.")


# =======================================================================================================================
# FUNÇÕES
# =======================================================================================================================

from _modulos import aux_functions_vcm
fx = aux_functions_vcm()

# =======================================================================================================================
# DEFINIR ARQUIVOS
# =======================================================================================================================

from _dicionarios import arquivos_primarios, tp_dado_arquivos, rename_dataframes

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
                            sheet_name = arquivos_primarios['periodos_sn'],
                         usecols=list(tp_dado_arquivos['periodos'].keys()),
                         dtype=tp_dado_arquivos['periodos'])
df_periodos = df_periodos.rename(columns=rename_dataframes['df_periodos_tax'])

# Dataframe :: Cadastro Produtos
df_produtos = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['cadastro_produtos']),
                                  sheet_name = arquivos_primarios['cadastro_produtos_sn01'],
                                  usecols = list(tp_dado_arquivos['cadastro_produtos_sn01'].keys()),
                                  dtype = tp_dado_arquivos['cadastro_produtos_sn01'])
df_produtos = df_produtos.rename(columns=rename_dataframes['df_produtos'])

# DataFrame :: Update de Correntes
df_correntes = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['arq_tbUpdateCorrentes']),
                                    sheet_name= arquivos_primarios['arq_tbUpdateCorrentes_sn'], 
                                    usecols=list(tp_dado_arquivos['arq_tbUpdateCorrentes'].keys()),
                                    dtype=tp_dado_arquivos['arq_tbUpdateCorrentes']).applymap(fx.padronizar)
df_correntes = df_correntes.rename(columns=rename_dataframes['df_correntes'])

# Dataframe :: Abertura de Correntes e produtos
df_corrente_produto = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['template_corrente_produto']),
                                            usecols=list(tp_dado_arquivos['template_corrente_produto'].keys()),
                                            dtype=tp_dado_arquivos['template_corrente_produto'])
df_corrente_produto = df_corrente_produto.rename(columns={'Produto':'PRD-VCM'})

# Dataframe :: Template Impostos Entrada
#validar_data_arquivo(os.path.join(cwd, path + arquivos_primarios['template_imp_entrada']))
df_template_icms_entrada = pd.read_csv(os.path.join(cwd, path + arquivos_primarios['template_imp_entrada']), delimiter = ';',
                       encoding = 'utf-8', usecols=list(tp_dado_arquivos['template_imp_entrada'].keys()),
                       dtype=tp_dado_arquivos['template_imp_entrada'])

# Dataframe :: Template Impostos Saida
#validar_data_arquivo(os.path.join(cwd, path + arquivos_primarios['template_imp_saida']))
df_template_icms_saida = pd.read_csv(os.path.join(cwd, path + arquivos_primarios['template_imp_saida']), delimiter = ';',
                       encoding = 'utf-8', usecols=list(tp_dado_arquivos['template_imp_saida'].keys()),
                       dtype=tp_dado_arquivos['template_imp_saida'])

# Dataframe :: Custo de Reposição
df_valor_compra =  pd.read_excel(os.path.join(cwd, path + arquivos_primarios['custo_reposicao']),
                                        sheet_name= arquivos_primarios['custo_reposicao_sn'], 
                                        usecols=list(tp_dado_arquivos['custo_reposicao'].keys()),
                                        dtype=tp_dado_arquivos['custo_reposicao'])
df_valor_compra = df_valor_compra.rename(columns=rename_dataframes['df_valor_compra'])

# Dataframe :: Lista Preço
df_valor_venda = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['lista_preco']),
                                sheet_name= arquivos_primarios['lista_preco_sn'], 
                                usecols=list(tp_dado_arquivos['lista_preco'].keys()),
                                dtype=tp_dado_arquivos['lista_preco']).applymap(fx.padronizar)
df_valor_venda = df_valor_venda.rename(columns=rename_dataframes['df_valor_venda'])

# Dataframe :: Unidades ICMS
df_pontos_balanco = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['unidades_icms']),
                                        sheet_name = arquivos_primarios['unidades_icms_sn'],
                                        usecols = list(tp_dado_arquivos['unidades_icms'].keys()),
                                        dtype = tp_dado_arquivos['unidades_icms']).applymap(fx.padronizar)
df_pontos_balanco = df_pontos_balanco.rename(columns=rename_dataframes['unidades_icms'])

# Dataframe :: Agrupamento
agrupamento_produtos = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['cadastro_produtos']),
                                          sheet_name = arquivos_primarios['cadastro_produtos_sn02'],
                                          usecols = list(tp_dado_arquivos['cadastro_produtos_sn02'].keys()),
                                          dtype = tp_dado_arquivos['cadastro_produtos_sn02'])

proxy_agrupamento = df_produtos[['ITEM_CODE','DESCRICAO']]
proxy_agrupamento = proxy_agrupamento.rename(columns={'ITEM_CODE':'COD_ESPECIFICO','DESCRICAO':'DESCRICAO_ESPECIFICA'})
proxy_agrupamento['CODIGO_AGRUPADO'] = proxy_agrupamento['COD_ESPECIFICO']
proxy_agrupamento['AGRUPAMENTO_MP'] = proxy_agrupamento['DESCRICAO_ESPECIFICA']
agrupamento_produtos = pd.concat([agrupamento_produtos,proxy_agrupamento])
agrupamento_produtos = agrupamento_produtos.drop_duplicates(subset = 'COD_ESPECIFICO')


# =======================================================================================================================
# EXECUÇÃO DE SCRIPTS
# =======================================================================================================================

df_valor_compra = df_valor_compra.merge(df_periodos[['PERIODO','Periodo_VCM']], how = 'cross')
df_valor_compra['Validar'] = (df_valor_compra['PERIODO'] >= df_valor_compra['Data Inicial']) & (df_valor_compra['PERIODO'] <= df_valor_compra['Data Final'])
df_valor_compra = df_valor_compra.loc[df_valor_compra.Validar == True]
df_valor_compra = df_valor_compra.reset_index().drop(columns=['index','Validar','Data Inicial','Data Final'])

# Ajustes de dados da base de preços de lista
# df_valor_venda['Código do Produto'] = df_valor_venda['Código do Produto'].str.replace("'","")
# df_valor_venda = df_valor_venda.astype({'Preço':str,'Ptax USD':str})
# df_valor_venda["Preço"] = df_valor_venda["Preço"].str.replace("'","")
# df_valor_venda["Preço"] = df_valor_venda["Preço"].str.replace(",",".")
# df_valor_venda["Ptax USD"] = df_valor_venda["Ptax USD"].str.replace("'","")
# df_valor_venda["Ptax USD"] = df_valor_venda["Ptax USD"].str.replace(",",".")
# df_valor_venda = df_valor_venda.astype({"Preço" : float, "Ptax USD" : float})

df_valor_venda = df_valor_venda.merge(df_periodos[['PERIODO','Periodo_VCM']], how = 'cross')
df_valor_venda['Validar'] = (df_valor_venda['PERIODO'] >= df_valor_venda['Data Inicio']) & (df_valor_venda['PERIODO'] <= df_valor_venda['Data fim'])
df_valor_venda = df_valor_venda.loc[df_valor_venda.Validar == True]
df_valor_venda = df_valor_venda.reset_index().drop(columns=['index','Validar','Data Inicio','Data fim'])

df_valor_compra = fx.left_outer_join(df_valor_compra,agrupamento_produtos,left_on='CD_PRODUTO_FTO',right_on='COD_ESPECIFICO',
                                     name_right='Custo de Reposição',name_left='Agrupamento de Produtos')
df_valor_venda = fx.left_outer_join(df_valor_venda,agrupamento_produtos,left_on='Código do Produto',right_on='COD_ESPECIFICO',
                                    name_right='Lista Preço',name_left='Agrupamento de Produtos')
df_valor_venda = df_valor_venda.dropna(subset = 'COD_ESPECIFICO')
df_valor_compra.rename(columns = {"CODIGO_AGRUPADO": "ITEM_CODE"},
                       inplace=True)
df_valor_venda.rename(columns = {"CODIGO_AGRUPADO": "ITEM_CODE"},
                       inplace=True)

# Tópico 1
# Verificar se não faz mais sentido puxar isso de uma tabela?
# Manter por enquanto!
df_valor_compra['Desc. Empresa'] = df_valor_compra['Desc. Empresa'].replace(['ARO', 'BCO', 'CTO', 'LMO', 'PLO', 'PNO', 'QRO', 'RNO', 'SLO', 'SNO', 'PVO'],\
                                                                              ['BRFTO:ARAGUARI OPM','BRFTO:BARCARENA OPM','BRFTO:CATALAO OPM',\
                                                                               'BRFTO:LUIS EDUARDO MAGALHAES OPM','BRFTO: PAULINIA OPM',\
                                                                                'BRFTO:PORTO NACIONAL OPM','BRFTO:QUERENCIA OPM',\
                                                                                'BRFTO:RONDONOPOLIS OPM','BRFTO:SAO LUIS OPM','BRFTO:SINOP OPM',\
                                                                                'BRFTO:PORTO VELHO OPM'])

df_valor_compra = df_valor_compra[["Periodo_VCM","Moeda", "ITEM_CODE",
                                   "Desc. Empresa", "Ptax Dia Anterior",
                                   "Custo Rep. Mercado"]]
df_valor_compra = df_valor_compra.merge(df_produtos[["ITEM_CODE", "PRD-VCM"]],
                                        on = "ITEM_CODE", how="inner")
# Convertendo preços de USD para BRL quando necessário
df_valor_compra["Preço Compra"] = np.where(df_valor_compra["Moeda"] == "BRL",
                                    df_valor_compra["Custo Rep. Mercado"],
                                    df_valor_compra["Custo Rep. Mercado"] *                                           
                                    df_valor_compra["Ptax Dia Anterior"])
df_valor_compra =  df_valor_compra[["Periodo_VCM","PRD-VCM", "Desc. Empresa", "Preço Compra"]]

# Tópico 2
## Cabe decisão se devemos realmente tirar uma média considerando todos os códigos agrupados ou não
## ================================================================================================
df_valor_compra = df_valor_compra.groupby(by=["Periodo_VCM","Desc. Empresa", "PRD-VCM"]).mean()
df_valor_compra = df_valor_compra.reset_index()

# Tópico 3
## Cabe decisão se o agrupamento para preços médios deverá ser por produto ou produto + período
## ============================================================================================
df_valor_compra.drop_duplicates(subset = ["Periodo_VCM","Desc. Empresa", "PRD-VCM"],
                               inplace = True)
# Tirando duplicatas em casos de produtos que tinham preços em real e dólar originalmente
valor_compra_medio = df_valor_compra.groupby(by=["PRD-VCM"])['Preço Compra'].mean()
valor_compra_medio = valor_compra_medio.to_frame()
valor_compra_medio = valor_compra_medio.reset_index()
valor_compra_medio.rename(columns = {"Preço Compra": "Preço Compra Médio"}, inplace=True)

# Estrutura do Valor venda
# Convertendo os preços para números
# df_valor_venda["Preço"] = df_valor_venda["Preço"].str.replace("'","")
# df_valor_venda["Preço"] = df_valor_venda["Preço"].str.replace(",",".")

# df_valor_venda["Ptax USD"] = df_valor_venda["Ptax USD"].str.replace("'","")
# df_valor_venda["Ptax USD"] = df_valor_venda["Ptax USD"].str.replace(",",".")

df_valor_venda = df_valor_venda.astype({"Preço" : float, "Ptax USD" : float})

# Convertendo de dólares para reais quando necessário
df_valor_venda["Preço Venda"] = np.where(df_valor_venda["Moeda"] == "BRL",
                                    df_valor_venda["Preço"],
                                    df_valor_venda["Preço"] *                                           
                                    df_valor_venda["Ptax USD"])

# Tirando a apóstrofe que está presente também no ITEM_CODE nesta planilha
df_valor_venda["ITEM_CODE"] = df_valor_venda["ITEM_CODE"].str.replace("'","")
df_valor_venda = df_valor_venda.merge(df_produtos[["ITEM_CODE", "PRD-VCM"]],
                                        on = "ITEM_CODE", how="inner")

df_valor_venda["Desc. Empresa"] = df_valor_venda["Nome da Lista"]

df_valor_venda = df_valor_venda[["Periodo_VCM","Desc. Empresa", "PRD-VCM", "Preço Venda"]]
df_valor_venda.drop_duplicates(subset = ["Desc. Empresa", "PRD-VCM"],
                               inplace = True)

# Tirando duplicatas em casos de produtos que tinham preços em real e dólar originalmente
## Cabe decisão se devemos também considerar o período no preço médio
valor_venda_medio = df_valor_venda.groupby(by = ["PRD-VCM"])['Preço Venda'].mean()
valor_venda_medio = valor_venda_medio.to_frame()
valor_venda_medio.reset_index(inplace = True)
valor_venda_medio.rename(columns = {"Preço Venda": "Preço Venda Médio"},
                         inplace=True)

# Base Saída  
df_base_saida = df_pontos_balanco.loc[df_pontos_balanco["Zerar saída"] == 0,
                                        ["Unidades", "Desc. Empresa"]]

# Trazendo todos as correntes saindo dessas unidades e seus pontos de chegada
df_base_saida = df_base_saida.merge(df_correntes,
                  left_on = "Unidades", right_on = "Origem", how = "inner" 
                  )[["Origem", "Desc. Empresa", "Destino", "Corrente", "Tipo"]]
# Reordenando as colunas apenas porque me pareceu mais legível assim
# para diferenciar do df_base_entrada

# Filtrando para retirar movimentações internas
df_base_saida = df_base_saida[(df_base_saida["Tipo"] == "INBOUND") |
                              (df_base_saida["Tipo"] == "OUTBOUND") |
                              (df_base_saida["Tipo"] == "TRANSFERENCIA")]

# Trazendo os produtos presentes em cada uma das correntes
df_base_saida = fx.left_outer_join(df_base_saida,df_corrente_produto, left_on= "Corrente", right_on= "Corrente",
                                    struct=False, name_right='Base de Saída',name_left='Abertura de Correntes e Produtos')

## Inserindo dimensão temporal através do df_periodos
## Posteriormente, realizar o merge considerando também períodos
df_base_saida = df_base_saida.merge(df_periodos[['Periodo_VCM']], how = 'cross')
df_base_saida = fx.left_outer_join(df_base_saida,df_valor_venda, 
                                    left_on = ["Periodo_VCM","Desc. Empresa", "PRD-VCM"],
                                    right_on = ["Periodo_VCM","Desc. Empresa", "PRD-VCM"],
                                    name_right='Base de Saída',name_left='Lista Preço')
df_base_saida["Preço Venda"] = df_base_saida["Preço Venda"].fillna(0)
df_base_saida = fx.left_outer_join(df_base_saida, valor_venda_medio, 
                                    left_on = "PRD-VCM", right_on="PRD-VCM",
                                    name_right='Base de Saída',name_left='Preço Médio')
df_base_saida["Preço Venda Médio"] = df_base_saida["Preço Venda Médio"].fillna(0)

# Atribuindo o valor da base de cálculo a partir do preço de venda ou do estoque
df_base_saida_periodos = df_base_saida.copy()
df_base_saida_periodos.loc[df_base_saida_periodos["Preço Venda"] != 0,
                             'Base de Cálculo'] = df_base_saida_periodos["Preço Venda"]

df_base_saida_periodos.loc[df_base_saida_periodos["Preço Venda"] == 0,
                            'Base de Cálculo'] = df_base_saida_periodos["Preço Venda Médio"]

# Deixando na mesma forma que o template VCM
df_base_saida_periodos.rename(columns = {"Origem" : "Unidade Origem",
                                         "Destino" : "Unidade Destino",
                                          "PRD-VCM" : "Produto",
                                          "Periodo_VCM" : "Período"},
                              inplace = True)
df_base_saida_periodos.drop(labels=["Desc. Empresa", "Tipo", "Preço Venda", 
                                      "Preço Venda", "Preço Venda Médio"],
                            axis = 1, inplace = True)

print(df_base_saida_periodos.columns)

# Comparação com o template gerado pelo VCM
# Ideia: pegar todas as linhas que apareçam no template e usar apenas o valor novo calculado
df_template_icms_saida.drop(labels="Base de Cálculo", axis=1, inplace=True) 

df_template_icms_saida = fx.left_outer_join(df_template_icms_saida,df_base_saida_periodos,left_on=["Unidade Origem", "Unidade Destino",
                                         "Corrente", "Produto", "Período"],right_on=["Unidade Origem", "Unidade Destino",
                                         "Corrente", "Produto", "Período"], name_right='Template Impostos Saida',name_left='Base de Saída')

df_template_icms_saida.fillna(0, inplace = True)

# Inserindo a matriz de balanço de impostos para entrada
df_template_icms_saida = fx.left_outer_join(df_template_icms_saida,df_pontos_balanco[['Unidades','Zerar entrada','Zerar saída']],left_on='Unidade Origem',right_on='Unidades',
                                            name_right='Template Impostos Saída', name_left='Unidades ICMS - Entrada')
df_template_icms_saida = df_template_icms_saida[["Unidade Origem",
                        "Unidade Destino", "Corrente", "Produto",
                        "Período", "Base de Cálculo", "ICMS-SUBST", "ICMS-ST",
                        "Zerar entrada","Zerar saída"]]
df_template_icms_saida = df_template_icms_saida.rename(columns={'Zerar entrada':'ORG-IN','Zerar saída':'ORG-OUT'})

# Inserindo a matriz de balanço de impostos para saída
df_template_icms_saida = fx.left_outer_join(df_template_icms_saida,df_pontos_balanco[['Unidades','Zerar entrada','Zerar saída']],left_on='Unidade Destino',right_on='Unidades',
                                            name_right='Template Impostos Saída', name_left='Unidades ICMS - Saída')
df_template_icms_saida = df_template_icms_saida[["Unidade Origem",
                        "Unidade Destino", "Corrente", "Produto",
                        "Período", "Base de Cálculo", "ICMS-SUBST", "ICMS-ST",
                        "ORG-IN","ORG-OUT", "Zerar entrada","Zerar saída"]]
df_template_icms_saida = df_template_icms_saida.rename(columns={'Zerar entrada':'DEST-IN','Zerar saída':'DEST-OUT'})
df_template_icms_saida = df_template_icms_saida.astype({'ORG-OUT':np.float64,'DEST-IN':np.float64,'Base de Cálculo':np.float64})
df_template_icms_saida['Tax.Check'] = df_template_icms_saida['ORG-OUT'] + df_template_icms_saida['DEST-IN']
df_template_icms_saida['Tax.Check'] = df_template_icms_saida['Tax.Check'].replace(0.0,np.nan)
df_template_icms_saida['Tax.Check'] = df_template_icms_saida['Tax.Check'].replace([1.0,2.0],0.0)
df_template_icms_saida['Tax.Check'] = df_template_icms_saida['Tax.Check'].fillna(1.0)
df_template_icms_saida['Base de Cálculo'] = df_template_icms_saida['Base de Cálculo']*df_template_icms_saida['Tax.Check']
df_template_icms_saida['Base de Cálculo'] = df_template_icms_saida['Base de Cálculo'].round(2)

# Ajustando a ordem das colunas
df_template_icms_saida = df_template_icms_saida[["Unidade Origem",
                        "Unidade Destino", "Corrente", "Produto",
                        "Período", "Base de Cálculo", "ICMS-SUBST", "ICMS-ST"]]

# Criando arquivo output para ICMS de Saída
# 12/04/2024: Alterando encoding para utf-8 e delimitador (sep) para >> ; <<
df_template_icms_saida.to_csv(os.path.join(cwd,output_path + "tbOutImpICMSSaida.csv"),
                                          #sheet_name = "ICMS_Saida",
                                          sep = ';', encoding = 'utf-8-sig',
                                          index = False)

print('Arquivos de ICMS de Saída preenchido com Sucesso!')
print('\n')


# =============
# Base Entrada
df_base_entrada = df_pontos_balanco.loc[df_pontos_balanco["Zerar entrada"] == 0,
                                        ["Unidades", "Desc. Empresa"]]

# Trazendo todos as correntes entrando nessas ue seus pontos de origem
df_base_entrada = df_base_entrada.merge(df_correntes,
                    left_on = "Unidades", right_on = "Destino", how = "inner"
                    )[["Destino", "Desc. Empresa", "Origem", "Corrente", "Tipo"]]

# Filtrando para retirar movimentações internas
df_base_entrada = df_base_entrada[(df_base_entrada["Tipo"] == "INBOUND") | (df_base_entrada["Tipo"] == "INTERNO") |
                              (df_base_entrada["Tipo"] == "OUTBOUND") |
                              (df_base_entrada["Tipo"] == "TRANSFERENCIA")]

# Trazendo os produtos presentes em cada uma das correntes
df_base_entrada = fx.left_outer_join(df_base_entrada, df_corrente_produto, left_on="Corrente",
                                       right_on="Corrente", struct=False, 
                                       name_right='Base de Entrada', name_left='Abertura de Correntes e Produtos')
df_base_entrada = df_base_entrada.merge(df_periodos[['Periodo_VCM']], how = 'cross')
df_base_entrada = fx.left_outer_join(df_base_entrada, df_valor_compra, 
                                    left_on = ["Periodo_VCM","Desc. Empresa", "PRD-VCM"], right_on= ["Periodo_VCM","Desc. Empresa", "PRD-VCM"],
                                    name_right='Base de Entrada', name_left='Custo de Reposição')
df_base_entrada["Preço Compra"] = df_base_entrada["Preço Compra"].fillna(0)
df_base_entrada = fx.left_outer_join(df_base_entrada, valor_compra_medio,
                                left_on = "PRD-VCM", right_on="PRD-VCM",
                                name_right='Base de Entrada', name_left='Custo Médio')
df_base_entrada["Preço Compra Médio"] = df_base_entrada["Preço Compra Médio"].fillna(0)
df_base_entrada_periodos = df_base_entrada.copy()

# Atribuindo o valor da base de cálculo a partir do custo de reposição ou do estoque
df_base_entrada_periodos.loc[df_base_entrada_periodos["Preço Compra"] != 0,
                             'Base de Cálculo'] = df_base_entrada_periodos["Preço Compra"]

df_base_entrada_periodos.loc[df_base_entrada_periodos["Preço Compra"] == 0,
                            'Base de Cálculo'] = df_base_entrada_periodos["Preço Compra Médio"]

# Deixando na mesma forma que o template do VCM
df_base_entrada_periodos.rename(columns = {"Destino" : "Unidade Destino",
                                           "Origem" : "Unidade Origem",
                                           "PRD-VCM" : "Produto",
                                           "Periodo_VCM" : "Período"},
                                inplace = True)
df_base_entrada_periodos.drop(labels=["Desc. Empresa", "Tipo", "Preço Compra", "Preço Compra Médio"],
                              axis = 1, inplace = True)
print(df_base_entrada_periodos.columns)

# Comparação com o template gerado pelo VCM
# Ideia: pegar todas as linhas que apareçam no template e usar apenas o valor
# novo calculado
df_template_icms_entrada.drop(labels="Base de Cálculo", axis=1, inplace=True) 
df_template_icms_entrada = fx.left_outer_join(df_template_icms_entrada, df_base_entrada_periodos, 
                                   left_on = ["Unidade Destino", "Unidade Origem",
                                         "Corrente", "Produto", "Período"], 
                                   right_on= ["Unidade Destino", "Unidade Origem",
                                         "Corrente", "Produto", "Período"],
                                         name_right='Template ICMS Entrada', name_left='Base de Entrada')
df_template_icms_entrada.fillna(0, inplace = True)

# Dados de Entrada
df_pontos_balanco = df_pontos_balanco[['Unidades','Zerar entrada','Zerar saída']]
df_template_icms_entrada = fx.left_outer_join(df_template_icms_entrada,df_pontos_balanco,
                left_on='Unidade Origem',right_on='Unidades',
                name_right='Template ICMS Entrada', name_left='Unidades ICMS - Entrada')

df_template_icms_entrada = df_template_icms_entrada[["Unidade Origem",
                        "Unidade Destino", "Corrente", "Produto",
                        "Período", "Base de Cálculo", "ICMS-SUBST", "ICMS-ST",
                        "Zerar entrada","Zerar saída"]]
df_template_icms_entrada = df_template_icms_entrada.rename(columns={'Zerar entrada':'ORG-IN','Zerar saída':'ORG-OUT'})

# Inserindo a matriz de balanço de impostos para saída
df_template_icms_entrada = fx.left_outer_join(df_template_icms_entrada,df_pontos_balanco,left_on='Unidade Destino',right_on='Unidades',
                                              name_right='Template ICMS Entrada', name_left='Unidades ICMS - Saída')
df_template_icms_entrada = df_template_icms_entrada[["Unidade Origem",
                        "Unidade Destino", "Corrente", "Produto",
                        "Período", "Base de Cálculo", "ICMS-SUBST", "ICMS-ST",
                        "ORG-IN","ORG-OUT",
                        "Zerar entrada","Zerar saída"]]
df_template_icms_entrada = df_template_icms_entrada.rename(columns={'Zerar entrada':'DEST-IN','Zerar saída':'DEST-OUT'})
df_template_icms_entrada = df_template_icms_entrada.astype({'ORG-OUT':np.float64,'DEST-IN':np.float64,'Base de Cálculo':np.float64})
df_template_icms_entrada['Tax.Check'] = df_template_icms_entrada['ORG-OUT'] + df_template_icms_entrada['DEST-IN']
df_template_icms_entrada['Tax.Check'] = df_template_icms_entrada['Tax.Check'].replace(0.0,np.nan)
df_template_icms_entrada['Tax.Check'] = df_template_icms_entrada['Tax.Check'].replace([1.0,2.0],0.0)
df_template_icms_entrada['Tax.Check'] = df_template_icms_entrada['Tax.Check'].fillna(1.0)
df_template_icms_entrada['Base de Cálculo'] = df_template_icms_entrada['Base de Cálculo']*df_template_icms_entrada['Tax.Check']
df_template_icms_entrada['Base de Cálculo'] =df_template_icms_entrada['Base de Cálculo'].round(2)

# Dados de Entrada
# Ajustando a ordem das colunas
df_template_icms_entrada = df_template_icms_entrada[["Unidade Destino",
                            "Unidade Origem", "Corrente", "Produto",
                            "Período", "Base de Cálculo", "ICMS-SUBST", "ICMS-ST"]]

# Criando arquivo de output para o ICMS de Entrada
# 12/04/2024: Alterando o encoding para utf-8 e especificando o delimitador (sep) para >> ; <<
df_template_icms_entrada.to_csv(os.path.join(cwd,output_path + "tbOutImpICMSEntrada.csv"),
                                          #sheet_name = "ICMS_Entrada",
                                          sep = ';', encoding = 'utf-8-sig',
                                          index = False)

print('\nArquivos de ICMS de Entrada preenchido com Sucesso!')
print('Fim da execução dos Scripts de atualização de ICMS!')
end_time = time.time()
print(f'Tempo de Execução: {round(end_time - start_time,2)} segundos')
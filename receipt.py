print('\n')
print('╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗')
print('║                                           ATUALIZACAO DE DADOS - VCM                                           ║')
print('║                                               >>  receipt.py  <<                                               ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ Criado por:    Isabela Nunes dos Santos        Data: 24/03/2025                                                ║')
print('║ Editado por:   Isabela Nunes dos Santos        Data: 08/08/2025                                                ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ CHANGELOG:                                                                                                     ║')
print('║ - v1.0.0 (24/03/2025): Criação da primeira versão do script unificado com edições estruturais nos arquivos     ║')
print('║                        de depara e dado primário.                                                              ║')
print('║                                                                                                                ║')
print('║ - v1.0.1 (07/07/2025): Criação de orientação a objeto para execução de scripts integrados.                     ║')
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
                         usecols=list(tp_dado_arquivos['periodos'].keys()),
                         dtype=tp_dado_arquivos['periodos'])

# Dataframe :: Cadastro Produtos
df_produtos = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['cadastro_produtos']),
                                  sheet_name = arquivos_primarios['cadastro_produtos_sn01'],
                                  usecols = list(tp_dado_arquivos['cadastro_produtos_sn01'].keys()),
                                  dtype = tp_dado_arquivos['cadastro_produtos_sn01'])

# DataFrame :: cadastro de matérias-primas :: filtro no tipo de material da tabela CADASTRO
cadastro_pf = df_produtos[(df_produtos['TIPO_MATERIAL'].str.split('-',expand=True)[0].str.strip() == 'PF')]


# Dataframe :: Agrupamento
agrupamento_pf = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['cadastro_produtos']),
                                  sheet_name = arquivos_primarios['cadastro_produtos_sn02'],
                                  usecols = list(tp_dado_arquivos['cadastro_produtos_sn02'].keys()),
                                  dtype = tp_dado_arquivos['cadastro_produtos_sn02'])
agrupamento_pf = agrupamento_pf[agrupamento_pf['TIPO_MATERIAL'] == 'PF']
agrupamento_pf.drop(columns='TIPO_MATERIAL')
proxy_agrupamento = cadastro_pf[['CODIGO_ITEM','DESCRICAO']]
proxy_agrupamento = proxy_agrupamento.rename(columns={'CODIGO_ITEM':'COD_ESPECIFICO','DESCRICAO':'DESCRICAO_ESPECIFICA'})
proxy_agrupamento['CODIGO_AGRUPADO'] = proxy_agrupamento['COD_ESPECIFICO']
proxy_agrupamento['AGRUPAMENTO_MP'] = proxy_agrupamento['DESCRICAO_ESPECIFICA']
agrupamento_produtos = pd.concat([agrupamento_pf,proxy_agrupamento])
agrupamento_produtos = agrupamento_produtos.drop_duplicates(subset = 'COD_ESPECIFICO')

# DataFrame :: Update de Correntes
df_correntes = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['correntes']),
                         sheet_name= arquivos_primarios['correntes_sn'], 
                       usecols=list(tp_dado_arquivos['correntes'].keys()),
                       dtype=tp_dado_arquivos['correntes']).rename(columns = {'ConjuntoCorrentes':'Corrente',\
                                            'Unidade-Origem':'Origem', 'Unidade-Destino':'Destino'})

# DataFrame :: Lista Preço
df_valor_venda = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['lista_preco']),
                         sheet_name= arquivos_primarios['lista_preco_sn'], 
                       usecols=list(tp_dado_arquivos['lista_preco'].keys()),
                       dtype=tp_dado_arquivos['lista_preco']).applymap(fx.padronizar)
df_valor_venda = df_valor_venda.rename(columns=rename_dataframes['df_valor_venda'])

# DataFrame :: Unidades Receita Movimentação
df_pontos_venda = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['unidades_icms']),
                         sheet_name= arquivos_primarios['unidades_icms_sn'], 
                       usecols=list(tp_dado_arquivos['unidades_icms'].keys()),
                       dtype=tp_dado_arquivos['unidades_icms']).applymap(fx.padronizar)
df_pontos_venda = df_pontos_venda.rename(columns=rename_dataframes['df_pontos_venda'])

# DataFrame :: Template Receita Movimentação
#validar_data_arquivo(os.path.join(cwd, path + arquivos_primarios['unidades_rec_mov']))
df_template_rmov = pd.read_csv(os.path.join(cwd, path + arquivos_primarios['template_rec_mov']), delimiter = ';', encoding = 'utf-8', 
                               usecols=list(tp_dado_arquivos['template_rec_mov_sn'].keys()), dtype=tp_dado_arquivos['template_rec_mov_sn'])

df_corrente_produto = df_template_rmov.copy()
df_corrente_produto = df_corrente_produto[['Corrente', 'Produto']]
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

# agrupamento_pf = agrupamento_pf.drop_duplicates(subset = 'COD_ESPECIFICO')
df_valor_venda = fx.left_outer_join(df_valor_venda, agrupamento_produtos, left_on = 'Código do Produto', right_on = 'COD_ESPECIFICO',
                                    name_left='Lista Preço', name_right='Agrupamento de Produtos')
df_valor_venda.rename(columns = {"CODIGO_AGRUPADO": "CODIGO_ITEM"}, inplace=True)
df_valor_venda = df_valor_venda.astype({'Ptax USD':np.float32,'Preço':np.float32})
df_valor_venda = df_valor_venda.merge(cadastro_pf[["CODIGO_ITEM", "PRD-VCM"]], on = "CODIGO_ITEM", how="inner")

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

df_pontos_venda = fx.left_outer_join(df_pontos_venda, df_correntes, left_on = "Origem", right_on="Origem", struct=False,
                                     name_left='Unidades de Receita Movimentação', name_right='Update de Correntes')
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

df_pontos_venda["Desc. Empresa"] = df_pontos_venda["Origem EC"]
df_pontos_venda.drop(labels = ["Origem EC"], axis = 1, inplace = True)

df_receita_movimentacao = fx.left_outer_join(df_pontos_venda, df_valor_venda, left_on = ["Desc. Empresa", "PRD-VCM","NOME_PERIODO"], right_on=["Desc. Empresa", "PRD-VCM","NOME_PERIODO"],
                                             name_left='Unidades de Receita Movimentação', name_right='Lista Preço')
df_receita_movimentacao["Preço Venda"] = (df_receita_movimentacao["Preço Venda"].fillna(0))

# Trazendo preços médios para quando não houver preços específicos
df_receita_movimentacao = fx.left_outer_join(df_receita_movimentacao,valor_venda_medio, left_on = ["PRD-VCM"], right_on=["PRD-VCM"],
                                             name_left='Receita Movimentação', name_right='Preço Médio')
df_receita_movimentacao["Preço Venda Médio"] = (df_receita_movimentacao["Preço Venda Médio"].fillna(0))

# Classificação de produtos para eliminar o que não é produto final
df_receita_movimentacao = fx.left_outer_join(df_receita_movimentacao, cadastro_pf[["PRD-VCM", "TIPO_MATERIAL"]], left_on = "PRD-VCM", right_on="PRD-VCM",
                                             name_left='Receita Movimentação', name_right='Cadastro de Produtos')

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
df_receita_movimentacao_periodos = fx.left_outer_join(df_template_rmov,
                                        df_receita_movimentacao_periodos, 
                                        left_on = ["Origem", "Destino", "Corrente", "Produto", "Periodo"], 
                                        right_on = ["Origem", "Destino", "Corrente", "Produto", "Periodo"],
                                        name_left='Template Receita Movimentação', name_right='Receita Movimentação')
df_receita_movimentacao_periodos.fillna(0, inplace = True)

df_receita_movimentacao_periodos['Valor'] = df_receita_movimentacao_periodos['Valor'].round(2)
# (03/07/2025) Zerando receita movimentação pois já usamos preço fixo para isso.
df_receita_movimentacao_periodos['Valor'] = 0.0
# 12/04/2024: Alterando enconding para utf-8 como alinhado com o time da OP2B
df_receita_movimentacao_periodos.to_csv(os.path.join(cwd,output_path + "tbOutReceitaMov.csv"),
                                          sep = ';', encoding = 'utf-8-sig', index = False)

print('Atualização de Receitas de Movimentação finalizada!')
end_time = time.time()
print(f'Tempo de Execução: {round(end_time - start_time,2)} segundos')
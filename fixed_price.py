print('\n')
print('╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗')
print('║                                           ATUALIZACAO DE DADOS - VCM                                           ║')
print('║                                             >>  fixed_price.py  <<                                             ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ Criado por:    Isabela Nunes dos Santos        Data: 11/04/2025                                                ║')
print('║ Editado por:   Isabela Nunes dos Santos        Data: 08/08/2025                                                ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ CHANGELOG:                                                                                                     ║')
print('║ - v1.0.0 (23/03/2025): Criação da primeira versão do script unificado com edições estruturais nos arquivos     ║')
print('║                        de depara e dado primário.                                                              ║')
print('║                                                                                                                ║')
print('║ - v1.0.1 (15/07/2025): Criação de orientação a objeto para execução de scripts integrados.                     ║')
print('║                                                                                                                ║')
print('║ - v1.0.2 (07/08/2025): Adicionando filtro de MP/PF no agrupamento de produtos.                                 ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ Este script é responsável pela atualização:                                                                    ║')
print('║ >> Precificação                                                                                                ║')
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
print('Carregando arquivos... \n')
#print('Tempo de execução esperado: por volta de 1 min \n')

# DataFrame :: Horizonte (Período) de Otimização
df_periodos = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['periodos']),
                         usecols=list(tp_dado_arquivos['periodos'].keys()),
                         dtype=tp_dado_arquivos['periodos'])
df_periodos = df_periodos.rename(columns=rename_dataframes['df_periodos_price'])

# DataFrame :: Geolocalizacao
df_localizacao = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['localizacao']),
                         sheet_name= arquivos_primarios['localizacao_sn'], 
                       usecols=list(tp_dado_arquivos['localizacao'].keys()),
                       dtype=tp_dado_arquivos['localizacao']).applymap(fx.padronizar)

# Dataframe :: Cadastro Produtos
df_produtos = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['cadastro_produtos']),
                                  sheet_name = arquivos_primarios['cadastro_produtos_sn01'],
                                  usecols = list(tp_dado_arquivos['cadastro_produtos_sn01'].keys()),
                                  dtype = tp_dado_arquivos['cadastro_produtos_sn01'])

# Dataframe :: Agrupamento
agrupamento_pf = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['cadastro_produtos']),
                                  sheet_name = arquivos_primarios['cadastro_produtos_sn02'],
                                  usecols = list(tp_dado_arquivos['cadastro_produtos_sn02'].keys()),
                                  dtype = tp_dado_arquivos['cadastro_produtos_sn02'])
agrupamento_pf = agrupamento_pf.loc[agrupamento_pf['TIPO_MATERIAL']=='PF']
agrupamento_pf = agrupamento_pf.drop(columns='TIPO_MATERIAL')
proxy_agrupamento = df_produtos[['CODIGO_ITEM','DESCRICAO']]
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
                                            'Unidade-Origem':'Origem', 'Unidade-Destino':'Destino'}).applymap(fx.padronizar)
unidades_interesse = df_correntes.copy()

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

# DataFrame :: Dados de Frete
df_fretes = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['fretes']),
                       usecols=list(tp_dado_arquivos['fretes'].keys()),
                       dtype=tp_dado_arquivos['fretes'])

# DataFrame :: Template Preço Exato
# fx.validar_data_arquivo(os.path.join(cwd, path + arquivos_primarios['template_preco']))
df_template_rmov = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['template_preco']),
                       usecols=list(tp_dado_arquivos['template_preco'].keys()),
                       dtype=tp_dado_arquivos['template_preco'])
df_corrente_mc = df_template_rmov.copy()
df_corrente_mc = df_corrente_mc.drop(columns={'Preço Exato'})


# =======================================================================================================================
# EXECUÇÃO DE SCRIPTS
# =======================================================================================================================

df_valor_venda["Preço"] = df_valor_venda["Preço"].str.replace("'","")
df_valor_venda["Preço"] = df_valor_venda["Preço"].str.replace(",",".")
df_valor_venda["Ptax USD"] = df_valor_venda["Ptax USD"].str.replace("'","")
df_valor_venda["Ptax USD"] = df_valor_venda["Ptax USD"].str.replace(",",".")

df_valor_venda = df_valor_venda.merge(df_periodos[['Nome','Periodo_VCM']], how = 'cross')
df_valor_venda['Validar'] = (df_valor_venda['Nome'] >= df_valor_venda['Data Inicio']) & (df_valor_venda['Nome'] <= df_valor_venda['Data fim'])
df_valor_venda = df_valor_venda.loc[df_valor_venda.Validar == True]
df_valor_venda = df_valor_venda.reset_index().drop(columns=['index','Validar','Data Inicio','Data fim'])
# agrupamento_pf = agrupamento_pf.drop_duplicates(subset = 'COD_ESPECIFICO')
df_valor_venda = fx.left_outer_join(df_valor_venda, agrupamento_produtos, left_on = 'Código do Produto', right_on = 'COD_ESPECIFICO',
                                    name_left = 'Lista Preço', name_right = 'Agrupamento de Produtos')
df_valor_venda.rename(columns = {"CODIGO_AGRUPADO": "CODIGO_ITEM"}, inplace=True)

df_valor_venda = df_valor_venda.astype({'Ptax USD':np.float32,'Preço':np.float32})
df_valor_venda = df_valor_venda.merge(df_produtos[["CODIGO_ITEM", "PRD-VCM"]], on = "CODIGO_ITEM", how="inner")

# Convertendo de dólares para reais quando necessário
df_valor_venda["Preço Venda"] = np.where(df_valor_venda["Moeda"] == "BRL", df_valor_venda["Preço"],
                                         df_valor_venda["Preço"] * df_valor_venda["Ptax USD"])

# (22/01/2025) Juntando o UpdateCorrentes com o UnidadesRecMov para descobrir o nome da lista,
# a partir disso, juntar o UnidadesRecMov com o novo template. :)
df_pontos_venda = fx.left_outer_join(df_pontos_venda, df_correntes, left_on="Origem", right_on="Origem", struct=False,
                                     name_left='Unidades Receita Movimentação', name_right='Update de Correntes')
# 14/05/2024 - Incluindo uma etapa de exclusão de NaN
df_pontos_venda = df_pontos_venda.dropna()
df_corrente_mc = fx.left_outer_join(df_corrente_mc, df_pontos_venda, left_on='Unidade', right_on='Destino', struct=False,
                                    name_left='Correntes do Template', name_right='Unidades Receita Movimentação')
df_corrente_mc = df_corrente_mc.dropna()
df_corrente_mc['Desc. Empresa'] = df_corrente_mc['Origem EC']
df_corrente_mc = df_corrente_mc.drop(columns={'Destino','Corrente','Origem','Origem EC'})
df_corrente_mc = df_corrente_mc.rename(columns={'Periodo':'Periodo_VCM','Produto':'PRD-VCM'})
df_valor_venda["Desc. Empresa"] = df_valor_venda["Nome da Lista"]
df_valor_venda = df_valor_venda[["Desc. Empresa", "PRD-VCM", "Preço Venda","Periodo_VCM"]]
df_valor_venda.drop_duplicates(subset = ["Desc. Empresa", "PRD-VCM"], inplace = True)

# Média para o preço de venda
valor_venda_medio = df_valor_venda.groupby(by = ["PRD-VCM"])['Preço Venda'].mean()
valor_venda_medio = valor_venda_medio.to_frame()
valor_venda_medio = valor_venda_medio.reset_index()
valor_venda_medio.rename(columns = {"Preço Venda": "Preço Venda Médio"}, inplace=True)

# Trazendo os preços praticados por cada empresa para cada produtos
# df_receita_movimentacao = df_pontos_venda.merge(df_valor_venda, on = ["Desc. Empresa", "PRD-VCM","Periodo_VCM"], how = "left")
df_receita_movimentacao = fx.left_outer_join(df_corrente_mc, df_valor_venda, left_on = ["Desc. Empresa", "PRD-VCM","Periodo_VCM"], right_on=["Desc. Empresa", "PRD-VCM","Periodo_VCM"],
                                             name_left='Correntes do Template', name_right='Lista Preço')
df_receita_movimentacao["Preço Venda"] = (df_receita_movimentacao["Preço Venda"].fillna(0))

# Trazendo preços médios para quando não houver preços específicos
df_receita_movimentacao = fx.left_outer_join(df_receita_movimentacao, valor_venda_medio, left_on = ["PRD-VCM"], right_on='PRD-VCM',
                                             name_left='Receita Movimentação', name_right='Preço Médio')
df_receita_movimentacao["Preço Venda Médio"] = (df_receita_movimentacao["Preço Venda Médio"].fillna(0))

# Classificação de produtos para eliminar o que não é produto final
df_receita_movimentacao = fx.left_outer_join(df_receita_movimentacao, df_produtos[["PRD-VCM", "TIPO_MATERIAL"]],
                                                        left_on = "PRD-VCM", right_on='PRD-VCM',
                                                        name_left='Receita Movimentação', name_right='Cadastro de Produtos')


# O que não é produto final fica com preço zerado
# O que tem preço específico é usado
print('╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗')
print('║ Preenchendo valores de venda...                                                                                ║')
print('║ Exceções: 1) Quando não há preço específico para unidade, usar o média                                         ║')
print('║           2) Quando SKU não diz respeito a produto acabado, desconsiderar                                      ║')
print('║           3) Preencher com valor >> 0 << quando não for possível estimar valor de venda                        ║')
print('╚════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝')

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
df_receita_movimentacao_periodos.sort_values(["PRD-VCM"], inplace = True)
# Deixando na mesma forma que o template vindo do VCM
df_receita_movimentacao_periodos.rename(columns = {"PRD-VCM" : "Produto", "Periodo_VCM" : "Periodo",
                                                   "Preço Final" : "Preço Exato"}, inplace=True)
# Alterando a ordem das colunas e dropando o que nao é necessário
df_receita_movimentacao_periodos = df_receita_movimentacao_periodos[
                                   ["Unidade", "Produto",
                                    "Periodo", "Preço Exato"]]

# (22/01/2025) O preço agora se definirá pelo MC, então se o mesmo mercado tem 2 preços diferentes
# (de unidades diferentes) para o mesmo período, considerar o menor preço.
# Fazendo o agruamento a partir da Unidade, produto e periodo e pegando o menor valor restante baseado nisso.
df_receita_movimentacao_periodos = df_receita_movimentacao_periodos.groupby(['Unidade', 'Produto', 'Periodo']).min().reset_index()

# Etapa comentada em FTO, mantendo comentada aqui.
# Etapa para considerar apenas correntes físicas
# Ou seja, desconsiderar correntes lógicas
# Edit: 29/09/2023
unidades_interesse = unidades_interesse.loc[unidades_interesse.Tipo == 'OUTBOUND'].reset_index().drop(columns='index')
# new = unidades_interesse['Origem'].str.split("-", n = 2, expand = True)
# unidades_interesse['Split1'] = new[0]
# unidades_interesse['Split2'] = new[2]
# unidades_interesse = unidades_interesse.loc[(unidades_interesse.Split2  == "SAI") & (unidades_interesse.Split1 == "IMP"), :]
unidades_interesse['Filtro'] = unidades_interesse['Destino'].str[:2]
# Ajustando as correntes outbound para desconsiderar a origem e considerar todas as unidades que tenham destino MC
unidades_interesse = unidades_interesse.loc[(unidades_interesse.Filtro == 'MC'),:]

depara_localizacao = df_localizacao[['Unidade','Estado','Município']]
depara_localizacao['ID Origem-Destino'] = depara_localizacao['Município'] + '-' + depara_localizacao['Estado']

id_periodos = df_periodos['Periodo_VCM'].to_frame()
df_periodos = df_periodos.astype({'NUMERO':str})
periodos_fretes = df_periodos.copy()
#periodos_fretes['Data_Fim'] = periodos_fretes['Nome'] + MonthEnd(0)
periodos_fretes['Data_Inicio'] = periodos_fretes['Nome'] + MonthBegin(0)
df_fretes = fx.left_outer_join(df_fretes, periodos_fretes, left_on = 'data_inicio', right_on = 'Data_Inicio',
                               name_left='Fretes', name_right='Períodos')
df_fretes = df_fretes.dropna()

unidades_interesse = fx.left_outer_join(unidades_interesse,depara_localizacao,left_on='Origem', right_on='Unidade',
                                        name_left='Unidades de Interesse - Origem', name_right='Geolocalização')
unidades_interesse = unidades_interesse[['Corrente','Origem','Destino','Unidade','ID Origem-Destino']]
unidades_interesse = unidades_interesse.rename(columns={'ID Origem-Destino':'Cidade (Origem)'})
unidades_interesse = fx.left_outer_join(unidades_interesse,depara_localizacao,left_on='Destino', right_on='Unidade',
                                        name_left='Unidades de Interesse - Destino', name_right='Geolocalização')
unidades_interesse = unidades_interesse[['Corrente','Origem','Destino','Cidade (Origem)','ID Origem-Destino']]
unidades_interesse = unidades_interesse.rename(columns = {'ID Origem-Destino':'Cidade (Destino)'})

unidades_interesse = unidades_interesse.merge(periodos_fretes, how = 'cross')
unidades_interesse['ID-LEFT'] = unidades_interesse['Cidade (Origem)'] + '-' + unidades_interesse['Cidade (Destino)'] + '-' + unidades_interesse['Periodo_VCM']
df_fretes['Origem'] = df_fretes['rota'].str.split(' x ').str[0]
df_fretes['Destino'] = df_fretes['rota'].str.split(' x ').str[1]
df_fretes['ID-RIGHT'] = df_fretes['Origem'] + '-' + df_fretes['Destino'] + '-' + df_fretes['Periodo_VCM']
df_fretes = df_fretes.groupby(by = ['Periodo_VCM','Data_Inicio','rota','Origem','Destino','ID-RIGHT'])['Valor'].mean()
df_fretes = df_fretes.reset_index()
unidades_interesse = fx.left_outer_join(unidades_interesse, df_fretes, left_on = 'ID-LEFT', right_on = 'ID-RIGHT',
                                        name_left='Unidades de Interesse - Origem/Destino ', name_right='Fretes')
unidades_interesse = unidades_interesse.drop(columns={'Corrente','Origem_x','Cidade (Origem)','Cidade (Destino)','NUMERO',
                                            'ID-LEFT','rota','Data_Inicio_x','Origem_y','Destino_y','ID-RIGHT','Nome','Periodo_VCM_y','Data_Inicio_y'})
unidades_interesse = unidades_interesse.rename(columns={'Periodo_VCM_x':'PeriodoFrete','Destino_x':'Unidade-Destino'})

# =======================================
# Alteração da Lógica de Cálculo Aplicada
# =======================================

unidades_interesse = unidades_interesse.groupby(['Unidade-Destino','PeriodoFrete'])['Valor'].min().reset_index()

unidades_interesse['Valor'] = unidades_interesse['Valor'].fillna(0.0)
media = unidades_interesse.groupby(['Unidade-Destino'])['Valor'].mean().reset_index()
media = media.rename(columns={'Unidade-Destino':'UnidadeMedia','Valor':'ValorMedia'})
unidades_interesse = fx.left_outer_join(unidades_interesse, media, left_on='Unidade-Destino', right_on='UnidadeMedia',
                                        name_left='Unidades de Interesse - Origem/Destino', name_right='Média Fretes')
unidades_interesse['ValorMedia'] = unidades_interesse['ValorMedia'].fillna(0.0)
unidades_interesse['Valor'] = unidades_interesse.apply(lambda x: x['ValorMedia'] if x['Valor']==0.0 and x['ValorMedia']!=0.0 else x['Valor'], axis=1)
unidades_interesse = unidades_interesse.drop(columns={'UnidadeMedia','ValorMedia'})
df_receita_movimentacao_periodos = fx.left_outer_join(df_receita_movimentacao_periodos, unidades_interesse,
                                        left_on=['Unidade', 'Periodo'], right_on=['Unidade-Destino', 'PeriodoFrete'],
                                        name_left='Receita Movimentação', name_right='Unidades de Interesse - Origem/Destino')
df_receita_movimentacao_periodos['Preço Exato'] = df_receita_movimentacao_periodos.apply(lambda x: x['Preço Exato'] + x['Valor']
                                                                                    if x['Preço Exato']!=0.0 else x['Preço Exato'], axis=1)
df_receita_movimentacao_periodos = df_receita_movimentacao_periodos.round({'Preço Exato':2})

# Comparação com o template gerado pelo VCM
df_template_rmov.drop(labels="Preço Exato", axis=1, inplace=True)
df_template_rmov = fx.left_outer_join(df_template_rmov,df_receita_movimentacao_periodos,left_on=["Unidade", "Produto", "Periodo"],
                right_on=["Unidade", "Produto", "Periodo"], name_left='Template Preço Exato', name_right='Receita Movimentação')
df_template_rmov.fillna(0.0, inplace = True)
df_template_rmov = df_template_rmov.drop(columns={'Unidade-Destino', 'PeriodoFrete', 'Valor'})

# 12/04/2024: Alterando enconding para utf-8 como alinhado com o time da OP2B
# df_template_rmov.to_csv(os.path.join(cwd,output_path + "tbOutPrecoExatoMC.csv"),
#                                           sep = ';', encoding = 'utf-8-sig', index = False)
df_template_rmov.to_excel(os.path.join(cwd,output_path + "tbOutPrecoExatoMC.xlsx"),
                                          index = False, sheet_name = 'SPOT_PRECOS_EXATO')

print('\nAtualização de Preços Fixos finalizada!')
end_time = time.time()
print(f'Tempo de Execução: {round(end_time - start_time,2)} segundos')
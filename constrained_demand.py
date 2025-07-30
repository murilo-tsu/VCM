print('\n')
print('╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗')
print('║                                         ATUALIZACAO DE DADOS - VCM                                             ║')
print('║                                         >> constrained_demand.py <<                                            ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ Criado  por: Murilo Lima Ribeiro  Data: 10/03/2025                                                             ║')
print('║ Editado por: Murilo Lima Ribeiro  Data: 02/06/2025                                                             ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ CHANGELOG:                                                                                                     ║')
print('║ - v1.0.0 (02/04/2025): Criação da primeira versão do script unificado com edições estruturais nos arquivos de  ║')
print('║                        depara e dado primário.                                                                 ║')
print('║ - v1.0.1 (30/05/2025): Criação de orientação a objeto para execução de scripts integrados                      ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ Este script é responsável pela atualização:                                                                    ║')
print('║ >> Demanda Restrita :: Criação do Arquivo de Forecast a partir do resultado VCM                                ║')
print('╚════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝')
print('\n')

# =======================================================================================================================
# IMPORTAR BIBLIOTECAS
# =======================================================================================================================

import os
import sys
import time
import datetime
import pandas as pd
import numpy as np
import warnings
from tkinter import messagebox
warnings.filterwarnings('ignore')
from pandas.tseries.offsets import MonthEnd
from pandas.tseries.offsets import MonthBegin
from unidecode import unidecode

# =======================================================================================================================
# CONFIGURAÇÕES INICIAIS
# =======================================================================================================================

start_time = time.time()
cwd = os.getcwd()

# Caminhos dos arquivos
structure_path = 'Structural Data/'            # Dados estruturais (topologia)
path = 'Input Data/'                           # Dados de entrada (ciclo de planejamento)
output_path = 'Output Data/'                   # Dados de saída (input para o VCM)
exec_log_path = 'Error Logs/'                  # Logs de erros durante a execução


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

print('Iniciando...')
print('Tempo de execução esperado: por volta de 2 min \n')
print('Tabelas carregadas...')
# DataFrame :: Horizonte (Período) de Otimização
# applymap(padronizar) não aplicado por se tratar de dados com a estrutura final do VCM
periodos = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['periodos']),
                         usecols=list(tp_dado_arquivos['periodos'].keys()),
                         dtype=tp_dado_arquivos['periodos']).applymap(fx.padronizar)

# DataFrame :: cadastro de materiais :: busca toda a lista de materiais (MP, PI, PF) no cadastrados VCM
cadastro = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['cadastro_produtos']),
                            sheet_name = arquivos_primarios['cadastro_produtos_sn01'],
                            usecols = list(tp_dado_arquivos['cadastro_produtos_sn01'].keys()),
                            dtype = tp_dado_arquivos['cadastro_produtos_sn01']).applymap(fx.padronizar)

cadastro_pf = cadastro[(cadastro['TIPO_MATERIAL'].str.split('-',expand = True)[0].str.strip() == 'PF')]
cadastro_mp = cadastro[(cadastro['TIPO_MATERIAL'].str.split('-',expand = True)[0].str.strip() == 'MP')]

# DataFrame :: agrupamento de materiais :: busca todo o de-para de códigos específicos em códigos agrupados
agrupamento_produtos = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['cadastro_produtos']),
                            sheet_name = arquivos_primarios['cadastro_produtos_sn02'],
                            usecols = list(tp_dado_arquivos['cadastro_produtos_sn02'].keys()),
                            dtype = tp_dado_arquivos['cadastro_produtos_sn02']).applymap(fx.padronizar)

proxy_agrupamento = cadastro[['CODIGO_ITEM','DESCRICAO']]
proxy_agrupamento = proxy_agrupamento.rename(columns={'CODIGO_ITEM':'COD_ESPECIFICO','DESCRICAO':'DESCRICAO_ESPECIFICA'})
proxy_agrupamento['CODIGO_AGRUPADO'] = proxy_agrupamento['COD_ESPECIFICO']
proxy_agrupamento['AGRUPAMENTO_MP'] = proxy_agrupamento['DESCRICAO_ESPECIFICA']
agrupamento_produtos = pd.concat([agrupamento_produtos,proxy_agrupamento])
agrupamento_produtos = agrupamento_produtos.drop_duplicates(subset = 'COD_ESPECIFICO')
agrupamento_cadastro = agrupamento_produtos.copy()

# DataFrame :: DE-PARA de unidades produtoras em relação aos dados da demanda
tbDeparaUnidadesProdutoras = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['unidades_exp']),
                             sheet_name = arquivos_primarios['unidades_exp'].split('.')[0],
                             usecols = list(tp_dado_arquivos['unidades_exp'].keys()),
                             dtype = tp_dado_arquivos['unidades_exp']).applymap(fx.padronizar)

tbDeparaUnidadesProdutoras = tbDeparaUnidadesProdutoras[(tbDeparaUnidadesProdutoras['TIPO_UNIDADE'] == 'UNIDADE PRODUTORA')|\
                             (tbDeparaUnidadesProdutoras['TIPO_UNIDADE'] == 'ARMAZEM PRODUTOR')|\
                             (tbDeparaUnidadesProdutoras['TIPO_UNIDADE'] == 'TOLLING')].copy()

tbDeparaUnidadesProdutoras = tbDeparaUnidadesProdutoras.drop(columns=['UNIDADE_ARMAZENAGEM_VCM'])
# DataFrame :: Mercados Consumidores da Estrutura Comercial
mercados = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['mercados']),
                         sheet_name = arquivos_primarios['mercados'].split('.')[0],
                         usecols = list(tp_dado_arquivos['mercados'].keys()),
                         dtype = tp_dado_arquivos['mercados']).applymap(fx.padronizar)

id_mercados_consumidores = mercados.copy()
id_mercados_consumidores = id_mercados_consumidores['VCM'].to_frame().rename({'VCM':'ID MC'})

# DataFramme :: Demanda Irrestrita
demanda_irrestrita = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['demanda']),
                        sheet_name = arquivos_primarios['demanda_sn'],
                        usecols = list(tp_dado_arquivos['demanda'].keys()),
                        dtype = tp_dado_arquivos['demanda']).applymap(fx.padronizar)
demanda_irrestrita = demanda_irrestrita.loc[demanda_irrestrita.QUANTIDADE > 0.0,:].reset_index().drop(columns='index')

# DataFrame :: WIZARD_RENDIMENTO_ENTRADA :: Carrega lista técnica que foi utilizada no VCM :: Componentes
RendEntr = pd.read_excel(os.path.join(cwd, output_path + arquivos_primarios['arq_RendEntr']),
                             sheet_name = arquivos_primarios['arq_RendEntr_sn01'].split('.')[0],
                             usecols = list(tp_dado_arquivos['arq_RendEntr'].keys()),
                             dtype = tp_dado_arquivos['arq_RendEntr'])

RendEntr = RendEntr.loc[RendEntr['ValorEntrada'] > 0.0,:]
RendEntr = RendEntr.reset_index().drop(columns='index')

# DataFrame :: WIZARD_RENDIMENTO_SAIDA :: Carrega lista técnica que foi utilizada no VCM :: Produtos
RendSaida = pd.read_excel(os.path.join(cwd, output_path + arquivos_primarios['arq_RendSaida']),
                             sheet_name = arquivos_primarios['arq_RendSaida_sn01'].split('.')[0],
                             usecols = list(tp_dado_arquivos['arq_RendSaida'].keys()),
                             dtype = tp_dado_arquivos['arq_RendSaida'])

RendSaida = RendSaida.loc[RendSaida['ValorSaida'] == 1.0]
RendSaida = RendSaida.reset_index().drop(columns='index')
# DataFrame :: Arquivo de Resultados do VCM :: Output da rodada de otimização
fx.validar_data_arquivo(os.path.join(cwd, output_path + arquivos_primarios['arq_resultados_vcm']))
resultados_vcm = pd.read_excel(os.path.join(cwd, output_path + arquivos_primarios['arq_resultados_vcm']),
                               sheet_name = arquivos_primarios['arq_resultados_vcm_sn01'],
                               usecols = list(tp_dado_arquivos['arq_resultados_vcm'].keys()),
                               # 2025-06-03 :: REMOVIDO SKIPROWS
                               #dtype = tp_dado_arquivos['arq_resultados_vcm'], skiprows = 2).applymap(fx.padronizar)
                               dtype = tp_dado_arquivos['arq_resultados_vcm']).applymap(fx.padronizar)
# DataFrame :: Esqueleto topológico de correntes existentes no VCM
tbUpdateCorrentes = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['arq_tbUpdateCorrentes']),
                            sheet_name= arquivos_primarios['arq_tbUpdateCorrentes'].split('.')[0],
                            usecols=list(tp_dado_arquivos['arq_tbUpdateCorrentes'].keys()),
                            dtype=tp_dado_arquivos['arq_tbUpdateCorrentes']).applymap(fx.padronizar)

# DataFrame :: Esqueleto topológico de correntes existentes no VCM
tbUpdateCorrentes = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['arq_tbUpdateCorrentes']),
                            sheet_name= arquivos_primarios['arq_tbUpdateCorrentes'].split('.')[0],
                            usecols=list(tp_dado_arquivos['arq_tbUpdateCorrentes'].keys()),
                            dtype=tp_dado_arquivos['arq_tbUpdateCorrentes']).applymap(fx.padronizar)

# =======================================================================================================================
# EXECUÇÃO DE SCRIPTS
# =======================================================================================================================
print('╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗')
print('║  >>  DEMANDA RESTRITA  <<                                                                                      ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ # Forecast do Ciclo S&OP a partir dos Resultados VCM                                                           ║')
print('╚════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝')
print('Iniciando...')

# DataFrame da Demanda Irrestrita :: Realizar tratamento
demanda_irrestrita = fx.left_outer_join(demanda_irrestrita, agrupamento_cadastro, left_on = 'CODIGO PRODUTO', right_on = 'COD_ESPECIFICO',
                     name_left='Demanda Irrestrita',name_right='Agrupamento de Produtos')
demanda_irrestrita['CODIGO PRODUTO'] = demanda_irrestrita['CODIGO_AGRUPADO']
demanda_irrestrita = demanda_irrestrita.drop(columns=['CODIGO_AGRUPADO','COD_ESPECIFICO'])
demanda_irrestrita['id'] = demanda_irrestrita['PERIODO'].astype('str') + '-' + demanda_irrestrita['CODIGO PRODUTO'].astype('str') + '-' + demanda_irrestrita['GERENCIA'] + '-' + demanda_irrestrita['CONSULTORIA']
demanda_irrestrita['id_highlevel'] = demanda_irrestrita['CODIGO PRODUTO'].astype('str') + '-' + demanda_irrestrita['GERENCIA'] + '-' + demanda_irrestrita['CONSULTORIA']
di_cultura = demanda_irrestrita.groupby(['id','CULTURA'])['QUANTIDADE'].sum().reset_index()
di_cultura_hl = demanda_irrestrita.groupby(['id_highlevel','CULTURA'])['QUANTIDADE'].sum().reset_index()
di_cultura_cons = demanda_irrestrita.groupby(['CONSULTORIA','CULTURA'])['QUANTIDADE'].sum().reset_index()
di_cultura_total = demanda_irrestrita.groupby('id')['QUANTIDADE'].sum().reset_index()
di_cultura_total_hl = demanda_irrestrita.groupby('id_highlevel')['QUANTIDADE'].sum().reset_index()
di_cultura_total_cons = demanda_irrestrita.groupby('CONSULTORIA')['QUANTIDADE'].sum().reset_index()
di_cultura = di_cultura.merge(di_cultura_total, how = 'left', on = 'id')
di_cultura_hl = di_cultura_hl.merge(di_cultura_total_hl, how = 'left', on = 'id_highlevel')
di_cultura_cons = di_cultura_cons.merge(di_cultura_total_cons, how = 'left', on = 'CONSULTORIA')
di_cultura['perc'] = di_cultura['QUANTIDADE_x']/di_cultura['QUANTIDADE_y']
di_cultura_hl['perc'] = di_cultura_hl['QUANTIDADE_x']/di_cultura_hl['QUANTIDADE_y']
di_cultura_cons['perc'] = di_cultura_cons['QUANTIDADE_x']/di_cultura_cons['QUANTIDADE_y']
di_cultura = di_cultura[['id','CULTURA','perc']]
di_cultura_hl = di_cultura_hl[['id_highlevel','CULTURA','perc']]
di_cultura_cons = di_cultura_cons[['CONSULTORIA','CULTURA','perc']]
di_grupoprod = demanda_irrestrita.groupby(['id','GRUPO PRODUTO'])['QUANTIDADE'].sum().reset_index()
di_grupoprod_hl = demanda_irrestrita.groupby(['id_highlevel','GRUPO PRODUTO'])['QUANTIDADE'].sum().reset_index()
di_grupoprod_prod = demanda_irrestrita.groupby(['CODIGO PRODUTO','GRUPO PRODUTO'])['QUANTIDADE'].sum().reset_index()
di_grupoprod_total = demanda_irrestrita.groupby('id')['QUANTIDADE'].sum().reset_index()
di_grupoprod_total_hl = demanda_irrestrita.groupby('id_highlevel')['QUANTIDADE'].sum().reset_index()
di_grupoprod_total_prod = demanda_irrestrita.groupby('CODIGO PRODUTO')['QUANTIDADE'].sum().reset_index()
di_grupoprod = di_grupoprod.merge(di_grupoprod_total, how = 'left', on = 'id')
di_grupoprod_hl = di_grupoprod_hl.merge(di_grupoprod_total_hl, how = 'left', on = 'id_highlevel')
di_grupoprod_prod = di_grupoprod_prod.merge(di_grupoprod_total_prod, how = 'left', on = 'CODIGO PRODUTO')
di_grupoprod['perc'] = di_grupoprod['QUANTIDADE_x']/di_grupoprod['QUANTIDADE_y']
di_grupoprod_hl['perc'] = di_grupoprod_hl['QUANTIDADE_x']/di_grupoprod_hl['QUANTIDADE_y']
di_grupoprod_prod['perc'] = di_grupoprod_prod['QUANTIDADE_x']/di_grupoprod_prod['QUANTIDADE_y']
di_grupoprod = di_grupoprod[['id','GRUPO PRODUTO','perc']]
di_grupoprod_hl = di_grupoprod_hl[['id_highlevel','GRUPO PRODUTO','perc']]
di_grupoprod_prod = di_grupoprod_prod[['CODIGO PRODUTO','GRUPO PRODUTO','perc']].drop_duplicates()

# ================================================= DEPRECADO ============================================================
# 2025-03-25: etapas desativadas devido à ausência de CLIENTE_GRC no arquivo de demanda
#di_cliente = demanda_irrestrita.groupby(['id','CLIENTE GRC'])['QUANTIDADE'].sum().reset_index()
#di_cliente_hl = demanda_irrestrita.groupby(['id_highlevel','CLIENTE GRC'])['QUANTIDADE'].sum().reset_index()
#di_cliente_total = demanda_irrestrita.groupby('id')['QUANTIDADE'].sum().reset_index()
#di_cliente_hl_total = demanda_irrestrita.groupby('id_highlevel')['QUANTIDADE'].sum().reset_index()
#di_cliente = di_cliente.merge(di_cliente_total, how = 'left', on = 'id')
#di_cliente_hl = di_cliente_hl.merge(di_cliente_hl_total, how = 'left', on = 'id_highlevel')
#di_cliente['perc'] = di_cliente['QUANTIDADE_x']/di_cliente['QUANTIDADE_y']
#di_cliente_hl['perc'] = di_cliente_hl['QUANTIDADE_x']/di_cliente_hl['QUANTIDADE_y']
#di_cliente = di_cliente[['id','CLIENTE GRC','perc']]
#di_cliente_hl = di_cliente_hl[['id_highlevel','CLIENTE GRC','perc']]
# ================================================= DEPRECADO ============================================================

# DataFrame de Resultados VCM :: mesclagem de dados primários
deliveries_resultados_vcm = resultados_vcm.loc[resultados_vcm['Unidade-Destino-VCM'].str[:2] == 'MC',:]
deliveries_resultados_vcm = deliveries_resultados_vcm.loc[deliveries_resultados_vcm['Produto-VCM'].str[:2] == 'PF',:]
deliveries_resultados_vcm = deliveries_resultados_vcm.reset_index().drop(columns='index')
esqueleto_explosao = deliveries_resultados_vcm[['Produto-VCM','Unidade-Origem-VCM']].copy()
proxy_unidade_resultado_vcm = resultados_vcm.copy()
# CORRIGINDO NOME!!!!!!!!!!
tbDeparaUnidadesProdutoras = tbDeparaUnidadesProdutoras.rename(columns={'UNIDADE_EXPEDICAO_VCM':'UNIDADE_VCM'})
proxy_unidade_resultado_vcm = fx.left_outer_join(proxy_unidade_resultado_vcm, tbDeparaUnidadesProdutoras,
                              left_on = 'Unidade-Origem-VCM', right_on = 'UNIDADE_VCM',
                              name_left='Unidades VCM no Resultado', name_right='Depara Unidades VCM')
proxy_unidade_resultado_vcm = proxy_unidade_resultado_vcm.loc[proxy_unidade_resultado_vcm['UNIDADE_VCM'].notnull(),:].reset_index().drop(columns='index')
proxy_unidade_resultado_vcm = fx.left_outer_join(proxy_unidade_resultado_vcm,periodos, left_on = 'Período-VCM', right_on = 'NOME_PERIODO',
                              name_left='Unidades VCM no Resultado', name_right='Períodos')
proxy_unidade_resultado_vcm['proxy_unidade'] = proxy_unidade_resultado_vcm['PERIODO'].astype('str') + '-' + proxy_unidade_resultado_vcm['Produto-VCM'] + '-' + proxy_unidade_resultado_vcm['Unidade-Destino-VCM']
headers = ['proxy_unidade','PLANTA','DEPOSITO','Quantidade']
proxy_unidade_resultado_vcm = proxy_unidade_resultado_vcm[headers]
proxy_unidade_resultado_vcm_total = proxy_unidade_resultado_vcm.groupby('proxy_unidade')['Quantidade'].sum().reset_index()
proxy_unidade_resultado_vcm = fx.left_outer_join(proxy_unidade_resultado_vcm, proxy_unidade_resultado_vcm_total, 
                              left_on='proxy_unidade', right_on='proxy_unidade',
                              name_left='Unidades VCM no Resultado', name_right='Unidades VCM Agrupado')
proxy_unidade_resultado_vcm['perc'] = proxy_unidade_resultado_vcm['Quantidade_x']/proxy_unidade_resultado_vcm['Quantidade_y']
proxy_unidade_resultado_vcm = proxy_unidade_resultado_vcm[['proxy_unidade','DEPOSITO','PLANTA','perc']].rename(columns={'PLANTA':'UNIDADE_FATURAMENTO','DEPOSITO':'UNIDADE_PRODUTORA'})
Explosion = RendSaida.copy()
Explosion = fx.left_outer_join(Explosion, RendEntr, left_on = ['Unidade','Receita'], right_on = ['Unidade','Receita'],
            name_left='Rendimento Saída', name_right='Rendimento Entrada', struct=False)
Explosion = Explosion.rename(columns={'Produto_x':'FG','Produto_y':'RM','ValorEntrada':'CompVol'})
Explosion = Explosion.drop(columns='ValorSaida')

# ================================================= DEPRECADO ============================================================
# Lista técnica na demanda :: exceções pela ausência de lista técnica
# 25/03/2025: Removendo "CODIGO MP" & "MATERIA PRIMA" devido a alteração do layout
# da demanda irrestrita a partir do ciclo de 2025-03
#explosion_di = demanda_irrestrita.copy()
#explosion_di_hl = demanda_irrestrita.copy()
#explosion_di['proxy_explosao'] = explosion_di['CODIGO PRODUTO'] + '-' + explosion_di['UNIDADE PRODUTORA']
# explosion_di = explosion_di[['proxy_explosao','MATERIA PRIMA','CODIGO MP','QUANTIDADE']]
#explosion_di = explosion_di.groupby(['proxy_explosao','CODIGO MP','MATERIA PRIMA'])['QUANTIDADE'].sum().reset_index()
#explosion_di_hl = explosion_di_hl.groupby(['CODIGO PRODUTO','MATERIA PRIMA','CODIGO MP'])['QUANTIDADE'].sum().reset_index()
#explosion_di_total = explosion_di.groupby('proxy_explosao')['QUANTIDADE'].sum().reset_index()
#explosion_di_hl_total = explosion_di_hl.groupby('CODIGO PRODUTO')['QUANTIDADE'].sum().reset_index()
#explosion_di = explosion_di.merge(explosion_di_total, how = 'left', on = 'proxy_explosao')
#explosion_di_hl = explosion_di_hl.merge(explosion_di_hl_total, how = 'left', on = 'CODIGO PRODUTO')
#explosion_di['perc'] = explosion_di['QUANTIDADE_x']/explosion_di['QUANTIDADE_y']
#explosion_di_hl['perc'] = explosion_di_hl['QUANTIDADE_x']/explosion_di_hl['QUANTIDADE_y']
#explosion_di = explosion_di[['proxy_explosao','MATERIA PRIMA','CODIGO MP','perc']]
#explosion_di_hl = explosion_di_hl[['CODIGO PRODUTO','MATERIA PRIMA','CODIGO MP','perc']]
#explosion_di = explosion_di.astype({'CODIGO MP':'str'})
#explosion_di_hl = explosion_di_hl.astype({'CODIGO MP':'str'}
# ================================================= DEPRECADO ============================================================

# Framework para explosão do resultado
esqueleto_explosao['id'] = esqueleto_explosao['Produto-VCM'] + '-' + esqueleto_explosao['Unidade-Origem-VCM']
esqueleto_explosao = esqueleto_explosao.merge(tbUpdateCorrentes, how = 'left', left_on = 'Unidade-Origem-VCM', right_on = 'Unidade-Destino')
esqueleto_explosao = esqueleto_explosao.merge(Explosion, how = 'left', left_on = ['Produto-VCM','Unidade-Origem'], right_on = ['FG','Unidade'])
esqueleto_explosao = esqueleto_explosao.dropna()
esqueleto_explosao_mix = esqueleto_explosao.loc[esqueleto_explosao['RM'].str[:2] == 'PI',:].reset_index().drop(columns='index')
esqueleto_explosao_mix = esqueleto_explosao_mix.drop_duplicates()
esqueleto_explosao_simples = esqueleto_explosao.loc[esqueleto_explosao['RM'].str[:2] == 'MP',:].reset_index().drop(columns='index')
esqueleto_explosao_simples = esqueleto_explosao_simples.drop_duplicates()
esqueleto_explosao_mix = esqueleto_explosao_mix.merge(tbUpdateCorrentes, how = 'left', left_on = 'Unidade-Origem', right_on = 'Unidade-Destino')
esqueleto_explosao_mix = esqueleto_explosao_mix[['id','Produto-VCM','Unidade-Origem-VCM','Unidade-Origem_y','Unidade-Destino_y','Receita','FG','RM','CompVol']]
esqueleto_explosao_mix = esqueleto_explosao_mix.rename(columns={'Unidade-Origem_y':'Unidade-Origem','Unidade-Destino_y':'Unidade-Destino'})
esqueleto_explosao_mix = esqueleto_explosao_mix.merge(Explosion, how = 'left', left_on = ['Unidade-Origem','RM'], right_on = ['Unidade','FG'])
esqueleto_explosao_mix = esqueleto_explosao_mix[['id','Produto-VCM','Unidade-Origem-VCM','Unidade-Origem','Unidade-Destino','Receita_y','FG_x','RM_y','CompVol_y']]
esqueleto_explosao_mix = esqueleto_explosao_mix.rename(columns={'FG_x':'FG','RM_y':'RM','Receita_y':'Receita','CompVol_y':'CompVol'})
esqueleto_explosao = pd.concat([esqueleto_explosao_simples,esqueleto_explosao_mix])
# Deliveries a partir do VCM
deliveries_resultados_vcm = deliveries_resultados_vcm.merge(mercados, how = 'left', left_on = 'Unidade-Destino-VCM', right_on = 'VCM')
deliveries_resultados_vcm = deliveries_resultados_vcm.merge(periodos, how = 'left', left_on = 'Período-VCM', right_on = 'NOME_PERIODO')
deliveries_resultados_vcm['id'] = deliveries_resultados_vcm['Produto-VCM'] + '-' + deliveries_resultados_vcm['Unidade-Origem-VCM']
deliveries_resultados_vcm = deliveries_resultados_vcm.merge(esqueleto_explosao, how = 'left', left_on = 'id', right_on = 'id')
headers = ['Unidade-Origem-VCM_x','Produto-VCM_x','PERIODO','DIRETORIA','GERENCIA','CONSULTORIA','Unidade-Destino-VCM','Unidade-Origem','Unidade-Destino','Receita','FG','RM','CompVol','Quantidade']
deliveries_resultados_vcm = deliveries_resultados_vcm[headers]
# deliveries_resultados_vcm = deliveries_resultados_vcm.rename(columns={'Nome':'PERIODO'})

# Separar a demanda do VCM no que foi possível explodir (1) e no que foi impossivel explodir (2)
# Possível de Explodir (1)
deliveries_resultados_vcm_1 = deliveries_resultados_vcm.loc[deliveries_resultados_vcm['CompVol'].notnull(),:].reset_index().drop(columns='index').drop(columns=['Unidade-Origem-VCM_x','Produto-VCM_x'])
# Impossível de Explodir (2)
# As etapas subsequentes em (2) serão inativadas devido a ausência de lista técnica na demanda
deliveries_resultados_vcm_2 = deliveries_resultados_vcm.loc[deliveries_resultados_vcm['CompVol'].isna(),:].reset_index().drop(columns='index')
# FOCO: (1)
deliveries_resultados_vcm_1 = deliveries_resultados_vcm_1.merge(tbDeparaUnidadesProdutoras, how = 'left', left_on='Unidade-Origem', right_on = 'UP_MISTURADORA_VCM')
headers = ['PERIODO','DIRETORIA','GERENCIA','CONSULTORIA','DEPOSITO','PLANTA','Unidade-Destino-VCM','Unidade-Origem','Unidade-Destino','Receita','FG','RM','CompVol','Quantidade']
deliveries_resultados_vcm_1 = deliveries_resultados_vcm_1[headers]
deliveries_resultados_vcm_1.loc[deliveries_resultados_vcm_1['DEPOSITO'].isna(),:]
deliveries_resultados_vcm_1 = deliveries_resultados_vcm_1.rename(columns={'DEPOSITO':'UNIDADE PRODUTORA','PLANTA':'UNIDADE FATURAMENTO'})
deliveries_resultados_vcm_1['QUANTIDADE'] = deliveries_resultados_vcm_1['CompVol']*deliveries_resultados_vcm_1['Quantidade']
deliveries_resultados_vcm_1 = deliveries_resultados_vcm_1.merge(cadastro_pf, how = 'left', left_on='FG', right_on='PRD-VCM')
deliveries_resultados_vcm_1 = deliveries_resultados_vcm_1.merge(cadastro_mp, how='left', left_on='RM', right_on='PRD-VCM')
headers = ['PERIODO','DIRETORIA','GERENCIA','CONSULTORIA','UNIDADE PRODUTORA','UNIDADE FATURAMENTO',
           'DESCRICAO_x','CODIGO_ITEM_x','DESCRICAO_y','CODIGO_ITEM_y','Unidade-Destino-VCM','Unidade-Origem','Unidade-Destino','Receita','FG','RM','CompVol','QUANTIDADE']
deliveries_resultados_vcm_1 = deliveries_resultados_vcm_1[headers]
deliveries_resultados_vcm_1 = deliveries_resultados_vcm_1.rename(columns={'DESCRICAO_x':'PRODUTO','DESCRICAO_y':'MATERIA PRIMA'})
deliveries_resultados_vcm_1 = deliveries_resultados_vcm_1.rename(columns={'CODIGO_ITEM_x':'CODIGO PRODUTO','CODIGO_ITEM_y':'CODIGO MP'})
deliveries_resultados_vcm_1['id'] = deliveries_resultados_vcm_1['PERIODO'].astype('str') + '-' + deliveries_resultados_vcm_1['CODIGO PRODUTO'] + '-' + deliveries_resultados_vcm_1['GERENCIA'] + '-' + deliveries_resultados_vcm_1['CONSULTORIA']
deliveries_resultados_vcm_1['id_highlevel'] = deliveries_resultados_vcm_1['CODIGO PRODUTO'] + '-' + deliveries_resultados_vcm_1['GERENCIA'] + '-' + deliveries_resultados_vcm_1['CONSULTORIA']
deliveries_resultados_vcm_1 = deliveries_resultados_vcm_1.merge(di_cultura, how = 'left', on = 'id')
deliveries_resultados_vcm_1['perc'] = deliveries_resultados_vcm_1['perc'].fillna(1.0)
deliveries_resultados_vcm_1['QUANTIDADE'] = deliveries_resultados_vcm_1['QUANTIDADE']*deliveries_resultados_vcm_1['perc']
deliveries_resultados_vcm_1 = deliveries_resultados_vcm_1.drop(columns='perc')
deliveries_resultados_vcm_1_sem_cultura = deliveries_resultados_vcm_1.loc[deliveries_resultados_vcm_1['CULTURA'].isna(),:].drop(columns='CULTURA')
deliveries_resultados_vcm_1_com_cultura = deliveries_resultados_vcm_1.loc[deliveries_resultados_vcm_1['CULTURA'].notnull(),:]
deliveries_resultados_vcm_1_sem_cultura = deliveries_resultados_vcm_1_sem_cultura.merge(di_cultura_hl, how = 'left', on = 'id_highlevel')
deliveries_resultados_vcm_1_sem_cultura['perc'] = deliveries_resultados_vcm_1_sem_cultura['perc'].fillna(1.0)
deliveries_resultados_vcm_1_sem_cultura['QUANTIDADE'] = deliveries_resultados_vcm_1_sem_cultura['QUANTIDADE']*deliveries_resultados_vcm_1_sem_cultura['perc']
deliveries_resultados_vcm_1_sem_cultura_OK = deliveries_resultados_vcm_1_sem_cultura.loc[deliveries_resultados_vcm_1_sem_cultura['CULTURA'].notnull(),:]
deliveries_resultados_vcm_1_sem_cultura_preencher = deliveries_resultados_vcm_1_sem_cultura.loc[deliveries_resultados_vcm_1_sem_cultura['CULTURA'].isna(),:].drop(columns=['CULTURA','perc'])
deliveries_resultados_vcm_1_sem_cultura_preencher = deliveries_resultados_vcm_1_sem_cultura_preencher.merge(di_cultura_cons, how = 'left', on = 'CONSULTORIA')
deliveries_resultados_vcm_1_sem_cultura_preencher['QUANTIDADE'] = deliveries_resultados_vcm_1_sem_cultura_preencher['QUANTIDADE']*deliveries_resultados_vcm_1_sem_cultura_preencher['perc']
deliveries_resultados_vcm_1 = pd.concat([deliveries_resultados_vcm_1_com_cultura,deliveries_resultados_vcm_1_sem_cultura])
headers = ['id','id_highlevel','PERIODO','DIRETORIA','GERENCIA','CONSULTORIA','UNIDADE PRODUTORA','UNIDADE FATURAMENTO','CULTURA','PRODUTO','CODIGO PRODUTO','MATERIA PRIMA','CODIGO MP','QUANTIDADE']
deliveries_resultados_vcm_1 = deliveries_resultados_vcm_1[headers]
deliveries_resultados_vcm_1 = deliveries_resultados_vcm_1.merge(di_grupoprod, how = 'left', on = 'id')
deliveries_resultados_vcm_1['perc'] = deliveries_resultados_vcm_1['perc'].fillna(1.0)
deliveries_resultados_vcm_1['QUANTIDADE'] = deliveries_resultados_vcm_1['QUANTIDADE']*deliveries_resultados_vcm_1['perc']
deliveries_resultados_vcm_1_com_grupo = deliveries_resultados_vcm_1.loc[deliveries_resultados_vcm_1['GRUPO PRODUTO'].notnull(),:].drop(columns='perc')
deliveries_resultados_vcm_1_sem_grupo = deliveries_resultados_vcm_1.loc[deliveries_resultados_vcm_1['GRUPO PRODUTO'].isna(),:].drop(columns=['GRUPO PRODUTO','perc'])
deliveries_resultados_vcm_1_sem_grupo = deliveries_resultados_vcm_1_sem_grupo.merge(di_grupoprod_hl, how = 'left', on = 'id_highlevel')
deliveries_resultados_vcm_1_sem_grupo['perc'] = deliveries_resultados_vcm_1_sem_grupo['perc'].fillna(1.0)
deliveries_resultados_vcm_1_sem_grupo['QUANTIDADE'] = deliveries_resultados_vcm_1_sem_grupo['QUANTIDADE']*deliveries_resultados_vcm_1_sem_grupo['perc']
deliveries_resultados_vcm_1_sem_grupo_OK = deliveries_resultados_vcm_1_sem_grupo.loc[deliveries_resultados_vcm_1_sem_grupo['GRUPO PRODUTO'].notnull(),:].drop(columns='perc')
deliveries_resultados_vcm_1_sem_grupo_preencher = deliveries_resultados_vcm_1_sem_grupo.loc[deliveries_resultados_vcm_1_sem_grupo['GRUPO PRODUTO'].isna(),:].drop(columns=['GRUPO PRODUTO','perc'])
deliveries_resultados_vcm_1_sem_grupo_preencher = deliveries_resultados_vcm_1_sem_grupo_preencher.merge(di_grupoprod_prod, how = 'left', on = 'CODIGO PRODUTO')
deliveries_resultados_vcm_1_sem_grupo_preencher['perc'] = deliveries_resultados_vcm_1_sem_grupo_preencher['perc'].fillna(1.0)
deliveries_resultados_vcm_1_sem_grupo_preencher['QUANTIDADE'] = deliveries_resultados_vcm_1_sem_grupo_preencher['QUANTIDADE']*deliveries_resultados_vcm_1_sem_grupo_preencher['perc']
deliveries_resultados_vcm_1_sem_grupo_preencher = deliveries_resultados_vcm_1_sem_grupo_preencher.drop(columns='perc')
deliveries_resultados_vcm_1_sem_grupo = pd.concat([deliveries_resultados_vcm_1_sem_grupo_OK,deliveries_resultados_vcm_1_sem_grupo_preencher])
deliveries_resultados_vcm_1 = pd.concat([deliveries_resultados_vcm_1_com_grupo,deliveries_resultados_vcm_1_sem_grupo])

# ================================================= DEPRECADO ============================================================
# 2025-03-25: Inativando as instruções abaixo devido a remoção do "CLIENTE GRC" da demanda irrestrita
#deliveries_resultados_vcm_1 = deliveries_resultados_vcm_1.merge(di_cliente, how = 'left', on = 'id')
#deliveries_resultados_vcm_1['perc'] = deliveries_resultados_vcm_1['perc'].fillna(1.0)
#deliveries_resultados_vcm_1['QUANTIDADE'] = deliveries_resultados_vcm_1['QUANTIDADE']*deliveries_resultados_vcm_1['perc']
#deliveries_resultados_vcm_1 = deliveries_resultados_vcm_1.drop(columns='perc')
#deliveries_resultados_vcm_1_com_cliente = deliveries_resultados_vcm_1.loc[deliveries_resultados_vcm_1['CLIENTE GRC'].notnull(),:]
#deliveries_resultados_vcm_1_sem_cliente = deliveries_resultados_vcm_1.loc[deliveries_resultados_vcm_1['CLIENTE GRC'].isna(),:].drop(columns='CLIENTE GRC')
#deliveries_resultados_vcm_1_sem_cliente = deliveries_resultados_vcm_1_sem_cliente.merge(di_cliente_hl, how = 'left', on='id_highlevel')
#deliveries_resultados_vcm_1_sem_cliente['perc'] = deliveries_resultados_vcm_1_sem_cliente['perc'].fillna(1.0)
#deliveries_resultados_vcm_1_sem_cliente['QUANTIDADE'] = deliveries_resultados_vcm_1_sem_cliente['QUANTIDADE']*deliveries_resultados_vcm_1_sem_cliente['perc']
#deliveries_resultados_vcm_1_sem_cliente = deliveries_resultados_vcm_1_sem_cliente.drop(columns='perc')
#deliveries_resultados_vcm_1 = pd.concat([deliveries_resultados_vcm_1_com_cliente,deliveries_resultados_vcm_1_sem_cliente])
#headers = ['PERIODO','DIRETORIA','GERENCIA','CONSULTORIA','UNIDADE PRODUTORA','UNIDADE FATURAMENTO','CULTURA','GRUPO PRODUTO','PRODUTO','CODIGO PRODUTO','MATERIA PRIMA','CODIGO MP','CLIENTE GRC','QUANTIDADE']
#deliveries_resultados_vcm_1 = deliveries_resultados_vcm_1[headers]

# FOCO: (2)
# As etapas subsequentes em (2) serão inativadas devido a ausência de lista técnica na demanda
#headers = ['PERIODO','DIRETORIA','GERENCIA','CONSULTORIA','Unidade-Origem-VCM_x','Produto-VCM_x','Quantidade']
#deliveries_resultados_vcm_2 = deliveries_resultados_vcm_2[headers]
#deliveries_resultados_vcm_2['proxy_unidade'] = deliveries_resultados_vcm_2['PERIODO'].astype('str') + '-' + deliveries_resultados_vcm_2['Produto-VCM_x'] + '-' + deliveries_resultados_vcm_2['Unidade-Origem-VCM_x']
#deliveries_resultados_vcm_2 = deliveries_resultados_vcm_2.merge(proxy_unidade_resultado_vcm, how = 'left', on = 'proxy_unidade')
#deliveries_resultados_vcm_2['QUANTIDADE'] = deliveries_resultados_vcm_2['Quantidade']*deliveries_resultados_vcm_2['perc']
#deliveries_resultados_vcm_2 = deliveries_resultados_vcm_2.rename(columns={'Produto-VCM_x':'PF'})
#headers = ['PERIODO','DIRETORIA','GERENCIA','CONSULTORIA','UNIDADE PRODUTORA','UNIDADE FATURAMENTO','PF','QUANTIDADE']
#deliveries_resultados_vcm_2 = deliveries_resultados_vcm_2[headers]
#deliveries_resultados_vcm_2 = deliveries_resultados_vcm_2.merge(cadastro_pf, how = 'left', left_on='PF', right_on='PRD-VCM')
#headers = ['PERIODO','DIRETORIA','GERENCIA','CONSULTORIA','UNIDADE PRODUTORA','UNIDADE FATURAMENTO','ITEM_DESCRIPTION','ITEM_CODE','QUANTIDADE']
#deliveries_resultados_vcm_2 = deliveries_resultados_vcm_2[headers]
#deliveries_resultados_vcm_2 = deliveries_resultados_vcm_2.rename(columns={'ITEM_CODE':'CODIGO PRODUTO','ITEM_DESCRIPTION':'PRODUTO'})
#deliveries_resultados_vcm_2['id'] = deliveries_resultados_vcm_2['PERIODO'].astype('str') + '-' + deliveries_resultados_vcm_2['CODIGO PRODUTO'] + '-' + deliveries_resultados_vcm_2['GERENCIA'] + '-' + deliveries_resultados_vcm_2['CONSULTORIA']
#deliveries_resultados_vcm_2['id_highlevel'] = deliveries_resultados_vcm_2['CODIGO PRODUTO'] + '-' + deliveries_resultados_vcm_2['GERENCIA'] + '-' + deliveries_resultados_vcm_2['CONSULTORIA']
#deliveries_resultados_vcm_2 = deliveries_resultados_vcm_2.merge(di_cultura, how = 'left', on = 'id')
#deliveries_resultados_vcm_2['perc'] = deliveries_resultados_vcm_2['perc'].fillna(1.0)
#deliveries_resultados_vcm_2['QUANTIDADE'] = deliveries_resultados_vcm_2['QUANTIDADE']*deliveries_resultados_vcm_2['perc']
#deliveries_resultados_vcm_2_sem_cultura = deliveries_resultados_vcm_2.loc[deliveries_resultados_vcm_2['CULTURA'].isna(),:].drop(columns=['CULTURA','perc'])
#deliveries_resultados_vcm_2_com_cultura = deliveries_resultados_vcm_2.loc[deliveries_resultados_vcm_2['CULTURA'].notnull(),:].drop(columns='perc')
#deliveries_resultados_vcm_2_sem_cultura = deliveries_resultados_vcm_2_sem_cultura.merge(di_cultura_hl, how = 'left', on = 'id_highlevel')
#deliveries_resultados_vcm_2_sem_cultura['perc'] = deliveries_resultados_vcm_2_sem_cultura['perc'].fillna(1.0)
#deliveries_resultados_vcm_2_sem_cultura['QUANTIDADE'] = deliveries_resultados_vcm_2_sem_cultura['QUANTIDADE']*deliveries_resultados_vcm_2_sem_cultura['perc']
#deliveries_resultados_vcm_2_sem_cultura_OK = deliveries_resultados_vcm_2_sem_cultura.loc[deliveries_resultados_vcm_2_sem_cultura['CULTURA'].notnull(),:].drop(columns='perc')
#deliveries_resultados_vcm_2_sem_cultura_preencher = deliveries_resultados_vcm_2_sem_cultura.loc[deliveries_resultados_vcm_2_sem_cultura['CULTURA'].isna(),:].drop(columns=['CULTURA','perc'])
#deliveries_resultados_vcm_2_sem_cultura_preencher = deliveries_resultados_vcm_2_sem_cultura_preencher.merge(di_cultura_cons, how = 'left', on = 'CONSULTORIA')
#deliveries_resultados_vcm_2_sem_cultura_preencher['QUANTIDADE'] = deliveries_resultados_vcm_2_sem_cultura_preencher['QUANTIDADE']*deliveries_resultados_vcm_2_sem_cultura_preencher['perc']
#deliveries_resultados_vcm_2_sem_cultura = pd.concat([deliveries_resultados_vcm_2_sem_cultura_OK,deliveries_resultados_vcm_2_sem_cultura_preencher])
#deliveries_resultados_vcm_2 = pd.concat([deliveries_resultados_vcm_2_com_cultura,deliveries_resultados_vcm_2_sem_cultura])
#deliveries_resultados_vcm_2 = deliveries_resultados_vcm_2.drop(columns='perc')
#headers = ['id','id_highlevel','PERIODO','DIRETORIA','GERENCIA','CONSULTORIA','CULTURA','UNIDADE PRODUTORA','UNIDADE FATURAMENTO','PRODUTO','CODIGO PRODUTO','QUANTIDADE']
#deliveries_resultados_vcm_2 = deliveries_resultados_vcm_2[headers]
#deliveries_resultados_vcm_2 = deliveries_resultados_vcm_2.merge(di_grupoprod, how = 'left', on = 'id')
#deliveries_resultados_vcm_2['perc'] = deliveries_resultados_vcm_2['perc'].fillna(1.0)
#deliveries_resultados_vcm_2['QUANTIDADE'] = deliveries_resultados_vcm_2['QUANTIDADE']*deliveries_resultados_vcm_2['perc']
#deliveries_resultados_vcm_2_com_grupo = deliveries_resultados_vcm_2.loc[deliveries_resultados_vcm_2['GRUPO PRODUTO'].notnull(),:].drop(columns='perc')
#deliveries_resultados_vcm_2_sem_grupo = deliveries_resultados_vcm_2.loc[deliveries_resultados_vcm_2['GRUPO PRODUTO'].isna(),:].drop(columns=['GRUPO PRODUTO','perc'])
#deliveries_resultados_vcm_2_sem_grupo = deliveries_resultados_vcm_2_sem_grupo.merge(di_grupoprod_hl, how = 'left', on = 'id_highlevel')
#deliveries_resultados_vcm_2_sem_grupo['perc'] = deliveries_resultados_vcm_2_sem_grupo['perc'].fillna(1.0)
#deliveries_resultados_vcm_2_sem_grupo['QUANTIDADE'] = deliveries_resultados_vcm_2_sem_grupo['QUANTIDADE']*deliveries_resultados_vcm_2_sem_grupo['perc']
#deliveries_resultados_vcm_2_sem_grupo_OK = deliveries_resultados_vcm_2_sem_grupo.loc[deliveries_resultados_vcm_2_sem_grupo['CULTURA'].notnull(),:].drop(columns='perc')
#deliveries_resultados_vcm_2_sem_grupo_preencher = deliveries_resultados_vcm_2_sem_grupo.loc[deliveries_resultados_vcm_2_sem_grupo['CULTURA'].notnull(),:].drop(columns=['GRUPO PRODUTO','perc'])
#deliveries_resultados_vcm_2_sem_grupo_preencher = deliveries_resultados_vcm_2_sem_grupo_preencher.merge(di_grupoprod_prod, how = 'left', on = 'CODIGO PRODUTO')
#deliveries_resultados_vcm_2_sem_grupo_preencher['perc'] = deliveries_resultados_vcm_2_sem_grupo_preencher['perc'].fillna(1.0)
#deliveries_resultados_vcm_2_sem_grupo_preencher['QUANTIDADE'] = deliveries_resultados_vcm_2_sem_grupo_preencher['QUANTIDADE']*deliveries_resultados_vcm_2_sem_grupo_preencher['perc']
#deliveries_resultados_vcm_2_sem_grupo = pd.concat([deliveries_resultados_vcm_2_sem_grupo_OK,deliveries_resultados_vcm_2_sem_grupo_preencher])
#deliveries_resultados_vcm_2 = pd.concat([deliveries_resultados_vcm_2_com_grupo,deliveries_resultados_vcm_2_sem_grupo])
#headers = ['id','id_highlevel','PERIODO','DIRETORIA','GERENCIA','CONSULTORIA','CULTURA','UNIDADE PRODUTORA','UNIDADE FATURAMENTO','GRUPO PRODUTO','PRODUTO','CODIGO PRODUTO','QUANTIDADE']
#deliveries_resultados_vcm_2 = deliveries_resultados_vcm_2[headers]
#deliveries_resultados_vcm_2 = deliveries_resultados_vcm_2.merge(di_cliente, how = 'left', on = 'id')
#deliveries_resultados_vcm_2['perc'] = deliveries_resultados_vcm_2['perc'].fillna(1.0)
#deliveries_resultados_vcm_2['QUANTIDADE'] = deliveries_resultados_vcm_2['QUANTIDADE']*deliveries_resultados_vcm_2['perc']
#deliveries_resultados_vcm_2_com_cliente = deliveries_resultados_vcm_2.loc[deliveries_resultados_vcm_2['CLIENTE GRC'].notnull(),:].drop(columns='perc')
#deliveries_resultados_vcm_2_sem_cliente = deliveries_resultados_vcm_2.loc[deliveries_resultados_vcm_2['CLIENTE GRC'].isna(),:].drop(columns=['CLIENTE GRC','perc'])
#deliveries_resultados_vcm_2_sem_cliente = deliveries_resultados_vcm_2_sem_cliente.merge(di_cliente_hl, how = 'left', on = 'id_highlevel')
#deliveries_resultados_vcm_2_sem_cliente['perc'] = deliveries_resultados_vcm_2_sem_cliente['perc'].fillna(1.0)
#deliveries_resultados_vcm_2_sem_cliente['QUANTIDADE'] = deliveries_resultados_vcm_2_sem_cliente['QUANTIDADE']*deliveries_resultados_vcm_2_sem_cliente['perc']
#deliveries_resultados_vcm_2_sem_cliente = deliveries_resultados_vcm_2_sem_cliente.drop(columns='perc')
#deliveries_resultados_vcm_2 = pd.concat([deliveries_resultados_vcm_2_com_cliente,deliveries_resultados_vcm_2_sem_cliente])
#headers = ['PERIODO','DIRETORIA','GERENCIA','CONSULTORIA','CULTURA','UNIDADE PRODUTORA','UNIDADE FATURAMENTO','GRUPO PRODUTO','PRODUTO','CODIGO PRODUTO','CLIENTE GRC','QUANTIDADE']
#deliveries_resultados_vcm_2 = deliveries_resultados_vcm_2[headers]
#deliveries_resultados_vcm_2['proxy_explosao'] = deliveries_resultados_vcm_2['CODIGO PRODUTO'] + '-' + deliveries_resultados_vcm_2['UNIDADE PRODUTORA']
#deliveries_resultados_vcm_2 = deliveries_resultados_vcm_2.merge(explosion_di, how = 'left', on = 'proxy_explosao')
#deliveries_resultados_vcm_2['perc'] = deliveries_resultados_vcm_2['perc'].fillna(1.0)
#deliveries_resultados_vcm_2['QUANTIDADE'] = deliveries_resultados_vcm_2['QUANTIDADE']*deliveries_resultados_vcm_2['perc']
#deliveries_resultados_vcm_2_OK = deliveries_resultados_vcm_2.loc[deliveries_resultados_vcm_2['CODIGO MP'].notnull(),:].drop(columns='perc')
#deliveries_resultados_vcm_2_preencher = deliveries_resultados_vcm_2.loc[deliveries_resultados_vcm_2['CODIGO MP'].isna(),:].drop(columns=['MATERIA PRIMA','CODIGO MP','perc'])
#deliveries_resultados_vcm_2_preencher['proxy_explosao'] = deliveries_resultados_vcm_2_preencher['CODIGO PRODUTO'] + '-' + deliveries_resultados_vcm_2_preencher['UNIDADE FATURAMENTO']
#deliveries_resultados_vcm_2_preencher = deliveries_resultados_vcm_2_preencher.merge(explosion_di, how = 'left', on = 'proxy_explosao')
#deliveries_resultados_vcm_2_preencher['perc'] = deliveries_resultados_vcm_2_preencher['perc'].fillna(1.0)
#deliveries_resultados_vcm_2_preencher['QUANTIDADE'] = deliveries_resultados_vcm_2_preencher['QUANTIDADE']*deliveries_resultados_vcm_2_preencher['perc']
#deliveries_resultados_vcm_2_preencher_resolvido = deliveries_resultados_vcm_2_preencher.loc[deliveries_resultados_vcm_2_preencher['CODIGO MP'].notna(),:].drop(columns='perc')
#deliveries_resultados_vcm_2_preencher_com_produto = deliveries_resultados_vcm_2_preencher.loc[deliveries_resultados_vcm_2_preencher['CODIGO MP'].isna(),:].drop(columns=['MATERIA PRIMA','CODIGO MP','perc'])
#deliveries_resultados_vcm_2_preencher_com_produto = deliveries_resultados_vcm_2_preencher_com_produto.merge(explosion_di_hl, how = 'left', on = 'CODIGO PRODUTO')
#deliveries_resultados_vcm_2_preencher_com_produto['perc'] = deliveries_resultados_vcm_2_preencher_com_produto['perc'].fillna(1.0)
#deliveries_resultados_vcm_2_preencher_com_produto['QUANTIDADE'] = deliveries_resultados_vcm_2_preencher_com_produto['QUANTIDADE']*deliveries_resultados_vcm_2_preencher_com_produto['perc']
#deliveries_resultados_vcm_2_preencher_com_produto = deliveries_resultados_vcm_2_preencher_com_produto.drop(columns='perc')
#deliveries_resultados_vcm_2_preencher = pd.concat([deliveries_resultados_vcm_2_preencher_resolvido,deliveries_resultados_vcm_2_preencher_com_produto])
#deliveries_resultados_vcm_2 = pd.concat([deliveries_resultados_vcm_2_OK, deliveries_resultados_vcm_2_preencher])
#headers = ['PERIODO','DIRETORIA','GERENCIA','CONSULTORIA','CULTURA','UNIDADE PRODUTORA','UNIDADE FATURAMENTO','GRUPO PRODUTO','PRODUTO','CODIGO PRODUTO','MATERIA PRIMA','CODIGO MP','CLIENTE GRC','QUANTIDADE']
#deliveries_resultados_vcm_2 = deliveries_resultados_vcm_2[headers]
#deliveries_resultados_vcm = pd.concat([deliveries_resultados_vcm_1,deliveries_resultados_vcm_2])
# ================================================= DEPRECADO ============================================================

deliveries_resultados_vcm = pd.concat([deliveries_resultados_vcm_1])
deliveries_resultados_vcm = deliveries_resultados_vcm.drop(columns=['id','id_highlevel'])
deliveries_resultados_vcm['PERIODO'] = deliveries_resultados_vcm['PERIODO'].dt.date
headers = ['PERIODO','DIRETORIA','GERENCIA','CONSULTORIA','UNIDADE PRODUTORA','UNIDADE FATURAMENTO',
           'CULTURA','GRUPO PRODUTO','PRODUTO','CODIGO PRODUTO','MATERIA PRIMA','CODIGO MP','QUANTIDADE']
deliveries_resultados_vcm = deliveries_resultados_vcm[headers]
deliveries_resultados_vcm.to_excel(os.path.join(cwd,output_path+'tbOutDemandaRestrita.xlsx'), sheet_name='Demanda Restrita', index=False)
print('Demanda Restrita criada na pasta Output!')
end_time = time.time()
print(f'\nTempo de Execução: {round(end_time - start_time,2)} segundos')

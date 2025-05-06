print('\n')
print('╔════════════════════════════════════════════════════════════════════╗')
print('║                    ATUALIZACAO DE DADOS - VCM                      ║')
print('║                  >>  constrained_demand.py     <<                  ║')
print('╠════════════════════════════════════════════════════════════════════╣')
print('║ Criado por:       Murilo Lima Ribeiro          Data: 21/03/2025    ║')
print('║ Editado por:      Murilo Lima Ribeiro          Data: 21/03/2025    ║')
print('╠════════════════════════════════════════════════════════════════════╣')
print('║ CHANGELOG:                                                         ║')
print('║ - v1.0.0 (21/03/2025): Criação da primeira versão do script unifi- ║')
print('║   cado com edições estruturais nos arquivos de depara e dado pri-  ║')
print('║   mário.                                                           ║')
print('╠════════════════════════════════════════════════════════════════════╣')
print('║ Este script é responsável pela atualização:                        ║')
print('║ >> Demanda Restrita :: Criação do Arquivo Final de Forecast        ║')
print('╚════════════════════════════════════════════════════════════════════╝')
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

# CHECAGEM DE ARQUIVOS
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
    print('\n')

def padronizar(value):
    if isinstance(value, str):
        value = value.upper().strip()
        value = unidecode(value)
    return value

# =======================================================================================================================
# DEFINIR ARQUIVOS
# =======================================================================================================================

arquivos_primarios = {
    'arq_demanda_irrestrita': 'iptDemandaIrrestrita.xlsx',
    'arq_demanda_irrestrita_sn01': 'Demanda',
    'arq_periodos': 'iptPeriodos.xlsx',
    'arq_resultados_vcm': 'Resultados.xlsx',
    'arq_resultados_vcm_sn01': 'Resultados',
    'arq_tbUpdateCorrentes': 'iptUpdateCorrentes.xlsx',
    'arq_RendEntr': 'WIZARD_RENDIMENTO_ENTRADA.xlsx',
    'arq_RendEntr_sn01': 'RENDIMENTO_ENTRADA_PROD',
    'arq_RendSaida': 'WIZARD_RENDIMENTO_SAIDA.xlsx',
    'arq_RendSaida_sn01': 'RENDIMENTO_SAIDA_PROD',
    'arq_cadastro': 'depSKU.xlsx',
    'arq_cadastro_sn01':'CADASTRO',
    'arq_cadastro_sn02':'AGRUPAMENTO',
    'arq_tbDeparaMercadoConsumidor': 'depEstruturaComercial.xlsx',
    'arq_tbDeparaUnidadesProdutoras': 'depUnidadesProdutivas.xlsx'

}

tp_dado_arquivos = {
    'arq_demanda_irrestrita':{'PERIODO':'datetime64[ns]','DIRETORIA':str,'GERENCIA':str,'CONSULTORIA':str,'UNIDADE PRODUTORA':str,
               'CULTURA':str,'GRUPO PRODUTO':str,'PRODUTO':str,'CODIGO PRODUTO':str,
               'RM_PREMIUM_DESCRIPTION_ENG':str,'QUANTIDADE':np.float32,'MP AGRUPADA':str},
    'arq_periodos':{'NUMERO':str,'PERIODO':'datetime64[ns]', 'NOME_PERIODO':str},
    'arq_resultados_vcm':{'Corrente-VCM':str,'Produto-VCM':str,'Período-VCM':str, 'Quantidade':np.float64,
                          'Unidade-Origem-VCM':str,'Unidade-Destino-VCM':str,'Corredor':str},
    'arq_tbUpdateCorrentes':{'ConjuntoCorrentes':str, 'Unidade-Origem':str, 'Unidade-Destino':str, 'Tipo':str},
    'arq_RendSaida':{'Unidade':str, 'Receita':str, 'Produto':str, 'ValorSaida':np.float64},
    'arq_RendEntr':{'Unidade':str, 'Receita':str, 'Produto':str, 'ValorEntrada':np.float64},
    'arq_cadastro_sn01':{'PRD-VCM':str,'CODIGO_ITEM':str,'DESCRICAO':str,'TIPO_MATERIAL':str,'CATEGORIA':str},
    'arq_cadastro_sn02':{'COD_ESPECIFICO':str,'DESCRICAO_ESPECIFICA':str,'CODIGO_AGRUPADO':str,
                                'AGRUPAMENTO_MP':str},
    'arq_tbDeparaMercadoConsumidor':{'DIRETORIA':str,'GERENCIA':str,'CONSULTORIA':str,'CENTROID':str,'UF':str,'VCM':str},
    'arq_tbDeparaUnidadesProdutoras':{'DEPOSITO':str,'PLANTA':str,'DESCRICAO_DEPOSITO':str,'DESCRICAO_PLANTA':str,
                     'TIPO_UNIDADE':str,'UP_MISTURADORA_VCM':str,'UP_EMBALADORA_VCM':str}
}

# =======================================================================================================================
# CARREGAR DATAFRAMES
# =======================================================================================================================
print('Iniciando...')
print('Tabelas carregadas...')
# DataFrame :: Horizonte (Período) de Otimização
# applymap(padronizar) não aplicado por se tratar de dados com a estrutura final do VCM
periodos = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['arq_periodos']), 
                         usecols=list(tp_dado_arquivos['arq_periodos'].keys()),
                         dtype=tp_dado_arquivos['arq_periodos']).applymap(padronizar)

# DataFrame :: cadastro de materiais :: busca toda a lista de materiais (MP, PI, PF) no cadastrados VCM
cadastro = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['arq_cadastro']),
                            sheet_name = arquivos_primarios['arq_cadastro_sn01'],
                            usecols = list(tp_dado_arquivos['arq_cadastro_sn01'].keys()),
                            dtype = tp_dado_arquivos['arq_cadastro_sn01']).applymap(padronizar)

cadastro_pf = cadastro[(cadastro['TIPO_MATERIAL'].str.split('-',expand = True)[0].str.strip() == 'PF')]
cadastro_mp = cadastro[(cadastro['TIPO_MATERIAL'].str.split('-',expand = True)[0].str.strip() == 'MP')]

# DataFrame :: agrupamento de materiais :: busca todo o de-para de códigos específicos em códigos agrupados
agrupamento_produtos = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['arq_cadastro']),
                            sheet_name = arquivos_primarios['arq_cadastro_sn02'],
                            usecols = list(tp_dado_arquivos['arq_cadastro_sn02'].keys()),
                            dtype = tp_dado_arquivos['arq_cadastro_sn02']).applymap(padronizar)

proxy_agrupamento = cadastro[['CODIGO_ITEM','DESCRICAO']]
proxy_agrupamento = proxy_agrupamento.rename(columns={'CODIGO_ITEM':'COD_ESPECIFICO','DESCRICAO':'DESCRICAO_ESPECIFICA'})
proxy_agrupamento['CODIGO_AGRUPADO'] = proxy_agrupamento['COD_ESPECIFICO']
proxy_agrupamento['AGRUPAMENTO_MP'] = proxy_agrupamento['DESCRICAO_ESPECIFICA']
agrupamento_produtos = pd.concat([agrupamento_produtos,proxy_agrupamento])
agrupamento_produtos = agrupamento_produtos.drop_duplicates(subset = 'COD_ESPECIFICO')
agrupamento_cadastro = agrupamento_produtos.copy()

# DataFrame :: DE-PARA de unidades produtoras em relação aos dados da demanda
tbDeparaUnidadesProdutoras = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['arq_tbDeparaUnidadesProdutoras']),
                             sheet_name = arquivos_primarios['arq_tbDeparaUnidadesProdutoras'].split('.')[0],
                             usecols = list(tp_dado_arquivos['arq_tbDeparaUnidadesProdutoras'].keys()),
                             dtype = tp_dado_arquivos['arq_tbDeparaUnidadesProdutoras']).applymap(padronizar)

tbDeparaUnidadesProdutoras['UP_EMBALADORA_VCM_2'] = 'TN-' + tbDeparaUnidadesProdutoras['UP_EMBALADORA_VCM'].str.split('-',expand=True)[1] + '-' +\
                                                    tbDeparaUnidadesProdutoras['UP_EMBALADORA_VCM'].str.split('-',expand=True)[2]

tbDeparaUnidadesProdutoras = tbDeparaUnidadesProdutoras.melt(id_vars=list(tbDeparaUnidadesProdutoras.columns)[:-2],
                                                             value_vars=['UP_EMBALADORA_VCM','UP_EMBALADORA_VCM_2'],
                                                             var_name = 'TIPO_UNIDADE_VCM',
                                                             value_name = 'UNIDADE_VCM')

# DataFrame :: Mercados Consumidores da Estrutura Comercial
mercados = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['arq_tbDeparaMercadoConsumidor']),
                         sheet_name = arquivos_primarios['arq_tbDeparaMercadoConsumidor'].split('.')[0],
                         usecols = list(tp_dado_arquivos['arq_tbDeparaMercadoConsumidor'].keys()),
                         dtype = tp_dado_arquivos['arq_tbDeparaMercadoConsumidor']).applymap(padronizar)

id_mercados_consumidores = mercados.copy()
id_mercados_consumidores = id_mercados_consumidores['VCM'].to_frame().rename({'VCM':'ID MC'})

# DataFramme :: Demanda Irrestrita
demanda_irrestrita = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['arq_demanda_irrestrita']),
                        sheet_name = arquivos_primarios['arq_demanda_irrestrita_sn01'],
                        usecols = list(tp_dado_arquivos['arq_demanda_irrestrita'].keys()),
                        dtype = tp_dado_arquivos['arq_demanda_irrestrita']).applymap(padronizar)
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
validar_data_arquivo(os.path.join(cwd, output_path + arquivos_primarios['arq_resultados_vcm']))
resultados_vcm = pd.read_excel(os.path.join(cwd, output_path + arquivos_primarios['arq_resultados_vcm']),
                               sheet_name = arquivos_primarios['arq_resultados_vcm_sn01'],
                               usecols = list(tp_dado_arquivos['arq_resultados_vcm'].keys()),
                               dtype = tp_dado_arquivos['arq_resultados_vcm'],
                               skiprows = 2).applymap(padronizar)

# DataFrame :: Esqueleto topológico de correntes existentes no VCM
tbUpdateCorrentes = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['arq_tbUpdateCorrentes']),
                            sheet_name= arquivos_primarios['arq_tbUpdateCorrentes'].split('.')[0], 
                            usecols=list(tp_dado_arquivos['arq_tbUpdateCorrentes'].keys()),
                            dtype=tp_dado_arquivos['arq_tbUpdateCorrentes']).applymap(padronizar)

# =======================================================================================================================
# EXECUÇÃO DE SCRIPTS
# =======================================================================================================================
print('╔════════════════════════════════════════════════════════════════════╗')
print('║                      >>   DEMANDA RESTRITA    <<                   ║')
print('╠════════════════════════════════════════════════════════════════════╣')
print('║ # Forecast do Ciclo S&OP a partir dos Resultados VCM               ║')
print('╚════════════════════════════════════════════════════════════════════╝')
print('\n')

# DataFrame da Demanda Irrestrita :: Realizar tratamento 
left_outer_join(demanda_irrestrita, agrupamento_cadastro, left_on = 'CODIGO PRODUTO', right_on = 'COD_ESPECIFICO')
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

# DataFrame de Resultados VCM :: mesclagem de dados primários
deliveries_resultados_vcm = resultados_vcm.loc[resultados_vcm['Unidade-Destino-VCM'].str[:2] == 'MC',:]
deliveries_resultados_vcm = deliveries_resultados_vcm.loc[deliveries_resultados_vcm['Produto-VCM'].str[:2] == 'PF',:]
deliveries_resultados_vcm = deliveries_resultados_vcm.reset_index().drop(columns='index')
esqueleto_explosao = deliveries_resultados_vcm[['Produto-VCM','Unidade-Origem-VCM']].copy()
proxy_unidade_resultado_vcm = resultados_vcm.copy()
left_outer_join(proxy_unidade_resultado_vcm, tbDeparaUnidadesProdutoras, left_on = 'Unidade-Origem-VCM', right_on = 'UNIDADE_VCM')
proxy_unidade_resultado_vcm = proxy_unidade_resultado_vcm.loc[proxy_unidade_resultado_vcm['UNIDADE_VCM'].notnull(),:].reset_index().drop(columns='index')
left_outer_join(proxy_unidade_resultado_vcm,periodos, left_on = 'Período-VCM', right_on = 'NOME_PERIODO')
proxy_unidade_resultado_vcm['proxy_unidade'] = proxy_unidade_resultado_vcm['PERIODO'].astype('str') + '-' + proxy_unidade_resultado_vcm['Produto-VCM'] + '-' + proxy_unidade_resultado_vcm['Unidade-Destino-VCM']
headers = ['proxy_unidade','PLANTA','DEPOSITO','Quantidade']
proxy_unidade_resultado_vcm = proxy_unidade_resultado_vcm[headers]
proxy_unidade_resultado_vcm_total = proxy_unidade_resultado_vcm.groupby('proxy_unidade')['Quantidade'].sum().reset_index()
left_outer_join(proxy_unidade_resultado_vcm, proxy_unidade_resultado_vcm_total, left_on='proxy_unidade', right_on='proxy_unidade')
proxy_unidade_resultado_vcm['perc'] = proxy_unidade_resultado_vcm['Quantidade_x']/proxy_unidade_resultado_vcm['Quantidade_y']
proxy_unidade_resultado_vcm = proxy_unidade_resultado_vcm[['proxy_unidade','DEPOSITO','PLANTA','perc']].rename(columns={'PLANTA':'UNIDADE_FATURAMENTO','DEPOSITO':'UNIDADE_PRODUTORA'})
Explosion = RendSaida.copy()
left_outer_join(Explosion, RendEntr, left_on = ['Unidade','Receita'], right_on = ['Unidade','Receita'])
Explosion = Explosion.rename(columns={'Produto_x':'FG','Produto_y':'RM','ValorEntrada':'CompVol'})
Explosion = Explosion.drop(columns='ValorSaida')

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
deliveries_resultados_vcm_1 = deliveries_resultados_vcm_1.merge(tbDeparaUnidadesProdutoras, how = 'left', left_on='Unidade-Origem', right_on = 'UNIDADE_VCM')
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
deliveries_resultados_vcm = pd.concat([deliveries_resultados_vcm_1])
deliveries_resultados_vcm = deliveries_resultados_vcm.drop(columns=['id','id_highlevel'])
deliveries_resultados_vcm['PERIODO'] = deliveries_resultados_vcm['PERIODO'].dt.date
headers = ['PERIODO','DIRETORIA','GERENCIA','CONSULTORIA','UNIDADE PRODUTORA','UNIDADE FATURAMENTO',
           'CULTURA','GRUPO PRODUTO','PRODUTO','CODIGO PRODUTO','MATERIA PRIMA','CODIGO MP','QUANTIDADE']
deliveries_resultados_vcm = deliveries_resultados_vcm[headers]
deliveries_resultados_vcm.to_excel(os.path.join(cwd,output_path+'Demanda Restrita.xlsx'), sheet_name='Demanda Restrita', index=False)
print('Demanda Restrita criada na pasta Output!')
end_time = time.time()
print(f'\nTempo de Execução: {round(end_time - start_time,2)} segundos')
print('\n')
print('╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗')
print('║                                           ATUALIZACAO DE DADOS - VCM                                           ║')
print('║                                               >>  freight.py  <<                                               ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ Criado por:    Isabela Nunes dos Santos        Data: 10/03/2025                                                ║')
print('║ Editado por:   Isabela Nunes dos Santos        Data: 13/03/2025                                                ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ CHANGELOG:                                                                                                     ║')
print('║ - v1.0.0 (13/03/2025): Criação da primeira versão do script unificado com edições estruturais nos arquivos     ║')
print('║                        de depara e dado primário.                                                              ║')
print('║                                                                                                                ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ Este script é responsável pela atualização:                                                                    ║')
print('║ >> Tabela de Fretes Rodoviários Outbound                                                                       ║')
print('║ >> Tabela de Fretes Rodoviários Inbound                                                                        ║')
print('║ >> Custos de Internalização para fretes Ferroviário e Hidroviário                                              ║')
print('║ >> Leadtimes (Inativo)                                                                                         ║')
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

# LEFT JOIN
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
     'periodos': 'iptPeriodos.xlsx',
     'fretes': 'iptTabelaFretes.xlsx',
     'fretes_sn': 'iptTabelaFretes',
     'up_correntes':'iptUpdateCorrentes.xlsx',
     'up_correntes_sn': 'iptUpdateCorrentes',
     'localizacao': 'depGeolocalizacao.xlsx',
     'localizacao_sn': 'depGeolocalizacao',
     'unidades_expedicao': 'depUnidadesProdutivas.xlsx',
     'unidades_expedicao_sn': 'depUnidadesProdutivas',
     'portos': 'depUnidadesPortuarias.xlsx',
     'portos_sn': 'depUnidadesPortuarias',
     'custo_internalizacao': 'iptCustoInternalizacao.xlsx',
     'custo_internalizacao_sn': 'iptCustoInternalizacao',
     'template_fretes': 'tmpFretes.xlsx'
}

tp_dado_arquivos = {
     'periodos':{'NUMERO':str,'PERIODO':'datetime64[ns]', 'NOME_PERIODO':str},
     'fretes': {'data_inicio':'datetime64[ns]','rota':str,'Valor':'float64'},
     'up_correntes': {'ConjuntoCorrentes':str, 'Unidade-Origem':str, 'Unidade-Destino':str, 'Tipo':str},
     'localizacao': {'Unidade':str, 'Estado':str, 'Município':str},
     'unidades_expedicao': {'PLANTA':str, 'UNIDADE_EXPEDICAO_VCM':str},
     'portos': {'PORTO':str,'NOME_PORTO_VCM':str},
     'custo_internalizacao': {'MODAL':str, 'Origem':str, 'Origem Rodo':str, 'Destino':str, 'Corrente VCM':str}, #, 'Periodo':'datetime64[ns]'
     'template_fretes': {'Origem':str, 'Destino':str, 'Corrente':str, 'Periodo':str, 'ValorVariavel':'float64', 'ValorContainer':'float64'},
}


# =======================================================================================================================
# CARREGAR DATAFRAMES
# =======================================================================================================================

print('Carregando arquivos necessários... \n')

# DataFrame :: Horizonte (Período) de Otimização
periodos = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['periodos']),
                         usecols=list(tp_dado_arquivos['periodos'].keys()),
                         dtype=tp_dado_arquivos['periodos'])

id_periodos = periodos['NOME_PERIODO'].to_frame()
periodos_fretes = periodos.copy()
periodos_fretes['Data_Inicio'] = periodos_fretes['PERIODO'] + MonthBegin(0)

# DataFrame :: Chaves identificadores dos Portos
porto = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['portos']), 
                      usecols = list(tp_dado_arquivos['portos'].keys()),
                      dtype = tp_dado_arquivos['portos'])

id_portos = porto.drop(columns=['PORTO']).drop_duplicates()
id_portos_inbound = id_portos.copy()
id_portos_inbound = id_portos_inbound['NOME_PORTO_VCM'].str.split("-", n = 2, expand = True)
id_portos_inbound = id_portos_inbound.loc[(id_portos_inbound[1] != 'NAC'),:]
id_portos_inbound = id_portos_inbound[1].drop_duplicates().tolist()

# DataFrame :: Dados de Frete
fretes = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['fretes']),
                       usecols=list(tp_dado_arquivos['fretes'].keys()),
                       dtype=tp_dado_arquivos['fretes']).applymap(padronizar)

new = fretes['rota'].str.split(" X ", n=2, expand=True)
fretes['Origem'] = new[0]
fretes['Destino'] = new[1]

# DataFrame :: Update de Correntes
correntes = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['up_correntes']),
                         sheet_name= arquivos_primarios['up_correntes_sn'], 
                       usecols=list(tp_dado_arquivos['up_correntes'].keys()),
                       dtype=tp_dado_arquivos['up_correntes']).applymap(padronizar)

correntes['ID'] = correntes['Unidade-Origem'] + '-' + correntes['Unidade-Destino']
unidades_interesse = correntes.copy()

# DataFrame :: Geolocalizacao
localizacao = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['localizacao']),
                         sheet_name= arquivos_primarios['localizacao_sn'], 
                       usecols=list(tp_dado_arquivos['localizacao'].keys()),
                       dtype=tp_dado_arquivos['localizacao']).applymap(padronizar)

localizacao['ID Origem-Destino'] = localizacao['Município'] + '-' + localizacao['Estado']

# DataFrame :: Depara Unidades
depara_unidades = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['unidades_expedicao']),
                        sheet_name= arquivos_primarios['unidades_expedicao_sn'],
                       usecols=list(tp_dado_arquivos['unidades_expedicao'].keys()),
                       dtype=tp_dado_arquivos['unidades_expedicao'])

id_unidades = depara_unidades['UNIDADE_EXPEDICAO_VCM'].str.split("-", n=2, expand = True)
id_unidades = id_unidades[1].drop_duplicates().tolist()

# DataFrame :: Custos de Frete Inbound Ferroviário e Hidroviário
frete_inbound_ferro_hidro = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['custo_internalizacao']),
                        sheet_name= arquivos_primarios['custo_internalizacao_sn'],
                       #usecols=list(tp_dado_arquivos['custo_internalizacao'].keys()), (desativado pois não faz sentido copiar os nomes das colunas que sempre trocam)
                       dtype=tp_dado_arquivos['custo_internalizacao'])

frete_inbound_ferro_hidro = pd.melt(frete_inbound_ferro_hidro, id_vars = ['MODAL','Origem','Origem Rodo','Destino','Corrente VCM'],
                                                               var_name = 'Periodo',
                                                               value_name = 'Custo (BRL/ton)')

frete_inbound_ferro_hidro['Periodo'] = frete_inbound_ferro_hidro['Periodo'].astype('datetime64[ns]')

# DataFrame :: Custos de Frete Inbound - Excluindo rotas Ferroviárias redundantes
exclude_routes = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['custo_internalizacao']),
                        sheet_name= arquivos_primarios['custo_internalizacao_sn'],
                       #usecols=list(tp_dado_arquivos['custo_internalizacao'].keys()), (desativado pois não faz sentido copiar os nomes das colunas que sempre trocam)
                       dtype=tp_dado_arquivos['custo_internalizacao'])

exclude_routes = pd.melt(exclude_routes, id_vars = ['MODAL','Origem','Origem Rodo','Destino','Corrente VCM'],
                                                               var_name = 'Periodo',
                                                               value_name = 'Custo (BRL/ton)')
exclude_routes = exclude_routes[['Corrente VCM','MODAL']].drop_duplicates()

# DataFrame :: Template de Fretes
validar_data_arquivo(os.path.join(cwd, path + arquivos_primarios['template_fretes']))
template_fretes = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['template_fretes']),
                       usecols=list(tp_dado_arquivos['template_fretes'].keys()),
                       dtype=tp_dado_arquivos['template_fretes'])

template_fretes['ID'] = template_fretes['Origem'] + '-' + template_fretes['Destino'] + '-' +\
                        template_fretes['Corrente'] + '-' + template_fretes['Periodo']


# =======================================================================================================================
# EXECUÇÃO DE SCRIPTS
# =======================================================================================================================

# Desconsiderando correntes lógicas
unidades_interesse = unidades_interesse.loc[unidades_interesse.Tipo == 'OUTBOUND'].reset_index().drop(columns='index')
new = unidades_interesse['Unidade-Origem'].str.split("-", n=2, expand=True)
unidades_interesse['Split1'] = new[0]
unidades_interesse['Split2'] = new[2]
unidades_interesse['Filtro'] = unidades_interesse['Unidade-Destino'].str[:2]
unidades_interesse = unidades_interesse.loc[(unidades_interesse.Filtro=='MC'),:]

fretes = fretes.merge(periodos_fretes, how='left', left_on='data_inicio', right_on='Data_Inicio')
fretes = fretes.dropna()
unidades_interesse = unidades_interesse.merge(localizacao, how='left', left_on='Unidade-Origem', right_on='Unidade')
unidades_interesse = unidades_interesse[['ConjuntoCorrentes', 'Unidade-Origem', 'Unidade-Destino', 'Unidade', 'ID Origem-Destino']]
unidades_interesse = unidades_interesse.rename(columns={'ID Origem-Destino':'Cidade (Origem)'})
unidades_interesse = unidades_interesse.merge(localizacao, how='left', left_on='Unidade-Destino', right_on='Unidade')
unidades_interesse = unidades_interesse[['ConjuntoCorrentes','Unidade-Origem','Unidade-Destino','Cidade (Origem)','ID Origem-Destino']]
unidades_interesse = unidades_interesse.rename(columns = {'ID Origem-Destino':'Cidade (Destino)'})
unidades_interesse = unidades_interesse.merge(periodos_fretes, how = 'cross')
unidades_interesse['ID-LEFT'] = unidades_interesse['Cidade (Origem)'] + '-' + unidades_interesse['Cidade (Destino)'] + '-' + unidades_interesse['NOME_PERIODO']
fretes['ID-RIGHT'] = fretes['Origem'] + '-' + fretes['Destino'] + '-' + fretes['NOME_PERIODO']
fretes = fretes.groupby(by = ['NOME_PERIODO','Data_Inicio','rota','Origem','Destino','ID-RIGHT'])['Valor'].mean().reset_index()
unidades_interesse = unidades_interesse.merge(fretes, how = 'left', left_on = 'ID-LEFT', right_on = 'ID-RIGHT')

# Abaixo, não são preenchidos valores proibitivos para os custos de frete
frete_outbound = unidades_interesse.dropna()
frete_outbound = frete_outbound[['ConjuntoCorrentes', 'Unidade-Origem', 'Unidade-Destino', 'NOME_PERIODO_x', 'Valor']]
frete_outbound = frete_outbound.rename(columns={'ConjuntoCorrentes':'Corrente', 'Unidade-Origem':'Origem', 'Unidade-Destino':'Destino', 'NOME_PERIODO_x':'Periodo_VCM', 'Valor':'Frete Médio (BRL/ton)'})
fretes_outbound_nao_listados = unidades_interesse.loc[(unidades_interesse.Valor.isnull()),:]

frete_outbound['Modal'] = 'Rodoviário'
frete_outbound['ValorContainer'] = 0
frete_outbound = frete_outbound[['Origem','Destino','Corrente','Periodo_VCM','Modal','Frete Médio (BRL/ton)','ValorContainer']]
frete_outbound = frete_outbound.rename(columns={'Periodo_VCM':'Periodo','Frete Médio (BRL/ton)':'ValorVariavel'})

# Tabelas de Custos e Fretes Inbound Rodoviário
unidades_interesse_inbound = correntes.copy()
unidades_interesse_inbound = unidades_interesse_inbound.loc[unidades_interesse_inbound.Tipo == 'INBOUND']
unidades_interesse_inbound = unidades_interesse_inbound.reset_index().drop(columns = 'index')

# (13/03/2025) Etapa comentada pois estava sendo desnecessária.

# inb_orig_aux = unidades_interesse_inbound['Unidade-Origem'].str.split("-", n = 2, expand = True)
# inb_dest_aux = unidades_interesse_inbound['Unidade-Destino'].str.split("-", n = 2, expand = True)
# unidades_interesse_inbound['O1'] = inb_orig_aux[0]
# unidades_interesse_inbound['O2'] = inb_orig_aux[1]
# unidades_interesse_inbound['O3'] = inb_orig_aux[2]
# unidades_interesse_inbound['D1'] = inb_dest_aux[0]
# unidades_interesse_inbound['D2'] = inb_dest_aux[1]
# unidades_interesse_inbound['D3'] = inb_dest_aux[2]

# print('Separando correntes físicas de correntes lógicas...')
# unidades_interesse_inbound_1 = unidades_interesse_inbound.copy()
# unidades_interesse_inbound_1['Check'] = np.NaN
# for i in range(unidades_interesse_inbound_1.shape[0]):
#     if unidades_interesse_inbound_1['O2'][i] in id_portos_inbound \
#        and unidades_interesse_inbound_1['O1'][i] == 'UC'\
#        and (unidades_interesse_inbound_1['D2'][i] in id_unidades or unidades_interesse_inbound_1['D2'][i] == 'TOL')\
#        and unidades_interesse_inbound_1['D1'][i] == 'DC':
#             unidades_interesse_inbound_1['Check'][i] = True
#     else:
#             unidades_interesse_inbound_1['Check'][i] = False

# unidades_interesse_inbound_2 = unidades_interesse_inbound.copy()
# unidades_interesse_inbound_2['Check'] = np.NaN
# for i in range(unidades_interesse_inbound_2.shape[0]):
#     if unidades_interesse_inbound_2['O2'][i] in id_portos_inbound \
#        and unidades_interesse_inbound_2['O1'][i] == 'DC'\
#        and unidades_interesse_inbound_2['D1'][i] == 'AEX':
#             unidades_interesse_inbound_2['Check'][i] = True
#     else:
#             unidades_interesse_inbound_2['Check'][i] = False

# unidades_interesse_inbound_3 = unidades_interesse_inbound.copy()
# unidades_interesse_inbound_3['Check'] = np.NaN
# for i in range(unidades_interesse_inbound_3.shape[0]):
#     if unidades_interesse_inbound_3['O2'][i] in id_portos_inbound \
#        and unidades_interesse_inbound_3['O1'][i] == 'UC'\
#        and (unidades_interesse_inbound_3['D2'][i] == 'FAB' or unidades_interesse_inbound_3['D2'][i] == 'TOL')\
#        and unidades_interesse_inbound_3['D1'][i] == 'AIN':
#             unidades_interesse_inbound_3['Check'][i] = True
#     else:
#             unidades_interesse_inbound_3['Check'][i] = False

# unidades_interesse_inbound_4 = unidades_interesse_inbound.copy()
# unidades_interesse_inbound_4['Check'] = np.NaN
# for i in range(unidades_interesse_inbound_4.shape[0]):
#     if unidades_interesse_inbound_4['O2'][i] in id_unidades \
#        and unidades_interesse_inbound_4['O1'][i] == 'UC'\
#        and unidades_interesse_inbound_4['D2'][i] in id_unidades\
#        and unidades_interesse_inbound_4['D1'][i] == 'DC':
#             unidades_interesse_inbound_4['Check'][i] = True
#     else:
#             unidades_interesse_inbound_4['Check'][i] = False

# unidades_interesse_inbound_5 = unidades_interesse_inbound.copy()
# unidades_interesse_inbound_5['Check'] = np.NaN
# for i in range(unidades_interesse_inbound_5.shape[0]):
#     if unidades_interesse_inbound_5['O2'][i] in id_portos_inbound \
#        and unidades_interesse_inbound_5['O1'][i] == 'DC'\
#        and unidades_interesse_inbound_5['D1'][i] == 'AIN':
#             unidades_interesse_inbound_5['Check'][i] = True
#     else:
#             unidades_interesse_inbound_5['Check'][i] = False

# # Novo dataframe para contabilizar também correntes de transferência
# # Quando existe movimento de estoques atráves de correntes de IMPOSTOS
# # Edit: 29/09/2023
# unidades_interesse_inbound_6 = unidades_interesse_inbound.copy()
# unidades_interesse_inbound_6['Check'] = np.NaN
# for i in range(unidades_interesse_inbound_6.shape[0]):
#     if unidades_interesse_inbound_6['O1'][i] == 'IMP'\
#        and unidades_interesse_inbound_6['D1'][i] == 'IMP':
#             unidades_interesse_inbound_6['Check'][i] = True
#     else:
#             unidades_interesse_inbound_6['Check'][i] = False

# unidade_interesse_inbound_1 = unidades_interesse_inbound_1.loc[unidades_interesse_inbound_1.Check == True, :]
# unidade_interesse_inbound_2 = unidades_interesse_inbound_2.loc[unidades_interesse_inbound_2.Check == True, :]
# unidade_interesse_inbound_3 = unidades_interesse_inbound_3.loc[unidades_interesse_inbound_3.Check == True, :]
# unidade_interesse_inbound_4 = unidades_interesse_inbound_4.loc[unidades_interesse_inbound_4.Check == True, :]
# unidade_interesse_inbound_5 = unidades_interesse_inbound_5.loc[unidades_interesse_inbound_5.Check == True, :]
# unidade_interesse_inbound_6 = unidades_interesse_inbound_6.loc[unidades_interesse_inbound_6.Check == True, :]

# Desativando a etapa abaixo, porque não haverá mais regras de preenchimento
#unidade_interesse_inbound = unidade_interesse_inbound_1

# Método append deprecado, aplicando método concat
#unidade_interesse_inbound = unidade_interesse_inbound.append(unidade_interesse_inbound_2)
#unidade_interesse_inbound = unidade_interesse_inbound.append(unidade_interesse_inbound_3)
#unidade_interesse_inbound = unidade_interesse_inbound.append(unidade_interesse_inbound_4)

# Novo método não concatena mais DFs 1,2,3,4,5,6
# Ao invés, apenas considerar que o filtro Tipo = Inbound é suficiente
#unidade_interesse_inbound = pd.concat([unidade_interesse_inbound_1,\
#                                       unidade_interesse_inbound_2,\
#                                       unidade_interesse_inbound_3,\
#                                       unidade_interesse_inbound_4,\
#                                       unidade_interesse_inbound_5,\
#                                       unidade_interesse_inbound_6])

unidade_interesse_inbound = unidades_interesse_inbound.copy()
unidade_interesse_inbound = unidade_interesse_inbound.merge(localizacao, how = 'left', left_on = 'Unidade-Origem', right_on = 'Unidade')
unidade_interesse_inbound = unidade_interesse_inbound.merge(localizacao, how = 'left', left_on = 'Unidade-Destino', right_on = 'Unidade')
unidade_interesse_inbound = unidade_interesse_inbound.rename(columns = {'ID Origem-Destino_x':'Cidade (Origem)',
                                                                        'ID Origem-Destino_y':'Cidade (Destino)'})

unidade_interesse_inbound = unidade_interesse_inbound[['ConjuntoCorrentes','Unidade-Origem','Unidade-Destino','Cidade (Origem)','Cidade (Destino)']]
unidade_interesse_inbound = unidade_interesse_inbound.merge(periodos_fretes, how = 'cross')
unidade_interesse_inbound = unidade_interesse_inbound[['ConjuntoCorrentes','Unidade-Origem','Unidade-Destino','Cidade (Origem)','Cidade (Destino)','NOME_PERIODO']]
unidade_interesse_inbound['ID-LEFT'] = unidade_interesse_inbound['Cidade (Origem)'] + '-' + \
                                        unidade_interesse_inbound['Cidade (Destino)'] + '-' + \
                                        unidade_interesse_inbound['NOME_PERIODO']

unidade_interesse_inbound = unidade_interesse_inbound.merge(fretes, how = 'left', left_on = 'ID-LEFT', right_on = 'ID-RIGHT')
wizard_frete_rodoviario_inbound = unidade_interesse_inbound.dropna()

# Gerar o LOG de erros para fretes que não estão mapeados
# No preenchimento, tratar esses fretes como fretes proibitivos
frete_inbound_rodo_sem_valor = unidade_interesse_inbound.loc[(unidade_interesse_inbound.Valor.isnull()),:]
wizard_frete_rodoviario_inbound = wizard_frete_rodoviario_inbound[['ConjuntoCorrentes','Unidade-Origem','Unidade-Destino','NOME_PERIODO_y','Valor']]
wizard_frete_rodoviario_inbound = wizard_frete_rodoviario_inbound.rename(columns = {'ConjuntoCorrentes':'Corrente',
                                                                                    'Unidade-Origem':'Origem',
                                                                                    'Unidade-Destino':'Destino',
                                                                                    'NOME_PERIODO_y':'Periodo_VCM',
                                                                                    'Valor':'Frete Médio (BRL/ton)'})

wizard_frete_rodoviario_inbound['Modal'] = 'Rodoviário'
wizard_frete_rodoviario_inbound['ValorContainer'] = 0
wizard_frete_rodoviario_inbound = wizard_frete_rodoviario_inbound[['Origem','Destino','Corrente','Periodo_VCM','Modal','Frete Médio (BRL/ton)','ValorContainer']]
wizard_frete_rodoviario_inbound = wizard_frete_rodoviario_inbound.rename(columns={'Periodo_VCM':'Periodo','Frete Médio (BRL/ton)':'ValorVariavel'})
wizard_frete_rodoviario_inbound = wizard_frete_rodoviario_inbound.merge(exclude_routes, how = 'left', left_on = 'Corrente', right_on = 'Corrente VCM')
wizard_frete_rodoviario_inbound = wizard_frete_rodoviario_inbound.loc[wizard_frete_rodoviario_inbound['Corrente VCM'].isna()]
wizard_frete_rodoviario_inbound = wizard_frete_rodoviario_inbound[['Origem','Destino','Corrente','Periodo','Modal','ValorVariavel','ValorContainer']]
wizard_frete_rodoviario_inbound = wizard_frete_rodoviario_inbound.reset_index().drop(columns = 'index')

# Criando uma lista de correntes a serem expurgadas
id_correntes_expurgar = frete_inbound_ferro_hidro[['Corrente VCM','MODAL']].drop_duplicates()

frete_inbound_ferro_hidro = frete_inbound_ferro_hidro.merge(periodos, how = 'left', left_on = 'Periodo', right_on = 'PERIODO')
frete_inbound_ferro_hidro = frete_inbound_ferro_hidro.dropna()
frete_inbound_ferro_hidro = frete_inbound_ferro_hidro.drop(columns = ['NUMERO','PERIODO'])

wizard_frete_inbound_ferro_hidro = frete_inbound_ferro_hidro
wizard_frete_inbound_ferro_hidro = wizard_frete_inbound_ferro_hidro[['MODAL','Corrente VCM','NOME_PERIODO','Custo (BRL/ton)']]
wizard_frete_inbound_ferro_hidro = wizard_frete_inbound_ferro_hidro.rename({'Corrente VCM':'Corrente','NOME_PERIODO':'Período','Custo (BRL/ton)':'Valor'})

unidade_interesse_inbound_proxy = correntes[['ConjuntoCorrentes','Unidade-Origem','Unidade-Destino']].drop_duplicates()

wizard_frete_inbound_ferro_hidro = wizard_frete_inbound_ferro_hidro.merge(unidade_interesse_inbound_proxy, how = 'left', left_on = 'Corrente VCM', right_on='ConjuntoCorrentes')

wizard_frete_inbound_ferro_hidro = wizard_frete_inbound_ferro_hidro[['Unidade-Origem','Unidade-Destino','Corrente VCM','NOME_PERIODO','MODAL','Custo (BRL/ton)']]

wizard_frete_inbound_ferro_hidro['ValorContainer'] = 0
wizard_frete_inbound_ferro_hidro = wizard_frete_inbound_ferro_hidro.rename(columns={'Unidade-Origem':'Origem',\
                                                                                    'Unidade-Destino':'Destino',\
                                                                                    'Corrente VCM':'Corrente',\
                                                                                    'NOME_PERIODO':'Periodo',\
                                                                                    'MODAL':'Modal',\
                                                                                    'Custo (BRL/ton)':'ValorVariavel'})
wizard_frete_inbound_ferro_hidro = wizard_frete_inbound_ferro_hidro[['Origem','Destino','Corrente','Periodo','Modal','ValorVariavel','ValorContainer']]

print('Criando uma base de fretes sem valor...')
frete_inbound_rodo_sem_valor = frete_inbound_rodo_sem_valor[['ConjuntoCorrentes','Unidade-Origem','Cidade (Origem)','Unidade-Destino','Cidade (Destino)','ID-LEFT']]

fretes_outbound_nao_listados = fretes_outbound_nao_listados[['ConjuntoCorrentes','Unidade-Origem','Cidade (Origem)','Unidade-Destino','Cidade (Destino)','ID-LEFT']]

print('Estabelecendo os fretes proibitivos para valores ausentes na tabela de fretes...')
fretes_proibitivos = pd.concat([frete_inbound_rodo_sem_valor, fretes_outbound_nao_listados])
# Inserir valores de fretes proibitivos
# 26/03/2024 - Solicitação do negócio: alterar proibitivos para 1.000,00 -> 1.000.000,00
fretes_proibitivos['Valor'] = 0.00
fretes_proibitivos['Periodo'] = fretes_proibitivos['ID-LEFT'].str.split('-',n=4).str[4]
fretes_proibitivos['IDx'] = fretes_proibitivos['Unidade-Origem'] + '-' + \
                           fretes_proibitivos['Unidade-Destino'] + '-' + \
                           fretes_proibitivos['ConjuntoCorrentes'] + '-' + \
                           fretes_proibitivos['ID-LEFT'].str.split('-',n=4).str[4]

# Excluir correntes Ferroviárias e Hidroviárias
fretes_proibitivos = fretes_proibitivos.merge(id_correntes_expurgar, how = 'left', left_on = 'ConjuntoCorrentes', right_on = 'Corrente VCM')
fretes_proibitivos = fretes_proibitivos.loc[(fretes_proibitivos.MODAL != 'Ferroviário')&\
                                            (fretes_proibitivos.MODAL != 'Hidroviário')].reset_index().drop(columns = 'index')
fretes_proibitivos = fretes_proibitivos[['IDx','Valor']].rename(columns={'Valor':'ValorVariavel'})
fretes_proibitivos['ValorContainer'] = 0.0

# CRIANDO UMA TABELA CONSOLIDADA PARA OS VALORES DE FRETE
# =======================================================

# Excluir correntes Ferroviárias e Hidroviárias
wizard_frete_rodoviario_inbound = wizard_frete_rodoviario_inbound.merge(id_correntes_expurgar, how = 'left', left_on = 'Corrente', right_on = 'Corrente VCM')
wizard_frete_rodoviario_inbound = wizard_frete_rodoviario_inbound.loc[(wizard_frete_rodoviario_inbound['MODAL'] != 'Ferroviário')\
                                                                      &(wizard_frete_rodoviario_inbound['MODAL'] != 'Hidroviário')].reset_index().drop(columns='index')
wizard_custo_frete = pd.concat([wizard_frete_inbound_ferro_hidro,frete_outbound,wizard_frete_rodoviario_inbound])
wizard_custo_frete = wizard_custo_frete.drop_duplicates()
wizard_custo_frete['IDx'] = wizard_custo_frete['Origem'] + '-' +\
                                     wizard_custo_frete['Destino'] + '-' +\
                                     wizard_custo_frete['Corrente'] + '-' +\
                                     wizard_custo_frete['Periodo']
wizard_custo_frete = wizard_custo_frete[['IDx','ValorVariavel','ValorContainer']]
wizard_custo_frete = pd.concat([wizard_custo_frete, fretes_proibitivos])
wizard_custo_frete = wizard_custo_frete.drop_duplicates(keep = 'first')

template_fretes = template_fretes.drop(columns=['ValorVariavel', 'ValorContainer'])
template_fretes = template_fretes.merge(wizard_custo_frete, how = 'left', left_on='ID', right_on='IDx')
template_fretes = template_fretes.drop(columns = ['ID','IDx'])

# =========================================================================
#  (18/02/2025) Nova lógica de média
# Adicionando uma média para todos os fretes nos meses em que não tem dado primario (mas que tem margem para média).
# Para as correntes que nem apareceram no dado primario, ou temos mais do que 3 meses zerados, preencher como 1M.
media = template_fretes.groupby(by=['Corrente'])['ValorVariavel'].mean()
media = media.reset_index()
media = media.rename(columns={'Corrente':'CorrenteOut','ValorVariavel':'ValorMedia'})

template_fretes = template_fretes.merge(media, how = 'left', left_on='Corrente', right_on='CorrenteOut')
template_fretes['ValorMedia'] = template_fretes['ValorMedia'].fillna(0.0)
template_fretes['ValorVariavel'] = template_fretes['ValorVariavel'].fillna(0.0)

# Somar quantos períodos ficaram em branco por Corrente e depois fazer merge com o original,
# vendo se a soma é <= 3. O que for maior que isso fica como proibitivo.
# Variável soma tem só as correntes que podem ser populadas com média, vamos juntar as
# correntes e filtrar por indicator "both", o que for "both" pode ser populado SE estiver zerado.
soma = template_fretes.copy()
soma['soma'] = soma.apply(lambda x: 1 if x['ValorVariavel']==0.0 else 0, axis=1)
soma = soma.groupby(by=['Corrente'])['soma'].sum().reset_index()
soma = soma.loc[soma['soma']<=3]
soma = soma.drop(columns={'soma'})

wizard_custo_frete_structure = template_fretes.merge(soma, how='left', on='Corrente', indicator=True)
wizard_custo_frete_structure['ValorVariavel'] = wizard_custo_frete_structure.apply(lambda x: x['ValorMedia'] if x['ValorVariavel']==0.0 and x['ValorMedia']!=0.0 and x['_merge']=='both' else x['ValorVariavel'], axis=1)
wizard_custo_frete_structure = wizard_custo_frete_structure.drop(columns={'CorrenteOut','ValorMedia','_merge'})

# (11/06/2025) Para os casos que temos menos de 2 meses populados, mas temos pelo menos um valor, replicar esse valor.
limite = wizard_custo_frete_structure.shape[0]
for i in range(wizard_custo_frete_structure.shape[0]):
    if i!=0 and wizard_custo_frete_structure['ValorVariavel'][i]==0.0 and wizard_custo_frete_structure['Corrente'][i]==wizard_custo_frete_structure['Corrente'][i-1] and wizard_custo_frete_structure['ValorVariavel'][i-1]!=0.0:
        wizard_custo_frete_structure['ValorVariavel'][i] = wizard_custo_frete_structure['ValorVariavel'][i-1]
    elif i!=limite-1 and wizard_custo_frete_structure['ValorVariavel'][i]==0.0 and wizard_custo_frete_structure['Corrente'][i]==wizard_custo_frete_structure['Corrente'][i+1] and wizard_custo_frete_structure['ValorVariavel'][i+1]!=0.0:
        wizard_custo_frete_structure['ValorVariavel'][i] = wizard_custo_frete_structure['ValorVariavel'][i+1]

# ==========================================================================

# CRIANDO UM PONTO DE CONTROLE PARA AVALIAR OS FRETES PROIBITIVOS
wizard_custo_frete_structure = wizard_custo_frete_structure.fillna(0.0)
# (21/01/2025) Como solicitado, adicionando o 1M para todos os valores zerados (incluindo outbound).
wizard_custo_frete_structure['ValorVariavel'] = wizard_custo_frete_structure['ValorVariavel'].replace(0.0,1000000.00)

wizard_custo_frete_structure.to_excel(os.path.join(cwd,output_path + 'WIZARD_CUSTO_FRETE.xlsx'), index = False, sheet_name = 'FRETES_PERIODOS')
fretes_outbound_nao_listados = fretes_outbound_nao_listados.to_excel(os.path.join(cwd,exec_log_path + 'LOG ERROR - Erros Frete Outbound.xlsx'), index = False, sheet_name = 'Erros')
frete_inbound_rodo_sem_valor.to_excel(os.path.join(cwd,exec_log_path + 'LOG ERROR - Erros Frete Inbound.xlsx'), index = False, sheet_name = 'Erros')
print('Arquivo WIZARD_CUSTO_FRETE.xlsx foi Atualizado com Sucesso!')
end_time = time.time()
print(f'Tempo de Execução: {round(end_time - start_time,2)} segundos')
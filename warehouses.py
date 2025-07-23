print('\n')
print('╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗')
print('║                                           ATUALIZACAO DE DADOS - VCM                                           ║')
print('║                                             >>  warehouses.py  <<                                              ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ Criado por:    Isabela Nunes dos Santos        Data: 23/04/2025                                                ║')
print('║ Editado por:   Isabela Nunes dos Santos        Data: 08/07/2025                                                ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ CHANGELOG:                                                                                                     ║')
print('║ - v1.0.0 (24/04/2025): Criação da primeira versão do script unificado com edições estruturais nos arquivos     ║')
print('║                        de depara e dado primário.                                                              ║')
print('║                                                                                                                ║')
print('║ - v1.0.1 (08/07/2025): Trazendo o trecho de capacidade de armazenagem do script de limits.                     ║')
print('║                                                                                                                ║')
print('║ - v1.0.2 (08/07/2025): Criação de orientação a objeto para execução de scripts integrados.                     ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ Este script é responsável pela atualização:                                                                    ║')
print('║ >> Custos de Armazenagem e Handling                                                                            ║')
print('║ >> Capacidades de Armazenagem                                                                                  ║')
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
#print('Tempo de execução esperado: por volta de 20s \n')

# DataFrame ::  Dicionário Genérico
dicgen = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['dicgen']),
                       sheet_name = arquivos_primarios['dicgen'].split('.')[0],
                       usecols = list(tp_dado_arquivos['dicgen'].keys()),
                       dtype = tp_dado_arquivos['dicgen'])

# DataFrame :: DEPARA UNIDADES DE ARMAZENAGEM
df_unidades_armz = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['localizacao']),
                         sheet_name= arquivos_primarios['localizacao_sn'],
                         usecols=list(tp_dado_arquivos['localizacao'].keys()),
                         dtype=tp_dado_arquivos['localizacao'])

# DataFrame :: DADO PRIMARIO DE ARMAZENAGEM E HANDLING
df_custos_armz = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['custos_armz']),
                         sheet_name= arquivos_primarios['custos_armz_sn'],
                         usecols=list(tp_dado_arquivos['custos_armz'].keys()),
                         dtype=tp_dado_arquivos['custos_armz']).applymap(fx.padronizar)

# DataFrame :: DADO PRIMARIO DE CAPACIDADE DE ARMAZENAGEM INTERNA E EXTERNA
df_cap_armz = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['cap_prod']),
                         sheet_name= arquivos_primarios['cap_prod_sn'],
                         usecols=list(tp_dado_arquivos['cap_prod'].keys()),
                         dtype=tp_dado_arquivos['cap_prod']).applymap(fx.padronizar)

df_cap_arm_maxmin = df_cap_armz.copy()

# DataFrame :: Unidades de Expedição e Descarga
# DataFrame :: Depara Unidades Produtivas / Armazenagem / Expedição
df_unidades = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['unidades_exp']),
                         sheet_name= arquivos_primarios['unidades_exp_sn'], 
                       usecols=list(tp_dado_arquivos['unidades_exp'].keys()),
                       dtype=tp_dado_arquivos['unidades_exp']).applymap(fx.padronizar)
unid_arm = df_unidades[['DEPOSITO','PLANTA','UNIDADE_ARMAZENAGEM_VCM']]

# DataFrame :: Horizonte (Período) de Otimização
df_periodos = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['periodos']),
                         usecols=list(tp_dado_arquivos['periodos'].keys()),
                         dtype=tp_dado_arquivos['periodos'])
df_periodos = df_periodos.rename(columns=rename_dataframes['df_periodos'])

# DataFrame :: TEMPLATE DE CUSTO DE HANDLING PARA ARMAZÉNS EXTERNOS
#validar_data_arquivo(os.path.join(cwd, path + arquivos_primarios['template_hand_armz']))
df_template_hand_armz = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['template_hand_armz']),
                         usecols=list(tp_dado_arquivos['template_hand_armz'].keys()),
                         dtype=tp_dado_arquivos['template_hand_armz'])
df_template_hand_armz['Recebimento'] = 0.0
df_template_hand_armz['Expedição'] = 0.0

# DataFrame :: TEMPLATE DE CUSTOS VARIAVEIS PARA ARMAZÉNS EXTERNOS
#validar_data_arquivo(os.path.join(cwd, path + arquivos_primarios['template_var_armz']))
df_template_var_armz = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['template_var_armz']),
                         usecols=list(tp_dado_arquivos['template_var_armz'].keys()),
                         dtype=tp_dado_arquivos['template_var_armz'])
df_template_var_armz['Valor'] = 0.0
df_template_var_armz['Custo Financeiro'] = 0.0
df_template_var_armz['Custo Variável'] = 0.0

# DataFrame :: Template Capacidade
#validar_data_arquivo(os.path.join(cwd, path + arquivos_primarios['template_capacidade']))
template_capacidade = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['template_capacidade']),
                       usecols=list(tp_dado_arquivos['template_capacidade'].keys()),
                       dtype=tp_dado_arquivos['template_capacidade'])
template_capacidade['Volume Mínimo'] = 0.0
template_capacidade['Volume Máximo'] = 0.0

# =======================================================================================================================
# EXECUÇÃO DE SCRIPTS
# =======================================================================================================================

print('╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗')
print('║ >> Etapa 01/02: Preenchimento de Custos de Handling e Armazenagem <<                                           ║')
print('╚════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝')
# ==========================================
# >>>>>>>>> POR ESTADO TÁ ERRADO!! <<<<<<<<<
# ==========================================
# Tópico 1: (faremos por estado só por enquanto)
# Mesclando o template com o depGeolocalizacao pela unidade, para pegar o Estado.
# Faria sentido puxar isso por unidade direto? 
# (Sem passar pelo estado, mas ai ia ter que vir a unidade no dado de custo)

df_template_hand_armz = fx.left_outer_join(df_template_hand_armz,df_unidades_armz,left_on='Unidade', right_on='Unidade',
                                           name_left='Template de Custos de Handling', name_right='Localização')
# Substituindo o nome pela sigla.
df_custos_armz['Estado'] = df_custos_armz['Estado'].replace(['BAHIA', 'ESPIRITO SANTO', 'GOIAS',
                    'PARANA', 'RIO GRANDE DO SUL', 'SANTA CATARINA', 'SAO PAULO', 'SERGIPE',],\
                    ['BA', 'ES', 'GO', 'PR', 'RS', 'SC', 'SP', 'SE'])
df_template_hand_armz = fx.left_outer_join(df_template_hand_armz,df_custos_armz,left_on=['Estado','Unidade'], right_on=['Estado','Terceiro'],
                                           name_left='Template de Custos de Handling', name_right='DADO PRIMARIO DE ARMAZENAGEM E HANDLING')
df_template_hand_armz['Recebimento'] = df_template_hand_armz['Handling (R$/ton)'].fillna(0.0)
df_template_hand_armz = df_template_hand_armz[['Unidade','Produto','Periodo','Recebimento','Expedição']]
df_template_hand_armz.to_excel(os.path.join(cwd,output_path+'tbOutCustosHandlingArmz.xlsx'),index=False, sheet_name='HANDLING')

df_template_var_armz = fx.left_outer_join(df_template_var_armz,df_unidades_armz,left_on='Unidade', right_on='Unidade',
                                          name_left='Template de Custos Variáveis', name_right='Localização')
df_template_var_armz = fx.left_outer_join(df_template_var_armz,df_custos_armz,left_on=['Estado','Unidade'], right_on=['Estado','Terceiro'],
                                          name_left='Template de Custos Variáveis', name_right='ADO PRIMARIO DE ARMAZENAGEM E HANDLING')
df_template_var_armz['Custo Variável'] = df_template_var_armz['Armazenagem (R$/ton)'].fillna(0.0)
# (03/12/2024) Como pedido pelo Ricardo, caso o custo variável esteja zerado, preencher com um valor específico para cada unidade.
df_template_var_armz['ID'] = df_template_var_armz['Unidade'].str[:3]
custo_paliativo = {'ID':['AEX', 'APO', 'AIN', 'TER'],\
                      'Custo Paliativo':[25, 50, 1, 35]}
custo_paliativo = pd.DataFrame.from_dict(custo_paliativo)
df_template_var_armz = df_template_var_armz.merge(custo_paliativo, how='left', on='ID')
df_template_var_armz['Custo Variável'] = df_template_var_armz.apply(lambda x: x['Custo Paliativo'] if 
                                                            x['Custo Variável']==0.0 else x['Custo Variável'], axis=1)

df_template_var_armz = df_template_var_armz[['Unidade','Produto','Periodo','Valor','Custo Financeiro','Custo Variável']]
df_template_var_armz.to_excel(os.path.join(cwd,output_path+'tbOutCustosVariaveisArmz.xlsx'),index=False, sheet_name='CUSTOS_VARIAVEIS')
print('\nFinalizado: Wizard de Custos de Armazenagem')

# (08/07/2025) Como conversado com o Matheus, adicionando a etapa abaixo 
# nesse script, que antes estava em limits.
print('╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗')
print('║  >> Etapa 02/02:  LIMITES DE CAPACIDADE MÍNIMO E MÁXIMO  <<                                                    ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ # Popula a capacidade de armazenagem interno (AIN) e externo (AEX)                                             ║')
print('╚════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝')

# (08/07/2025) Como conversado com o Matheus, retirando a coluna de 
# data de referência para armazenagem, para evitar problemas.
df_cap_arm_maxmin = df_cap_arm_maxmin.loc[df_cap_arm_maxmin['Agrupador']=='CAPACIDADE ARMAZENAGEM'].copy()
df_cap_arm_maxmin['Unidade'] = df_cap_arm_maxmin['Unidade'].replace(list(dicgen['DE']),list(dicgen['PARA']))
unid_arm['Local'] = np.where(unid_arm['UNIDADE_ARMAZENAGEM_VCM'].str[:3] == 'AIN','INTERNO','EXTERNO')
#unid_arm = unid_arm.drop_duplicates(subset=['PLANTA'])
unid_arm = unid_arm.loc[unid_arm['DEPOSITO']=='1001']
df_cap_arm_maxmin = df_cap_arm_maxmin[['Unidade','Quantidade','Local']]
# (08/07/2025) Desativando a ideia de média.
# (25/06/2025) Fazendo uma média das quantidades por unidade, para tirar as duplicatas.
# df_cap_arm_maxmin = df_cap_arm_maxmin.groupby(by=['Unidade','Local'])['Quantidade'].mean().round(2)
# df_cap_arm_maxmin = df_cap_arm_maxmin.to_frame()
# df_cap_arm_maxmin.reset_index(inplace = True)
df_cap_arm_maxmin = fx.left_outer_join(df_cap_arm_maxmin, unid_arm, left_on=['Unidade','Local'], right_on = ['PLANTA','Local'],
                   name_left='Capacidade de Armazenagem INTERNO e EXTERNO', name_right='Depara de Unidades')
# (08/07/2025) Adicionando o período como cross, pq cada linha precisa de uma referência de período para o template!
df_cap_arm_maxmin = df_cap_arm_maxmin.merge(df_periodos, how='cross')
template_capacidade = fx.left_outer_join(template_capacidade, df_cap_arm_maxmin, left_on=['Unidade','Periodo'], right_on=['UNIDADE_ARMAZENAGEM_VCM','Nome VCM'],
                                         name_left = 'Template Capacidade', name_right = 'Capacidade de Armazenagem INTERNO e EXTERNO')
template_capacidade['Volume Máximo'] = template_capacidade['Quantidade']
template_capacidade = template_capacidade[['Unidade_x','Periodo','Volume Mínimo','Volume Máximo']]
template_capacidade = template_capacidade.rename(columns={'Unidade_x':'Unidade'})
template_capacidade['Volume Máximo'] =  template_capacidade['Volume Máximo'].fillna(0.0)
template_capacidade.to_excel(os.path.join(cwd,output_path+'tmpCapacidadeArmazenagem.xlsx'), index=False, sheet_name='VOLUME_AGRUPADO')

# (08/07/2025) Como conversado com o Matheus, desativando o trecho abaixo,
# pois estamos usando a lógica que estava em limits.
# print('\n')
# print('╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗')
# print('║ Etapa 02/02: Preenchimento de Capacidades de Armazenagem                                                       ║')
# print('╚════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝')
# df_cap_armz = df_cap_armz.loc[df_cap_armz['Agrupador']=='CAPACIDADE ARMAZENAGEM']
# df_cap_armz = df_cap_armz.merge(df_unidades_armz, how = 'left', left_on = 'Unidade', right_on='Abrev-P02')
# left_outer_join(df_template_cap_amrz,df_cap_armz,left_on='Unidade', right_on='Unidade_y')
# df_template_cap_amrz['Vol. Max. Aj.'] = df_template_cap_amrz.apply(lambda x: x['Quantidade'] if x['Quantidade'] > 0.0 else x['Volume Máximo'], axis = 1)
# cols = ['Unidade','Periodo','Vol. Max. Aj.']
# df_template_cap_amrz = df_template_cap_amrz[cols].rename(columns={'Vol. Max. Aj.':'Limite'})
# # (06/11/2024) Regra para preeencher com 100.000 os Volumes Máximos que forem iguais a 0
# df_template_cap_amrz['Limite'] = df_template_cap_amrz.apply(lambda x: 100000 if x['Limite'] == 0.0 and x['Unidade'][:3]!='APO' else x['Limite'], axis = 1)
# df_template_cap_amrz = df_template_cap_amrz.loc[df_template_cap_amrz['Limite']!=0.0]
# df_template_cap_amrz.to_excel(os.path.join(cwd,output_path+'tbOutCapacidadeArmazenagem.xlsx'),
#                                       index = False, sheet_name = 'VOLUME_AGRUPADO')
print('\nFinalizado: Wizard de Capacidade de Armazenagem')
end_time = time.time()
print(f'Tempo de Execução: {round(end_time - start_time,2)} segundos')
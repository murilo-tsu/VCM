print('\n')
print('╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗')
print('║                                           ATUALIZACAO DE DADOS - VCM                                           ║')
print('║                                             >>  warehouses.py  <<                                              ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ Criado por:    Isabela Nunes dos Santos        Data: 23/04/2025                                                ║')
print('║ Editado por:   Isabela Nunes dos Santos        Data: 24/04/2025                                                ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ CHANGELOG:                                                                                                     ║')
print('║ - v1.0.0 (24/04/2025): Criação da primeira versão do script unificado com edições estruturais nos arquivos     ║')
print('║                        de depara e dado primário.                                                              ║')
print('║                                                                                                                ║')
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
     'unidades_armz': 'depGeolocalizacao.xlsx',
     'unidades_armz_sn': 'depGeolocalizacao',
     'custos_armz': 'tbDadoPrimarioCustosArmazens.xlsx',
     'custos_armz_sn': 'tbDadoPrimarioCustosArmazens',
     'cap_armz':'tbDadoPrimarioCapacidadeArmz.xlsx',
     'cap_armz_sn': 'tbDadoPrimarioCapacidadeArmz',
     'template_hand_armz': 'tmpCustosHandlingArmz.xlsx',
     'template_var_armz': 'tmpCustosVaiaveisArmz.xlsx',
     'template_cap_armz': 'tmpCapacidadeArmz.xlsx',
}

tp_dado_arquivos = {
     'unidades_armz':{'Unidade':str,'Nome-Unidade':str, 'Abrev-P02':str, 'Estado':str},
     'custos_armz': {'Estado':str, 'Terceiro':str, 'Armazenagem (R$/ton)':np.float64, 'Handling (R$/ton)':np.float64},
     'cap_armz': {'Unidade':str, 'Local':str, 'Capacidade Armazenagem':np.int64},
     'template_hand_armz': {'Unidade':str, 'Produto':str, 'Periodo':str, 'Recebimento':np.float64, 'Expedição':np.float64},
     'template_var_armz': {'Unidade':str, 'Produto':str, 'Periodo':str, 'Valor':np.float64, 'Custo Financeiro':np.float64, 'Custo Variável':np.float64},
     'template_cap_armz': {'Unidade':str, 'Periodo':str, 'Volume Mínimo':np.float64, 'Volume Máximo':np.float64},
}

rename_dataframes = {
    'df_periodos':{'NOME_PERIODO':'Periodo_VCM', 'PERIODO':'Nome'},
}


# =======================================================================================================================
# CARREGAR DATAFRAMES
# =======================================================================================================================
print('Carregando arquivos necessários... \n')
#print('Tempo de execução esperado: por volta de 20s \n')

# DataFrame :: DEPARA UNIDADES DE ARMAZENAGEM
df_unidades_armz = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['unidades_armz']),
                         sheet_name= arquivos_primarios['unidades_armz_sn'],
                         usecols=list(tp_dado_arquivos['unidades_armz'].keys()),
                         dtype=tp_dado_arquivos['unidades_armz'])
#df_unidades_armz = df_unidades_armz.rename(columns=rename_dataframes['df_periodos'])

# DataFrame :: DADO PRIMARIO DE ARMAZENAGEM E HANDLING
df_custos_armz = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['custos_armz']),
                         sheet_name= arquivos_primarios['custos_armz_sn'],
                         usecols=list(tp_dado_arquivos['custos_armz'].keys()),
                         dtype=tp_dado_arquivos['custos_armz']).applymap(padronizar)

# DataFrame :: DADO PRIMARIO DE CAPACIDADE DE ARMAZENAGEM INTERNA E EXTERNA
df_cap_armz = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['cap_armz']),
                         sheet_name= arquivos_primarios['cap_armz_sn'],
                         usecols=list(tp_dado_arquivos['cap_armz'].keys()),
                         dtype=tp_dado_arquivos['cap_armz']).applymap(padronizar)

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

# DataFrame :: TEMPLATE DE CAPACIDADE DE ARMAZENAGEM
#validar_data_arquivo(os.path.join(cwd, path + arquivos_primarios['template_cap_armz']))
df_template_cap_amrz = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['template_cap_armz']),
                         usecols=list(tp_dado_arquivos['template_cap_armz'].keys()),
                         dtype=tp_dado_arquivos['template_cap_armz'])
df_template_cap_amrz['Volume Mínimo'] = 0.0
df_template_cap_amrz['Volume Máximo'] = 0.0


# =======================================================================================================================
# EXECUÇÃO DE SCRIPTS
# =======================================================================================================================

print('╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗')
print('║ Etapa 01/02: Preenchimento de Custos de Armazenagem                                                            ║')
print('╚════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝')
# ==========================================
# >>>>>>>>> POR ESTADO TÁ ERRADO!! <<<<<<<<<
# ==========================================
# (faremos por estado só por enquanto)
# Faz mais sentido mesclar o template com o depGeolocalizacao pela unidade, para pegar o Estado
# (tá em sigla, faz sentido o dadoPrimario ser em sigla OU fazer a substituição da sigla dentro do script?) (Tanto faz, é fácil resolver)

left_outer_join(df_template_hand_armz,df_unidades_armz,left_on='Unidade', right_on='Unidade')
df_custos_armz['Estado'] = df_custos_armz['Estado'].replace(['BAHIA', 'ESPIRITO SANTO', 'GOIAS',
                    'PARANA', 'RIO GRANDE DO SUL', 'SANTA CATARINA', 'SAO PAULO', 'SERGIPE',],\
                    ['BA', 'ES', 'GO', 'PR', 'RS', 'SC', 'SP', 'SE'])
left_outer_join(df_template_hand_armz,df_custos_armz,left_on='Estado', right_on='Estado')
df_template_hand_armz['Recebimento'] = df_template_hand_armz['Handling (R$/ton)'].fillna(0.0)
df_template_hand_armz = df_template_hand_armz[['Unidade','Produto','Periodo','Recebimento','Expedição']]
df_template_hand_armz.to_excel(os.path.join(cwd,output_path+'tbOutCustosHandlingArmz.xlsx'),index=False, sheet_name='HANDLING')

left_outer_join(df_template_var_armz,df_unidades_armz,left_on='Unidade', right_on='Unidade')
left_outer_join(df_template_var_armz,df_custos_armz,left_on='Estado', right_on='Estado')
df_template_var_armz['Custo Variável'] = df_template_var_armz['Armazenagem (R$/ton)'].fillna(0.0)
# Tópico 1: Usando Custos Paliativos de FTO.
# Custos de FHG: 'Custo Paliativo':[50, 100, 1, 80] (msm sigla, msm ordem)
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

# Tópico 2: Usa o cap Portos aqui, e o output é de cap portos + armz
# Faz mais sentido tirar a parte de portos msm e o output ser só de armz? (Tirei)
print('\n')
print('╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗')
print('║ Etapa 02/02: Preenchimento de Capacidades de Armazenagem                                                       ║')
print('╚════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝')
df_cap_armz = df_cap_armz.merge(df_unidades_armz, how = 'left', left_on = 'Unidade', right_on='Abrev-P02')
df_cap_interno = df_cap_armz.copy()
df_cap_interno = df_cap_interno.loc[df_cap_interno['Local'] == 'INTERNO',:]
df_cap_interno = df_cap_interno.reset_index().drop(columns='index')
df_cap_externo = df_cap_armz.copy()
df_cap_externo = df_cap_externo.loc[df_cap_externo['Local'] != 'INTERNO',:]
df_cap_externo = df_cap_externo.reset_index().drop(columns='index')

# Tópico 3:
# SERÁ NECESSÁRIO ADICIONAR A CAPACIDADE DO ARMAZÉM EXTERNO
# Tá separando pq???? Se vai juntar dnv? (Alguém tem que ver isso ai... (facewithtongue))
df_cap_armz = df_cap_interno[['Unidade_y','Capacidade Armazenagem']].copy()
left_outer_join(df_template_cap_amrz,df_cap_armz,left_on='Unidade', right_on='Unidade_y')
df_template_cap_amrz['Vol. Max. Aj.'] = df_template_cap_amrz.apply(lambda x: x['Capacidade Armazenagem'] if x['Capacidade Armazenagem'] > 0.0 else x['Volume Máximo'], axis = 1)
cols = ['Unidade','Periodo','Vol. Max. Aj.']
df_template_cap_amrz = df_template_cap_amrz[cols].rename(columns={'Vol. Max. Aj.':'Limite'})
# (06/11/2024) Adicionando uma regra para preeencher com 100.000 os Volumes Máximos que forem iguais a 0
df_template_cap_amrz['Limite'] = df_template_cap_amrz.apply(lambda x: 100000 if x['Limite'] == 0.0 and x['Unidade'][:3]!='APO' else x['Limite'], axis = 1)
df_template_cap_amrz = df_template_cap_amrz.loc[df_template_cap_amrz['Limite']!=0.0]
df_template_cap_amrz.to_excel(os.path.join(cwd,output_path+'tbOutCapacidadeArmazenagem.xlsx'),
                                      index = False, sheet_name = 'VOLUME_AGRUPADO')
print('\nFinalizado: Wizard de Capacidade de Armazenagem')
end_time = time.time()
print(f'Tempo de Execução: {round(end_time - start_time,2)} segundos')
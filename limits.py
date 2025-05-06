print('\n')
print('╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗')
print('║                                           ATUALIZACAO DE DADOS - VCM                                           ║')
print('║                                                >>  limits.py  <<                                               ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ Criado por:    Isabela Nunes dos Santos        Data: 08/04/2025                                                ║')
print('║ Editado por:   Isabela Nunes dos Santos        Data: 10/04/2025                                                ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ CHANGELOG:                                                                                                     ║')
print('║ - v1.0.0 (10/04/2025): Criação da primeira versão do script unificado com edições estruturais nos arquivos     ║')
print('║                        de depara e dado primário.                                                              ║')
print('║                                                                                                                ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ Este script é responsável pela atualização:                                                                    ║')
print('║ >> Limites de Descarga                                                                                         ║')
print('║ >> Limites de Produção                                                                                         ║')
print('║ >> Limites de Portos                                                                                           ║')
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
     'periodos': 'iptPeriodos.xlsx',
     'periodos_sn': 'Períodos de Otimização',
     'portos_armz_apo': 'depUnidadesPortuarias.xlsx',
     'portos_armz_apo_sn': 'depUnidadesPortuarias',
     'cap_prod': 'tbDadoPrimarioCapProducao.xlsx',
     'cap_prod_sn': 'tbDadoPrimarioCapProducao',
     'unidades_exp':'depUnidadesProdutivas.xlsx',
     'unidades_exp_sn': 'depUnidadesProdutivas',
     'cap_portos' : 'iptCapacidadePortuaria.xlsx',
     'cap_portos_sn' : 'iptCapacidadePortuaria',
     'cap_desc' : 'tbDadoPrimarioCapDescarga.xlsx',
     'cap_desc_sn' : 'tbDadoPrimarioCapDescarga',
     'template_saida' : 'tmpSaida.csv',
     'template_entrada' : 'tmpEntrada.csv',
     'template_capacidade' : 'tmpCapacidade.xlsx',
     'unidades':'depUnidadesPortuarias.xlsx',
     'unidades_sn':'depUnidadesPortuarias',
}

tp_dado_arquivos = {
     'periodos':{'NUMERO':np.int64,'PERIODO':'datetime64[ns]', 'NOME_PERIODO':str},
     'portos_armz_apo': {'PORTO':str, 'NOME_AZ_PORTO_VCM':str},
     'cap_prod': {'Unidade':str,'Nome Unidade':str,'Dt/Ref':'datetime64[ns]', 'Quantidade':str},
     'unidades_exp': {'DESCRICAO_PLANTA':str, 'UNIDADE_ARMAZENAGEM_VCM':str}, #'DEPOSITO':str, 'PLANTA':str, 
     'cap_portos': {'PERIODO':'datetime64[ns]','Aratu':np.float64, 'Vila Do Conde':np.float64, 'Itaqui':np.float64, 'Vitória':np.float64, 
                    'Hidrovias Miritituba':np.float64, 'Santos Termag':np.float64, 'Paranaguá':np.float64, 'Açu':np.float64, 'Salvador':np.float64,
                    'Pecém':np.float64, 'Santos Comercial':np.float64, 'Vitória TPD':np.float64, 'Santos Hidrovias':np.float64, 'Santarem':np.float64, 'Recife':np.float64},
     'cap_desc': {'Unidade':str, 'Nome Unidade':str, 'Dt/Ref':'datetime64[ns]', 'Quantidade':np.float64},
     'template_saida': {'Unidade':str, 'Periodo':str, 'Limite':str, 'Ativo':str},
     'template_entrada' : {'Unidade':str, 'Periodo':str, 'Limite':str, 'Ativo':str},
     'template_capacidade' : {'Unidade':str, 'Periodo':str, 'Volume Mínimo':np.int64, 'Volume Máximo':np.int64},
     'unidades' : {'UNIDADE':str, 'NOME_AZ_PORTO_VCM':str},
}

rename_dataframes = {
    'df_periodos':{'NUMERO':'Numero','NOME_PERIODO':'Nome VCM', 'PERIODO':'Nome'},
}


# =======================================================================================================================
# CARREGAR DATAFRAMES
# =======================================================================================================================
print('Carregando arquivos necessários... \n')

# DataFrame :: Horizonte (Período) de Otimização
df_periodos = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['periodos']),
                         usecols=list(tp_dado_arquivos['periodos'].keys()),
                         dtype=tp_dado_arquivos['periodos'])
df_periodos = df_periodos.rename(columns=rename_dataframes['df_periodos'])

# DataFrame :: Portos APO
df_portosAPO = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['portos_armz_apo']),
                         sheet_name= arquivos_primarios['portos_armz_apo_sn'], 
                       usecols=list(tp_dado_arquivos['portos_armz_apo'].keys()),
                       dtype=tp_dado_arquivos['portos_armz_apo']).applymap(padronizar)

# DataFrame :: Capacidade de Produção
df_cap_producao = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['cap_prod']),
                         sheet_name= arquivos_primarios['cap_prod_sn'], 
                       usecols=list(tp_dado_arquivos['cap_prod'].keys()),
                       dtype=tp_dado_arquivos['cap_prod']).applymap(padronizar)

# DataFrame :: Unidades de Expedição e Descarga
df_expedicao = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['unidades_exp']),
                         sheet_name= arquivos_primarios['unidades_exp_sn'], 
                       usecols=list(tp_dado_arquivos['unidades_exp'].keys()),
                       dtype=tp_dado_arquivos['unidades_exp']).applymap(padronizar)

# DataFrame :: Capacidade Portos
df_cap_portos = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['cap_portos']),
                         sheet_name= arquivos_primarios['cap_portos_sn'],
                       dtype=tp_dado_arquivos['cap_portos']).applymap(padronizar)

# DataFrame :: Capacidade de Descarga das Fábricas
df_descarga = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['cap_desc']),
                         sheet_name= arquivos_primarios['cap_desc_sn'], 
                       usecols=list(tp_dado_arquivos['cap_desc'].keys()),
                       dtype=tp_dado_arquivos['cap_desc']).applymap(padronizar)

# DataFrame :: Template Saída
validar_data_arquivo(os.path.join(cwd, path + arquivos_primarios['template_saida']))
template_saida = pd.read_csv(os.path.join(cwd, path + arquivos_primarios['template_saida']),
                       delimiter = ';', encoding = 'utf-8-sig',
                       usecols=list(tp_dado_arquivos['template_saida'].keys()),
                       dtype=tp_dado_arquivos['template_saida'])

# DataFrame :: Template Entrada
validar_data_arquivo(os.path.join(cwd, path + arquivos_primarios['template_entrada']))
template_entrada = pd.read_csv(os.path.join(cwd, path + arquivos_primarios['template_entrada']),
                       delimiter = ';', encoding = 'utf-8-sig',
                       usecols=list(tp_dado_arquivos['template_entrada'].keys()),
                       dtype=tp_dado_arquivos['template_entrada'])

# DataFrame :: Template Capacidade
validar_data_arquivo(os.path.join(cwd, path + arquivos_primarios['template_capacidade']))
template_capacidade = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['template_capacidade']),
                       usecols=list(tp_dado_arquivos['template_capacidade'].keys()),
                       dtype=tp_dado_arquivos['template_capacidade'])

# DataFrame :: Depara Unidades
df_unidades = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['unidades']),
                         sheet_name= arquivos_primarios['unidades_sn'], 
                       usecols=list(tp_dado_arquivos['unidades'].keys()),
                       dtype=tp_dado_arquivos['unidades']).applymap(padronizar)


# =======================================================================================================================
# EXECUÇÃO DE SCRIPTS
# =======================================================================================================================

# Parte 1 - Limites de Saida/Expedição
# Destativando esse trecho pois já é preenchido em outro script
# print('Iniciando preenchimento de limites de expedição')
# left_outer_join(df_cap_producao,df_periodos,left_on='Dt/Ref', right_on='Nome')
# df_cap_producao = df_cap_producao.dropna()
# df_cap_producao['Ativo'] = 'True'
# left_outer_join(df_cap_producao,df_unidades,left_on='Nome Unidade', right_on='DESCRICAO')
# df_cap_producao = df_cap_producao.rename(columns={'Unidade':'Sigla','NOME_VCM':'Unidade','Nome VCM':'Periodo','Quantidade':'Limite'})
# template_saida = template_saida.drop(columns={'Limite','Ativo'})
# template_saida = template_saida.merge(df_cap_producao, how='left', left_on=['Unidade','Periodo'], right_on=['Unidade','Periodo'])
# template_saida = template_saida[['Unidade','Periodo','Limite','Ativo']]
# template_saida['Ativo'] = template_saida['Ativo'].fillna('False')
# template_saida['Limite'] = template_saida['Limite'].fillna(0.0)
# template_saida.to_excel(os.path.join(cwd,output_path+'tbOutLimitesSaida.xlsx'), index = False, sheet_name='LimitesSaida')
# print('\nLimites de capacidade de expedição preenchidos!')

# Parte 2 - Limites Cap. Portuária
# Tópico 1: Volume Minimo faz sentido manter zero ou tem alguma regra de preenchimento?
# Tópico 1.1: Regras pra preencher o máximo? Caso não tenha no dado!
print('╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗')
print('║ Iniciando preenchimento de limites de capacidade portuária                                                     ║')
print('╚════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝')

df_cap_portos = pd.melt(df_cap_portos, id_vars=['PERIODO'], var_name='Porto', value_name='Capacidade')
df_cap_portos = df_cap_portos.applymap(padronizar)
df_portosAPO = df_portosAPO.drop_duplicates(['PORTO','NOME_AZ_PORTO_VCM'])
left_outer_join(df_cap_portos,df_portosAPO,left_on='Porto', right_on='PORTO')
left_outer_join(df_cap_portos,df_periodos,left_on='PERIODO', right_on='Nome')
df_cap_portos = df_cap_portos[['NOME_AZ_PORTO_VCM','Nome VCM','Capacidade']]
df_cap_portos = df_cap_portos.rename(columns={'NOME_AZ_PORTO_VCM':'Unidade','Nome VCM':'Periodo','Capacidade':'Limite'})
df_cap_portos = df_cap_portos.dropna()
df_cap_portos['Limite'] = df_cap_portos['Limite']*1000
left_outer_join(template_capacidade,df_cap_portos,left_on=['Unidade','Periodo'], right_on=['Unidade','Periodo'])
template_capacidade['Limite'] = template_capacidade['Limite'].fillna(0.0)
template_capacidade['Volume Máximo'] = template_capacidade['Limite']
template_capacidade = template_capacidade.drop(columns={'Limite'})
template_capacidade.to_excel(os.path.join(cwd, output_path+'tbOutLimitesPortosAPO.xlsx'),index=False,sheet_name='LimitesPortosAPO')
print('\nLimites de capacidade portuária preenchidos!\n')

# Parte 3
print('╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗')
print('║ Iniciando preenchimento de limites de Descarga                                                                 ║')
print('╚════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝')

df_expedicao['DESCRICAO_PLANTA'] = df_expedicao['DESCRICAO_PLANTA'].str.slice(3)
left_outer_join(df_descarga,df_periodos,left_on='Dt/Ref',right_on='Nome')
df_descarga = df_descarga.merge(df_expedicao,how='left',left_on='Nome Unidade',right_on='DESCRICAO_PLANTA')
df_descarga = df_descarga[['UNIDADE_ARMAZENAGEM_VCM','Nome VCM','Quantidade']]
df_descarga = df_descarga.dropna()
df_descarga = df_descarga.rename(columns={'UNIDADE_ARMAZENAGEM_VCM':'Unidade','Nome VCM':'Periodo','Quantidade':'Limite'})
df_descarga['Ativo'] = ''
template_entrada = template_entrada.drop(columns={'Limite','Ativo'})
left_outer_join(template_entrada,df_descarga,left_on=['Unidade','Periodo'],right_on=['Unidade','Periodo'])
template_entrada['Limite'] = template_entrada['Limite'].fillna(0.0)
template_entrada['Ativo'] = template_entrada.apply(lambda x: 'True' if x['Limite']!=0.0 else 'False', axis=1)
template_entrada.to_excel(os.path.join(cwd,output_path+'tbOutLimitesEntrada.xlsx'),index=False,sheet_name='LimitesEntrada')
print('\nLimites de Descarga preenchidos!\n')

end_time = time.time()
print(f'Tempo de Execução: {round(end_time - start_time,2)} segundos')
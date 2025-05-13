print('\n')
print('╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗')
print('║                                           ATUALIZACAO DE DADOS - VCM                                           ║')
print('║                                        >>  unconstrained_demand.py  <<                                         ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ Criado  por: Murilo Lima Ribeiro        Data: 20/03/2025                                                       ║')
print('║ Editado por: Murilo Lima Ribeiro        Data: 20/03/2025                                                       ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ CHANGELOG:                                                                                                     ║')
print('║ - v1.0.0 (20/03/2025): Criação da primeira versão do script unificado com edições estruturais nos arquivos     ║')
print('║                        de depara e dado primário.                                                              ║')
print('║                                                                                                                ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ Este script é responsável pela atualização:                                                                    ║')
print('║ >> Demanda Irrestrita                                                                                          ║')
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
import plotext as plt
from tqdm import tqdm
from tabulate import tabulate
from pandas.tseries.offsets import MonthEnd
from pandas.tseries.offsets import MonthBegin
from tkinter import messagebox
from unidecode import unidecode
warnings.filterwarnings('ignore')
start_time = time.time()

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
     'periodos': 'iptPeriodos.xlsx',
     'cadastro_produtos': 'depSKU.xlsx',
     'cadastro_produtos_sn01':'CADASTRO',
     'cadastro_produtos_sn02':'AGRUPAMENTO',
     'demanda':'iptDemandaIrrestrita.xlsx',
     'demanda_sn01':'Demanda',
     'unidades_exp':'depUnidadesProdutivas.xlsx',
     'unidades_terc':'depUnidadesGerencias.xlsx',
     'mercados':'depEstruturaComercial.xlsx',
     'wizard_spot_demanda_produto_faixa':'tmpDemanda.xlsx',
     'wizard_spot_demanda_produto_faixa_sn01':'SPOT_DEMANDA_PRODUTO_FAIXA'

}

tp_dado_arquivos = {
     'periodos':{'NUMERO':str,'PERIODO':'datetime64[ns]', 'NOME_PERIODO':str},
     'cadastro_produtos_sn01': {'PRD-VCM':str,'CODIGO_ITEM':str,'DESCRICAO':str,'TIPO_MATERIAL':str,'CATEGORIA':str},
     'cadastro_produtos_sn02': {'COD_ESPECIFICO':str,'DESCRICAO_ESPECIFICA':str,'CODIGO_AGRUPADO':str,
                                'AGRUPAMENTO_MP':str},
     'unidades_exp':{'DEPOSITO':str,'PLANTA':str,'DESCRICAO_DEPOSITO':str,'DESCRICAO_PLANTA':str,
                     'TIPO_UNIDADE':str,'UNIDADE_EXPEDICAO_VCM':str},                           
     'unidades_terc':{'UNIDADE PRODUTORA':str,'UNIDADE FATURAMENTO':str,'GERENCIA':str,'CONSULTORIA':str},
     'mercados':{'DIRETORIA':str,'GERENCIA':str,'CONSULTORIA':str,'CENTROID':str,'UF':str,'VCM':str},
     'demanda':{'PERIODO':'datetime64[ns]','DIRETORIA':str,'GERENCIA':str,'CONSULTORIA':str,'UNIDADE PRODUTORA':str,
               'CULTURA':str,'GRUPO PRODUTO':str,'PRODUTO':str,'CODIGO PRODUTO':str,
               'RM_PREMIUM_DESCRIPTION_ENG':str,'QUANTIDADE':np.float32,'MP AGRUPADA':str},
     'wizard_spot_demanda_produto_faixa':{'Unidade':str,'Produto':str,'Periodo':str,
                                          'Demanda Mínima':str,'Demanda Máxima':str}
    
}

# =======================================================================================================================
# CARREGAR DATAFRAMES
# =======================================================================================================================

# DataFrame :: Horizonte (Período) de Otimização
# applymap(padronizar) não aplicado por se tratar de dados com a estrutura final do VCM
periodos = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['periodos']), 
                         usecols=list(tp_dado_arquivos['periodos'].keys()),
                         dtype=tp_dado_arquivos['periodos'])

# DataFrame :: cadastro de materiais :: busca toda a lista de materiais (MP, PI, PF) no cadastrados VCM
cadastro_produtos = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['cadastro_produtos']),
                            sheet_name = arquivos_primarios['cadastro_produtos_sn01'],
                            usecols = list(tp_dado_arquivos['cadastro_produtos_sn01'].keys()),
                            dtype = tp_dado_arquivos['cadastro_produtos_sn01']).applymap(padronizar)

pf_cadastrada = cadastro_produtos[(cadastro_produtos['TIPO_MATERIAL'].str.split('-',expand = True)[0].str.strip() == 'PF')]

# DataFrame :: agrupamento de materiais :: busca todo o de-para de códigos específicos em códigos agrupados
agrupamento_produtos = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['cadastro_produtos']),
                            sheet_name = arquivos_primarios['cadastro_produtos_sn02'],
                            usecols = list(tp_dado_arquivos['cadastro_produtos_sn02'].keys()),
                            dtype = tp_dado_arquivos['cadastro_produtos_sn02']).applymap(padronizar)

proxy_agrupamento = cadastro_produtos[['CODIGO_ITEM','DESCRICAO']]
proxy_agrupamento = proxy_agrupamento.rename(columns={'CODIGO_ITEM':'COD_ESPECIFICO','DESCRICAO':'DESCRICAO_ESPECIFICA'})
proxy_agrupamento['CODIGO_AGRUPADO'] = proxy_agrupamento['COD_ESPECIFICO']
proxy_agrupamento['AGRUPAMENTO_MP'] = proxy_agrupamento['DESCRICAO_ESPECIFICA']
agrupamento_produtos = pd.concat([agrupamento_produtos,proxy_agrupamento])
agrupamento_produtos = agrupamento_produtos.drop_duplicates(subset = 'COD_ESPECIFICO')

# DataFrame :: DE-PARA de unidades externas em relação às gerências de vendas
unidades_terc = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['unidades_terc']),
                             sheet_name = arquivos_primarios['unidades_terc'].split('.')[0],
                             usecols = list(tp_dado_arquivos['unidades_terc'].keys()),
                             dtype = tp_dado_arquivos['unidades_terc']).applymap(padronizar)

# DataFrame :: DE-PARA de unidades produtoras em relação aos dados da demanda
unidades_exp = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['unidades_exp']),
                             sheet_name = arquivos_primarios['unidades_exp'].split('.')[0],
                             usecols = list(tp_dado_arquivos['unidades_exp'].keys()),
                             dtype = tp_dado_arquivos['unidades_exp']).applymap(padronizar)

# Carregando uma lista de depara de supervisões para MERCADO CONSUMIDOR
mercados = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['mercados']),
                         sheet_name = arquivos_primarios['mercados'].split('.')[0],
                         usecols = list(tp_dado_arquivos['mercados'].keys()),
                         dtype = tp_dado_arquivos['mercados'])

id_mercados_consumidores = mercados.copy()
id_mercados_consumidores = id_mercados_consumidores['VCM'].to_frame().rename({'VCM':'ID MC'})

# DataFrame :: Template de Demanda :: Validar se data de atualização do arquivo consta no mês atual
validar_data_arquivo(os.path.join(cwd, path + arquivos_primarios['wizard_spot_demanda_produto_faixa']))
wizard_spot_demanda_produto_faixa = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['wizard_spot_demanda_produto_faixa']),
                                                  sheet_name = arquivos_primarios['wizard_spot_demanda_produto_faixa_sn01'],
                                                  usecols = list(tp_dado_arquivos['wizard_spot_demanda_produto_faixa'].keys()),
                                                  dtype = tp_dado_arquivos['wizard_spot_demanda_produto_faixa'])

demanda = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['demanda']),
                        sheet_name = arquivos_primarios['demanda_sn01'],
                        usecols = list(tp_dado_arquivos['demanda'].keys()),
                        dtype = tp_dado_arquivos['demanda'])

# =======================================================================================================================
# EXECUÇÃO DE SCRIPTS
# =======================================================================================================================
print('╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗')
print('║                                           >>  DEMANDA IRRESTRITA  <<                                           ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ # WIZARD_SPOT_DEMANDA_PRODUTO_FAIXA :: Plano de Entregas Irrestrito                                            ║')
print('╚════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝')
print('\n')
print('Iniciando...')
print('Tabelas carregadas...')

# CODIFICACAO DOS ITENS PARA O VCM
# Identificar agrupamentos de PF
left_outer_join(demanda, agrupamento_produtos, left_on = 'CODIGO PRODUTO', right_on = 'COD_ESPECIFICO')
left_outer_join(demanda, pf_cadastrada, left_on = 'CODIGO_AGRUPADO', right_on = 'CODIGO_ITEM')
# Criar uma lista de unidades terceiras relevantes para uma determinada fábrica (unidade de expedição)
# Buscar apenas as unidades terceiras
unique = unidades_terc['UNIDADE PRODUTORA'].drop_duplicates().to_list()
# Preencher proxy.Faturamento somente se for uma unidade terceira
demanda['proxy.Faturamento'] = demanda['UNIDADE PRODUTORA'].apply(lambda x: x if x not in unique else np.NaN)
# Separar o arquivo de demanda com base na existência ou não na lista "unique"
demanda_unidade_standard = demanda[(demanda['proxy.Faturamento'].notna())].reset_index().drop(columns='index')
demanda_unidade_terceira = demanda[(demanda['proxy.Faturamento'].isna())].reset_index().drop(columns='index')
print(f"Foram identificados {demanda.loc[demanda['proxy.Faturamento'].isna(),:].shape[0]} linhas da demanda com problemas no depUnidadesGerencias.xlsx")
print('Verificar se todas as gerências estão cadastradas!')
# Para o DataFrame >> demanda_unidade_standard << a Unidade Faturamento é a própria unidade produtora
demanda_unidade_standard['UNIDADE FATURAMENTO'] = demanda_unidade_standard['proxy.Faturamento']
# Para o DataFrame >> demanda_unidade_terceira << a Unidade Faturamento é determinada através de regras
# 01: Se e existe uma CONSULTORIA específica no arquivo depUnidadesGerencias.xlsx, então, existe uma regra especial para 
# atribuir a UNIDADE FATURAMENTO a partir do depUnidadesGerencias, logo: >> demanda_unidade_terceira_na <<
# 02: Não existe CONSULTORIA específica, portanto, podemos seguir uma regra geral por GERENCIA, logo: >> demanda_unidade_terceira_notna <<
unique = unidades_terc.loc[unidades_terc['CONSULTORIA'].notna(),:]['GERENCIA'].drop_duplicates().to_list()
demanda_unidade_terceira['proxy.Consultoria'] = demanda_unidade_terceira['GERENCIA'].apply(lambda x: np.NaN if x not in unique else x)
# Tratando os casos em que a CONSULTORIA está vazia
demanda_unidade_terceira_na = demanda_unidade_terceira.loc[demanda_unidade_terceira['proxy.Consultoria'].isna(),:].reset_index().drop(columns='index')
proxy_unidades_terc = unidades_terc.loc[unidades_terc['CONSULTORIA'].isna(),:].reset_index().drop(columns='index')
proxy_unidades_terc = proxy_unidades_terc.rename(columns={'UNIDADE PRODUTORA':'UNIDADE PRODUTORA.2',
                                                          'CONSULTORIA':'CONSULTORIA.2','GERENCIA':'GERENCIA.2'})
left_outer_join(demanda_unidade_terceira_na, proxy_unidades_terc,
                left_on = ['UNIDADE PRODUTORA','GERENCIA'],
                right_on = ['UNIDADE PRODUTORA.2','GERENCIA.2'])
# Tratando as exceções em que a CONSULTORIA é relevante para determinação da UNIDADE DE FATURAMENTO
proxy_unidades_terc = unidades_terc.loc[unidades_terc['CONSULTORIA'].notna(),:].reset_index().drop(columns='index')
demanda_unidade_terceira_notna = demanda_unidade_terceira.loc[demanda_unidade_terceira['proxy.Consultoria'].notna(),:].reset_index().drop(columns='index')
proxy_unidades_terc = proxy_unidades_terc.rename(columns={'UNIDADE PRODUTORA':'UNIDADE PRODUTORA.2',
                                                          'CONSULTORIA':'CONSULTORIA.2','GERENCIA':'GERENCIA.2'})
left_outer_join(demanda_unidade_terceira_notna, proxy_unidades_terc,
                left_on = ['UNIDADE PRODUTORA','GERENCIA'],
                right_on = ['UNIDADE PRODUTORA.2','GERENCIA.2'])
demanda = pd.concat([demanda_unidade_standard, demanda_unidade_terceira_na, demanda_unidade_terceira_notna])
demanda['pkLEFT'] = demanda['UNIDADE PRODUTORA'] + '-' + demanda['UNIDADE FATURAMENTO']
unidades_exp['pkRIGHT.2'] = unidades_exp['DEPOSITO'] + '-' + unidades_exp['PLANTA']
left_outer_join(demanda, unidades_exp, left_on = 'pkLEFT', right_on = 'pkRIGHT.2')
demanda['pkLEFT'] = demanda['GERENCIA'] + '-' + demanda['CONSULTORIA']
mercados['pkRIGHT.3'] = mercados['GERENCIA'] + '-' + mercados['CONSULTORIA']
mercados = mercados.rename(columns={'DIRETORIA':'DIRETORIA.3','GERENCIA':'GERENCIA.3','CONSULTORIA':'CONSULTORIA.3'})
left_outer_join(demanda, mercados, left_on = 'pkLEFT', right_on = 'pkRIGHT.3')
left_outer_join(demanda, periodos, left_on = 'PERIODO', right_on = 'PERIODO')
demanda['pk'] = demanda['VCM'] + '-' + demanda['PRD-VCM'] + '-' + demanda['NOME_PERIODO']
wizard_spot_demanda_produto_faixa['pk'] = wizard_spot_demanda_produto_faixa['Unidade'] + '-' +\
                                          wizard_spot_demanda_produto_faixa['Produto'] + '-' +\
                                          wizard_spot_demanda_produto_faixa['Periodo']
history_demanda = demanda.copy()
print('Excluindo linhas da demanda para as quais não foi possível localizar um PRD-VCM')
print('Para avaliar os SKUs da demanda irrestrita, verificar as saídas do script 1st Deploy')
demanda = demanda.dropna(subset = ['PRD-VCM'])
excluded_volume = demanda.loc[demanda['pk'].isna(),:]['QUANTIDADE'].sum().round(2)
print(f'O volume desconsiderado da demanda irrestrita é de {excluded_volume}')
print(f'Gerado um log de erro das linhas de demanda expurgadas: LOG ERROR - Linhas Ignoradas da Demanda')
demanda.loc[demanda['pk'].isna(),:].to_excel(os.path.join(cwd,exec_log_path+'LOG ERROR - Linhas Ignoradas da Demanda.xlsx'),index=False)
demanda = demanda.groupby(['pk']).agg({'QUANTIDADE':'sum'}).reset_index()
print('Preenchendo o estrutura topológica...')
left_outer_join(wizard_spot_demanda_produto_faixa, demanda, left_on = 'pk', right_on = 'pk')
wizard_spot_demanda_produto_faixa['QUANTIDADE'] = wizard_spot_demanda_produto_faixa['QUANTIDADE'].fillna(0)
for i in tqdm(range(wizard_spot_demanda_produto_faixa.shape[0]),desc='Preenchendo Template...', unit = 'rows'):
    wizard_spot_demanda_produto_faixa['Demanda Mínima'][i] = 0.0
    wizard_spot_demanda_produto_faixa['Demanda Máxima'][i] = 0.0
    if wizard_spot_demanda_produto_faixa['QUANTIDADE'][i] > 0:
        # Aqui é possível definir uma variável para afrouxar os limites da demanda
        # Seja x a variável auxiliar que define os limites 
        x = 0.005
        wizard_spot_demanda_produto_faixa['Demanda Mínima'][i] = wizard_spot_demanda_produto_faixa['QUANTIDADE'][i] - 0*wizard_spot_demanda_produto_faixa['QUANTIDADE'][i]
        wizard_spot_demanda_produto_faixa['Demanda Máxima'][i] = wizard_spot_demanda_produto_faixa['QUANTIDADE'][i] + x*wizard_spot_demanda_produto_faixa['QUANTIDADE'][i]
columns = ['Unidade','Produto','Periodo','Demanda Mínima','Demanda Máxima']
wizard_spot_demanda_produto_faixa = wizard_spot_demanda_produto_faixa[columns]

# Atualização 14/03/2023: Inserir uma etapa de agregação das demandas
# Atualização 20/08/2024: Etapa de agregação da demanda com problema de dimensionalidade
# Hipótese: essa agregação está aumentando o volume total da demanda
# Melhoria: realizar a agregação na demanda antes de fazerr a mesclagem
# wizard_spot_demanda_produto_faixa = wizard_spot_demanda_produto_faixa.groupby(['Unidade','Produto','Periodo']).agg({'Demanda Mínima':'sum','Demanda Máxima':'sum'}).reset_index()
decimals_kg = 2
wizard_spot_demanda_produto_faixa['Demanda Mínima'] = wizard_spot_demanda_produto_faixa['Demanda Mínima'].apply(lambda x: round(x, decimals_kg))
wizard_spot_demanda_produto_faixa['Demanda Máxima'] = wizard_spot_demanda_produto_faixa['Demanda Máxima'].apply(lambda x: round(x, decimals_kg))
wizard_spot_demanda_produto_faixa.to_excel(os.path.join(cwd,output_path + 'Wizard_Spot_Demanda_Produto_Faixa.xlsx'), sheet_name='SPOT_DEMANDA_PRODUTO_FAIXA', index = False)
print('Arquivo (Wizard_Spot_Demanda_Produto_Faixa.xlsx) foi Atualizado com Sucesso!')
demanda_resumida = wizard_spot_demanda_produto_faixa.copy()
left_outer_join(demanda_resumida, periodos, left_on = 'Periodo', right_on = 'NOME_PERIODO')
demanda_resumida = demanda_resumida.groupby('PERIODO')['Demanda Mínima'].sum().reset_index()
demanda_resumida = demanda_resumida.sort_values(by='PERIODO', ascending = True)
demanda_resumida['PERIODO'] = demanda_resumida['PERIODO'].dt.date
print('\nRESUMO DA DEMANDA MENSAL PARA O VCM')
print(tabulate(demanda_resumida, headers="keys"))
end_time = time.time()
print(f'\nTempo de Execução: {round(end_time - start_time,2)} segundos')
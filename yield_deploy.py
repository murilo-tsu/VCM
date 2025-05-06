print('\n')
print('╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗')
print('║                                         ATUALIZACAO DE DADOS - VCM                                             ║')
print('║                                            >> yield_deploy.py <<                                               ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ Criado  por: Murilo Lima Ribeiro  Data: 02/04/2025                                                             ║')
print('║ Editado por: Murilo Lima Ribeiro  Data: 02/04/2025                                                             ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ CHANGELOG:                                                                                                     ║')
print('║ - v1.0.0 (02/04/2025): Criação da primeira versão do script unificado com edições estruturais nos arquivos de  ║')
print('║                        depara e dado primário.                                                                 ║')
print('║                                                                                                                ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ Este script é responsável pela atualização:                                                                    ║')
print('║ >> Listagem de formulações relevantes para demanda                                                             ║')
print('╚════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝')
print('\n')

# =======================================================================================================================
# IMPORTAR BIBLIOTECAS
# =======================================================================================================================

import os
import sys
import pandas as pd
import numpy as np
import time
import datetime
from tqdm import tqdm
import logging
import inspect
from tqdm import tqdm
from pandas.tseries.offsets import MonthEnd
from pandas.tseries.offsets import MonthBegin
from tkinter import messagebox
from unidecode import unidecode
import warnings 
pd.options.mode.chained_assignment = None  # default='warn'
warnings.filterwarnings('ignore')

# =======================================================================================================================
# CONFIGURAÇÕES INICIAIS
# =======================================================================================================================

start_time = time.time()
cwd = os.getcwd()
# INPUT DATA - pasta com dados de entrada referentes ao ciclo de planejamento vigente
# Rotina de Atualização: mensal ou conforme necessidade do ciclo
path = 'Input Data/'

# OUTPUT DATA - pasta com dados de saída do script de atualização
# Rotina de Atualização: conforme a execução de scripts
# Os dados nessa pasta se tornam input para o VCM
output_path = 'Output Data/'

# ERROR LOGS - pasta com registros de erros durante a execução do script
# Rotina de Atualização: conforme execução de scripts
# Os arquivos servem para identificação de problemas nos inputs ou topologia
exec_log_path = 'Error Logs/'

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
    
    print(f'══════════════════════════════════════════════ LEFT JOIN ═════════════════════════════════════════════════════')
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
    'unidades_terc':'depUnidadesGerencias.xlsx',
    'demanda':'iptDemandaIrrestrita.xlsx',
    'demanda_sn01':'Demanda',
    'cadastro_produtos':'depSKU.xlsx',
    'cadastro_produtos_sn01':'CADASTRO',
    'cadastro_produtos_sn02':'AGRUPAMENTO',
    'unidades_produtoras':'depUnidadesProdutivas.xlsx',
    'bom':'iptListaTecnica.csv',
    'dicgen':'depDicionarioGenerico.xlsx',
    'template_entrada':'tmpReceitasEntrada.xlsx',
    'template_entrada_sn01':'RENDIMENTO_ENTRADA_PROD',
    'template_saida':'tmpReceitasSaida.xlsx',
    'template_saida_sn01':'RENDIMENTO_SAIDA_PROD',
    'template_entrada':'tmpReceitasEntrada.xlsx',
    'template_entrada_sn01':'RENDIMENTO_ENTRADA_PROD'
}

tp_dado_arquivos = {
    'unidades_terc':{'UNIDADE PRODUTORA':str,'UNIDADE FATURAMENTO':str,'GERENCIA':str,'CONSULTORIA':str},
    'demanda':{'PERIODO':'datetime64[ns]','DIRETORIA':str,'GERENCIA':str,'CONSULTORIA':str,'UNIDADE PRODUTORA':str,
               'CULTURA':str,'GRUPO PRODUTO':str,'PRODUTO':str,'CODIGO PRODUTO':str,
               'RM_PREMIUM_DESCRIPTION_ENG':str,'QUANTIDADE':np.float32,'MP AGRUPADA':str},
    'unidades_produtoras':{'DEPOSITO':str,'PLANTA':str,'DESCRICAO_DEPOSITO':str,'DESCRICAO_PLANTA':str,
                           'TIPO_UNIDADE':str,'UP_MISTURADORA_VCM':str},
    'bom':{'PLANT_CODE':str,'PRODUCTION_SITE':str,'FG_CODE':str,'FINISHED_GOOD':str,
           'RM_CODE':str,'RM_DESCRIPTION':str,'COMPONENT_QTY':np.float64,'UOM_FORMULA':str,
           'RECIPE_CODE':str,'RECIPE_VERSION':str,'PREFERENCE':str,'FORMULA_CODE':str,
           'FORMULA_VERSION':str,'COMPANY_CODE':str},
    'dicgen':{'DE':str,'PARA':str},
    'template_saida':{'Unidade':str,'Receita':str,'Produto':str,'ValorSaida':np.float64},
    'template_entrada':{'Unidade':str,'Receita':str,'Produto':str,'ValorEntrada':np.float64},
    'cadastro_produtos_sn01': {'PRD-VCM':str,'CODIGO_ITEM':str,'DESCRICAO':str,'TIPO_MATERIAL':str,'CATEGORIA':str},
    'cadastro_produtos_sn02': {'COD_ESPECIFICO':str,'DESCRICAO_ESPECIFICA':str,'CODIGO_AGRUPADO':str,
                                'AGRUPAMENTO_MP':str},
}

# =======================================================================================================================
# CARREGAR DATAFRAMES
# =======================================================================================================================

# DataFrame ::  Dicionário Genérico
dicgen = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['dicgen']),
                       sheet_name = arquivos_primarios['dicgen'].split('.')[0],
                       usecols = list(tp_dado_arquivos['dicgen'].keys()),
                       dtype = tp_dado_arquivos['dicgen'])

# DataFrame :: Lista Técnica
bom = pd.read_csv(os.path.join(cwd, path + arquivos_primarios['bom']),
                    usecols = list(tp_dado_arquivos['bom'].keys()),
                    dtype = tp_dado_arquivos['bom'])

bom['PRODUCTION_SITE'] = np.where(bom['PRODUCTION_SITE'] == '1001',
                                  bom['PLANT_CODE'],
                                  bom['PRODUCTION_SITE'])

bom['COMPONENT_QTY'] = np.where(bom['COMPANY_CODE'] == 'ECFHG',
                                bom['COMPONENT_QTY']/1000,
                                bom['COMPONENT_QTY'])

bom['ID'] = bom['PLANT_CODE'] + '-' + bom['PRODUCTION_SITE'] + '-' + bom['FG_CODE']
bom['COMPONENT_QTY'] = bom['COMPONENT_QTY'].round(2)
bom['UOM_FORMULA'] = np.where(bom['UOM_FORMULA'] == 'TO','TN',bom['UOM_FORMULA'])
bom = bom[(bom['UOM_FORMULA'] == 'TN')]

# DataFrame :: Contagem de componentes na Lista Técnica
bom_comp_count = bom.copy()
bom_comp_count = bom_comp_count.groupby(by = ['ID','PLANT_CODE','PRODUCTION_SITE','FG_CODE'])['RM_CODE'].count().reset_index()
bom_comp_count = bom_comp_count.rename(columns={'RM_CODE':'COUNT'})
proxy_comp_count = bom_comp_count.copy()
proxy_comp_count = proxy_comp_count.rename(columns={'PLANT_CODE':'PLANT_CODE_PC','PRODUCTION_SITE':'PRODUCTION_SITE_PC',
                                                    'FG_CODE':'FG_CODE_PC'})

# DataFrame :: Fechamento das Formulações da Lista Técnica
bom_comp_sum = bom.copy()
bom_comp_sum = bom_comp_sum.groupby(by = ['ID','PLANT_CODE','PRODUCTION_SITE','FG_CODE'])['COMPONENT_QTY'].sum().reset_index()
bom_comp_sum = bom_comp_sum.rename(columns = {'COMPONENT_QTY':'TOTAL_COMP'})
proxy_comp_sum = bom_comp_sum.copy()
proxy_comp_sum = proxy_comp_sum.rename(columns={'PLANT_CODE':'PLANT_CODE_PS','PRODUCTION_SITE':'PRODUCTION_SITE_PS','FG_CODE':'FG_CODE_PS'})


# DataFrame :: Criação de uma Lista Técnica Alternativa Irrestrita - Opção 02
# Apenas instaciando o DataFrame para uso posterior
bom_alt = pd.DataFrame()

# DataFrame :: cadastro de materiais :: busca toda a lista de materiais (MP, PI, PF) no cadastrados VCM
cadastro_produtos = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['cadastro_produtos']),
                            sheet_name = arquivos_primarios['cadastro_produtos_sn01'],
                            usecols = list(tp_dado_arquivos['cadastro_produtos_sn01'].keys()),
                            dtype = tp_dado_arquivos['cadastro_produtos_sn01']).applymap(padronizar)

# DataFrame :: cadastro de matérias-primas :: filtro no tipo de material da tabela CADASTRO
cadastro_mp = cadastro_produtos[(cadastro_produtos['TIPO_MATERIAL'].str.split('-',expand=True)[0].str.strip() == 'MP')]
cadastro_pf = cadastro_produtos[(cadastro_produtos['TIPO_MATERIAL'].str.split('-',expand=True)[0].str.strip() == 'PF')]

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

# DataFrame :: Unidades Produtoras relevantes para lista técnica
unidades_produtoras = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['unidades_produtoras']),
                                    sheet_name = arquivos_primarios['unidades_produtoras'].split('.')[0],
                                    usecols = list(tp_dado_arquivos['unidades_produtoras'].keys()),
                                    dtype = tp_dado_arquivos['unidades_produtoras'])

unidades_produtoras = unidades_produtoras.melt(id_vars = list(unidades_produtoras.columns[:-1]),
                                               var_name = 'TIPO_UNIDADE_VCM',
                                               value_name = 'UNIDADE_VCM',
                                               value_vars = list(unidades_produtoras.columns[-1:]))

unidades_produtoras = unidades_produtoras[(unidades_produtoras['UNIDADE_VCM'].notna())]
unidades_produtoras['DEPOSITO'] = np.where(unidades_produtoras['DEPOSITO'] == '1001',
                                           unidades_produtoras['PLANTA'],
                                           unidades_produtoras['DEPOSITO'])

# DataFrame :: Unidades Produtoras por Gerência/Consultoria de Vendas
unidades_terc = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['unidades_terc']),
                              usecols = list(tp_dado_arquivos['unidades_terc'].keys()),
                              dtype = tp_dado_arquivos['unidades_terc'])

unidades_terc['UNIDADE PRODUTORA'] = unidades_terc['UNIDADE PRODUTORA'].replace(list(dicgen['DE']),list(dicgen['PARA']))
unidades_terc['UNIDADE FATURAMENTO'] = unidades_terc['UNIDADE FATURAMENTO'].replace(list(dicgen['DE']),list(dicgen['PARA']))

# DataFrame ::  Arquivo de Demanda Irrestrita
demanda = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['demanda']),
                        sheet_name = arquivos_primarios['demanda_sn01'],
                        usecols = list(tp_dado_arquivos['demanda'].keys()),
                        dtype = tp_dado_arquivos['demanda'])

demanda['UNIDADE PRODUTORA'] = demanda['UNIDADE PRODUTORA'].replace(list(dicgen['DE']),list(dicgen['PARA']))

# DataFrame :: Template de Rendimento de Receitas Saída
validar_data_arquivo(os.path.join(cwd, path + arquivos_primarios['template_saida']))
template_saida = pd.read_excel(os.path.join(cwd,path + arquivos_primarios['template_saida']),
                                sheet_name = arquivos_primarios['template_saida_sn01'],
                                usecols=list(tp_dado_arquivos['template_saida'].keys()),
                                dtype = tp_dado_arquivos['template_saida'])

# DataFrame :: Template de Rendimento de Receitas de Entrada
validar_data_arquivo(os.path.join(cwd, path + arquivos_primarios['template_entrada']))
template_entrada = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['template_entrada']),
                                 sheet_name = arquivos_primarios['template_entrada_sn01'],
                                 usecols = list(tp_dado_arquivos['template_entrada'].keys()),
                                 dtype = tp_dado_arquivos['template_entrada'])

# DataFrame :: Criação de uma Lista Técnica Alternativa com  base em preenchimentos anteriores VCM - Opção 03
bom_alt_vcm = template_saida.copy()
bom_alt_vcm = bom_alt_vcm[(bom_alt_vcm['ValorSaida'] == 1.0)].reset_index().drop(columns='index')
bom_alt_vcm_mp = template_entrada.copy()
bom_alt_vcm_mp = bom_alt_vcm_mp[(bom_alt_vcm_mp['ValorEntrada'] > 0.0)].reset_index().drop(columns='index')
bom_alt_vcm_mp = bom_alt_vcm_mp.rename(columns={'Produto':'MP'})

# =======================================================================================================================
# EXECUÇÃO DE SCRIPTS
# =======================================================================================================================

print('╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗')
print('║                                         >>  EXPLOSÃO DA LISTA TÉCNICA  <<                                      ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ # WIZARD_RENDIMENTO_ENTRADA :: Receitas por MP                                                                 ║')
print('║ # WIZARD_RENDIMENTO_SAIDA :: Receitas por PF                                                                   ║')
print('╚════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝')
print('\n')
print('Iniciando...')
print('Etapa 01: Criando uma matriz identidade de receitas por PF')
template_saida['ValorSaida'] = 0.0
template_saida['Nro. Produto'] = template_saida['Produto'].str[2:]
template_saida['Nro. Produto'] = template_saida['Nro. Produto'].astype(np.int64)
template_saida['Nro. Receita'] = template_saida['Receita'].str[2:]
template_saida['Nro. Receita'] = template_saida['Nro. Receita'].astype(np.int64)
template_saida = template_saida.sort_values(by=['Unidade','Nro. Produto','Nro. Receita'], ascending = True)
template_saida['Proxy MR'] = template_saida['Unidade'] + '-' + template_saida['Receita']
template_saida['Proxy PR'] = template_saida['Unidade'] + '-' + template_saida['Produto']
template_saida = template_saida.reset_index().drop(columns='index')
print('Iniciando looping para preencher matriz...')
for i in tqdm(range(template_saida.shape[0])):
    proxy_mr_find = template_saida['Proxy MR'][i]
    proxy_pr_find = template_saida['Proxy PR'][i]
    proxy_mr_look = template_saida.loc[template_saida.ValorSaida == 1,:]['Proxy MR'][:i].to_list()
    proxy_pr_look = template_saida.loc[template_saida.ValorSaida == 1,:]['Proxy PR'][:i].to_list()
    if proxy_mr_find not in proxy_mr_look and proxy_pr_find not in proxy_pr_look:
         template_saida['ValorSaida'][i] = 1.0
               
    else:
         template_saida['ValorSaida'][i] = 0.0
print('Matriz de Produtos Acabados preenchida com sucesso!')
template_saida.to_excel(os.path.join(cwd,output_path + 'WIZARD_RENDIMENTO_SAIDA.xlsx'), index = False, sheet_name = 'RENDIMENTO_SAIDA_PROD')
template_saida = template_saida[(template_saida['ValorSaida'] == 1.0)][['Proxy PR','Receita']]

print('\nEstratégia 01 :: Avaliando balanceamento da lista técnica')
print('Contagem de componentes...')
left_outer_join(bom, proxy_comp_count, left_on = 'ID', right_on = 'ID')
print('Fechamento das fórmulas...')
left_outer_join(bom, proxy_comp_sum, left_on = 'ID', right_on = 'ID')
# A regra para definir o balanceamento da fórmula pode ser inserido aqui
bom['STATUS'] = np.where(bom['TOTAL_COMP'] < 1.0,
                         'Desbalanceada','Balanceada')
bom_alt = bom.copy()
bom = bom[(bom['STATUS'] == 'Balanceada')]
proxy_bom = bom.copy()
proxy_bom['ID'] = proxy_bom['FG_CODE']
proxy_bom = proxy_bom.groupby(by = ['FORMULA_CODE','FG_CODE','FINISHED_GOOD','RM_CODE','RM_DESCRIPTION'])['COMPONENT_QTY'].sum().reset_index()
proxy_bom = proxy_bom.sort_values(by=['FG_CODE','FORMULA_CODE'], ascending = True).reset_index().drop(columns='index')
proxy_bom['ACC_QTY'] = 0.0
fg_list = []
lt = False
fechamento = 0.0
for i in range(proxy_bom.shape[0]):
    if i == 0:
        proxy_bom['ACC_QTY'][i] = proxy_bom['COMPONENT_QTY'][i]
        fechamento = proxy_bom['ACC_QTY'][i]
    elif fechamento + proxy_bom['COMPONENT_QTY'][i] <= 1.0 and proxy_bom['FG_CODE'][i] in fg_list and lt == False:
        proxy_bom['ACC_QTY'][i] = fechamento + proxy_bom['COMPONENT_QTY'][i]
        fechamento = proxy_bom['ACC_QTY'][i]
    elif (fechamento + proxy_bom['COMPONENT_QTY'][i] > 1.0 or lt == True) and proxy_bom['FG_CODE'][i] in fg_list:
        lt = True
        proxy_bom['ACC_QTY'][i] = 0.0
        fechamento = proxy_bom['ACC_QTY'][i]
    else:
        fg_list.append(proxy_bom['FG_CODE'][i])
        proxy_bom['ACC_QTY'][i] = proxy_bom['COMPONENT_QTY'][i]
        fechamento = proxy_bom['ACC_QTY'][i]
        lt = False

proxy_bom = proxy_bom[(proxy_bom['ACC_QTY'] > 0.0)]
proxy_bom['COMPONENT_QTY'] = proxy_bom['COMPONENT_QTY'].round(2)
print('\nEstratégia 02 :: Avaliando balanceamento alternativo de SKUs')
bom_alt = bom_alt[(bom_alt['STATUS'] == 'Desbalanceada')]
# Guardar um report com todas as fórmulas desbalanceadas
bom_alt.to_excel(os.path.join(cwd,output_path + 'bom_alt.xlsx'))
bom_alt = bom_alt.sort_values(by=['FG_CODE','FORMULA_CODE'], ascending = True)
bom_alt = bom_alt[['PLANT_CODE','PRODUCTION_SITE','FG_CODE','FINISHED_GOOD']]
bom_alt = bom_alt.drop_duplicates().reset_index().drop(columns='index')
print('Obtendo lista técnica alternativa...')
left_outer_join(bom_alt, proxy_bom, left_on = 'FG_CODE', right_on = 'FG_CODE')
print('Eliminando valores vazios após a mesclagem...')
bom_alt = bom_alt.dropna(subset='FORMULA_CODE').reset_index().drop(columns='index')
bom_alt = bom_alt.sort_values(by=['FG_CODE','FORMULA_CODE'], ascending=True)
fg_list = []
lt = False
fechamento = 0.0
bom_alt['ACC_QTY'] = 0.0
for i in tqdm(range(bom_alt.shape[0])):
    if i == 0:
        bom_alt['ACC_QTY'][i] = bom_alt['COMPONENT_QTY'][i]
        fechamento = bom_alt['ACC_QTY'][i]
        fg_list.append(bom_alt['FG_CODE'][i])
    elif fechamento + bom_alt['COMPONENT_QTY'][i] <= 1.0 and bom_alt['FG_CODE'][i] in fg_list and lt == False:
        bom_alt['ACC_QTY'][i] = fechamento + bom_alt['COMPONENT_QTY'][i]
        fechamento = bom_alt['ACC_QTY'][i]
    elif (fechamento + bom_alt['COMPONENT_QTY'][i] > 1.0 or lt == True ) and bom_alt['FG_CODE'][i] in fg_list:
        lt = True
        bom_alt['ACC_QTY'][i] = 0.0
        fechamento = bom_alt['ACC_QTY'][i]
    else: 
        fg_list.append(bom_alt['FG_CODE'][i])
        bom_alt['ACC_QTY'][i] = bom_alt['COMPONENT_QTY'][i]
        fechamento = bom_alt['ACC_QTY'][i]
        lt = False  

proxy_bom.to_excel(os.path.join(cwd,output_path+'proxy_bom.xlsx'))
bom_alt_sum = bom_alt.copy()
bom_alt_sum = bom_alt_sum.groupby(by=['PLANT_CODE','PRODUCTION_SITE','FG_CODE'])['COMPONENT_QTY'].sum().reset_index()
bom_alt_sum['COMPONENT_QTY'] = bom_alt_sum['COMPONENT_QTY'].round(2)
bom_alt_sum = bom_alt_sum[(bom_alt_sum['COMPONENT_QTY'] == 1)]
bom_alt_sum['CHECK']= 'X'
bom_alt_sum['ID'] = bom_alt_sum['PLANT_CODE'] + '-' + bom_alt_sum['PRODUCTION_SITE'] + '-' + bom_alt_sum['FG_CODE']
bom_alt_sum = bom_alt_sum[['ID','CHECK']]
bom_alt['ID'] = bom_alt['PLANT_CODE'] + '-' + bom_alt['PRODUCTION_SITE'] + '-' + bom_alt['FG_CODE']
left_outer_join(bom_alt, bom_alt_sum, left_on='ID',right_on='ID')
bom_alt = bom_alt[(bom_alt['CHECK'] == 'X')].drop(columns='CHECK')
bom_alt = bom_alt[(bom_alt.ACC_QTY > 0.0)]
bom_alt = bom_alt.sort_values(by=['FG_CODE','FORMULA_CODE','ACC_QTY']).drop(columns='ACC_QTY')
bom_alt['TOTAL_COMP'] = 1.0
bom_alt['STATUS'] = 'Balanceada BASE-PF'
columns = ['PLANT_CODE','PRODUCTION_SITE','FG_CODE','FINISHED_GOOD','RM_CODE','RM_DESCRIPTION','COMPONENT_QTY','FORMULA_CODE','STATUS']
bom = bom[columns]
bom_alt = bom_alt.rename(columns={'FINISHED_GOOD_y':'FINISHED_GOOD'})[columns]
bom = pd.concat([bom, bom_alt])
bom = bom.reset_index().drop(columns='index')
left_outer_join(bom, agrupamento_produtos, left_on = 'FG_CODE', right_on = 'COD_ESPECIFICO')
left_outer_join(bom, cadastro_pf, left_on = 'CODIGO_AGRUPADO', right_on = 'CODIGO_ITEM')

# Estatística de quantidade de valores vazios em relação ao total de linhas
bom.loc[bom['PRD-VCM'].isna()].shape[0]/bom.shape[0]

columns = ['PLANT_CODE', 'PRODUCTION_SITE', 'PRD-VCM','FG_CODE', 'FINISHED_GOOD', 
           'RM_CODE','RM_DESCRIPTION', 'COMPONENT_QTY', 'FORMULA_CODE', 'STATUS']
bom = bom[columns].rename(columns={'PRD-VCM':'PF-VCM'})
left_outer_join(bom,agrupamento_produtos,left_on = 'RM_CODE', right_on = 'COD_ESPECIFICO')
left_outer_join(bom, cadastro_mp, left_on = 'CODIGO_AGRUPADO', right_on = 'CODIGO_ITEM')
columns = ['PLANT_CODE', 'PRODUCTION_SITE', 'PF-VCM','FG_CODE', 'FINISHED_GOOD', 
           'PRD-VCM','RM_CODE','RM_DESCRIPTION', 'COMPONENT_QTY', 'FORMULA_CODE', 'STATUS']
bom = bom[columns].rename(columns={'PRD-VCM':'MP-VCM'})
left_outer_join(bom, unidades_produtoras, left_on = ['PLANT_CODE','PRODUCTION_SITE'], right_on = ['PLANTA','DEPOSITO'])
columns = ['UNIDADE_VCM','PLANT_CODE', 'PRODUCTION_SITE', 'DESCRICAO_PLANTA', 'DESCRICAO_DEPOSITO', 'PF-VCM', 'FG_CODE',
           'FINISHED_GOOD', 'MP-VCM', 'RM_CODE', 'RM_DESCRIPTION', 'COMPONENT_QTY', 'FORMULA_CODE', 'STATUS']
bom = bom[columns]
bom = bom.sort_values(by=['PLANT_CODE','FG_CODE','FORMULA_CODE'])
# Lista Técnica conciliada para o VCM
bom.to_excel(os.path.join(cwd,exec_log_path + 'Lista Técnica Completa.xlsx'))
bom_to_vcm = bom.copy()
bom_to_vcm = bom_to_vcm.dropna(subset=['UNIDADE_VCM','PF-VCM','MP-VCM'])
bom_to_vcm = bom_to_vcm.groupby(by=['UNIDADE_VCM','PF-VCM','MP-VCM','FORMULA_CODE'])['COMPONENT_QTY'].sum().reset_index()
bom_to_vcm = bom_to_vcm.sort_values(by=['UNIDADE_VCM','PF-VCM','FORMULA_CODE']).reset_index().drop(columns='index')
bom_to_vcm_sum = bom_to_vcm.copy()
bom_to_vcm_sum = bom_to_vcm_sum.groupby(by=['UNIDADE_VCM','PF-VCM','FORMULA_CODE'])['COMPONENT_QTY'].sum().reset_index()
bom_to_vcm_sum = bom_to_vcm_sum[(bom_to_vcm_sum['COMPONENT_QTY'] == 1.0)]
# A medida com base no DataFrame bom_to_vcm_sum não foi utilizada, mas fica disponível caso seja necessária
control_list = []
lt = False
fechamento = 0.0
bom_to_vcm['ACC_QTY'] = 0.0
for i in tqdm(range(bom_to_vcm.shape[0])):
    if i == 0:
        bom_to_vcm['ACC_QTY'][i] = bom_to_vcm['COMPONENT_QTY'][i]
        fechamento = bom_to_vcm['ACC_QTY'][i]
        control_list.append(bom_to_vcm['UNIDADE_VCM'][i] + '-' + bom_to_vcm['PF-VCM'][i])
    elif fechamento + bom_to_vcm['COMPONENT_QTY'][i] <= 1.0 and bom_to_vcm['UNIDADE_VCM'][i] + '-' + bom_to_vcm['PF-VCM'][i] in control_list and lt == False:
        bom_to_vcm['ACC_QTY'][i] = fechamento + bom_to_vcm['COMPONENT_QTY'][i]
        fechamento = bom_to_vcm['ACC_QTY'][i]
    elif (fechamento + bom_to_vcm['COMPONENT_QTY'][i] > 1.0 or lt == True ) and bom_to_vcm['UNIDADE_VCM'][i] + '-' + bom_to_vcm['PF-VCM'][i] in control_list:
        lt = True
        bom_to_vcm['ACC_QTY'][i] = 0.0
        fechamento = bom_to_vcm['ACC_QTY'][i]
    else: 
        control_list.append(bom_to_vcm['UNIDADE_VCM'][i] + '-' + bom_to_vcm['PF-VCM'][i])
        bom_to_vcm['ACC_QTY'][i] = bom_to_vcm['COMPONENT_QTY'][i]
        fechamento = bom_to_vcm['ACC_QTY'][i]
        lt = False  
bom_to_vcm.to_excel(os.path.join(cwd,exec_log_path+'bom_to_vcm.xlsx'))
bom_to_vcm = bom_to_vcm[(bom_to_vcm['ACC_QTY'] > 0.0)]
bom_to_vcm['Proxy PR'] = bom_to_vcm['UNIDADE_VCM'] + '-' + bom_to_vcm['PF-VCM']
left_outer_join(bom_to_vcm, template_saida, left_on = 'Proxy PR', right_on = 'Proxy PR')
left_outer_join(template_entrada, bom_to_vcm, left_on = ['Unidade','Receita'], right_on = ['UNIDADE_VCM','Receita'])
template_entrada = template_entrada[['Proxy PR','Unidade', 'Receita', 'Produto', 'ValorEntrada',
                                     'MP-VCM', 'FORMULA_CODE', 'COMPONENT_QTY']]
print('\nEstratégia 03 :: Criando uma lista técnica alternativa baseada em ciclos anteiores...')
left_outer_join(bom_alt_vcm,bom_alt_vcm_mp, left_on=['Unidade','Receita'], right_on=['Unidade','Receita'])
bom_alt_vcm['Produto'] = bom_alt_vcm['MP']
bom_alt_vcm = bom_alt_vcm.drop(columns=['ValorSaida','MP']).rename(columns={'ValorEntrada':'COMPONENT_QTY_NV2'})
left_outer_join(template_entrada, bom_alt_vcm, left_on = ['Unidade','Receita','Produto'], right_on = ['Unidade','Receita','Produto'])
template_entrada_sum = template_entrada.copy()
template_entrada_sum = template_entrada_sum.groupby(by=['Unidade','Receita'])['COMPONENT_QTY'].sum().reset_index()
template_entrada_sum = template_entrada_sum.rename(columns={'COMPONENT_QTY':'FECHAMENTO'})
template_entrada_sum = template_entrada_sum[(template_entrada_sum['FECHAMENTO'] == 1.0)]
left_outer_join(template_entrada, template_entrada_sum, left_on = ['Unidade','Receita'], right_on = ['Unidade','Receita'])

for i in tqdm(range(template_entrada.shape[0])):
    if template_entrada['FECHAMENTO'][i] == 1.0 and template_entrada['MP-VCM'][i] == template_entrada['Produto'][i]:
        template_entrada['ValorEntrada'][i] = template_entrada['COMPONENT_QTY'][i]
    elif template_entrada['FECHAMENTO'][i] < 1.0 and template_entrada['COMPONENT_QTY_NV2'][i] > 0.0:
        template_entrada['ValorEntrada'][i] = template_entrada['COMPONENT_QTY_NV2'][i]
    else:
        template_entrada['ValorEntrada'][i] = 0.0

template_entrada = template_entrada[['Unidade','Receita','Produto','ValorEntrada']]
template_entrada.to_excel(os.path.join(cwd,output_path + 'WIZARD_RENDIMENTO_ENTRADA.xlsx'), index = False, sheet_name = 'RENDIMENTO_ENTRADA_PROD')
print('\n')
print('╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗')
print('║                                           ATUALIZACAO DE DADOS - VCM                                           ║')
print('║                                             >>  fixed_price.py  <<                                             ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ Criado por:    Isabela Nunes dos Santos        Data: 11/04/2025                                                ║')
print('║ Editado por:   Isabela Nunes dos Santos        Data: 23/04/2025                                                ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ CHANGELOG:                                                                                                     ║')
print('║ - v1.0.0 (23/03/2025): Criação da primeira versão do script unificado com edições estruturais nos arquivos     ║')
print('║                        de depara e dado primário.                                                              ║')
print('║                                                                                                                ║')
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
     'localizacao': 'depGeolocalizacao.xlsx',
     'localizacao_sn': 'depGeolocalizacao',
     'cadastro_produtos': 'depSKU.xlsx',
     'cadastro_produtos_sn': 'CADASTRO',     
     'agrupamento_sn':'AGRUPAMENTO',
     'up_correntes':'iptUpdateCorrentes.xlsx',
     'up_correntes_sn': 'iptUpdateCorrentes',
     'lista_preco' : 'iptListaPreco.xlsx',
     'lista_preco_sn' : 'iptListaPreco',
     'unidades_rec_mov' : 'depAtvcBalancosFin.xlsx',
     'unidades_rec_mov_sn' : 'depAtvcBalancosFin',
     'fretes': 'iptTabelaFretes.xlsx',
     'fretes_sn': 'iptTabelaFretes',
     'template_preco': 'tmpPrecoExato.xlsx',
}

tp_dado_arquivos = {
     'periodos':{'NUMERO':np.int64,'PERIODO':'datetime64[ns]', 'NOME_PERIODO':str},
     'localizacao': {'Unidade':str, 'Estado':str, 'Município':str},
     'cadastro_produtos': {'PRD-VCM':str,'CODIGO_ITEM':str,'DESCRICAO':str, 'TIPO_MATERIAL':str, 'CATEGORIA':str},
     'agrupamento': {'COD_ESPECIFICO':str, 'CODIGO_AGRUPADO':str},
     'up_correntes': {'ConjuntoCorrentes':str, 'Unidade-Origem':str, 'Unidade-Destino':str, 'Tipo':str},
     'lista_preco': {'DATA':'datetime64[ns]', 'DH_INICIAL':'datetime64[ns]', 'DH_FINAL':'datetime64[ns]', 'LISTA':str, 
                     'FILIAL':str, 'ITEM':str, 'DESCRICAO':str, 'MOEDA':str, 'PTAX':str, 'PRECO':str},
     'unidades_rec_mov': {'UNIDADE':str, 'DESC_UNIDADE':str},
     'fretes': {'data_inicio':'datetime64[ns]','rota':str,'Valor':'float64', },
     'template_preco': {'Unidade':str, 'Produto':str, 'Periodo':str, 'Preço Exato':np.float64},
}

rename_dataframes = {
    'df_periodos':{'NOME_PERIODO':'Periodo_VCM', 'PERIODO':'Nome'},
    'df_pontos_venda':{'UNIDADE':'Origem','DESC_UNIDADE':'Origem EC'},
    'df_valor_venda' :{'ITEM':'Código do Produto', 'PRECO':'Preço', 'PTAX':'Ptax USD', 'DH_INICIAL':'Data Inicio',
                       'DH_FINAL':'Data fim', 'MOEDA':'Moeda', 'LISTA':'Nome da Lista'},
}


# =======================================================================================================================
# CARREGAR DATAFRAMES
# =======================================================================================================================
print('Carregando arquivos... \n')
#print('Tempo de execução esperado: por volta de 1 min \n')

# DataFrame :: Horizonte (Período) de Otimização
df_periodos = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['periodos']),
                         usecols=list(tp_dado_arquivos['periodos'].keys()),
                         dtype=tp_dado_arquivos['periodos'])
df_periodos = df_periodos.rename(columns=rename_dataframes['df_periodos'])

# DataFrame :: Geolocalizacao
df_localizacao = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['localizacao']),
                         sheet_name= arquivos_primarios['localizacao_sn'], 
                       usecols=list(tp_dado_arquivos['localizacao'].keys()),
                       dtype=tp_dado_arquivos['localizacao']).applymap(padronizar)

# Dataframe :: Cadastro Produtos
df_produtos = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['cadastro_produtos']),
                                  sheet_name = arquivos_primarios['cadastro_produtos_sn'],
                                  usecols = list(tp_dado_arquivos['cadastro_produtos'].keys()),
                                  dtype = tp_dado_arquivos['cadastro_produtos'])

# Dataframe :: Agrupamento
agrupamento_pf = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['cadastro_produtos']),
                                  sheet_name = arquivos_primarios['agrupamento_sn'],
                                  usecols = list(tp_dado_arquivos['agrupamento'].keys()),
                                  dtype = tp_dado_arquivos['agrupamento'])

# DataFrame :: Update de Correntes
df_correntes = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['up_correntes']),
                         sheet_name= arquivos_primarios['up_correntes_sn'], 
                       usecols=list(tp_dado_arquivos['up_correntes'].keys()),
                       dtype=tp_dado_arquivos['up_correntes']).rename(columns = {'ConjuntoCorrentes':'Corrente',\
                                            'Unidade-Origem':'Origem', 'Unidade-Destino':'Destino'}).applymap(padronizar)
unidades_interesse = df_correntes.copy()

# DataFrame :: Lista Preço
df_valor_venda = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['lista_preco']),
                         sheet_name= arquivos_primarios['lista_preco_sn'], 
                       usecols=list(tp_dado_arquivos['lista_preco'].keys()),
                       dtype=tp_dado_arquivos['lista_preco']).applymap(padronizar)
df_valor_venda = df_valor_venda.rename(columns=rename_dataframes['df_valor_venda'])

# DataFrame :: Unidades Receita Movimentação
df_pontos_venda = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['unidades_rec_mov']),
                         sheet_name= arquivos_primarios['unidades_rec_mov_sn'], 
                       usecols=list(tp_dado_arquivos['unidades_rec_mov'].keys()),
                       dtype=tp_dado_arquivos['unidades_rec_mov']).applymap(padronizar)
df_pontos_venda = df_pontos_venda.rename(columns=rename_dataframes['df_pontos_venda'])

# DataFrame :: Dados de Frete
df_fretes = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['fretes']),
                       usecols=list(tp_dado_arquivos['fretes'].keys()),
                       dtype=tp_dado_arquivos['fretes'])

# DataFrame :: Template Preço Exato
validar_data_arquivo(os.path.join(cwd, path + arquivos_primarios['template_preco']))
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
agrupamento_pf = agrupamento_pf.drop_duplicates(subset = 'COD_ESPECIFICO')
df_valor_venda = df_valor_venda.merge(agrupamento_pf, how = 'left', left_on = 'Código do Produto', right_on = 'COD_ESPECIFICO')
df_valor_venda.rename(columns = {"CODIGO_AGRUPADO": "CODIGO_ITEM"}, inplace=True)

df_valor_venda = df_valor_venda.astype({'Ptax USD':np.float32,'Preço':np.float32})
df_valor_venda = df_valor_venda.merge(df_produtos[["CODIGO_ITEM", "PRD-VCM"]], on = "CODIGO_ITEM", how="inner")

# Convertendo de dólares para reais quando necessário
df_valor_venda["Preço Venda"] = np.where(df_valor_venda["Moeda"] == "BRL", df_valor_venda["Preço"],
                                         df_valor_venda["Preço"] * df_valor_venda["Ptax USD"])

# (22/01/2025) Juntando o UpdateCorrentes com o UnidadesRecMov para descobrir o nome da lista,
# a partir disso, juntar o UnidadesRecMov com o novo template. :)
df_pontos_venda = df_pontos_venda.merge(df_correntes, on = "Origem", how = "left")
# 14/05/2024 - Incluindo uma etapa de exclusão de NaN
df_pontos_venda = df_pontos_venda.dropna()
df_corrente_mc = df_corrente_mc.merge(df_pontos_venda, how='left', left_on='Unidade', right_on='Destino')
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
df_receita_movimentacao = df_corrente_mc.merge(df_valor_venda, on = ["Desc. Empresa", "PRD-VCM","Periodo_VCM"], how = "left")
df_receita_movimentacao["Preço Venda"] = (df_receita_movimentacao["Preço Venda"].fillna(0))
# Trazendo preços médios para quando não houver preços específicos
df_receita_movimentacao = df_receita_movimentacao.merge(valor_venda_medio, on = ["PRD-VCM"], how = "left")
df_receita_movimentacao["Preço Venda Médio"] = (df_receita_movimentacao["Preço Venda Médio"].fillna(0))
# Classificação de produtos para eliminar o que não é produto final
df_receita_movimentacao = df_receita_movimentacao.merge(df_produtos[["PRD-VCM", "TIPO_MATERIAL"]],
                                                        on = "PRD-VCM", how = "left")


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
df_fretes = df_fretes.merge(periodos_fretes, how = 'left', left_on = 'data_inicio', right_on = 'Data_Inicio')
df_fretes = df_fretes.dropna()

left_outer_join(unidades_interesse,depara_localizacao,left_on='Origem', right_on='Unidade')
unidades_interesse = unidades_interesse[['Corrente','Origem','Destino','Unidade','ID Origem-Destino']]
unidades_interesse = unidades_interesse.rename(columns={'ID Origem-Destino':'Cidade (Origem)'})
left_outer_join(unidades_interesse,depara_localizacao,left_on='Destino', right_on='Unidade')
unidades_interesse = unidades_interesse[['Corrente','Origem','Destino','Cidade (Origem)','ID Origem-Destino']]
unidades_interesse = unidades_interesse.rename(columns = {'ID Origem-Destino':'Cidade (Destino)'})

unidades_interesse = unidades_interesse.merge(periodos_fretes, how = 'cross')
unidades_interesse['ID-LEFT'] = unidades_interesse['Cidade (Origem)'] + '-' + unidades_interesse['Cidade (Destino)'] + '-' + unidades_interesse['Periodo_VCM']
df_fretes['Origem'] = df_fretes['rota'].str.split(' x ').str[0]
df_fretes['Destino'] = df_fretes['rota'].str.split(' x ').str[1]
df_fretes['ID-RIGHT'] = df_fretes['Origem'] + '-' + df_fretes['Destino'] + '-' + df_fretes['Periodo_VCM']
df_fretes = df_fretes.groupby(by = ['Periodo_VCM','Data_Inicio','rota','Origem','Destino','ID-RIGHT'])['Valor'].mean()
df_fretes = df_fretes.reset_index()
unidades_interesse = unidades_interesse.merge(df_fretes, how = 'left', left_on = 'ID-LEFT', right_on = 'ID-RIGHT')
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

unidades_interesse = unidades_interesse.merge(media, how='left', left_on='Unidade-Destino', right_on='UnidadeMedia')
unidades_interesse['ValorMedia'] = unidades_interesse['ValorMedia'].fillna(0.0)
unidades_interesse['Valor'] = unidades_interesse.apply(lambda x: x['ValorMedia'] if x['Valor']==0.0 and x['ValorMedia']!=0.0 else x['Valor'], axis=1)
unidades_interesse = unidades_interesse.drop(columns={'UnidadeMedia','ValorMedia'})
df_receita_movimentacao_periodos = df_receita_movimentacao_periodos.merge(unidades_interesse, how='left',
                                        left_on=['Unidade', 'Periodo'], right_on=['Unidade-Destino', 'PeriodoFrete'])
df_receita_movimentacao_periodos['Preço Exato'] = df_receita_movimentacao_periodos.apply(lambda x: x['Preço Exato'] + x['Valor']
                                                                                    if x['Preço Exato']!=0.0 else x['Preço Exato'], axis=1)
df_receita_movimentacao_periodos = df_receita_movimentacao_periodos.round({'Preço Exato':2})

# Comparação com o template gerado pelo VCM
df_template_rmov.drop(labels="Preço Exato", axis=1, inplace=True)
left_outer_join(df_template_rmov,df_receita_movimentacao_periodos,left_on=["Unidade", "Produto", "Periodo"],
                right_on=["Unidade", "Produto", "Periodo"])
df_template_rmov.fillna(0.0, inplace = True)
df_template_rmov = df_template_rmov.drop(columns={'Unidade-Destino', 'PeriodoFrete', 'Valor'})

# 12/04/2024: Alterando enconding para utf-8 como alinhado com o time da OP2B
df_template_rmov.to_csv(os.path.join(cwd,output_path + "Precos Fixos Para VCM.csv"),
                                          sep = ';', encoding = 'utf-8-sig', index = False)

print('\nAtualização de Preços Fixos finalizada!')
end_time = time.time()
print(f'Tempo de Execução: {round(end_time - start_time,2)} segundos')
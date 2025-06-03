import numpy as np

# =======================================================================================================================
# DEFINIR ARQUIVOS
# =======================================================================================================================

arquivos_primarios = {
     'dicgen':'depDicionarioGenerico.xlsx',
     'periodos': 'iptPeriodos.xlsx',
     'periodos_sn': 'Períodos de Otimização',
     'cadastro_produtos': 'depSKU.xlsx',
     'cadastro_produtos_sn01':'CADASTRO',
     'cadastro_produtos_sn02':'AGRUPAMENTO',
     'portos': 'depUnidadesPortuarias.xlsx',
     'df_revisao_importada':'iptComprasImportadas.xlsx',
     'df_revisao_nacional':'iptComprasNacionais.xlsx',
     'wizard_suprimento_faixa':'tmpSuprimentoFaixa.xlsx',
     'wizard_suprimento_faixa_sn01':'SUPRIMENTO_FAIXA',
     'capacidade_portos':'iptCapacidadePortuaria.xlsx',
     'cap_op_portos':'iptCapOperPorto.xlsx',
     'correntes':'iptUpdateCorrentes.xlsx',
     # Os nomes de tabela abaixo foram agrupados pela redução dos arquivos primários
     'cap_prod': 'iptCapOperPlantas.xlsx',
     'cap_prod_sn': 'iptCapOperPlantas',
     'cap_desc' : 'iptCapOperPlantas.xlsx',
     'cap_desc_sn' : 'iptCapOperPlantas',
     # Unidades Armazenagem / Produtivas / Expedição
     'unidades_exp':'depUnidadesProdutivas.xlsx',
     'unidades_exp_sn': 'depUnidadesProdutivas',
     'cap_portos' : 'iptCapacidadePortuaria.xlsx',
     'cap_portos_sn' : 'iptCapacidadePortuaria',
     'template_saida' : 'tmpSaida.csv',
     'template_entrada' : 'tmpEntrada.csv',
     'template_capacidade' : 'tmpCapacidadeArmazenagem.xlsx',
     'unidades_por':'depUnidadesPortuarias.xlsx',
     'unidades_por_sn':'depUnidadesPortuarias',
     'arq_tbDeparaMercadoConsumidor': 'depEstruturaComercial.xlsx',
     'arq_demanda_irrestrita': 'iptDemandaIrrestrita.xlsx',
     'arq_demanda_irrestrita_sn01': 'Demanda',     
     'arq_RendEntr': 'WIZARD_RENDIMENTO_ENTRADA.xlsx',
     'arq_RendEntr_sn01': 'RENDIMENTO_ENTRADA_PROD',
     'arq_RendSaida': 'WIZARD_RENDIMENTO_SAIDA.xlsx',
     'arq_RendSaida_sn01': 'RENDIMENTO_SAIDA_PROD',   
     'arq_resultados_vcm': 'Resultados.xlsx',
     'arq_resultados_vcm_sn01': 'RESULTADOS',    
     'arq_tbUpdateCorrentes': 'iptUpdateCorrentes.xlsx',        
     # Identificar onde usa os elementas abaixo para deprecar
     'portos_armz_apo': 'depUnidadesPortuarias.xlsx',
     'portos_armz_apo_sn': 'depUnidadesPortuarias',
}

tp_dado_arquivos = {
     'dicgen':{'DE':str,'PARA':str},
     'periodos':{'NUMERO':str,'PERIODO':'datetime64[ns]', 'NOME_PERIODO':str},
     'cadastro_produtos_sn01': {'PRD-VCM':str,'CODIGO_ITEM':str,'DESCRICAO':str,'TIPO_MATERIAL':str,'CATEGORIA':str},
     'cadastro_produtos_sn02': {'COD_ESPECIFICO':str,'DESCRICAO_ESPECIFICA':str,'CODIGO_AGRUPADO':str,
                                'AGRUPAMENTO_MP':str},
     'portos': {'NOME_PORTO_VCM':str,'NOME_AZ_PORTO_VCM':str,'PORTO':str,'UNIDADE':str,'CORRENTE':str},
     'df_revisao_importada':{'Porto':str,'Fábrica':str,'Matéria-prima':str,'Mês Entrega':'datetime64[ns]',
                   'BALANCE (TONS)':np.float32,'Status':str,'COMPANY':str,'RAW MATERIAL COD.':str},
     'df_revisao_nacional':{'Porto':str,'Fábrica':str,'Matéria-prima':str,'Status':str,'COMPANY':str,
                            'RAW MATERIAL COD.':str,'Mês000':np.float32,'Mês001':np.float32,'Mês002':np.float32,
                            'Mês003':np.float32,'Mês004':np.float32,'Mês005':np.float32,'Mês006':np.float32,
                            'Mês007':np.float32,'Mês008':np.float32,'Mês009':np.float32,'Mês010':np.float32,
                            'Mês011':np.float32,'Mês012':np.float32},
     'wizard_suprimento_faixa':{'Unidade':str,'Produto':str,'Periodo':str,
                                'Suprimento Mínimo':str,'Suprimento Máximo':str},
     'capacidade_portos':{'PERIODO':'datetime64[ns]'},
     'cap_op_portos':{'Porto':str,'Origem':str,'Destino':str,'Variável':str},
     'correntes':{'ConjuntoCorrentes':str,'Unidade-Origem':str,'Unidade-Destino':str,'Tipo':str},
     'cap_prod': {'Unidade':str, 'Nome Unidade':str, 'Dt/Ref':'datetime64[ns]', 'Quantidade':np.float64,'Agrupador':str,'Local':str},
     'cap_desc': {'Unidade':str, 'Nome Unidade':str, 'Dt/Ref':'datetime64[ns]', 'Quantidade':np.float64,'Agrupador':str,'Local':str},
     'cap_portos': {'PERIODO':'datetime64[ns]', '__default__':np.float64},
     'unidades_exp': {'DEPOSITO':'str','PLANTA':str, 'DESCRICAO_DEPOSITO':str, 'DESCRICAO_PLANTA':str, 'TIPO_UNIDADE':'str',
                      'UNIDADE_ARMAZENAGEM_VCM':str,'UP_MISTURADORA_VCM':str,'UNIDADE_EXPEDICAO_VCM':str},
     'unidades_por': {'NOME_PORTO_VCM':str, 'NOME_AZ_PORTO_VCM':str, 'PORTO':str, 'UNIDADE':str, 'CORRENTE':str},
     'template_saida': {'Unidade':str, 'Periodo':str, 'Limite':str, 'Ativo':str},
     'template_entrada' : {'Unidade':str, 'Periodo':str, 'Limite':str, 'Ativo':str},
     'template_capacidade' : {'Unidade':str, 'Periodo':str, 'Volume Mínimo':np.int64, 'Volume Máximo':np.int64},
     'arq_tbDeparaMercadoConsumidor':{'DIRETORIA':str,'GERENCIA':str,'CONSULTORIA':str,'CENTROID':str,'UF':str,'VCM':str},
     'arq_demanda_irrestrita':{'PERIODO':'datetime64[ns]','DIRETORIA':str,'GERENCIA':str,'CONSULTORIA':str,'UNIDADE PRODUTORA':str,
               'CULTURA':str,'GRUPO PRODUTO':str,'PRODUTO':str,'CODIGO PRODUTO':str,
               'RM_PREMIUM_DESCRIPTION_ENG':str,'QUANTIDADE':np.float32,'MP AGRUPADA':str},
     'arq_RendSaida':{'Unidade':str, 'Receita':str, 'Produto':str, 'ValorSaida':np.float64},
     'arq_RendEntr':{'Unidade':str, 'Receita':str, 'Produto':str, 'ValorEntrada':np.float64},
     'arq_resultados_vcm':{'Corrente-VCM':str,'Produto-VCM':str,'Período-VCM':str, 'Quantidade':np.float64,
                          'Unidade-Origem-VCM':str,'Unidade-Destino-VCM':str,'Corredor':str},    
     'arq_tbUpdateCorrentes':{'ConjuntoCorrentes':str, 'Unidade-Origem':str, 'Unidade-Destino':str, 'Tipo':str}    
}

rename_dataframes = {
    'df_revisao_importada':{'Porto':'PORTO','Fábrica':'PLANTA','Matéria-prima':'MP','Mês Entrega':'DT_REMESSA',
                   'BALANCE (TONS)':'BALANCE_TONS','Status':'STATUS','COMPANY':'COMPANY','RAW MATERIAL COD.':'CODIGO_MP'},
    'df_revisao_nacional':{'Porto':'PORTO','Fábrica':'PLANTA','Matéria-prima':'MP','Status':'STATUS','COMPANY':'COMPANY',
                            'RAW MATERIAL COD.':'CODIGO_MP'},
    'df_periodos':{'NUMERO':'Numero','NOME_PERIODO':'Nome VCM', 'PERIODO':'Nome'}
}
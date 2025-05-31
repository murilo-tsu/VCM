#╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗')
#║                                     MÓDULOS DE SUPORTE EXECUÇÃO DE SCRIPTS                                     ║')
#║                                     ======================================                                     ║')
#╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
#║ Criado  por: Murilo Lima Ribeiro  Data: 30/05/2025                                                             ║')
#║ Editado por: Murilo Lima Ribeiro  Data: 30/05/2025                                                             ║')
#╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
#║ CHANGELOG:                                                                                                     ║')
#║ - v1.0.0 (30/05/2025): Criação da primeira versão do script de módulos auxiliares de execução dos scripts      ║')
#║                                                                                                                ║')
#╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
#║ Este script é responsável pela atualização:                                                                    ║')  
#║ - Biblioteca de arquivos                                                                                       ║')  
#║ - Biblioteca de tipos de dados                                                                                 ║')
#║ - Funções utilizadas nas execuções de script                                                                   ║')
#╚════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝')

# ==================================================================================================================
# IMPORTAR BIBLIOTECAS
# ==================================================================================================================

import os
import sys
import pandas as pd
import numpy as np
import warnings
import time
import datetime
import logging
import inspect
from tqdm import tqdm
from pandas.tseries.offsets import MonthEnd
from pandas.tseries.offsets import MonthBegin
from tkinter import messagebox
from unidecode import unidecode
warnings.filterwarnings('ignore')
start_time = time.time()

class aux_functions_vcm:
    
    def validar_data_arquivo(self,arquivo):
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

    # Função de padronização das colunas
    def padronizar(self,value):
        if isinstance(value, str):
            value = value.upper().strip()
            value = unidecode(value)
        return value

    # Criando uma função custom para ajustar leadtimes
    def custom_round(self,value):
        if value > 2.5:
            return 3
        elif value > 1.5:
            return 2
        elif value > 0.5:
            return 1
        else:
            return 0.0

    # LEFT JOIN personalizado para indicar o progresso, tabelas e resultado do join
    def left_outer_join(self,df_left, df_right, left_on, right_on, name_left=None, name_right=None):
        try:
            msg_inicial = f"INICIO DO LEFT JOIN :: {name_left} x {name_right}"
            msg_final = f"FIM DO LEFT JOIN :: Resultado = "
            #tamanho_final = max(len(msg_inicial) , len(msg_final) + 3)
            tamanho_final = 110
            
            # Criar as bordas do JOIN
            borda_superior = '╔' + '═' * (tamanho_final + 2) + '╗'
            borda_inferior = '╚' + '═' * (tamanho_final + 2) + '╝'
            #print(f' \nIniciando mesclagem :: {name_left} x {name_right}')
            print(borda_superior)
            print(f'║ {msg_inicial.ljust(tamanho_final)} ║')
            x1 = df_left.shape[0]
            print(f'║ A quantidade de linhas antes do join é {x1}'.ljust(tamanho_final + 3) + '║')
            merged_df = df_left.merge(df_right, how = 'left', left_on = left_on, right_on = right_on)
            # Limpar o DataFrame original e aplicar as novas colunas
            df_left.drop(df_left.columns, axis=1, inplace=True) 
            for col in merged_df.columns:
                df_left[col] = merged_df[col]  # Copiar colunas do merged_df

            x2 = df_left.shape[0]
            print(f'║ A quantidade de linhas após o join é {x2}'.ljust(tamanho_final + 3) + '║')
            if x1 == x2:
                y = '√'
                print(f'║ {msg_final}{y}'.ljust(tamanho_final + 3) + '║')
                print(borda_inferior)
            else:
                y = 'X'
                print(f'║ Checar por duplicidades em {name_right}'.ljust(tamanho_final + 3) + '║')
                print(f'║ {msg_final}{y}'.ljust(tamanho_final + 3) + '║')
                print(borda_inferior)
                raise LookupError()
           
        except Exception as erro:
            print(f'║ Erro de mesclagem identificado :: {str(erro)}'.ljust(tamanho_final + 3) + '║')
            print('║ SCRIPT FINALIZADO.'.ljust(tamanho_final + 3) + '║')
            print(borda_inferior)
            
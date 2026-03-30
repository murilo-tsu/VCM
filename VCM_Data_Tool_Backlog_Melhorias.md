# VCM Data Tool — Backlog de Melhorias Futuras

**Projeto:** VCM Data Tool App  
**Data da revisão:** 30/03/2026  
**Status do projeto:** Funcional e em produção  

> Este documento registra oportunidades de melhoria identificadas durante a revisão de código do projeto VCM Data Tool. Todas as sugestões são voltadas para sustentabilidade de longo prazo, manutenibilidade e preparação para o futuro módulo de otimização. Nenhum dos itens abaixo compromete o funcionamento atual da pipeline.

---

## 1. Bug Identificado

### 1.1 Botão "[13] Limites" mapeado incorretamente no SCRIPT.py

**Arquivo:** `SCRIPT.py`  
**Severidade:** Média  
**Descrição:** O botão `[13] Limites: Descarga e Produção` está chamando a função `fixed_price` em vez de `limits`. Ao clicar, o usuário executa o script de precificação ao invés do script de limites.  
**Correção:** Alterar o `command` do botão de `fixed_price` para `limits`.

---

## 2. Redução de Código Repetido

### 2.1 Centralizar configurações iniciais

**Arquivos afetados:** Todos os 14 scripts de domínio  
**Prioridade:** Alta  
**Descrição:** Praticamente todos os módulos repetem o mesmo bloco de configurações: definição de `cwd`, paths (`structure_path`, `path`, `output_path`, `exec_log_path`), imports de bibliotecas, configuração de logging e instância de `aux_functions_vcm`. Uma mudança em qualquer um desses elementos exige edição em 14+ arquivos.  
**Sugestão:** Criar um módulo `_config.py` que centralize todas as configurações e seja importado por cada script. Exemplo:

```python
# _config.py
import os, sys, logging, warnings
import pandas as pd, numpy as np
from _modulos import aux_functions_vcm
from _dicionarios import arquivos_primarios, tp_dado_arquivos, rename_dataframes

warnings.filterwarnings('ignore')
cwd = os.getcwd()
structure_path = 'Structural Data/'
path = 'Input Data/'
output_path = 'Output Data/'
exec_log_path = 'Error Logs/'

fx = aux_functions_vcm()
```

### 2.2 Centralizar carga de DataFrames comuns

**Arquivos afetados:** Todos os scripts de domínio  
**Prioridade:** Média  
**Descrição:** DataFrames como `periodos`, `cadastro_produtos`, `agrupamento_produtos`, `portos`, `dicgen` e `unidades_exp` são carregados repetidamente em quase todos os scripts com o mesmo código de leitura. Isso representa duplicação significativa e risco de inconsistência se o formato de um arquivo de entrada mudar.  
**Sugestão:** Criar um módulo `_dataloader.py` com funções de carga lazy (carrega sob demanda) para cada DataFrame compartilhado, com cache em memória para evitar releitura.

---

## 3. Compatibilidade e Modernização

### 3.1 Substituir `applymap` por `map` (deprecação Pandas)

**Arquivos afetados:** `supply.py`, `bind.py`, `sku_activation.py`, `freight.py`, `fixed_price.py`, `limits.py`, `warehouses.py`, `reposition_cost.py`, `receipt.py`, `tax.py`, entre outros  
**Prioridade:** Alta (preventiva)  
**Descrição:** O método `DataFrame.applymap()` foi deprecado no Pandas 2.1 e será removido em versões futuras. Atualmente o projeto utiliza `applymap(fx.padronizar)` extensivamente.  
**Correção:** Substituir todas as ocorrências de `.applymap(fx.padronizar)` por `.map(fx.padronizar)`.

### 3.2 Dependência de Windows em funções utilitárias

**Arquivo:** `_modulos.py` (função `validar_data_arquivo`)  
**Prioridade:** Baixa  
**Descrição:** A função utiliza `os.startfile()` e `tkinter.messagebox`, que são dependentes de Windows. Se houver necessidade futura de rodar em Linux/Mac (ex: em um servidor ou container), essas chamadas falharão.  
**Sugestão:** Abstrair a notificação ao usuário em uma função separada que possa ser substituída por logging em ambientes não-GUI.

---

## 4. Performance

### 4.1 Vetorizar loops em `reposition_cost.py`

**Arquivo:** `reposition_cost.py`  
**Prioridade:** Alta  
**Descrição:** O loop principal que preenche custos de matéria-prima e demurrage percorre cada linha do `wizard_custo_suprimento_faixa` e, para cada uma, itera sobre `custos_mp` e `demurrage`. Isso resulta em complexidade O(n × m), o que se torna lento com volumes maiores de dados.  
**Sugestão:** Substituir por operações vetorizadas usando `merge` + `np.where`/`np.select`, seguindo o padrão já aplicado com sucesso em `yield_deploy.py`.

### 4.2 Vetorizar loops em `bind.py`

**Arquivo:** `bind.py`  
**Prioridade:** Média  
**Descrição:** O loop com `tqdm` que preenche `template_correntes` e `template_limites` pode ser substituído por operações vetorizadas, já que a lógica consiste em comparações e atribuições condicionais simples.  
**Sugestão:** Usar `np.where` ou atribuição condicional com máscaras booleanas.

---

## 5. Robustez e Tratamento de Erros

### 5.1 Substituir `sys.exit()` por exceções customizadas

**Arquivo:** `_modulos.py` (função `left_outer_join`)  
**Prioridade:** Média  
**Descrição:** Quando o `left_outer_join` detecta um problema de cardinalidade (linhas aumentaram inesperadamente), ele chama `sys.exit()`. Isso encerra o processo inteiro, incluindo a GUI. Num contexto interativo, isso resulta em uma experiência abrupta para o usuário.  
**Sugestão:** Criar uma exceção customizada (ex: `JoinCardinalityError`) e capturá-la na GUI para exibir uma mensagem de erro sem derrubar a aplicação.

```python
class JoinCardinalityError(Exception):
    pass

# Em left_outer_join:
raise JoinCardinalityError(f"Duplicidades em {name_right}")

# Na GUI:
try:
    import supply
except JoinCardinalityError as e:
    messagebox.showerror("Erro de Mesclagem", str(e))
```

### 5.2 Inconsistência no parâmetro `sheet_name` para periodos

**Arquivos afetados:** Vários  
**Prioridade:** Baixa  
**Descrição:** A leitura do arquivo de períodos às vezes passa `sheet_name` explicitamente (ex: `bind.py`, `tax.py`) e às vezes omite (ex: `supply.py`, `unconstrained_demand.py`). Se o Excel tiver múltiplas abas, o comportamento padrão do Pandas (ler a primeira aba) pode não corresponder à aba correta em todos os casos.  
**Sugestão:** Padronizar o uso de `sheet_name` em todas as leituras de periodos, preferencialmente centralizando na proposta do item 2.2.

---

## 6. Qualidade e Testes

### 6.1 Implementar testes automatizados de integridade

**Prioridade:** Média (torna-se Alta ao iniciar o módulo de otimização)  
**Descrição:** Atualmente a validação é feita manualmente e pelo `left_outer_join`. Não existem verificações automatizadas para os DataFrames de saída.  
**Sugestão:** Criar um módulo `_tests.py` ou usar `pytest` com assertions sobre os outputs:

- Colunas obrigatórias não devem conter `NaN`
- Chaves primárias devem ser únicas
- Totais de volume devem ser consistentes (entrada = saída onde aplicável)
- Tipos de dados devem corresponder ao esperado pelo VCM
- Períodos nos outputs devem estar contidos no horizonte de otimização

Esses testes serão especialmente valiosos quando o módulo de otimização for integrado, pois garantem que os inputs para o solver estão corretos.

---

## Ordem Sugerida de Implementação

| Ordem | Item | Justificativa |
|-------|------|---------------|
| 1 | 1.1 — Bug do botão | Correção imediata, uma linha |
| 2 | 3.1 — `applymap` → `map` | Previne quebra em atualização do Pandas |
| 3 | 2.1 — Centralizar configs | Reduz risco de inconsistência entre scripts |
| 4 | 4.1 — Vetorizar `reposition_cost` | Maior ganho de performance |
| 5 | 2.2 — Centralizar carga de dados | Maior redução de código repetido |
| 6 | 5.1 — Exceções customizadas | Melhora UX e facilita debugging |
| 7 | 4.2 — Vetorizar `bind.py` | Ganho incremental de performance |
| 8 | 6.1 — Testes automatizados | Essencial antes do módulo de otimização |
| 9 | 5.2 — Padronizar `sheet_name` | Previne bugs sutis |
| 10 | 3.2 — Portabilidade cross-platform | Só se necessário |

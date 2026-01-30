# BET7K – Data Automation & Reporting Pipeline

## Visão Geral do Projeto

O projeto **BET7K** implementa um pipeline de automação e processamento de dados em **Python**, voltado para a **extração, tratamento e disponibilização de métricas operacionais e financeiras**.

O objetivo do projeto é automatizar fluxos que antes dependiam de execução manual, garantindo:

- padronização dos dados  
- consistência entre execuções  
- redução de erros operacionais  
- suporte direto a análises e dashboards  

O projeto foi estruturado com foco em **clareza, modularidade, segurança e manutenção**.

---

##  Tecnologias

- **Python 3.10+** (recomendado)
- **Playwright** (automação web)
- **Pandas** (tratamento dos dados)
- **Google Sheets API** (envio para planilha)
- **Google Auth** (Service Account)

---

## Escopo do Pipeline

O pipeline do BET7K é responsável por:

- Executar autenticação automática via login interno do projeto  
- Extrair dados da fonte configurada  
- Armazenar dados brutos para auditoria  
- Tratar e normalizar dados  
- Padronizar datas e valores monetários  
- Organizar colunas na ordem final definida  
- Enviar dados tratados para o Google Sheets  

A estrutura permite expansão sem reescrita do fluxo principal.

---

## Arquitetura Geral

Fluxo de execução:

1. Execução do script principal  
2. Autenticação automática (login interno já configurado)  
3. Extração dos dados  
4. Salvamento dos dados brutos  
5. Processamento e normalização  
6. Organização das colunas finais  
7. Envio dos dados ao Google Sheets  



---

## Arquivos Principais

### report_7k_partners.py

Script principal do projeto.

Responsável por:

- orquestrar todo o pipeline  
- utilizar o login interno configurado  
- extrair dados  
- aplicar regras de tratamento  
- enviar dados finais ao Google Sheets  

> ⚠️ O login interno utilizado neste script **deve ser validado com o gestor pois e o login do site onde se realiza a busca dos dados**.

---

### bet7k_raw.csv

Armazena os dados brutos exatamente como capturados.

Utilizado para:

- auditoria  
- histórico  
- reprocessamento  

---

### bet7k_processed.csv

Contém os dados tratados e prontos para consumo.

---

### credenciais.json

Arquivo de credenciais do Google Sheets API.

⚠️ **Não deve ser versionado**  


---

### utils/

Funções auxiliares reutilizáveis:

- tratamento de datas  
- correção de valores numéricos  
- integração com Google Sheets  

---

## Acesso, Permissões e Onboarding (Obrigatório)

Este projeto **já possui login e senha internos configurados no código**, necessários para que o script execute corretamente.

⚠️ **Essas credenciais NÃO substituem permissões de acesso ao Google Sheets.**

Para executar o projeto, o desenvolvedor **PRECISA cumprir TODOS os itens abaixo**.

---

### 1️⃣ Acesso à Planilha do Google Sheets

- O desenvolvedor deve **solicitar acesso ao gestor responsável**  
- Sem permissão na planilha, o envio de dados falhará (erro 403)  

📄 Documento oficial do projeto:  
https://docs.google.com/spreadsheets/d/1x3PLUE2ubJtMhlxG0eURHDvz5imnq3FUEJuAXcShOjs/edit?gid=773399482#gid=773399482

---

### 2️⃣ Credenciais Google Locais (Obrigatório)

Além do login interno do projeto, é obrigatório configurar as **credenciais Google locais**, conforme padrão da Google Sheets API.

Arquivos sensíveis **NÃO DEVEM ser versionados**:

- credenciais.json  
- token.json  
- .env  

---

### 3️⃣ Onboarding Obrigatório

Todo novo desenvolvedor deve seguir o documento oficial de onboarding antes de executar o projeto.

📄 Documento de Onboarding BET7K:  
https://docs.google.com/document/d/1JGA0azxBkmlul4lV8989DT0yb8_D4qVjzf98QMtFC7I/edit?tab=t.0

---

## Requisitos do Ambiente

- Python **3.10 ou superior**

---

## Instalação das Dependências

```bash
pip install pandas google-api-python-client google-auth google-auth-oauthlib
```

## Execução do Pipeline

Após:

- ter acesso à planilha  
- configurar as credenciais Google  
- concluir o onboarding  

Execute:

```bash
python report_7k_partners.py
```
## Boas Práticas do Projeto

- Separação entre dados brutos e processados

- Padronização rigorosa de datas e valores

- Login interno centralizado

- Credenciais fora do versionamento

- Estrutura preparada para crescimento

## Manutenção e Evolução

O projeto permite:

- inclusão de novas métricas

- adição de novas colunas

- ajustes pontuais sem quebra do pipeline

## Conclusão

O BET7K fornece uma base sólida para automação, tratamento e disponibilização de dados, com foco em confiabilidade, segurança e escalabilidade.

---


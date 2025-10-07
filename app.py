import streamlit as st

import pandas as pd

from datetime import date, timedelta

import time

import gspread # Biblioteca para Google Sheets

import numpy as np



# ==============================================================================

# 🚨 CONFIGURAÇÃO GOOGLE SHEETS E CONEXÃO 🚨

# ==============================================================================



# Defina a URL ou ID da sua planilha AQUI

SHEET_ID = '1BNjgWhvEj8NbnGr4x7F42LW7QbQiG5kZ1FBhfr9Q-4g' # <--- SUBSTITUA PELO SEU SHEET ID REAL!



@st.cache_resource(ttl=3600) # Cache para a conexão não abrir a cada execução

def get_gspread_client():

    """Retorna o cliente Gspread autenticado."""

    try:

        # Tenta carregar as credenciais do Streamlit Secrets

        creds_info = st.secrets["gcp_service_account"]

        gc = gspread.service_account_from_dict(creds_info)

        return gc

    except KeyError:

        st.error("⚠️ Credenciais do Google Sheets não encontradas. Certifique-se de que o 'gcp_service_account' está configurado em .streamlit/secrets.toml.")

        st.stop()

    except Exception as e:

        # Este erro agora é mais específico para problemas na chave (Base64/Padding)

        st.error(f"Erro de autenticação Gspread. Verifique seu ID da planilha e o compartilhamento com a Service Account: {e}")

        st.stop()



@st.cache_data(ttl=5) # Cache para leitura rápida (revalida a cada 5 segundos)

def get_sheet_data(sheet_name):

    """Lê os dados de uma aba/sheet e retorna um DataFrame, com conversões iniciais."""

    try:

        gc = get_gspread_client()

        sh = gc.open_by_key(SHEET_ID)

        worksheet = sh.worksheet(sheet_name)

       

        # Lê todos os dados como lista de listas

        data = worksheet.get_all_records()

        df = pd.DataFrame(data)



        if df.empty:

            return df

           

        # Garante que as colunas de ID sejam tratadas como inteiros

        id_col = f'id_{sheet_name}' if sheet_name in ('veiculo', 'prestador') else 'id_servico'

        if id_col in df.columns:

            df[id_col] = pd.to_numeric(df[id_col], errors='coerce').fillna(0).astype(int)

       

        # 🚀 ESTABILIZAÇÃO: CONVERSÃO INICIAL DE TIPOS CHAVE LOGO APÓS A LEITURA

        if sheet_name == 'veiculo':

            # Conversão para float e data no df_veiculo

            if 'valor_pago' in df.columns:

                 df['valor_pago'] = pd.to_numeric(df['valor_pago'], errors='coerce').fillna(0.0).astype(float)

            if 'data_compra' in df.columns:

                 df['data_compra'] = pd.to_datetime(df['data_compra'], errors='coerce')



        if sheet_name == 'servico':

             # Conversão para tipos numéricos de serviço

             for col in ['valor', 'garantia_dias', 'km_realizado', 'km_proxima_revisao']:

                if col in df.columns:

                    # Trata a conversão de garantia_dias para int (necessário no cálculo de data_vencimento)

                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

             # CONVERSÃO REFORÇADA PARA DATA DE SERVIÇO E VENCIMENTO

             for col in ['data_servico', 'data_vencimento']:

                 if col in df.columns:

                     # Força a conversão para datetime.

                     df[col] = pd.to_datetime(df[col], errors='coerce')

       

        return df



    except gspread.WorksheetNotFound:

        st.error(f"A aba/sheet **'{sheet_name}'** não foi encontrada na planilha. Crie-a com os cabeçalhos corretos.")

        return pd.DataFrame()

    except Exception as e:

        st.error(f"Erro ao ler a sheet '{sheet_name}': {e}")

        return pd.DataFrame()





def write_sheet_data(sheet_name, df_new):

    """Sobrescreve a aba/sheet com o novo DataFrame."""

    try:

        gc = get_gspread_client()

        sh = gc.open_by_key(SHEET_ID)

        worksheet = sh.worksheet(sheet_name)



        # Converte o DataFrame para lista de listas, incluindo o cabeçalho

        data_to_write = [df_new.columns.tolist()] + df_new.values.tolist()

       

        # Sobrescreve toda a aba

        worksheet.clear()

        worksheet.update('A1', data_to_write, value_input_option='USER_ENTERED')

       

        # Limpa o cache do Streamlit para forçar a releitura imediata

        get_sheet_data.clear()

       

        return True



    except Exception as e:

        st.error(f"Erro ao escrever na sheet '{sheet_name}': {e}")

        return False



# ==============================================================================

# 🚨 FUNÇÕES DE ACESSO A DADOS (SIMULAÇÃO CRUD) 🚨

# ==============================================================================



def get_data(sheet_name, filter_col=None, filter_value=None):

    """Busca dados de uma aba/sheet e retorna um DataFrame do Pandas, com filtro opcional."""

    df = get_sheet_data(sheet_name)

    if df.empty:

        return df

   

    if filter_col and filter_value is not None:

        try:

            # Garante que o ID no DataFrame é inteiro para comparação

            if filter_col.startswith('id_'):

                 df[filter_col] = pd.to_numeric(df[filter_col], errors='coerce').fillna(0).astype(int)

                 # Garante que o valor de filtro seja inteiro

                 filter_value = int(filter_value) if pd.notna(filter_value) else 0

           

            df_filtered = df[df[filter_col] == filter_value]

            return df_filtered

        except:

            # Em caso de falha de filtro (por exemplo, coluna não existe), retorne um DF vazio.

            return pd.DataFrame()

   

    return df





def execute_crud_operation(sheet_name, data=None, id_col=None, id_value=None, operation='insert'):

    """Executa as operações CRUD no Google Sheets (Insert, Update, Delete)."""

    df = get_data(sheet_name)

   

    # 1. TRATAMENTO DE ID (SIMULAÇÃO DE AUTO_INCREMENT)

    new_id = None

    if operation == 'insert':

        # Calcula o próximo ID

        id_col = f'id_{sheet_name}' if id_col is None else id_col

        if df.empty:

            new_id = 1

            df = pd.DataFrame(columns=data.keys()) # Cria DF vazio com as colunas

        else:

            df[id_col] = pd.to_numeric(df[id_col], errors='coerce').fillna(0).astype(int)

            new_id = df[id_col].max() + 1

       

        data[id_col] = new_id

   

    # 2. INSERÇÃO (APPEND)

    if operation == 'insert':

        # Cria um novo DataFrame com os dados a serem inseridos

        df_new_row = pd.DataFrame([data])

        # Concatena a nova linha. Garante a ordem das colunas.

        if df.empty:

            df_updated = df_new_row

        else:

             df_updated = pd.concat([df, df_new_row], ignore_index=True)

             df_updated = df_updated[df.columns] # Reordena colunas

       

        success = write_sheet_data(sheet_name, df_updated)

        return success, new_id if success else None



    # 3. ATUALIZAÇÃO OU EXCLUSÃO (UPDATE/DELETE)

    elif operation in ['update', 'delete']:

        if df.empty or id_value is None:

            return False, None

       

        # Encontra o índice da linha

        id_col = f'id_{sheet_name}' if id_col is None else id_col

        df[id_col] = pd.to_numeric(df[id_col], errors='coerce').fillna(0).astype(int)

        index_to_modify = df[df[id_col] == int(id_value)].index

       

        if index_to_modify.empty:

            return False, None



        if operation == 'update':

            # Atualiza a linha

            for key, value in data.items():

                if key in df.columns:

                     df.loc[index_to_modify, key] = value

            df_updated = df

       

        elif operation == 'delete':

            # Remove a linha

            df_updated = df.drop(index_to_modify).reset_index(drop=True)



        success = write_sheet_data(sheet_name, df_updated)

        return success, id_value if success else None

       

    return False, None



# --- Funções de Inserção/Atualização/Exclusão (CRUD) ---

# Veículo

def insert_vehicle(nome, placa, ano, valor_pago, data_compra):

   

    # 🛑 ALTERAÇÃO: REMOVIDA A VERIFICAÇÃO DE PLACA OBRIGATÓRIA AQUI.

    # A verificação de duplicidade é mantida, mas só ocorre se a placa não for vazia.

    if placa:

        df_check = get_data('veiculo', 'placa', placa)

        if not df_check.empty:

            st.error(f"Placa '{placa}' já cadastrada.")

            return False

       

    data = {

        'id_veiculo': 0,

        'nome': nome, 'placa': placa,

        'ano': ano, 'valor_pago': float(valor_pago), 'data_compra': str(data_compra)

    }

   

    success, _ = execute_crud_operation('veiculo', data=data, id_col='id_veiculo', operation='insert')

   

    if success:

        st.success(f"Veículo '{nome}' ({placa}) cadastrado com sucesso!")

        st.session_state['edit_vehicle_id'] = None

        st.rerun()

    else:

        st.error("Falha ao cadastrar veículo.")



def update_vehicle(id_veiculo, nome, placa, ano, valor_pago, data_compra):

   

    # 🛑 ALTERAÇÃO: REMOVIDA A OBRIGATORIEDADE DE PLACA.

    # A verificação de duplicidade é mantida, mas só ocorre se a placa não for vazia.

    if placa:

        df_check = get_data('veiculo', 'placa', placa)

        if not df_check.empty:

            # Pega o ID do veículo encontrado (se houver) e converte para int

            found_id = df_check.iloc[0]['id_veiculo']

            if found_id != int(id_veiculo):

                st.error(f"Placa '{placa}' já cadastrada para outro veículo (ID {found_id}).")

                return False



    data = {

        'nome': nome, 'placa': placa,

        'ano': ano, 'valor_pago': float(valor_pago), 'data_compra': str(data_compra)

    }

   

    success, _ = execute_crud_operation('veiculo', data=data, id_col='id_veiculo', id_value=int(id_veiculo), operation='update')

   

    if success:

        st.success(f"Veículo '{nome}' ({placa}) atualizado com sucesso!")

        st.session_state['edit_vehicle_id'] = None

        st.rerun()

    else:

        st.error("Falha ao atualizar veículo.")



def delete_vehicle(id_veiculo):

    # Simulação da verificação de chave estrangeira

    df_servicos = get_data('servico', 'id_veiculo', int(id_veiculo))

    if not df_servicos.empty:

        st.error("Não é possível remover o veículo. Existem serviços vinculados a ele.")

        return False

       

    success, _ = execute_crud_operation('veiculo', id_col='id_veiculo', id_value=int(id_veiculo), operation='delete')

   

    if success:

        st.success("Veículo removido com sucesso!")

        time.sleep(1)

        st.rerun()

    else:

        st.error("Falha ao remover veículo.")



# Prestador

def insert_new_prestador(empresa, telefone, nome_prestador, cnpj, email, endereco, numero, cidade, bairro, cep):

    df_check = get_data("prestador", "empresa", empresa)

    if not df_check.empty:

        st.warning(f"A empresa '{empresa}' já está cadastrada.")

        return False

       

    data = {

        'id_prestador': 0, 'empresa': empresa, 'telefone': telefone, 'nome_prestador': nome_prestador,

        'cnpj': cnpj, 'email': email, 'endereco': endereco, 'numero': numero,

        'cidade': cidade, 'bairro': bairro, 'bairro': bairro, 'cep': cep

    }

   

    success, _ = execute_crud_operation('prestador', data=data, id_col='id_prestador', operation='insert')

   

    if success:

        st.success(f"Prestador '{empresa}' cadastrado com sucesso!")

        st.session_state['edit_prestador_id'] = None

        st.rerun()

        return True

    return False



def update_prestador(id_prestador, empresa, telefone, nome_prestador, cnpj, email, endereco, numero, cidade, bairro, cep):

    data = {

        'empresa': empresa, 'telefone': telefone, 'nome_prestador': nome_prestador,

        'cnpj': cnpj, 'email': email, 'endereco': endereco, 'numero': numero,

        'cidade': cidade, 'bairro': bairro, 'cep': cep

    }

   

    success, _ = execute_crud_operation('prestador', data=data, id_col='id_prestador', id_value=int(id_prestador), operation='update')

   

    if success:

        st.success(f"Prestador '{empresa}' atualizado com sucesso!")

        st.session_state['edit_prestador_id'] = None

        st.rerun()

        return True

    return False



def delete_prestador(id_prestador):

    df_servicos = get_data('servico', 'id_prestador', int(id_prestador))

    if not df_servicos.empty:

        st.error("Não é possível remover o prestador. Existem serviços vinculados a ele.")

        return False



    success, _ = execute_crud_operation('prestador', id_col='id_prestador', id_value=int(id_prestador), operation='delete')

   

    if success:

        st.success("Prestador removido com sucesso!")

        time.sleep(1)

        st.rerun()

    else:

        st.error("Falha ao remover prestador.")



def insert_prestador(empresa, telefone, nome_prestador, cnpj, email, endereco, numero, cidade, bairro, cep):

    """Insere ou atualiza um prestador (usado no cadastro de Serviço)."""

    df = get_data("prestador", "empresa", empresa)

   

    if not df.empty:

        # Se existe, retorna o ID e atualiza os dados

        id_prestador = df.iloc[0]['id_prestador']

        # Simula a atualização de dados do prestador

        update_prestador(id_prestador, empresa, telefone, nome_prestador, cnpj, email, endereco, numero, cidade, bairro, cep)

        st.info(f"Dados do Prestador '{empresa}' atualizados.")

        return id_prestador

   

    # Se não existe, insere

    data = {

        'id_prestador': 0, 'empresa': empresa, 'telefone': telefone, 'nome_prestador': nome_prestador,

        'cnpj': cnpj, 'email': email, 'endereco': endereco, 'numero': numero,

        'cidade': cidade, 'bairro': bairro, 'cep': cep

    }

    success, new_id = execute_crud_operation('prestador', data=data, id_col='id_prestador', operation='insert')

   

    return new_id if success else None



# Serviço

def insert_service(id_veiculo, id_prestador, nome_servico, data_servico, garantia_dias, valor, km_realizado, km_proxima_revisao, registro):

    data_servico_dt = pd.to_datetime(data_servico)

    # Garante que garantia_dias é inteiro para o timedelta

    garantia_dias_int = int(garantia_dias)

    data_vencimento = data_servico_dt + timedelta(days=garantia_dias_int)



    data = {

        'id_servico': 0, 'id_veiculo': int(id_veiculo), 'id_prestador': int(id_prestador),

        'nome_servico': nome_servico, 'data_servico': str(data_servico_dt.date()), # Armazena como string de data

        'garantia_dias': str(garantia_dias), 'valor': float(valor),

        'km_realizado': str(km_realizado), 'km_proxima_revisao': str(km_proxima_revisao),

        'registro': registro,

        'data_vencimento': str(data_vencimento.date()) # Armazena como string de data

    }

   

    success, _ = execute_crud_operation('servico', data=data, id_col='id_servico', operation='insert')

   

    if success:

        st.success(f"Serviço '{nome_servico}' cadastrado com sucesso!")

        if 'edit_service_id' in st.session_state:

            del st.session_state['edit_service_id']

        st.rerun()

    else:

        st.error("Falha ao cadastrar serviço.")



def update_service(id_servico, id_veiculo, id_prestador, nome_servico, data_servico, garantia_dias, valor, km_realizado, km_proxima_revisao, registro):

    data_servico_dt = pd.to_datetime(data_servico)

    # Garante que garantia_dias é inteiro para o timedelta

    garantia_dias_int = int(garantia_dias)

    data_vencimento = data_servico_dt + timedelta(days=garantia_dias_int)



    data = {

        'id_veiculo': int(id_veiculo), 'id_prestador': int(id_prestador),

        'nome_servico': nome_servico, 'data_servico': str(data_servico_dt.date()), # Armazena como string de data

        'garantia_dias': str(garantia_dias), 'valor': float(valor),

        'km_realizado': str(km_realizado), 'km_proxima_revisao': str(km_proxima_revisao),

        'registro': registro,

        'data_vencimento': str(data_vencimento.date()) # Armazena como string de data

    }

   

    success, _ = execute_crud_operation('servico', data=data, id_col='id_servico', id_value=int(id_servico), operation='update')

   

    if success:

        st.success(f"Serviço '{nome_servico}' atualizado com sucesso!")

        if 'edit_service_id' in st.session_state:

            del st.session_state['edit_service_id']

        st.rerun()

    else:

        st.error("Falha ao atualizar serviço.")



def delete_service(id_servico):

    success, _ = execute_crud_operation('servico', id_col='id_servico', id_value=int(id_servico), operation='delete')

   

    if success:

        st.success("Serviço removido com sucesso!")

        time.sleep(1)

        st.rerun()

    else:

        st.error("Falha ao remover serviço.")



# --- FUNÇÃO QUE SIMULA O JOIN DO SQL ---



def get_full_service_data(date_start=None, date_end=None):

    """Lê todos os dados e simula a operação JOIN do SQL no Pandas."""

   

    df_servicos = get_data('servico')

    df_veiculos = get_data('veiculo')

    df_prestadores = get_data('prestador')

   

    if df_servicos.empty or df_veiculos.empty or df_prestadores.empty:

        return pd.DataFrame()

   

    # Converte tipos para o merge

    df_servicos['id_veiculo'] = pd.to_numeric(df_servicos['id_veiculo'], errors='coerce').fillna(0).astype(int)

    df_servicos['id_prestador'] = pd.to_numeric(df_servicos['id_prestador'], errors='coerce').fillna(0).astype(int)

   

    # 🛑 CONVERSÃO FINAL DE TIPOS NUMÉRICOS 🛑

    df_servicos['valor'] = pd.to_numeric(df_servicos['valor'], errors='coerce').fillna(0.0)

    df_servicos['garantia_dias'] = pd.to_numeric(df_servicos['garantia_dias'], errors='coerce').fillna(0).astype(int)

    df_servicos['km_realizado'] = pd.to_numeric(df_servicos['km_realizado'], errors='coerce').fillna(0).astype(int)

    df_servicos['km_proxima_revisao'] = pd.to_numeric(df_servicos['km_proxima_revisao'], errors='coerce').fillna(0).astype(int)

    # -----------------------------------------------

    # 1. JOIN com Veículo

    df_merged = pd.merge(df_servicos, df_veiculos[['id_veiculo', 'nome', 'placa']], on='id_veiculo', how='left')

   

    # 2. JOIN com Prestador

    df_merged = pd.merge(df_merged, df_prestadores[['id_prestador', 'empresa', 'cidade']], on='id_prestador', how='left')

   

    # Renomeia colunas para o display

    df_merged = df_merged.rename(columns={'nome': 'Veículo', 'placa': 'Placa', 'empresa': 'Empresa', 'cidade': 'Cidade', 'nome_servico': 'Serviço', 'data_servico': 'Data', 'valor': 'Valor'})

   

    # Converte colunas de data (sem NaT) - já foram feitas em get_sheet_data

    df_merged['Data'] = pd.to_datetime(df_merged['Data'], errors='coerce')

    df_merged['data_vencimento'] = pd.to_datetime(df_merged['data_vencimento'], errors='coerce')



    # 3. Filtragem por Data (se necessário)

    if date_start and date_end:

        df_merged = df_merged[(df_merged['Data'] >= pd.to_datetime(date_start)) & (df_merged['Data'] <= pd.to_datetime(date_end))]

       

    return df_merged.sort_values(by='Data', ascending=False)



# ==============================================================================

# 🚨 CSS PERSONALIZADO PARA FORÇAR BOTÕES LADO A LADO NO CELULAR 🚨

# ==============================================================================

CUSTOM_CSS = """

/* Aplica display flex (alinhamento horizontal) e nowrap (não quebrar linha)

    aos containers de coluna que envolvem os botões de ação (lápis e lixeira).

    Isso garante que os botões fiquem lado a lado mesmo em telas muito pequenas. */

.st-emotion-cache-12fmwza, .st-emotion-cache-n2e28m { /* Classes específicas do Streamlit */

    display: flex;

    flex-wrap: nowrap !important;

    gap: 5px;

    align-items: center;

}



/* Garante que os containers internos dos botões ocupem o mínimo de espaço */

.st-emotion-cache-12fmwza > div, .st-emotion-cache-n2e28m > div {

    min-width: 0 !important;

    max-width: none !important;

}

/* Reduz o padding dos botões para economizar espaço e garantir o alinhamento */

.st-emotion-cache-n2e28m button, .st-emotion-cache-12fmwza button {

    padding: 0px 5px !important;

    line-height: 1.2 !important;

    font-size: 14px;

}

"""

# ==============================================================================





# --- COMPONENTES DE DISPLAY ---



def display_vehicle_table_and_actions(df_veiculos_listagem):

    """Exibe a tabela de veículos com layout adaptado para celular."""

    st.subheader("Manutenção de Veículos Existentes")

    st.markdown('---')

   

    for index, row in df_veiculos_listagem.iterrows():

        id_veiculo = int(row['id_veiculo'])

       

        # PROPORÇÃO PARA RESPONSIVIDADE: 85% para Dados, 15% para Ações.

        col_data, col_actions = st.columns([0.85, 0.15])

       

        # --- BLOC DE DADOS (COLUNA ESQUERDA) ---

        with col_data:

            st.markdown(f"**{row['nome']} ({row['placa'] or 'S/ Placa'})**") # Exibe 'S/ Placa' se for vazio

            st.markdown(f"Ano: **{row['ano']}**")

            st.markdown(f"Valor: **R$ {float(row['valor_pago']):.2f}**")

       

        # --- BLOCO DE AÇÃO (COLUNA DIREITA) ---

        with col_actions:

            col_act1, col_act2 = st.columns(2)

           

            with col_act1:

                if st.button("✏️", key=f"edit_v_{id_veiculo}", help=f"Editar Veículo ID {id_veiculo}"):

                    st.session_state['edit_vehicle_id'] = id_veiculo

                    st.rerun()



            with col_act2:

                if st.button("🗑️", key=f"delete_v_{id_veiculo}", help=f"Excluir Veículo ID {id_veiculo}"):

                    if st.session_state.get(f'confirm_delete_v_{id_veiculo}', False):

                        delete_vehicle(id_veiculo)

                    else:

                        st.session_state[f'confirm_delete_v_{id_veiculo}'] = True

                        st.rerun()

       

        # Linha de aviso de confirmação de exclusão (fora das colunas)

        if st.session_state.get(f'confirm_delete_v_{id_veiculo}', False) and not st.session_state.get('edit_vehicle_id'):

            st.error(f"⚠️ **Clique novamente** no botão 🗑️ acima para confirmar a exclusão do ID {id_veiculo}.")

       

        st.markdown("---")

           

def display_prestador_table_and_actions(df_prestadores_listagem):

    """Exibe a tabela de prestadores com layout adaptado para celular."""

    st.subheader("Manutenção de Prestadores Existentes")

    st.markdown('---')

   

    for index, row in df_prestadores_listagem.iterrows():

        id_prestador = int(row['id_prestador'])

       

        # PROPORÇÃO PARA RESPONSIVIDADE

        col_data, col_actions = st.columns([0.85, 0.15])

       

        # --- BLOC DA LINHA DE DADOS ---

        with col_data:

            st.markdown(f"**{row['empresa']}**")

            st.markdown(f"Contato: **{row['nome_prestador'] or 'N/A'}**")

            st.markdown(f"Telefone: **{row['telefone'] or 'N/A'}**")

            st.markdown(f"Cidade: **{row['cidade'] or 'N/A'}**")

       

        # --- BLOCO DE AÇÃO (COLUNA DIREITA) ---

        with col_actions:

            col_act1, col_act2 = st.columns(2)

           

            with col_act1:

                if st.button("✏️", key=f"edit_p_{id_prestador}", help=f"Editar Prestador ID {id_prestador}"):

                    st.session_state['edit_prestador_id'] = id_prestador

                    st.rerun()



            with col_act2:

                if st.button("🗑️", key=f"delete_p_{id_prestador}", help=f"Excluir Prestador ID {id_prestador}"):

                    if st.session_state.get(f'confirm_delete_p_{id_prestador}', False):

                        delete_prestador(id_prestador)

                    else:

                        st.session_state[f'confirm_delete_p_{id_prestador}'] = True

                        st.rerun()



        if st.session_state.get(f'confirm_delete_p_{id_prestador}', False) and not st.session_state.get('edit_prestador_id'):

            st.error(f"⚠️ **Clique novamente** no botão 🗑️ acima para confirmar a exclusão do ID {id_prestador}.")

           

        st.markdown("---")



def display_service_table_and_actions(df_servicos_listagem):

    """Exibe a tabela de serviços com layout adaptado para celular."""

    st.subheader("Manutenção de Serviços Existentes")

    st.markdown('---')

   

    for index, row in df_servicos_listagem.iterrows():

        # df_servicos_listagem é o resultado de get_full_service_data, que já tem o id_servico

        id_servico = int(row['id_servico'])

       

        # Formata a data para exibição (a coluna 'Data' já é datetime do Pandas)

        data_display = row['Data'].strftime('%d-%m-%Y') if pd.notna(row['Data']) else 'N/A'

       

        # PROPORÇÃO PARA RESPONSIVIDADE

        col_data, col_actions = st.columns([0.85, 0.15])

       

        # --- BLOC DA LINHA DE DADOS ---

        with col_data:

            st.markdown(f"**{row['Veículo']}** - {row['Serviço']}")

            st.markdown(f"Data: **{data_display}**")

            st.markdown(f"Empresa: **{row['Empresa']}**")



        # --- BLOCO DE AÇÃO (COLUNA DIREITA) ---

        with col_actions:

            col_act1, col_act2 = st.columns(2)



            with col_act1:

                if st.button("✏️", key=f"edit_{id_servico}", help=f"Editar Serviço ID {id_servico}"):

                    st.session_state['edit_service_id'] = id_servico

                    st.rerun()



            with col_act2:

                if st.button("🗑️", key=f"delete_{id_servico}", help=f"Excluir Serviço ID {id_servico}"):

                    if st.session_state.get(f'confirm_delete_{id_servico}', False):

                        delete_service(id_servico)

                    else:

                        st.session_state[f'confirm_delete_{id_servico}'] = True

                        st.rerun()



        if st.session_state.get(f'confirm_delete_{id_servico}', False) and not st.session_state.get('edit_service_id'):

            st.error(f"⚠️ **Clique novamente** no botão 🗑️ acima para confirmar a exclusão do ID {id_servico}.")



        st.markdown("---")





# --- Componentes de Gestão Unificada (Cadastro/Manutenção) ---



def manage_vehicle_form():

    """Formulário unificado para Cadastro e Manutenção de Veículos."""

   

    vehicle_id_to_edit = st.session_state.get('edit_vehicle_id', None)

    is_editing = vehicle_id_to_edit is not None

   

    # 🛑 ALTERAÇÃO: Remove o cabeçalho "Veículo" e mantém só o botão.

    if not is_editing:

        st.write("")

        _, col_button = st.columns([0.8, 0.2])

        with col_button:

             if st.button("➕ Novo Veículo", key="btn_novo_veiculo_lista", help="Iniciar um novo cadastro de veículo"):

                 st.session_state['edit_vehicle_id'] = 'NEW_MODE'

                 st.rerun()



    if is_editing or st.session_state.get('edit_vehicle_id') == 'NEW_MODE':

       

        is_new_mode = st.session_state.get('edit_vehicle_id') == 'NEW_MODE'

        df_veiculos = get_data("veiculo")



        if is_new_mode:

            st.header("➕ Novo Veículo")

            submit_label = 'Cadastrar Veículo'

            data = {

                'nome': '', 'placa': '', 'ano': date.today().year,

                'valor_pago': 0.0, 'data_compra': date.today()

            }

            if st.button("Cancelar Cadastro / Voltar para Lista"):

                del st.session_state['edit_vehicle_id']

                st.rerun()

                return

           

        else: # MODO EDIÇÃO

            submit_label = 'Atualizar Veículo'

           

            try:

                selected_row = df_veiculos[df_veiculos['id_veiculo'] == vehicle_id_to_edit].iloc[0]

            except:

                st.error("Dados do veículo não encontrados para edição.")

                del st.session_state['edit_vehicle_id']

                st.rerun()

                return

           

            data = selected_row.to_dict()

            data['data_compra'] = pd.to_datetime(data['data_compra'], errors='coerce').date() if pd.notna(data['data_compra']) else date.today()



            st.header(f"✏️ Editando Veículo ID: {vehicle_id_to_edit}")

            if st.button("Cancelar Edição / Voltar para Lista"):

                del st.session_state['edit_vehicle_id']

                st.rerun()

                return



        with st.form(key='manage_vehicle_form_edit'):

            st.caption("Informações Básicas")

            vehicle_name = st.text_input("Nome Amigável do Veículo (Obrigatório)", value=data['nome'], max_chars=100) # OBRIGATORIEDADE SÓ NO NOME

           

            col1, col2 = st.columns(2)

            with col1:

                placa = st.text_input("Placa (Opcional)", value=data['placa'], max_chars=10) # 🛑 ALTERAÇÃO: Placa opcional

           

            col3, col4 = st.columns(2)

            with col3:

                st.caption("Detalhes de Aquisição")

                current_year = date.today().year

                default_ano = int(data['ano']) if pd.notna(data.get('ano')) and str(data['ano']).isdigit() else current_year

                ano = st.number_input("Ano do Veículo", min_value=1900, max_value=current_year + 1, value=default_ano, step=1)

            with col4:

                st.caption(" ")

                default_valor = float(data['valor_pago']) if pd.notna(data.get('valor_pago')) else 0.0

                valor_pago = st.number_input("Valor Pago (R$)", min_value=0.0, format="%.2f", value=default_valor, step=1000.0)

           

            col5, col6 = st.columns(2)

            with col5:

                data_compra = st.date_input("Data de Compra", value=data['data_compra'])

           

            renavam_dummy = None



            submit_button = st.form_submit_button(label=submit_label)



            if submit_button:

                if not vehicle_name: # 🛑 ALTERAÇÃO: Placa não é mais obrigatória na validação

                    st.warning("O Nome é um campo obrigatório.")

                elif is_new_mode:

                    insert_vehicle(vehicle_name, placa, ano, valor_pago, data_compra)

                else:

                    update_vehicle(vehicle_id_to_edit, vehicle_name, placa, ano, valor_pago, data_compra)

       

        return



    # MODO LISTAGEM

    df_veiculos_listagem = get_data("veiculo")

    if not df_veiculos_listagem.empty:

        df_veiculos_listagem = df_veiculos_listagem.sort_values(by='nome')

        display_vehicle_table_and_actions(df_veiculos_listagem)

    else:

        st.info("Nenhum veículo cadastrado. Clique em '➕ Novo Veículo' para começar.")

        st.markdown("---")



def manage_prestador_form():

    """Formulário unificado para Cadastro e Manutenção de Prestadores."""

   

    prestador_id_to_edit = st.session_state.get('edit_prestador_id', None)

    is_editing = prestador_id_to_edit is not None

   

    # 🛑 ALTERAÇÃO: Remove o cabeçalho "Prestador" e mantém só o botão.

    if not is_editing:

        st.write("")

        _, col_button = st.columns([0.8, 0.2])

        with col_button:

             if st.button("➕ Novo Prestador", key="btn_novo_prestador_lista", help="Iniciar um novo cadastro de prestador"):

                 st.session_state['edit_prestador_id'] = 'NEW_MODE'

                 st.rerun()



    if is_editing or st.session_state.get('edit_prestador_id') == 'NEW_MODE':



        is_new_mode = st.session_state.get('edit_prestador_id') == 'NEW_MODE'

        df_prestadores = get_data("prestador")



        if is_new_mode:

            st.header("➕ Novo Prestador")

            submit_label = 'Cadastrar Prestador'

            data = {

                'empresa': '', 'telefone': '', 'nome_prestador': '', 'cnpj': '', 'email': '',

                'endereco': '', 'numero': '', 'cidade': '', 'bairro': '', 'cep': ''

            }

            if st.button("Cancelar Cadastro / Voltar para Lista"):

                del st.session_state['edit_prestador_id']

                st.rerun()

                return



        else: # MODO EDIÇÃO

            submit_label = 'Atualizar Prestador'

            try:

                selected_row = df_prestadores[df_prestadores['id_prestador'] == prestador_id_to_edit].iloc[0]

            except:

                st.error("Dados do prestador não encontrados para edição.")

                del st.session_state['edit_prestador_id']

                st.rerun()

                return



            data = selected_row.to_dict()

            st.header(f"✏️ Editando Prestador ID: {prestador_id_to_edit}")

           

            if st.button("Cancelar Edição / Voltar para Lista"):

                del st.session_state['edit_prestador_id']

                st.rerun()

                return



        with st.form(key='manage_prestador_form_edit'):

            st.caption("Dados da Empresa")

            company_name = st.text_input("Nome da Empresa/Oficina (Obrigatório)", value=data['empresa'], max_chars=100, disabled=(not is_new_mode))

           

            col_p1, col_p2 = st.columns(2)

            with col_p1:

                telefone = st.text_input("Telefone da Empresa", value=data['telefone'] or "", max_chars=20)

            with col_p2:

                nome_prestador = st.text_input("Nome do Prestador/Contato", value=data['nome_prestador'] or "", max_chars=100)

           

            col_p3, col_p4 = st.columns(2)

            with col_p3:

                cnpj = st.text_input("CNPJ", value=data['cnpj'] or "", max_chars=18)

            with col_p4:

                email = st.text_input("E-mail", value=data['email'] or "", max_chars=100)

           

            st.caption("Endereço")

            col_addr1, col_addr2 = st.columns([3, 1])

            with col_addr1:

                endereco = st.text_input("Endereço (Rua, Av.)", value=data['endereco'] or "", max_chars=255)

            with col_addr2:

                numero = st.text_input("Número", value=data['numero'] or "", max_chars=20)



            col_addr3, col_addr4, col_addr5 = st.columns([2, 2, 1])

            with col_addr3:

                bairro = st.text_input("Bairro", value=data['bairro'] or "", max_chars=100)

            with col_addr4:

                cidade = st.text_input("Cidade", value=data['cidade'] or "", max_chars=100)

            with col_addr5:

                cep = st.text_input("CEP", value=data['cep'] or "", max_chars=20)

               

            submit_button = st.form_submit_button(label=submit_label)

           

            if submit_button:

                if not company_name:

                    st.warning("O nome da empresa é obrigatório.")

                    return

               

                args = (company_name, telefone, nome_prestador, cnpj, email, endereco, numero, cidade, bairro, cep)



                if is_new_mode:

                    insert_new_prestador(*args)

                else:

                    update_prestador(prestador_id_to_edit, *args)

       

        return

   

    # MODO LISTAGEM

    df_prestadores_listagem = get_data("prestador")

    if not df_prestadores_listagem.empty:

        df_prestadores_listagem = df_prestadores_listagem.sort_values(by='empresa')

        display_prestador_table_and_actions(df_prestadores_listagem)

    else:

        st.info("Nenhum prestador cadastrado. Clique em '➕ Novo Prestador' para começar.")

        st.markdown("---")



def manage_service_form():

    """Gerencia o fluxo de Novo Cadastro, Edição e Listagem/Filtro de Serviços."""

   

    df_veiculos = get_data("veiculo").sort_values(by='nome')

    df_prestadores = get_data("prestador").sort_values(by='empresa')



    if df_veiculos.empty or df_prestadores.empty:

        st.warning("⚠️ Por favor, cadastre pelo menos um veículo e um prestador primeiro.")

        return

   

    df_veiculos['display_name'] = df_veiculos['nome'] + ' (' + df_veiculos['placa'] + ')'

    veiculos_map = pd.Series(df_veiculos.id_veiculo.values, index=df_veiculos.display_name).to_dict()

    veiculos_nomes = list(df_veiculos['display_name'])

    prestadores_nomes = list(df_prestadores['empresa'])

   

    service_id_to_edit = st.session_state.get('edit_service_id', None)

    is_editing = service_id_to_edit is not None

   

    # 🛑 ALTERAÇÃO: Remove o cabeçalho "Serviço" e mantém só o botão.

    if not is_editing:

        st.write("")

        _, col_button = st.columns([0.8, 0.2])

        with col_button:

             if st.button("➕ Novo Serviço", key="btn_novo_servico_lista", help="Iniciar um novo cadastro de serviço"):

                 st.session_state['edit_service_id'] = 'NEW_MODE'

                 st.rerun()



    if is_editing or st.session_state.get('edit_service_id') == 'NEW_MODE':

       

        is_new_mode = st.session_state.get('edit_service_id') == 'NEW_MODE'

       

        if is_new_mode:

             st.header("➕ Novo Serviço")

             submit_label = 'Cadastrar Serviço'

             data = {

                 'nome_servico': '', 'registro': '', 'data_servico': date.today(),

                 'garantia_dias': 90, 'valor': 0.0, 'km_realizado': 0, 'km_proxima_revisao': 0

             }

             selected_vehicle_idx = 0

             selected_prestador_idx = 0

             

             if st.button("Cancelar Cadastro / Voltar para Lista"):

                 del st.session_state['edit_service_id']

                 st.rerun()

                 return

       

        else: # MODO EDIÇÃO

            submit_label = 'Atualizar Serviço'

           

            try:

                df_data = get_data("servico", "id_servico", int(service_id_to_edit))

            except Exception as e:

                st.error(f"Erro ao buscar dados do serviço ID {service_id_to_edit}: {e}")

                df_data = pd.DataFrame()

           

            if df_data.empty:

                st.error("Dados do serviço não encontrados para edição.")

                del st.session_state['edit_service_id']

                st.rerun()

                return

               

            data = df_data.iloc[0].to_dict()

            st.header(f"✏️ Editando Serviço ID: {service_id_to_edit}")

           

            current_id_veiculo = int(data['id_veiculo'])

            current_id_prestador = int(data['id_prestador'])



            current_vehicle_row = df_veiculos[df_veiculos['id_veiculo'] == current_id_veiculo].iloc[0]

            current_vehicle_name = current_vehicle_row['display_name']

           

            current_prestador_name = df_prestadores[df_prestadores['id_prestador'] == current_id_prestador].iloc[0]['empresa']

           

            selected_vehicle_idx = veiculos_nomes.index(current_vehicle_name)

            selected_prestador_idx = prestadores_nomes.index(current_prestador_name) if current_prestador_name in prestadores_nomes else 0

           

            data['data_servico'] = pd.to_datetime(data['data_servico'], errors='coerce').date() if pd.notna(data['data_servico']) else date.today()



            if st.button("Cancelar Edição / Voltar para Lista"):

                del st.session_state['edit_service_id']

                st.rerun()

                return



        # --- FORMULÁRIO (Novo Cadastro ou Edição) ---

        with st.form(key='manage_service_form_edit'):

           

            st.caption("Veículo e Prestador")

            selected_vehicle = st.selectbox("Veículo", veiculos_nomes, index=selected_vehicle_idx, key="edit_service_vehicle", help="Comece a digitar para buscar o veículo.")

            selected_company_name = st.selectbox("Nome da Empresa/Oficina", prestadores_nomes, index=selected_prestador_idx, key='edit_service_company', help="Comece a digitar para buscar a empresa.")



            st.caption("Detalhes do Serviço")

            service_name = st.text_input("Nome do Serviço", value=data['nome_servico'], max_chars=100)

           

            registro = st.text_input("Registro Adicional (Ex: N° NF, Código)", value=data.get('registro') or "", max_chars=50)

           

            default_service_date = data['data_servico']

            service_date = st.date_input("Data do Serviço", value=default_service_date)

           

            col_s1, col_s2, col_s3, col_s4 = st.columns(4)

            with col_s1:

                default_garantia = int(data['garantia_dias']) if pd.notna(data.get('garantia_dias')) and str(data.get('garantia_dias')).isdigit() else 90

                garantia = st.number_input("Garantia (Dias)", min_value=0, max_value=3650, value=default_garantia, step=1)

            with col_s2:

                default_valor = float(data['valor']) if pd.notna(data.get('valor')) else 0.0

                value = st.number_input("Valor (R$)", min_value=0.0, format="%.2f", value=default_valor, step=10.0)

            with col_s3:

                default_km_current = int(data['km_realizado']) if pd.notna(data.get('km_realizado')) and str(data.get('km_realizado')).isdigit() else 0

                km_realizado = st.number_input("KM Realizado", min_value=0, value=default_km_current, step=100)

            with col_s4:

                default_km_next = int(data['km_proxima_revisao']) if pd.notna(data.get('km_proxima_revisao')) and str(data.get('km_proxima_revisao')).isdigit() else 0

                km_next = st.number_input("KM Próxima Revisão", min_value=0, value=default_km_next, step=1000)

               

            submit_button = st.form_submit_button(label=submit_label)



            if submit_button:

                if not selected_company_name:

                     st.error("Por favor, selecione uma Empresa/Oficina válida.")

                     return

                if not service_name:

                    st.warning("Preencha o Nome do Serviço.")

                    return



                new_id_veiculo = int(veiculos_map[selected_vehicle])

                prestador_row = df_prestadores[df_prestadores['empresa'] == selected_company_name]

                new_id_prestador = int(prestador_row.iloc[0]['id_prestador'])



                args_service = (

                    new_id_veiculo, new_id_prestador, service_name, service_date, garantia,

                    value, km_realizado, km_next, registro

                )



                if is_new_mode:

                    insert_service(*args_service)

                else:

                    update_service(int(service_id_to_edit), *args_service)

       

        return



    # --- MODO LISTAGEM / MANUTENÇÃO ---

    else:

        st.subheader("Manutenção de Serviços Existentes (Filtro e Edição)")

       

        col_filtro1, col_filtro2 = st.columns(2)

        with col_filtro1:

            date_end_default = date.today()

            date_start_default = date_end_default - timedelta(days=90)

            date_start = st.date_input("Filtrar por Data de Início", value=date_start_default)

        with col_filtro2:

            date_end = st.date_input("Filtrar por Data Final", value=date_end_default)



        df_servicos_listagem = get_full_service_data(date_start, date_end)

       

        if not df_servicos_listagem.empty:

            df_servicos_display = df_servicos_listagem[['id_servico', 'Veículo', 'Serviço', 'Data', 'Empresa']]

            display_service_table_and_actions(df_servicos_display)

        else:

            st.info("Nenhum serviço encontrado no período selecionado.")



# --- Layout Principal do Streamlit ---



def main():

    """Função principal que organiza as abas do aplicativo."""

    # 🚨 PASSO 1: INJETAR O CSS PERSONALIZADO (APLICA O TRUQUE DE RESPONSIVIDADE)

    st.markdown(f"<style>{CUSTOM_CSS}</style>", unsafe_allow_html=True)

   

    # Configuração de Página

    st.set_page_config(page_title="Controle Automotivo", layout="wide")

    st.title("🚗 Sistema de Controle Automotivo")



    # Inicialização do State

    if 'edit_service_id' not in st.session_state:

        st.session_state['edit_service_id'] = None

    if 'edit_vehicle_id' not in st.session_state:

        st.session_state['edit_vehicle_id'] = None

    if 'edit_prestador_id' not in st.session_state:

        st.session_state['edit_prestador_id'] = None



    # Abas

    tab_resumo, tab_historico, tab_cadastro = st.tabs(["📊 Resumo de Gastos", "📈 Histórico Detalhado", "➕ Cadastro e Manutenção"])



    # ----------------------------------------------------

    # 1. DASHBOARD: RESUMO DE GASTOS

    # ----------------------------------------------------

    with tab_resumo:

        st.header("Resumo de Gastos por Veículo")



        df_merged = get_full_service_data()



        if not df_merged.empty:

            df_resumo_raw = df_merged[['Veículo', 'Valor']].copy()

           

            resumo = df_resumo_raw.groupby('Veículo')['Valor'].sum().sort_values(ascending=False).reset_index()

            resumo.columns = ['Veículo', 'Total Gasto em Serviços']

           

            # Formata para R$

            resumo['Total Gasto em Serviços'] = resumo['Total Gasto em Serviços'].apply(lambda x: f'R$ {x:,.2f}'.replace('.', 'X').replace(',', '.').replace('X', ','))

           

            st.dataframe(resumo, hide_index=True, width='stretch')

           

        else:

            st.info("Nenhum dado de serviço encontrado para calcular o resumo.")



    # ----------------------------------------------------

    # 2. DASHBOARD: HISTÓRICO DETALHADO

    # ----------------------------------------------------

    with tab_historico:

        st.header("Histórico Completo de Serviços")

       

        df_historico = get_full_service_data()



        if not df_historico.empty:

            st.write("### Tabela Detalhada de Serviços")

           

            # 🛑 CORREÇÃO CRÍTICA DE TIPO PARA CÁLCULO DE DATA 🛑

            df_historico['data_vencimento'] = pd.to_datetime(df_historico['data_vencimento'], errors='coerce').fillna(pd.to_datetime(date.today()))

            df_historico['Data'] = pd.to_datetime(df_historico['Data'], errors='coerce').fillna(pd.to_datetime(date.today()))

           

            # Agora a linha funciona corretamente:

            df_historico['Dias para Vencer'] = (df_historico['data_vencimento'] - pd.to_datetime(date.today())).dt.days

           

            # Formatação de colunas

            df_historico['Data Serviço'] = df_historico['Data'].dt.strftime('%d-%m-%Y')

            df_historico['Data Vencimento'] = df_historico['data_vencimento'].dt.strftime('%d-%m-%Y')

           

            # O valor já é float, basta formatar.

            df_historico['Valor'] = df_historico['Valor'].apply(lambda x: f'R$ {x:,.2f}'.replace('.', 'X').replace(',', '.').replace('X', ','))

           

            # Seleção final das colunas

            df_historico_display = df_historico[[

                'Veículo', 'Serviço', 'Empresa', 'Data Serviço', 'Data Vencimento',

                'Dias para Vencer', 'Cidade', 'Valor', 'km_realizado', 'km_proxima_revisao'

            ]].rename(columns={

                'km_realizado': 'KM Realizado', 'km_proxima_revisao': 'KM Próxima Revisão'

            })

           

            st.dataframe(df_historico_display, width='stretch', hide_index=True)

           

        else:

            st.info("Nenhum serviço encontrado. Por favor, cadastre um serviço na aba 'Cadastro'.")



    # ----------------------------------------------------

    # 3. CADASTRO / MANUTENÇÃO UNIFICADA

    # ----------------------------------------------------

    with tab_cadastro:

        st.header("Gestão de Dados (Cadastro e Edição)")

       

        if 'cadastro_choice_unificado' not in st.session_state:

            st.session_state.cadastro_choice_unificado = "Veículo"

           

        choice = st.radio("Selecione a Tabela para Gerenciar:", ["Veículo", "Prestador", "Serviço"], horizontal=True, key='cadastro_choice_unificado')

        st.markdown("---")



        if choice == "Veículo":

            manage_vehicle_form()

        elif choice == "Prestador":

            manage_prestador_form()

        elif choice == "Serviço":

            manage_service_form()



if __name__ == '__main__':

    main()
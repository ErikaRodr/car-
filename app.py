import streamlit as st
import pandas as pd
from datetime import date, timedelta
import time
import gspread 
import numpy as np 
import uuid 

# ==============================================================================
# 🚨 CONFIGURAÇÃO GOOGLE SHEETS E CONEXÃO 🚨
# ==============================================================================

# Defina a URL ou ID da sua planilha AQUI
SHEET_ID = '1BNjgWhvEj8NbnGr4x7F42LW7QbQiG5kZ1FBHFr9Q-4g' 
PLANILHA_TITULO = 'Dados Automóvel' # ⬅️ USADO COMO FALLBACK
# Se o título falhar, tente remover o acento (Dados Automovel)

@st.cache_resource(ttl=3600) 
def get_gspread_client():
    """Retorna o cliente Gspread autenticado."""
    try:
        creds_info = st.secrets["gcp_service_account"]
        gc = gspread.service_account_from_dict(creds_info)
        return gc
    except KeyError:
        st.error("⚠️ Credenciais do Google Sheets não encontradas. Configure o 'gcp_service_account' em secrets.toml.")
        st.stop()
    except Exception as e:
        st.error(f"Erro de autenticação Gspread. {e}")
        st.stop()

@st.cache_data(ttl=5) 
def get_sheet_data(sheet_name):
    """Lê os dados de uma aba/sheet e retorna um DataFrame, com conversões iniciais."""
    
    expected_cols = {
        'veiculo': ['id_veiculo', 'nome', 'placa', 'renavam', 'ano', 'valor_pago', 'data_compra'],
        'prestador': ['id_prestador', 'empresa', 'telefone', 'nome_prestador', 'cnpj', 'email', 'endereco', 'numero', 'cidade', 'bairro', 'cep'],
        'servico': ['id_servico', 'id_veiculo', 'id_prestador', 'nome_servico', 'data_servico', 'garantia_dias', 'valor', 'km_realizado', 'km_proxima_revisao', 'registro', 'data_vencimento']
    }

    try:
        gc = get_gspread_client()
        
        # 🛑 REFAZENDO A LÓGICA DE CONEXÃO: Tenta por chave, se falhar, tenta por título.
        try:
            sh = gc.open_by_key(SHEET_ID)
        except Exception:
            # Planoi B, usando o TÍTULO (pode falhar devido a acentos)
            st.warning(f"Falha ao abrir por Chave (ID: {SHEET_ID}). Tentando por Título...")
            sh = gc.open(PLANILHA_TITULO)
        
        worksheet = sh.worksheet(sheet_name)
        
        data = worksheet.get_all_records()
        df = pd.DataFrame(data)

        if df.empty:
            return pd.DataFrame(columns=expected_cols.get(sheet_name, []))
        
        # CONVERSÃO INICIAL DE TIPOS CHAVE
        if sheet_name == 'veiculo':
            if 'valor_pago' in df.columns:
                df['valor_pago'] = pd.to_numeric(df['valor_pago'], errors='coerce').fillna(0.0).astype(float)
            if 'data_compra' in df.columns:
                df['data_compra'] = pd.to_datetime(df['data_compra'], errors='coerce')

        if sheet_name == 'servico':
            for col in ['valor', 'garantia_dias', 'km_realizado', 'km_proxima_revisao']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0) 
            for col in ['data_servico', 'data_vencimento']:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
        
        return df

    except gspread.WorksheetNotFound:
        st.error(f"A aba/sheet **'{sheet_name}'** não foi encontrada na planilha. Verifique a ortografia.")
        return pd.DataFrame(columns=expected_cols.get(sheet_name, []))
    except Exception as e:
        # Se a exceção for um 404/permissão, exibe a mensagem de erro específica.
        st.error(f"Falha Crítica ao conectar à planilha. Verifique se a Service Account tem permissão de EDITOR na planilha e tente limpar o cache: {e}")
        return pd.DataFrame(columns=expected_cols.get(sheet_name, []))


def write_sheet_data(sheet_name, df_new):
    """Sobrescreve a aba/sheet com o novo DataFrame (usado em Update/Delete)."""
    try:
        gc = get_gspread_client()
        
        # 🛑 REFAZENDO A LÓGICA DE CONEXÃO
        try:
            sh = gc.open_by_key(SHEET_ID)
        except Exception:
            sh = gc.open(PLANILHA_TITULO)

        worksheet = sh.worksheet(sheet_name)
        
        df_to_write = df_new
        data_to_write = [df_to_write.columns.tolist()] + df_to_write.values.tolist()
        
        worksheet.clear()
        worksheet.update('A1', data_to_write, value_input_option='USER_ENTERED')
        
        get_sheet_data.clear()
        
        return True

    except Exception as e:
        st.error(f"Erro ao escrever na sheet '{sheet_name}': {e}")
        return False

# [O restante do código (funções CRUD, get_full_service_data, etc.) permanece inalterado]

# --- O restante do código não foi alterado na lógica de dados ---

# --- CRUD Veiculo ---
def insert_vehicle(nome, placa, ano, valor_pago, data_compra):
    if placa:
        df_check = get_data('veiculo', 'placa', placa)
        if not df_check.empty:
            st.error(f"Placa '{placa}' já cadastrada.")
            return False
        
    data = {
        'id_veiculo': 0, 'nome': nome, 'placa': placa, 
        'ano': ano, 'valor_pago': float(valor_pago), 
        'data_compra': data_compra.isoformat() 
    }
    
    success, _ = execute_crud_operation('veiculo', data=data, id_col='id_veiculo', operation='insert')
    
    if success:
        st.success(f"Veiculo '{nome}' ({placa}) cadastrado com sucesso!")
        st.session_state['edit_vehicle_id'] = None
        st.rerun() 
    else:
        st.error("Falha ao cadastrar Veiculo.")

def update_vehicle(id_veiculo, nome, placa, ano, valor_pago, data_compra):
    if placa:
        df_check = get_data('veiculo', 'placa', placa)
        if not df_check.empty:
            found_id = str(df_check.iloc[0]['id_veiculo']) 
            if found_id != str(id_veiculo): 
                st.error(f"Placa '{placa}' já cadastrada para outro Veiculo (ID {found_id}).")
                return False

    data = {
        'nome': nome, 'placa': placa, 
        'ano': ano, 'valor_pago': float(valor_pago), 
        'data_compra': data_compra.isoformat() 
    }
    
    success, _ = execute_crud_operation('veiculo', data=data, id_col='id_veiculo', id_value=str(id_veiculo), operation='update')
    
    if success:
        st.success(f"Veiculo '{nome}' ({placa}) atualizado com sucesso!")
        st.session_state['edit_vehicle_id'] = None
        st.rerun() 
    else:
        st.error("Falha ao atualizar Veiculo.")

def delete_vehicle(id_veiculo):
    df_servicos = get_data('servico', 'id_veiculo', str(id_veiculo))
    if not df_servicos.empty:
        st.error("Não é possível remover o Veiculo. Existem serviços vinculados a ele.")
        return False
        
    success, _ = execute_crud_operation('veiculo', id_col='id_veiculo', id_value=str(id_veiculo), operation='delete')
    
    if success:
        st.success("Veiculo removido com sucesso!")
        time.sleep(1)
        st.rerun() 
    else:
        st.error("Falha ao remover Veiculo.")

# --- CRUD Prestador ---
def insert_new_prestador(empresa, telefone, nome_prestador, cnpj, email, endereco, numero, cidade, bairro, cep):
    df_check = get_data("prestador", "empresa", empresa)
    if not df_check.empty:
        st.warning(f"A empresa '{empresa}' já está cadastrada.")
        return False
        
    data = {
        'id_prestador': 0, 'empresa': empresa, 'telefone': telefone, 'nome_prestador': nome_prestador, 
        'cnpj': cnpj, 'email': email, 'endereco': endereco, 'numero': numero, 
        'cidade': cidade, 'bairro': bairro, 'cep': cep
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
    
    success, _ = execute_crud_operation('prestador', data=data, id_col='id_prestador', id_value=str(id_prestador), operation='update')
    
    if success:
        st.success(f"Prestador '{empresa}' atualizado com sucesso!")
        st.session_state['edit_prestador_id'] = None
        st.rerun() 
        return True
    return False

def delete_prestador(id_prestador):
    df_servicos = get_data('servico', 'id_prestador', str(id_prestador))
    if not df_servicos.empty:
        st.error("Não é possível remover o prestador. Existem serviços vinculados a ele.")
        return False

    success, _ = execute_crud_operation('prestador', id_col='id_prestador', id_value=str(id_prestador), operation='delete')
    
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
        id_prestador = str(df.iloc[0]['id_prestador'])
        update_prestador(id_prestador, empresa, telefone, nome_prestador, cnpj, email, endereco, numero, cidade, bairro, cep)
        st.info(f"Dados do Prestador '{empresa}' atualizados.")
        return id_prestador
    
    data = {
        'id_prestador': 0, 'empresa': empresa, 'telefone': telefone, 'nome_prestador': nome_prestador, 
        'cnpj': cnpj, 'email': email, 'endereco': endereco, 'numero': numero, 
        'cidade': cidade, 'bairro': bairro, 'cep': cep
    }
    success, new_id = execute_crud_operation('prestador', data=data, id_col='id_prestador', operation='insert')
    
    return new_id if success else None

# --- CRUD Serviço ---
def insert_service(id_veiculo, id_prestador, nome_servico, data_servico, garantia_dias, valor, km_realizado, km_proxima_revisao, registro):
    data_servico_dt = pd.to_datetime(data_servico)
    
    # CÁLCULO 1: DATA DE VENCIMENTO
    garantia_dias_int = int(garantia_dias) 
    data_vencimento = data_servico_dt + timedelta(days=garantia_dias_int)

    data = {
        'id_servico': 0, 
        'id_veiculo': str(id_veiculo), 
        'id_prestador': str(id_prestador), 
        'nome_servico': nome_servico, 
        'data_servico': data_servico_dt.date().isoformat(), 
        'garantia_dias': str(garantia_dias), 
        'valor': float(valor), 
        'km_realizado': str(km_realizado), 
        'km_proxima_revisao': str(km_proxima_revisao), 
        'registro': registro, 
        'data_vencimento': data_vencimento.date().isoformat() # Data de vencimento salva
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
    
    # CÁLCULO 1: DATA DE VENCIMENTO
    garantia_dias_int = int(garantia_dias) 
    data_vencimento = data_servico_dt + timedelta(days=garantia_dias_int)

    data = {
        'id_veiculo': str(id_veiculo), 
        'id_prestador': str(id_prestador), 
        'nome_servico': nome_servico, 
        'data_servico': data_servico_dt.date().isoformat(), 
        'garantia_dias': str(garantia_dias), 
        'valor': float(valor), 
        'km_realizado': str(km_realizado), 
        'km_proxima_revisao': str(km_proxima_revisao), 
        'registro': registro,
        'data_vencimento': data_vencimento.date().isoformat() # Data de vencimento salva
    }
    
    success, _ = execute_crud_operation('servico', data=data, id_col='id_servico', id_value=str(id_servico), operation='update')
    
    if success:
        st.success(f"Serviço '{nome_servico}' atualizado com sucesso!")
        if 'edit_service_id' in st.session_state:
            del st.session_state['edit_service_id']
        st.rerun() 
    else:
        st.error("Falha ao atualizar serviço.")

def delete_service(id_servico):
    success, _ = execute_crud_operation('servico', id_col='id_servico', id_value=str(id_servico), operation='delete')
    
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
    
    # Conversão de IDs para STRING para garantir o MERGE (Chave Estrangeira)
    df_servicos['id_veiculo'] = df_servicos['id_veiculo'].astype(str)
    df_servicos['id_prestador'] = df_servicos['id_prestador'].astype(str)
    df_veiculos['id_veiculo'] = df_veiculos['id_veiculo'].astype(str)
    df_prestadores['id_prestador'] = df_prestadores['id_prestador'].astype(str)
    
    # Conversões numéricas e de data
    df_servicos['valor'] = pd.to_numeric(df_servicos['valor'], errors='coerce').fillna(0.0).astype(float)
    df_servicos['garantia_dias'] = pd.to_numeric(df_servicos['garantia_dias'], errors='coerce').fillna(0.0).astype(int)
    df_servicos['km_realizado'] = pd.to_numeric(df_servicos['km_realizado'], errors='coerce').fillna(0.0).astype(int)
    df_servicos['km_proxima_revisao'] = pd.to_numeric(df_servicos['km_proxima_revisao'], errors='coerce').fillna(0.0).astype(int)
    
    # 1. JOIN com Veiculo
    df_merged = pd.merge(df_servicos, df_veiculos[['id_veiculo', 'nome', 'placa']], on='id_veiculo', how='left')
    
    # 2. JOIN com Prestador
    df_merged = pd.merge(df_merged, df_prestadores[['id_prestador', 'empresa', 'cidade']], on='id_prestador', how='left')
    
    # Renomeia colunas para o display
    df_merged = df_merged.rename(columns={'nome': 'Veiculo', 'placa': 'Placa', 'empresa': 'Empresa', 'cidade': 'Cidade', 'nome_servico': 'Serviço', 'data_servico': 'Data', 'valor': 'Valor'})
    
    # Converte colunas de data (sem NaT)
    df_merged['Data'] = pd.to_datetime(df_merged['Data'], errors='coerce')
    df_merged['data_vencimento'] = pd.to_datetime(df_merged['data_vencimento'], errors='coerce')

    # CÁLCULO 2: DIAS RESTANTES DA GARANTIA
    df_merged['Dias Restantes'] = (df_merged['data_vencimento'] - pd.to_datetime(date.today())).dt.days

    # 3. Filtragem por Data (se necessário)
    if date_start and date_end:
        df_merged = df_merged[(df_merged['Data'] >= pd.to_datetime(date_start)) & (df_merged['Data'] <= pd.to_datetime(date_end))]
        
    return df_merged.sort_values(by='Data', ascending=False)

# ==============================================================================
# 🚨 CSS PERSONALIZADO 🚨
# ==============================================================================
CUSTOM_CSS = """
/* Aplica display flex (alinhamento horizontal) e nowrap (não quebrar linha) 
   aos containers de coluna que envolvem os botões de ação (lápis e lixeira). */
.st-emotion-cache-12fmwza, .st-emotion-cache-n2e28m { 
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


# --- COMPONENTES DE DISPLAY ---

def display_vehicle_table_and_actions(df_veiculos_listagem):
    """Exibe a tabela de Veiculos com layout adaptado para celular."""
    st.subheader("Manutenção de Veiculos Existentes")
    st.markdown('---') 
    
    for index, row in df_veiculos_listagem.iterrows():
        id_veiculo = str(row['id_veiculo']) 
        
        col_data, col_actions = st.columns([0.85, 0.15]) 
        
        with col_data:
            st.markdown(f"**{row['nome']} ({row['placa'] or 'S/ Placa'})**") 
            st.markdown(f"Ano: **{row['ano']}**")
            st.markdown(f"Valor: **R$ {float(row['valor_pago']):.2f}**")
        
        with col_actions:
            col_act1, col_act2 = st.columns(2) 
            
            with col_act1:
                if st.button("✏️", key=f"edit_v_{id_veiculo}", help=f"Editar Veiculo ID {id_veiculo}"):
                    st.session_state['edit_vehicle_id'] = id_veiculo
                    st.rerun() 

            with col_act2:
                if st.button("🗑️", key=f"delete_v_{id_veiculo}", help=f"Excluir Veiculo ID {id_veiculo}"):
                    if st.session_state.get(f'confirm_delete_v_{id_veiculo}', False):
                        delete_vehicle(id_veiculo)
                    else:
                        st.session_state[f'confirm_delete_v_{id_veiculo}'] = True
                        st.rerun() 
        
        if st.session_state.get(f'confirm_delete_v_{id_veiculo}', False) and not st.session_state.get('edit_vehicle_id'):
            st.error(f"⚠️ **Clique novamente** no botão 🗑️ acima para confirmar a exclusão do ID {id_veiculo}.")
        
        st.markdown("---") 
            
def display_prestador_table_and_actions(df_prestadores_listagem):
    """Exibe a tabela de prestadores com layout adaptado para celular."""
    st.subheader("Manutenção de Prestadores Existentes")
    st.markdown('---') 
    
    for index, row in df_prestadores_listagem.iterrows():
        id_prestador = str(row['id_prestador']) 
        
        col_data, col_actions = st.columns([0.85, 0.15]) 
        
        with col_data:
            st.markdown(f"**{row['empresa']}**")
            st.markdown(f"Contato: **{row['nome_prestador'] or 'N/A'}**")
            st.markdown(f"Telefone: **{row['telefone'] or 'N/A'}**")
            st.markdown(f"Cidade: **{row['cidade'] or 'N/A'}**")
        
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
        id_servico = str(row['id_servico']) 
        
        data_display = row['Data'].strftime('%d-%m-%Y') if pd.notna(row['Data']) else 'N/A'
        
        col_data, col_actions = st.columns([0.85, 0.15]) 
        
        with col_data:
            st.markdown(f"**{row['Veiculo']}** - {row['Serviço']}")
            st.markdown(f"Data: **{data_display}**")
            st.markdown(f"Empresa: **{row['Empresa']}**")

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
    """Formulário unificado para Cadastro e Manutenção de Veiculos."""
    
    vehicle_id_to_edit = st.session_state.get('edit_vehicle_id', None)
    is_editing = vehicle_id_to_edit is not None
    
    if not is_editing:
        st.write("") 
        _, col_button = st.columns([0.8, 0.2]) 
        with col_button:
            if st.button("➕ Novo Veiculo", key="btn_novo_veiculo_lista", help="Iniciar um novo cadastro de Veiculo"):
                st.session_state['edit_vehicle_id'] = 'NEW_MODE'
                st.rerun()

    if is_editing or st.session_state.get('edit_vehicle_id') == 'NEW_MODE':
        
        is_new_mode = st.session_state.get('edit_vehicle_id') == 'NEW_MODE'
        df_veiculos = get_data("veiculo")

        if is_new_mode:
            st.header("➕ Novo Veiculo")
            submit_label = 'Cadastrar Veiculo'
            data = {
                'nome': '', 'placa': '', 'ano': date.today().year, 
                'valor_pago': 0.0, 'data_compra': date.today()
            }
            if st.button("Cancelar Cadastro / Voltar para Lista"):
                del st.session_state['edit_vehicle_id']
                st.rerun() 
                return
            
        else: # MODO EDIÇÃO
            submit_label = 'Atualizar Veiculo'
            
            try:
                selected_row = df_veiculos[df_veiculos['id_veiculo'] == str(vehicle_id_to_edit)].iloc[0]
            except:
                st.error("Dados do Veiculo não encontrados para edição.")
                del st.session_state['edit_vehicle_id']
                st.rerun() 
                return
            
            data = selected_row.to_dict()
            data['data_compra'] = pd.to_datetime(data['data_compra'], errors='coerce').date() if pd.notna(data['data_compra']) else date.today()

            st.header(f"✏️ Editando Veiculo ID: {vehicle_id_to_edit}")
            if st.button("Cancelar Edição / Voltar para Lista"):
                del st.session_state['edit_vehicle_id']
                st.rerun() 
                return

        with st.form(key='manage_vehicle_form_edit'):
            st.caption("Informações Básicas")
            vehicle_name = st.text_input("Nome Amigável do Veiculo (Obrigatório)", value=data['nome'], max_chars=100) 
            
            col1, col2 = st.columns(2)
            with col1:
                placa = st.text_input("Placa (Opcional)", value=data['placa'], max_chars=10) 
            
            col3, col4 = st.columns(2)
            with col3:
                st.caption("Detalhes de Aquisição")
                current_year = date.today().year
                default_ano = int(data['ano']) if pd.notna(data.get('ano')) and str(data['ano']).isdigit() else current_year
                ano = st.number_input("Ano do Veiculo", min_value=1900, max_value=current_year + 1, value=default_ano, step=1)
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
                if not vehicle_name: 
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
        st.info("Nenhum Veiculo cadastrado. Clique em '➕ Novo Veiculo' para começar.")
        st.markdown("---")

def manage_prestador_form():
    """Formulário unificado para Cadastro e Manutenção de Prestadores."""
    
    prestador_id_to_edit = st.session_state.get('edit_prestador_id', None)
    is_editing = prestador_id_to_edit is not None
    
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
                selected_row = df_prestadores[df_prestadores['id_prestador'] == str(prestador_id_to_edit)].iloc[0]
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
        st.warning("⚠️ Por favor, cadastre pelo menos um Veiculo e um prestador primeiro.")
        return
    
    df_veiculos['id_veiculo'] = df_veiculos['id_veiculo'].astype(str)
    df_prestadores['id_prestador'] = df_prestadores['id_prestador'].astype(str)
    
    df_veiculos['display_name'] = df_veiculos['nome'] + ' (' + df_veiculos['placa'] + ')'
    veiculos_map = pd.Series(df_veiculos.id_veiculo.values, index=df_veiculos.display_name).to_dict()
    veiculos_nomes = list(df_veiculos['display_name'])
    prestadores_nomes = list(df_prestadores['empresa']) 
    
    service_id_to_edit = st.session_state.get('edit_service_id', None)
    is_editing = service_id_to_edit is not None
    
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
                df_data = get_data("servico", "id_servico", str(service_id_to_edit))
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
            
            current_id_veiculo = str(data['id_veiculo'])
            current_id_prestador = str(data['id_prestador'])

            current_vehicle_row = df_veiculos[df_veiculos['id_veiculo'] == current_id_veiculo].iloc[0]
            current_vehicle_name = current_vehicle_row['display_name']
            
            current_prestador_row = df_prestadores[df_prestadores['id_prestador'] == current_id_prestador].iloc[0]
            current_prestador_name = current_prestador_row['empresa']
            
            selected_vehicle_idx = veiculos_nomes.index(current_vehicle_name)
            selected_prestador_idx = prestadores_nomes.index(current_prestador_name) if current_prestador_name in prestadores_nomes else 0
            
            data['data_servico'] = pd.to_datetime(data['data_servico'], errors='coerce').date() if pd.notna(data['data_servico']) else date.today()

            if st.button("Cancelar Edição / Voltar para Lista"):
                del st.session_state['edit_service_id']
                st.rerun() 
                return

        with st.form(key='manage_service_form_edit'):
            
            st.caption("Veiculo e Prestador")
            selected_vehicle = st.selectbox("Veiculo", veiculos_nomes, index=selected_vehicle_idx, key="edit_service_vehicle", help="Comece a digitar para buscar o Veiculo.")
            selected_company_name = st.selectbox("Nome da Empresa/Oficina", prestadores_nomes, index=selected_prestador_idx, key='edit_service_company', help="Comece a digitar para buscar a empresa.")

            st.caption("Detalhes do Serviço")
            service_name = st.text_input("Nome do Serviço", value=data['nome_servico'], max_chars=100)
            
            registro = st.text_input("Registro Adicional (Ex: N° NF, Código)", value=data.get('registro') or "", max_chars=50) 
            
            default_service_date = data['data_servico']
            service_date = st.date_input("Data do Serviço", value=default_service_date)
            
            col_s1, col_s2, col_s3, col_s4 = st.columns(4)
            with col_s1:
                default_garantia = int(float(data['garantia_dias'])) if pd.notna(data.get('garantia_dias')) else 90
                garantia = st.number_input("Garantia (Dias)", min_value=0, max_value=3650, value=default_garantia, step=1)
            with col_s2:
                default_valor = float(data['valor']) if pd.notna(data.get('valor')) else 0.0
                value = st.number_input("Valor (R$)", min_value=0.0, format="%.2f", value=default_valor, step=10.0)
            with col_s3:
                default_km_current = int(float(data['km_realizado'])) if pd.notna(data.get('km_realizado')) else 0
                km_realizado = st.number_input("KM Realizado", min_value=0, value=default_km_current, step=100)
            with col_s4:
                default_km_next = int(float(data['km_proxima_revisao'])) if pd.notna(data.get('km_proxima_revisao')) else 0
                km_next = st.number_input("KM Próxima Revisão", min_value=0, value=default_km_next, step=1000)
                
            submit_button = st.form_submit_button(label=submit_label)

            if submit_button:
                if not selected_company_name:
                    st.error("Por favor, selecione uma Empresa/Oficina válida.")
                    return
                if not service_name:
                    st.warning("Preencha o Nome do Serviço.")
                    return

                new_id_veiculo = veiculos_map[selected_vehicle]
                prestador_row = df_prestadores[df_prestadores['empresa'] == selected_company_name]
                new_id_prestador = prestador_row.iloc[0]['id_prestador']

                args_service = (
                    new_id_veiculo, new_id_prestador, service_name, service_date, garantia, 
                    value, km_realizado, km_next, registro
                )

                if is_new_mode:
                    insert_service(*args_service)
                else:
                    update_service(service_id_to_edit, *args_service)
        
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
            df_servicos_display = df_servicos_listagem[['id_servico', 'Veiculo', 'Serviço', 'Data', 'Empresa']]
            display_service_table_and_actions(df_servicos_display)
        else:
            st.info("Nenhum serviço encontrado no período selecionado.")

# --- Layout Principal do Streamlit ---

def main():
    """Função principal que organiza as abas do aplicativo."""
    
    st.markdown(f"<style>{CUSTOM_CSS}</style>", unsafe_allow_html=True)
    
    st.set_page_config(page_title="Controle Automotivo", layout="wide") 
    st.title("🚗 Sistema de Controle Automotivo")

    if 'edit_service_id' not in st.session_state:
        st.session_state['edit_service_id'] = None
    if 'edit_vehicle_id' not in st.session_state:
        st.session_state['edit_vehicle_id'] = None
    if 'edit_prestador_id' not in st.session_state:
        st.session_state['edit_prestador_id'] = None

    tab_resumo, tab_historico, tab_cadastro = st.tabs(["📊 Resumo de Gastos", "📈 Histórico Detalhado", "➕ Cadastro e Manutenção"])

    with tab_resumo:
        st.header("Resumo de Gastos por Veiculo")

        df_merged = get_full_service_data()

        if not df_merged.empty:
            df_resumo_raw = df_merged[['Veiculo', 'Valor']].copy()
            
            resumo = df_resumo_raw.groupby('Veiculo')['Valor'].sum().sort_values(ascending=False).reset_index()
            resumo.columns = ['Veiculo', 'Total Gasto em Serviços']
            
            resumo['Total Gasto em Serviços'] = resumo['Total Gasto em Serviços'].apply(lambda x: f'R$ {x:,.2f}'.replace('.', 'X').replace(',', '.').replace('X', ','))
            
            st.dataframe(resumo, hide_index=True, width='stretch')
            
        else:
            st.info("Nenhum dado de serviço encontrado para calcular o resumo.")

    with tab_historico:
        st.header("Histórico Completo de Serviços")
        
        df_historico = get_full_service_data()

        if not df_historico.empty:
            st.write("### Tabela Detalhada de Serviços")
            
            df_historico['Data Vencimento'] = df_historico['data_vencimento'].dt.strftime('%d-%m-%Y')
            df_historico['Data Serviço'] = df_historico['Data'].dt.strftime('%d-%m-%Y')
            
            df_historico['Valor'] = df_historico['Valor'].apply(lambda x: f'R$ {x:,.2f}'.replace('.', 'X').replace(',', '.').replace('X', ','))
            
            # Inclui o campo calculado "Dias Restantes"
            df_historico_display = df_historico[[
                'Veiculo', 'Serviço', 'Empresa', 'Data Serviço', 'Data Vencimento', 
                'Dias Restantes', 
                'Cidade', 'Valor', 'km_realizado', 'km_proxima_revisao'
            ]].rename(columns={
                'km_realizado': 'KM Realizado', 'km_proxima_revisao': 'KM Próxima Revisão'
            })
            
            st.dataframe(df_historico_display, width='stretch', hide_index=True)
            
        else:
            st.info("Nenhum serviço encontrado. Por favor, cadastre um serviço na aba 'Cadastro'.")

    with tab_cadastro:
        st.header("Gestão de Dados (Cadastro e Edição)")
        
        if 'cadastro_choice_unificado' not in st.session_state:
            st.session_state.cadastro_choice_unificado = "Veiculo" 
            
        choice = st.radio("Selecione a Tabela para Gerenciar:", ["Veiculo", "Prestador", "Serviço"], horizontal=True, key='cadastro_choice_unificado')
        st.markdown("---")

        if choice == "Veiculo":
            manage_vehicle_form()
        elif choice == "Prestador":
            manage_prestador_form()
        elif choice == "Serviço":
            manage_service_form()

if __name__ == '__main__':
    main()
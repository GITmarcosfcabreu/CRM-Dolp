# -*- coding: utf-8 -*-
"""
CRM Dolp Engenharia - Versão Completa com Todas as Melhorias

Funcionalidades implementadas:
- Sistema completo de CRM com funil de vendas
- Análise Prévia de Viabilidade com seleção de empresa referência
- Sumário Executivo com cálculos automáticos
- Volumetria por tipo de equipe
- Interface moderna sem cinzas
- Empresas referência para cálculos
"""

import tkinter as tk
from tkinter import ttk, messagebox, Toplevel, font
from PIL import Image, ImageTk
import requests
from io import BytesIO
import sqlite3
import os
import webbrowser
from tkcalendar import DateEntry
from datetime import datetime, timedelta
import json

# --- 1. CONFIGURAÇÕES GERAIS ---
DB_NAME = 'dolp_crm_final.db'
LOGO_PATH = "dolp_logo.png"
LOGO_URL = "https://mcusercontent.com/cfa43b95eeae85d65cf1366fb/images/a68e98a6-1595-5add-0b79-2e541e7faefa.png"

DOLP_COLORS = {
    'primary_blue': '#004b87', 'secondary_blue': "#4887ec", 'light_blue': '#eff6ff',
    'success_green': '#10b981', 'warning_orange': "#d02a2e", 'danger_red': '#ef4444',
    'white': '#ffffff', 'light_gray': '#ffffff', 'medium_gray': '#64748b',
    'dark_gray': '#1e293b', 'very_light_gray': '#f8fafc', 'dolp_cyan': '#06b6d4',
    'gradient_start': '#1e40af', 'gradient_end': '#3b82f6', 'shadow_color': '#00000020', 'border_color': '#e2e8f0'
}

ESTAGIOS_PIPELINE_DOLP = [
    "Clientes e Segmentos definidos (Playbook)",
    "Oportunidades",
    "Avaliação (Dolp)",
    "Qualificação (Cliente)",
    "Proposta Técnica",
    "Proposta Comercial",
    "Negociação",
    "Avaliação do Contrato",
    "Execução do Contrato",
    "Fidelização de Clientes",
    "Histórico"
]
BRAZILIAN_STATES = ["GO", "TO", "MT", "DF", "AC", "AL", "AP", "AM", "BA", "CE", "ES", "MA", "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE"]
SERVICE_TYPES = ["Linha Viva Cesto Duplo", "Linha Viva Cesto Simples", "Linha Morta Pesada 7 Elementos", "STC", "Plantão", "Perdas", "Motocicleta", "Atendimento Emergencial", "Novas Ligações", "Corte e Religação", "Subestações", "Grupos Geradores"]
INITIAL_SETORES = ["Distribuição", "Geração", "Transmissão", "Comercialização", "Industrial", "Corporativo"]
INITIAL_SEGMENTOS = ["Utilities", "Energia Renovável", "Óleo & Gás", "Manutenção Industrial", "Infraestrutura Elétrica", "Telecomunicações"]
CLIENT_STATUS_OPTIONS = ["Playbook e não cadastrado", "Playbook e cadastrado", "Cadastrado", "Não cadastrado"]

# --- 2. FUNÇÕES UTILITÁRIAS ---
def load_logo_image(size=(200, 75)):
    try:
        if os.path.exists(LOGO_PATH):
            img = Image.open(LOGO_PATH)
        else:
            response = requests.get(LOGO_URL, timeout=10)
            response.raise_for_status()
            img = Image.open(BytesIO(response.content))
            img.save(LOGO_PATH)
        img.thumbnail(size, Image.Resampling.LANCZOS)
        return ImageTk.PhotoImage(img)
    except Exception as e:
        print(f"Erro ao carregar logo: {e}")
        return None

def format_currency(value):
    try:
        return f"R$ {float(value):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except (ValueError, TypeError):
        return "R$ 0,00"

def open_link(url):
    try:
        if url and url != "---" and url.startswith(('http://', 'https://')):
            webbrowser.open(url)
        else:
            messagebox.showwarning("Link Inválido", "O link fornecido não é um endereço web válido (deve começar com http ou https).")
    except Exception as e:
        messagebox.showerror("Erro", f"Não foi possível abrir o link: {e}")

# --- 3. GERENCIADOR DE BANCO DE DADOS ---
class DatabaseManager:
    def __init__(self, db_name):
        self.db_name = db_name
        self._initialize_database()

    def _connect(self):
        conn = sqlite3.connect(self.db_name)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON;")
        return conn

    def _initialize_database(self):
        with self._connect() as conn:
            cursor = conn.cursor()
            # Unificando colunas na criação da tabela para evitar múltiplos ALTERs
            cursor.execute('''CREATE TABLE IF NOT EXISTS clientes (
                                id INTEGER PRIMARY KEY,
                                nome_empresa TEXT UNIQUE NOT NULL,
                                cnpj TEXT UNIQUE,
                                cidade TEXT,
                                estado TEXT,
                                setor_atuacao TEXT,
                                segmento_atuacao TEXT,
                                data_atualizacao TEXT,
                                link_portal TEXT,
                                status TEXT
                           )''')

            cursor.execute('CREATE TABLE IF NOT EXISTS pipeline_estagios (id INTEGER PRIMARY KEY, nome TEXT UNIQUE NOT NULL, ordem INTEGER)')

            # Tabela para Tipos de Serviço (anteriormente crm_tipos_equipe)
            cursor.execute('DROP TABLE IF EXISTS crm_servicos')
            cursor.execute('''CREATE TABLE crm_servicos (
                                id INTEGER PRIMARY KEY,
                                nome TEXT UNIQUE NOT NULL,
                                descricao TEXT,
                                categoria TEXT,
                                ativa INTEGER DEFAULT 1
                           )''')

            # Nova tabela para Tipos de Equipe, com Foreign Key para crm_servicos
            cursor.execute('DROP TABLE IF EXISTS crm_tipos_equipe')
            cursor.execute('''CREATE TABLE crm_tipos_equipe (
                                id INTEGER PRIMARY KEY,
                                nome TEXT NOT NULL,
                                servico_id INTEGER NOT NULL,
                                ativa INTEGER DEFAULT 1,
                                FOREIGN KEY (servico_id) REFERENCES crm_servicos(id) ON DELETE CASCADE
                           )''')

            # Tabela de Oportunidades Refatorada
            cursor.execute('DROP TABLE IF EXISTS oportunidades')
            cursor.execute('''CREATE TABLE oportunidades (
                                id INTEGER PRIMARY KEY,
                                titulo TEXT NOT NULL,
                                valor REAL DEFAULT 0,
                                cliente_id INTEGER NOT NULL,
                                estagio_id INTEGER NOT NULL,
                                data_criacao DATE,
                                -- Campos da Análise Prévia (APV)
                                tempo_contrato_meses INTEGER,
                                regional TEXT,
                                polo TEXT,
                                quantidade_bases INTEGER,
                                bases_nomes TEXT,          -- JSON com lista de nomes das bases
                                servicos_data TEXT,        -- NOVO JSON com a estrutura de serviços, equipes, qtd, volumetria, etc.
                                empresa_referencia TEXT,
                                -- Campos do Sumário Executivo
                                numero_edital TEXT,
                                data_abertura TEXT,
                                modalidade TEXT,
                                contato_principal TEXT,
                                link_documentos TEXT,
                                faturamento_estimado REAL,
                                duracao_contrato INTEGER,
                                total_pessoas INTEGER,
                                margem_contribuicao REAL,
                                descricao_detalhada TEXT,
                                FOREIGN KEY (cliente_id) REFERENCES clientes(id) ON DELETE CASCADE,
                                FOREIGN KEY (estagio_id) REFERENCES pipeline_estagios(id)
                           )''')

            cursor.execute('''CREATE TABLE IF NOT EXISTS crm_interacoes (id INTEGER PRIMARY KEY, oportunidade_id INTEGER NOT NULL, data_interacao TEXT, tipo TEXT, resumo TEXT, usuario TEXT,
                            FOREIGN KEY (oportunidade_id) REFERENCES oportunidades(id) ON DELETE CASCADE)''')
            cursor.execute('''CREATE TABLE IF NOT EXISTS crm_tarefas (id INTEGER PRIMARY KEY, oportunidade_id INTEGER NOT NULL, descricao TEXT, data_criacao TEXT, data_vencimento TEXT, responsavel TEXT, status TEXT,
                            FOREIGN KEY (oportunidade_id) REFERENCES oportunidades(id) ON DELETE CASCADE)''')
            cursor.execute('''CREATE TABLE IF NOT EXISTS crm_bases_alocadas (id INTEGER PRIMARY KEY, oportunidade_id INTEGER NOT NULL, nome_base TEXT, equipes_alocadas TEXT,
                            FOREIGN KEY (oportunidade_id) REFERENCES oportunidades(id) ON DELETE CASCADE)''')
            cursor.execute('DROP TABLE IF EXISTS crm_empresas_referencia')
            cursor.execute('''CREATE TABLE crm_empresas_referencia (id INTEGER PRIMARY KEY, nome_empresa TEXT NOT NULL, tipo_servico TEXT NOT NULL, valor_mensal REAL NOT NULL, volumetria_minima REAL NOT NULL, valor_por_pessoa REAL NOT NULL, ativa INTEGER DEFAULT 1, data_criacao TEXT DEFAULT CURRENT_TIMESTAMP)''')
            cursor.execute('CREATE TABLE IF NOT EXISTS crm_setores (id INTEGER PRIMARY KEY, nome TEXT UNIQUE NOT NULL)')
            cursor.execute('CREATE TABLE IF NOT EXISTS crm_segmentos (id INTEGER PRIMARY KEY, nome TEXT UNIQUE NOT NULL)')

            self._populate_initial_data(cursor)

    def _populate_initial_data(self, cursor):
        if cursor.execute("SELECT count(*) FROM pipeline_estagios").fetchone()[0] != len(ESTAGIOS_PIPELINE_DOLP):
            cursor.execute("PRAGMA foreign_keys = OFF;")
            cursor.execute("DELETE FROM pipeline_estagios")
            for i, nome in enumerate(ESTAGIOS_PIPELINE_DOLP):
                cursor.execute("INSERT OR IGNORE INTO pipeline_estagios (nome, ordem) VALUES (?, ?)", (nome, i))
            cursor.execute("PRAGMA foreign_keys = ON;")

        # Popula a nova tabela crm_servicos
        if cursor.execute("SELECT count(*) FROM crm_servicos").fetchone()[0] == 0:
            for service_type in SERVICE_TYPES:
                cursor.execute("INSERT INTO crm_servicos (nome, categoria) VALUES (?, ?)", (service_type, 'Serviços Elétricos'))

        if cursor.execute("SELECT count(*) FROM crm_setores").fetchone()[0] == 0:
            for setor in INITIAL_SETORES:
                cursor.execute("INSERT INTO crm_setores (nome) VALUES (?)", (setor,))
        if cursor.execute("SELECT count(*) FROM crm_segmentos").fetchone()[0] == 0:
            for segmento in INITIAL_SEGMENTOS:
                cursor.execute("INSERT INTO crm_segmentos (nome) VALUES (?)", (segmento,))

    # Métodos de Clientes
    def get_all_clients(self):
        with self._connect() as conn:
            return conn.execute("SELECT * FROM clientes ORDER BY nome_empresa").fetchall()

    def get_client_by_id(self, client_id):
        with self._connect() as conn:
            return conn.execute("SELECT * FROM clientes WHERE id = ?", (client_id,)).fetchone()

    def add_client(self, data):
        with self._connect() as conn:
            conn.execute("INSERT INTO clientes (nome_empresa, cnpj, cidade, estado, setor_atuacao, segmento_atuacao, data_atualizacao, link_portal, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", (data['nome_empresa'], data['cnpj'], data['cidade'], data['estado'], data['setor_atuacao'], data['segmento_atuacao'], data['data_atualizacao'], data['link_portal'], data['status']))

    def update_client(self, client_id, data):
        with self._connect() as conn:
            conn.execute("UPDATE clientes SET nome_empresa=?, cnpj=?, cidade=?, estado=?, setor_atuacao=?, segmento_atuacao=?, data_atualizacao=?, link_portal=?, status=? WHERE id=?", (data['nome_empresa'], data['cnpj'], data['cidade'], data['estado'], data['setor_atuacao'], data['segmento_atuacao'], data['data_atualizacao'], data['link_portal'], data['status'], client_id))

    # Métodos de Pipeline
    def get_pipeline_data(self):
        with self._connect() as conn:
            estagios = conn.execute("SELECT * FROM pipeline_estagios ORDER BY ordem").fetchall()
            oportunidades = conn.execute("SELECT o.id, o.titulo, o.valor, o.cliente_id, o.estagio_id, c.nome_empresa FROM oportunidades o JOIN clientes c ON o.cliente_id = c.id").fetchall()
            return estagios, oportunidades

    def get_opportunity_details(self, op_id):
        with self._connect() as conn:
            query = "SELECT o.*, c.nome_empresa, p.nome as estagio_nome FROM oportunidades o JOIN clientes c ON o.cliente_id = c.id JOIN pipeline_estagios p ON o.estagio_id = p.id WHERE o.id = ?"
            return conn.execute(query, (op_id,)).fetchone()

    def add_opportunity(self, data):
        with self._connect() as conn:
            query = '''INSERT INTO oportunidades (titulo, valor, cliente_id, estagio_id, data_criacao,
                        tempo_contrato_meses, regional, polo, quantidade_bases, bases_nomes, servicos_data, empresa_referencia,
                        numero_edital, data_abertura, modalidade, contato_principal, link_documentos,
                        faturamento_estimado, duracao_contrato, total_pessoas, margem_contribuicao, descricao_detalhada)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
            params = (data['titulo'], data['valor'], data['cliente_id'], data['estagio_id'], datetime.now().date(),
                      data.get('tempo_contrato_meses'), data.get('regional'), data.get('polo'), data.get('quantidade_bases'),
                      data.get('bases_nomes'), data.get('servicos_data'), data.get('empresa_referencia'),
                      data.get('numero_edital'), data.get('data_abertura'), data.get('modalidade'), data.get('contato_principal'),
                      data.get('link_documentos'), data.get('faturamento_estimado'), data.get('duracao_contrato'),
                      data.get('total_pessoas'), data.get('margem_contribuicao'), data.get('descricao_detalhada'))
            conn.execute(query, params)

    def update_opportunity(self, op_id, data):
        with self._connect() as conn:
            query = '''UPDATE oportunidades SET titulo=?, valor=?, cliente_id=?, estagio_id=?,
                        tempo_contrato_meses=?, regional=?, polo=?, quantidade_bases=?, bases_nomes=?, servicos_data=?, empresa_referencia=?,
                        numero_edital=?, data_abertura=?, modalidade=?, contato_principal=?, link_documentos=?,
                        faturamento_estimado=?, duracao_contrato=?, total_pessoas=?, margem_contribuicao=?, descricao_detalhada=?
                        WHERE id=?'''
            params = (data['titulo'], data['valor'], data['cliente_id'], data['estagio_id'],
                      data.get('tempo_contrato_meses'), data.get('regional'), data.get('polo'), data.get('quantidade_bases'),
                      data.get('bases_nomes'), data.get('servicos_data'), data.get('empresa_referencia'),
                      data.get('numero_edital'), data.get('data_abertura'), data.get('modalidade'), data.get('contato_principal'),
                      data.get('link_documentos'), data.get('faturamento_estimado'), data.get('duracao_contrato'),
                      data.get('total_pessoas'), data.get('margem_contribuicao'), data.get('descricao_detalhada'),
                      op_id)
            conn.execute(query, params)

    def update_opportunity_stage(self, op_id, new_stage_id):
        with self._connect() as conn:
            conn.execute("UPDATE oportunidades SET estagio_id = ? WHERE id = ?", (new_stage_id, op_id))

    def get_historico_oportunidades(self, filters=None):
        with self._connect() as conn:
            base_query = "SELECT o.id, o.titulo, o.valor, o.data_criacao, c.nome_empresa, p.nome as estagio_nome FROM oportunidades o JOIN clientes c ON o.cliente_id = c.id JOIN pipeline_estagios p ON o.estagio_id = p.id"
            conditions = []
            params = []

            if filters:
                if filters.get('cliente'):
                    conditions.append("c.nome_empresa = ?")
                    params.append(filters['cliente'])
                if filters.get('estagio'):
                    conditions.append("p.nome = ?")
                    params.append(filters['estagio'])
                if filters.get('valor_min'):
                    try:
                        conditions.append("o.valor >= ?")
                        params.append(float(filters['valor_min']))
                    except ValueError:
                        pass
                if filters.get('periodo'):
                    days = {'Última semana': 7, 'Último mês': 30, 'Últimos 3 meses': 90, 'Último ano': 365}
                    if filters['periodo'] in days:
                        date_limit = datetime.now() - timedelta(days=days[filters['periodo']])
                        conditions.append("o.data_criacao >= ?")
                        params.append(date_limit.strftime('%Y-%m-%d'))

            if conditions:
                base_query += " WHERE " + " AND ".join(conditions)

            base_query += " ORDER BY o.data_criacao DESC"
            return conn.execute(base_query, params).fetchall()

    def get_ultimo_resultado_oportunidade(self, op_id):
        with self._connect() as conn:
            query = "SELECT resumo FROM crm_interacoes WHERE oportunidade_id = ? AND tipo = 'Movimentação' ORDER BY id DESC LIMIT 1"
            result = conn.execute(query, (op_id,)).fetchone()
            if result and 'Resultado:' in result['resumo']:
                return result['resumo'].split('Resultado:')[1].strip()
            return '---'

    # Métodos de Tipos de Serviço
    def get_all_servicos(self):
        with self._connect() as conn:
            return conn.execute("SELECT * FROM crm_servicos ORDER BY nome").fetchall()

    def get_servico_by_id(self, servico_id):
        with self._connect() as conn:
            return conn.execute("SELECT * FROM crm_servicos WHERE id = ?", (servico_id,)).fetchone()

    def add_servico(self, data):
        with self._connect() as conn:
            conn.execute("INSERT INTO crm_servicos (nome, descricao, categoria, ativa) VALUES (?, ?, ?, ?)",(data['nome'], data['descricao'], data['categoria'], data['ativa']))

    def update_servico(self, servico_id, data):
        with self._connect() as conn:
            conn.execute("UPDATE crm_servicos SET nome=?, descricao=?, categoria=?, ativa=? WHERE id=?", (data['nome'], data['descricao'], data['categoria'], data['ativa'], servico_id))

    # Métodos de Tipos de Equipe
    def get_all_team_types(self):
        with self._connect() as conn:
            return conn.execute("SELECT te.*, s.nome as servico_nome FROM crm_tipos_equipe te JOIN crm_servicos s ON te.servico_id = s.id ORDER BY te.nome").fetchall()

    def get_team_types_for_service(self, servico_id):
        with self._connect() as conn:
            return conn.execute("SELECT * FROM crm_tipos_equipe WHERE servico_id = ? AND ativa = 1 ORDER BY nome", (servico_id,)).fetchall()

    def get_team_type_by_id(self, team_id):
        with self._connect() as conn:
            return conn.execute("SELECT * FROM crm_tipos_equipe WHERE id = ?", (team_id,)).fetchone()

    def add_team_type(self, data):
        with self._connect() as conn:
            conn.execute("INSERT INTO crm_tipos_equipe (nome, servico_id, ativa) VALUES (?, ?, ?)",(data['nome'], data['servico_id'], data['ativa']))

    def update_team_type(self, team_id, data):
        with self._connect() as conn:
            conn.execute("UPDATE crm_tipos_equipe SET nome=?, servico_id=?, ativa=? WHERE id=?", (data['nome'], data['servico_id'], data['ativa'], team_id))

    # Métodos de Interações
    def get_interactions_for_opportunity(self, op_id):
        with self._connect() as conn:
            return conn.execute("SELECT * FROM crm_interacoes WHERE oportunidade_id = ? ORDER BY data_interacao DESC", (op_id,)).fetchall()

    def add_interaction(self, data):
        with self._connect() as conn:
            conn.execute("INSERT INTO crm_interacoes (oportunidade_id, data_interacao, tipo, resumo, usuario) VALUES (?, ?, ?, ?, ?)", (data['oportunidade_id'], data['data_interacao'], data['tipo'], data['resumo'], data['usuario']))

    # Métodos de Tarefas
    def get_tasks_for_opportunity(self, op_id):
        with self._connect() as conn:
            return conn.execute("SELECT * FROM crm_tarefas WHERE oportunidade_id = ? ORDER BY status, data_vencimento", (op_id,)).fetchall()

    def add_task(self, data):
        with self._connect() as conn:
            conn.execute("INSERT INTO crm_tarefas (oportunidade_id, descricao, data_criacao, data_vencimento, responsavel, status) VALUES (?, ?, ?, ?, ?, ?)",(data['oportunidade_id'], data['descricao'], data['data_criacao'], data['data_vencimento'], data['responsavel'], data['status']))

    def update_task_status(self, task_id, status):
        with self._connect() as conn:
            conn.execute("UPDATE crm_tarefas SET status = ? WHERE id = ?", (status, task_id))

    # Métodos de Setores e Segmentos
    def get_all_setores(self):
        with self._connect() as conn:
            return [row['nome'] for row in conn.execute("SELECT nome FROM crm_setores ORDER BY nome").fetchall()]

    def add_setor(self, nome):
        with self._connect() as conn:
            conn.execute("INSERT INTO crm_setores (nome) VALUES (?)", (nome,))

    def delete_setor(self, nome):
        with self._connect() as conn:
            conn.execute("DELETE FROM crm_setores WHERE nome = ?", (nome,))

    def get_all_segmentos(self):
        with self._connect() as conn:
            return [row['nome'] for row in conn.execute("SELECT nome FROM crm_segmentos ORDER BY nome").fetchall()]

    def add_segmento(self, nome):
        with self._connect() as conn:
            conn.execute("INSERT INTO crm_segmentos (nome) VALUES (?)", (nome,))

    def delete_segmento(self, nome):
        with self._connect() as conn:
            conn.execute("DELETE FROM crm_segmentos WHERE nome = ?", (nome,))

    # Métodos de Bases Alocadas
    def get_bases_for_opportunity(self, op_id):
        with self._connect() as conn:
            return conn.execute("SELECT * FROM crm_bases_alocadas WHERE oportunidade_id = ?", (op_id,)).fetchall()

    def add_base_alocada(self, data):
        with self._connect() as conn:
            conn.execute("INSERT INTO crm_bases_alocadas (oportunidade_id, nome_base, equipes_alocadas) VALUES (?, ?, ?)", (data['oportunidade_id'], data['nome_base'], data['equipes_alocadas']))

    def delete_bases_for_opportunity(self, op_id):
        with self._connect() as conn:
            conn.execute("DELETE FROM crm_bases_alocadas WHERE oportunidade_id = ?", (op_id,))

    # Métodos de Empresas Referência
    def get_all_empresas_referencia(self):
        with self._connect() as conn:
            return conn.execute("SELECT * FROM crm_empresas_referencia ORDER BY nome_empresa, tipo_servico").fetchall()

    def get_empresa_referencia_by_id(self, empresa_id):
        with self._connect() as conn:
            return conn.execute("SELECT * FROM crm_empresas_referencia WHERE id = ?", (empresa_id,)).fetchone()

    def add_empresa_referencia(self, data):
        with self._connect() as conn:
            conn.execute("INSERT INTO crm_empresas_referencia (nome_empresa, tipo_servico, valor_mensal, volumetria_minima, valor_por_pessoa, ativa) VALUES (?, ?, ?, ?, ?, ?)", (data['nome_empresa'], data['tipo_servico'], data['valor_mensal'], data['volumetria_minima'], data['valor_por_pessoa'], data['ativa']))

    def update_empresa_referencia(self, empresa_id, data):
        with self._connect() as conn:
            conn.execute("UPDATE crm_empresas_referencia SET nome_empresa=?, tipo_servico=?, valor_mensal=?, volumetria_minima=?, valor_por_pessoa=?, ativa=? WHERE id=?", (data['nome_empresa'], data['tipo_servico'], data['valor_mensal'], data['volumetria_minima'], data['valor_por_pessoa'], data['ativa'], empresa_id))

    def get_empresa_referencia_by_tipo(self, tipo_servico):
        with self._connect() as conn:
            return conn.execute("SELECT * FROM crm_empresas_referencia WHERE tipo_servico = ? AND ativa = 1", (tipo_servico,)).fetchone()

    def get_empresa_referencia_by_nome_e_tipo(self, nome_empresa, tipo_servico):
        with self._connect() as conn:
            return conn.execute("SELECT * FROM crm_empresas_referencia WHERE nome_empresa = ? AND tipo_servico = ? AND ativa = 1", (nome_empresa, tipo_servico)).fetchone()

# --- 4. APLICAÇÃO PRINCIPAL ---
class CRMApp:
    def __init__(self, root):
        self.root = root
        self.db = DatabaseManager(DB_NAME)
        self.root.title("CRM Dolp Engenharia")
        self.root.geometry("1600x900")
        self.root.minsize(1280, 720)
        self.root.configure(bg=DOLP_COLORS['white'])
        self.logo_image = load_logo_image()
        self._configure_styles()
        self._create_main_container()
        self.show_main_menu()

    def _configure_styles(self):
        style = ttk.Style(self.root)
        style.theme_use('clam')

        # Estilos base modernos
        style.configure('TFrame', background=DOLP_COLORS['white'])
        style.configure('Header.TFrame', background=DOLP_COLORS['white'], relief='flat', borderwidth=0)

        # Botões modernos com gradiente visual
        style.configure('TButton', font=('Segoe UI', 11, 'normal'), padding=(15, 8), relief='flat', borderwidth=0)
        style.configure('Primary.TButton', background=DOLP_COLORS['primary_blue'], foreground='white', font=('Segoe UI', 11, 'bold'))
        style.map('Primary.TButton', background=[('active', DOLP_COLORS['secondary_blue']), ('pressed', DOLP_COLORS['gradient_start'])])

        style.configure('Success.TButton', background=DOLP_COLORS['success_green'], foreground='white', font=('Segoe UI', 11, 'bold'))
        style.map('Success.TButton', background=[('active', '#059669'), ('pressed', '#047857')])

        style.configure('Warning.TButton', background=DOLP_COLORS['warning_orange'], foreground='white', font=('Segoe UI', 11, 'bold'))
        style.map('Warning.TButton', background=[('active', '#d97706'), ('pressed', '#b45309')])

        style.configure('Danger.TButton', background=DOLP_COLORS['danger_red'], foreground='white', font=('Segoe UI', 11, 'bold'))
        style.map('Danger.TButton', background=[('active', '#dc2626'), ('pressed', '#b91c1c')])

        # Labels e outros elementos
        style.configure('TLabel', background=DOLP_COLORS['white'], foreground=DOLP_COLORS['dark_gray'], font=('Segoe UI', 10))
        style.configure('Header.TLabel', background=DOLP_COLORS['white'], foreground=DOLP_COLORS['primary_blue'], font=('Segoe UI', 16, 'bold'))
        style.configure('Title.TLabel', background=DOLP_COLORS['white'], foreground=DOLP_COLORS['dark_gray'], font=('Segoe UI', 14, 'bold'))

        # Estilos para LabelFrames
        style.configure('White.TLabelframe', background=DOLP_COLORS['white'], borderwidth=1, relief='solid')
        style.configure('White.TLabelframe.Label', background=DOLP_COLORS['white'], foreground=DOLP_COLORS['primary_blue'], font=('Segoe UI', 11, 'bold'))

        # Estilos para Labels específicos
        style.configure('Metric.White.TLabel', background=DOLP_COLORS['white'], foreground=DOLP_COLORS['primary_blue'], font=('Segoe UI', 12, 'bold'))
        style.configure('Value.White.TLabel', background=DOLP_COLORS['white'], foreground=DOLP_COLORS['dark_gray'], font=('Segoe UI', 11))
        style.configure('Link.White.TLabel', background=DOLP_COLORS['white'], foreground=DOLP_COLORS['secondary_blue'], font=('Segoe UI', 10, 'underline'))

        # Entry e Combobox
        style.configure('TEntry', fieldbackground='white', borderwidth=1, relief='solid')
        style.configure('TCombobox', fieldbackground='white', borderwidth=1, relief='solid')

        # Notebook (abas)
        style.configure('TNotebook', background=DOLP_COLORS['white'], borderwidth=0)
        style.configure('TNotebook.Tab', background=DOLP_COLORS['light_blue'], foreground=DOLP_COLORS['primary_blue'], padding=(20, 10), font=('Segoe UI', 11))
        style.map('TNotebook.Tab', background=[('selected', DOLP_COLORS['primary_blue']), ('active', DOLP_COLORS['secondary_blue'])], foreground=[('selected', 'white'), ('active', 'white')])

        # Treeview
        style.configure('Treeview', background='white', foreground=DOLP_COLORS['dark_gray'], font=('Segoe UI', 10), rowheight=25)
        style.configure('Treeview.Heading', background=DOLP_COLORS['primary_blue'], foreground='white', font=('Segoe UI', 10, 'bold'))
        style.map('Treeview', background=[('selected', DOLP_COLORS['light_blue'])])

        # --- NOVA SEÇÃO DE ESTILOS ADICIONADA ---
        # Estilos para os Cards de Oportunidade no Funil
        style.configure('Card.TFrame', background=DOLP_COLORS['light_blue'], relief='solid', borderwidth=2)
        style.configure('Card.TLabel', background=DOLP_COLORS['light_blue'], foreground=DOLP_COLORS['dark_gray'], font=('Segoe UI', 10))
        style.configure('Card.Title.TLabel', background=DOLP_COLORS['light_blue'], foreground=DOLP_COLORS['dark_gray'], font=('Segoe UI', 11, 'bold'))
        # ----------------------------------------

    def _create_main_container(self):
        # Container principal
        self.main_container = ttk.Frame(self.root, style='TFrame')
        self.main_container.pack(fill='both', expand=True)

        # Cabeçalho moderno
        header_frame = ttk.Frame(self.main_container, style='Header.TFrame', padding=(20, 15))
        header_frame.pack(fill='x', side='top')

        # Logo e título
        if self.logo_image:
            logo_label = ttk.Label(header_frame, image=self.logo_image, style='TLabel')
            logo_label.pack(side='left', padx=(0, 20))

        title_label = ttk.Label(header_frame, text="CRM Dolp Engenharia", style='Header.TLabel')
        title_label.pack(side='left')

        # Área de conteúdo
        self.content_frame = ttk.Frame(self.main_container, style='TFrame')
        self.content_frame.pack(fill='both', expand=True, padx=20, pady=(0, 20))

    def _create_scrollable_tab(self, parent_notebook, tab_text):
        tab_main_frame = ttk.Frame(parent_notebook)
        parent_notebook.add(tab_main_frame, text=tab_text)

        canvas = tk.Canvas(tab_main_frame, bg=DOLP_COLORS['white'], highlightthickness=0)
        scrollbar = ttk.Scrollbar(tab_main_frame, orient="vertical", command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas, style='TFrame', padding=20)

        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )

        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        return scrollable_frame

    def clear_content(self):
        # Limpar quaisquer eventos globais para evitar erros de widgets destruídos
        self.root.unbind_all("<MouseWheel>")
        for widget in self.content_frame.winfo_children():
            widget.destroy()

    def show_main_menu(self):
        self.clear_content()

        # Título da seção
        title_label = ttk.Label(self.content_frame, text="Menu Principal", style='Title.TLabel')
        title_label.pack(pady=(0, 30))

        # Container para botões
        buttons_frame = ttk.Frame(self.content_frame, style='TFrame')
        buttons_frame.pack(expand=True)

        # Botões do menu principal
        menu_buttons = [
            ("Funil de Vendas", self.show_kanban_view, 'Primary.TButton'),
            ("Clientes", self.show_clients_view, 'Primary.TButton'),
            ("Configurações do CRM", self.show_crm_settings, 'Warning.TButton')
        ]

        for i, (text, command, style) in enumerate(menu_buttons):
            btn = ttk.Button(buttons_frame, text=text, command=command, style=style, width=25)
            btn.pack(pady=10)

    def show_kanban_view(self):
        self.clear_content()

        # Título e botões
        title_frame = ttk.Frame(self.content_frame, style='TFrame')
        title_frame.pack(fill='x', pady=(0, 20))

        ttk.Label(title_frame, text="Funil de Vendas", style='Title.TLabel').pack(side='left')
        ttk.Button(title_frame, text="Histórico", command=self.show_historico_view, style='Warning.TButton').pack(side='right')
        ttk.Button(title_frame, text="Nova Oportunidade", command=lambda: self.show_opportunity_form(), style='Success.TButton').pack(side='right', padx=(0, 10))
        ttk.Button(title_frame, text="← Voltar", command=self.show_main_menu, style='TButton').pack(side='right', padx=(0, 10))

        # Frame principal com scrollbar vertical
        main_frame = ttk.Frame(self.content_frame, style='TFrame')
        main_frame.pack(fill='both', expand=True)

        # Canvas e scrollbar para o funil vertical
        canvas = tk.Canvas(main_frame, bg=DOLP_COLORS['white'], highlightthickness=0)
        v_scrollbar = ttk.Scrollbar(main_frame, orient="vertical", command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas, style='TFrame')

        scrollable_frame.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=v_scrollbar.set)

        # Posicionar canvas e scrollbar
        canvas.pack(side="left", fill="both", expand=True)
        v_scrollbar.pack(side="right", fill="y")

        # Obter dados do pipeline
        estagios, oportunidades = self.db.get_pipeline_data()
        clients = self.db.get_all_clients()

        # Container centralizado para o funil
        funil_container = ttk.Frame(scrollable_frame, style='TFrame')
        funil_container.pack(expand=True, pady=20)

        # Criar estágios do funil verticalmente
        for i, estagio in enumerate(estagios):
            # Frame para cada estágio
            stage_frame = ttk.Frame(funil_container, style='White.TLabelframe', padding=15)
            stage_frame.pack(fill='x', pady=10, padx=50)

            # Cabeçalho do estágio
            header_frame = ttk.Frame(stage_frame, style='TFrame')
            header_frame.pack(fill='x', pady=(0, 15))

            stage_title = ttk.Label(header_frame, text=estagio['nome'], style='Title.TLabel',
                                  font=('Segoe UI', 14, 'bold'), foreground=DOLP_COLORS['primary_blue'])
            stage_title.pack()

            # Tratamento especial para "Clientes e Segmentos definidos (Playbook)"
            if estagio['nome'] == "Clientes e Segmentos definidos (Playbook)":
                # Mostrar todos os clientes cadastrados
                clients_frame = ttk.Frame(stage_frame, style='TFrame')
                clients_frame.pack(fill='x')

                # Grid para organizar clientes em colunas
                col_count = 3
                for idx, client in enumerate(clients):
                    row = idx // col_count
                    col = idx % col_count

                    client_card = ttk.Frame(clients_frame, style='TFrame', padding=10)
                    client_card.grid(row=row, column=col, padx=5, pady=5, sticky='ew')
                    client_card.configure(relief='solid', borderwidth=1)

                    # Nome da empresa
                    ttk.Label(client_card, text=client['nome_empresa'], style='Value.White.TLabel',
                             font=('Segoe UI', 10, 'bold')).pack(anchor='w')

                    # Status
                    status = client['status'] or 'Não cadastrado'
                    ttk.Label(client_card, text=f"Status: {status}", style='Value.White.TLabel').pack(anchor='w')

                    # Setor
                    if client['setor_atuacao']:
                        ttk.Label(client_card, text=f"Setor: {client['setor_atuacao']}", style='Value.White.TLabel').pack(anchor='w')

                # Configurar colunas para expandir igualmente
                for col in range(col_count):
                    clients_frame.columnconfigure(col, weight=1)

            else:
                # Para outros estágios, mostrar oportunidades
                oportunidades_estagio = [op for op in oportunidades if op['estagio_id'] == estagio['id']]

                if oportunidades_estagio:
                    # Grid para organizar oportunidades em colunas
                    ops_frame = ttk.Frame(stage_frame, style='TFrame')
                    ops_frame.pack(fill='x')

                    col_count = 2
                    for idx, oportunidade in enumerate(oportunidades_estagio):
                        row = idx // col_count
                        col = idx % col_count

                        # --- ALTERAÇÃO APLICADA AQUI ---
                        op_card = ttk.Frame(ops_frame, style='Card.TFrame', padding=15)
                        op_card.grid(row=row, column=col, padx=10, pady=5, sticky='ew')

                        # Título da oportunidade (usando novo estilo)
                        title_label = ttk.Label(op_card, text=oportunidade['titulo'], style='Card.Title.TLabel')
                        title_label.pack(anchor='w')

                        # Cliente (usando novo estilo)
                        client_label = ttk.Label(op_card, text=f"Cliente: {oportunidade['nome_empresa']}", style='Card.TLabel')
                        client_label.pack(anchor='w')

                        # Valor (usando novo estilo)
                        valor_label = ttk.Label(op_card, text=f"Valor: {format_currency(oportunidade['valor'])}", style='Card.TLabel')
                        valor_label.pack(anchor='w')

                        # Frame para botões (usando novo estilo)
                        buttons_frame = ttk.Frame(op_card, style='Card.TFrame')
                        buttons_frame.pack(fill='x', pady=(10, 0))
                        # --------------------------------

                        # Botão Resultado (principal)
                        if estagio['nome'] != "Histórico":
                            result_btn = ttk.Button(buttons_frame, text="Resultado",
                                                   command=lambda op_id=oportunidade['id'], stage_id=estagio['id']: self.show_resultado_dialog(op_id, stage_id),
                                                   style='Primary.TButton')
                            result_btn.pack(side='left', padx=(0, 5))

                        # Bind duplo clique para ver detalhes
                        def on_double_click(event, op_id=oportunidade['id']):
                            self.show_opportunity_details(op_id)

                        op_card.bind("<Double-Button-1>", on_double_click)
                        title_label.bind("<Double-Button-1>", on_double_click)
                        client_label.bind("<Double-Button-1>", on_double_click)
                        valor_label.bind("<Double-Button-1>", on_double_click)

                    # Configurar colunas para expandir igualmente
                    for col in range(col_count):
                        ops_frame.columnconfigure(col, weight=1)
                else:
                    # Mensagem quando não há oportunidades
                    ttk.Label(stage_frame, text="Nenhuma oportunidade neste estágio",
                             style='Value.White.TLabel', font=('Segoe UI', 10, 'italic')).pack(pady=20)

        # Bind scroll do mouse
        def on_mousewheel(event):
            # Verificar se o widget canvas ainda existe antes de usá-lo
            if canvas.winfo_exists():
                canvas.yview_scroll(int(-1*(event.delta/120)), "units")

        self.root.bind_all("<MouseWheel>", on_mousewheel)

    def show_resultado_dialog(self, op_id, current_stage_id):
        """Mostra dialog para aprovar ou reprovar oportunidade"""
        dialog = Toplevel(self.root)
        dialog.title("Resultado da Avaliação")
        dialog.geometry("400x200")
        dialog.configure(bg=DOLP_COLORS['white'])
        dialog.transient(self.root)
        dialog.grab_set()

        # Centralizar dialog
        dialog.geometry("+%d+%d" % (self.root.winfo_rootx() + 50, self.root.winfo_rooty() + 50))

        main_frame = ttk.Frame(dialog, padding=20, style='TFrame')
        main_frame.pack(fill='both', expand=True)

        # Obter dados da oportunidade
        op_data = self.db.get_opportunity_details(op_id)

        ttk.Label(main_frame, text=f"Oportunidade: {op_data['titulo']}", style='Title.TLabel').pack(pady=(0, 10))
        ttk.Label(main_frame, text=f"Estágio Atual: {op_data['estagio_nome']}", style='Value.White.TLabel').pack(pady=(0, 20))

        ttk.Label(main_frame, text="Qual o resultado desta avaliação?", style='TLabel').pack(pady=(0, 20))

        # Botões de resultado
        buttons_frame = ttk.Frame(main_frame, style='TFrame')
        buttons_frame.pack(fill='x')

        def aprovar():
            # Mover para próximo estágio
            estagios = self.db.get_pipeline_data()[0]
            current_order = None
            for estagio in estagios:
                if estagio['id'] == current_stage_id:
                    current_order = estagio['ordem']
                    break

            # Encontrar próximo estágio
            next_stage = None
            for estagio in estagios:
                if estagio['ordem'] == current_order + 1:
                    next_stage = estagio
                    break

            if next_stage:
                self.db.update_opportunity_stage(op_id, next_stage['id'])
                # Registrar movimentação
                self.add_movement_record(op_id, op_data['estagio_nome'], next_stage['nome'], "Aprovado")
                messagebox.showinfo("Sucesso", f"Oportunidade aprovada e movida para: {next_stage['nome']}")
            else:
                messagebox.showinfo("Informação", "Esta oportunidade já está no último estágio.")

            dialog.destroy()
            self.show_kanban_view()

        def reprovar():
            # Mover para Histórico
            historico_stage = None
            estagios = self.db.get_pipeline_data()[0]
            for estagio in estagios:
                if estagio['nome'] == "Histórico":
                    historico_stage = estagio
                    break

            if historico_stage:
                self.db.update_opportunity_stage(op_id, historico_stage['id'])
                # Registrar movimentação
                self.add_movement_record(op_id, op_data['estagio_nome'], "Histórico", "Reprovado")
                messagebox.showinfo("Sucesso", "Oportunidade reprovada e movida para o Histórico.")

            dialog.destroy()
            self.show_kanban_view()

        ttk.Button(buttons_frame, text="✓ Aprovado", command=aprovar, style='Success.TButton').pack(side='left', padx=(0, 10))
        ttk.Button(buttons_frame, text="✗ Reprovado", command=reprovar, style='Danger.TButton').pack(side='left')
        ttk.Button(buttons_frame, text="Cancelar", command=dialog.destroy, style='TButton').pack(side='right')

    def add_movement_record(self, op_id, from_stage, to_stage, result):
        """Adiciona registro de movimentação no histórico"""
        data = {
            'oportunidade_id': op_id,
            'data_interacao': datetime.now().strftime('%d/%m/%Y %H:%M'),
            'tipo': 'Movimentação',
            'resumo': f"Movida de '{from_stage}' para '{to_stage}' - Resultado: {result}",
            'usuario': 'Sistema'
        }
        self.db.add_interaction(data)

    def show_historico_view(self):
        """Mostra histórico de oportunidades com filtros avançados"""
        self.clear_content()

        # Título e botões
        title_frame = ttk.Frame(self.content_frame, style='TFrame')
        title_frame.pack(fill='x', pady=(0, 20))

        ttk.Label(title_frame, text="Histórico de Oportunidades", style='Title.TLabel').pack(side='left')
        ttk.Button(title_frame, text="← Voltar", command=self.show_kanban_view, style='TButton').pack(side='right')

        # Frame de filtros
        filters_frame = ttk.LabelFrame(self.content_frame, text="Filtros de Busca", padding=15, style='White.TLabelframe')
        filters_frame.pack(fill='x', pady=(0, 20))

        # Primeira linha de filtros
        filter_row1 = ttk.Frame(filters_frame, style='TFrame')
        filter_row1.pack(fill='x', pady=(0, 10))

        # Cliente
        ttk.Label(filter_row1, text="Cliente:", style='TLabel').grid(row=0, column=0, sticky='w', padx=(0, 5))
        client_filter = ttk.Combobox(filter_row1, values=['Todos'] + [c['nome_empresa'] for c in self.db.get_all_clients()], width=20)
        client_filter.set('Todos')
        client_filter.grid(row=0, column=1, padx=(0, 20))

        # Estágio
        ttk.Label(filter_row1, text="Estágio:", style='TLabel').grid(row=0, column=2, sticky='w', padx=(0, 5))
        stage_filter = ttk.Combobox(filter_row1, values=['Todos'] + [e['nome'] for e in self.db.get_pipeline_data()[0]], width=25)
        stage_filter.set('Todos')
        stage_filter.grid(row=0, column=3, padx=(0, 20))

        # Resultado
        ttk.Label(filter_row1, text="Resultado:", style='TLabel').grid(row=0, column=4, sticky='w', padx=(0, 5))
        result_filter = ttk.Combobox(filter_row1, values=['Todos', 'Aprovado', 'Reprovado'], width=15)
        result_filter.set('Todos')
        result_filter.grid(row=0, column=5)

        # Segunda linha de filtros
        filter_row2 = ttk.Frame(filters_frame, style='TFrame')
        filter_row2.pack(fill='x', pady=(0, 10))

        # Período
        ttk.Label(filter_row2, text="Período:", style='TLabel').grid(row=0, column=0, sticky='w', padx=(0, 5))
        period_filter = ttk.Combobox(filter_row2, values=['Todos', 'Última semana', 'Último mês', 'Últimos 3 meses', 'Último ano'], width=20)
        period_filter.set('Todos')
        period_filter.grid(row=0, column=1, padx=(0, 20))

        # Valor mínimo
        ttk.Label(filter_row2, text="Valor mín. (R$):", style='TLabel').grid(row=0, column=2, sticky='w', padx=(0, 5))
        min_value_filter = ttk.Entry(filter_row2, width=15)
        min_value_filter.grid(row=0, column=3, padx=(0, 20))

        # Botão de busca
        search_btn = ttk.Button(filter_row2, text="🔍 Buscar", style='Primary.TButton',
                               command=lambda: self.apply_historico_filters(client_filter, stage_filter, result_filter, period_filter, min_value_filter, results_tree))
        search_btn.grid(row=0, column=4, padx=(20, 0))

        # Tabela de resultados
        results_frame = ttk.Frame(self.content_frame, style='TFrame')
        results_frame.pack(fill='both', expand=True)

        # Treeview para mostrar oportunidades
        columns = ('titulo', 'cliente', 'estagio', 'valor', 'data_criacao', 'ultimo_resultado')
        results_tree = ttk.Treeview(results_frame, columns=columns, show='headings', height=15)

        # Cabeçalhos
        results_tree.heading('titulo', text='Título')
        results_tree.heading('cliente', text='Cliente')
        results_tree.heading('estagio', text='Estágio Atual')
        results_tree.heading('valor', text='Valor (R$)')
        results_tree.heading('data_criacao', text='Data Criação')
        results_tree.heading('ultimo_resultado', text='Último Resultado')

        # Larguras das colunas
        results_tree.column('titulo', width=200)
        results_tree.column('cliente', width=150)
        results_tree.column('estagio', width=180)
        results_tree.column('valor', width=120, anchor='center')
        results_tree.column('data_criacao', width=100, anchor='center')
        results_tree.column('ultimo_resultado', width=120, anchor='center')

        # Scrollbars
        v_scrollbar = ttk.Scrollbar(results_frame, orient='vertical', command=results_tree.yview)
        h_scrollbar = ttk.Scrollbar(results_frame, orient='horizontal', command=results_tree.xview)
        results_tree.configure(yscrollcommand=v_scrollbar.set, xscrollcommand=h_scrollbar.set)

        # Posicionar elementos
        results_tree.grid(row=0, column=0, sticky='nsew')
        v_scrollbar.grid(row=0, column=1, sticky='ns')
        h_scrollbar.grid(row=1, column=0, sticky='ew')

        # Configurar grid
        results_frame.rowconfigure(0, weight=1)
        results_frame.columnconfigure(0, weight=1)

        # Duplo clique para ver detalhes
        def on_item_double_click(event):
            selection = results_tree.selection()
            if selection:
                item = results_tree.item(selection[0])
                op_id = item['tags'][0]  # ID da oportunidade armazenado nas tags
                self.show_opportunity_details(op_id)

        results_tree.bind('<Double-1>', on_item_double_click)

        # Carregar todas as oportunidades inicialmente
        self.load_historico_data(results_tree)

    def apply_historico_filters(self, client_filter, stage_filter, result_filter, period_filter, min_value_filter, results_tree):
        """Aplica filtros e atualiza a tabela de histórico"""
        filters = {
            'cliente': client_filter.get() if client_filter.get() != 'Todos' else None,
            'estagio': stage_filter.get() if stage_filter.get() != 'Todos' else None,
            'resultado': result_filter.get() if result_filter.get() != 'Todos' else None,
            'periodo': period_filter.get() if period_filter.get() != 'Todos' else None,
            'valor_min': min_value_filter.get().strip() if min_value_filter.get().strip() else None
        }

        self.load_historico_data(results_tree, filters)

    def load_historico_data(self, tree, filters=None):
        """Carrega dados do histórico na tabela"""
        # Limpar tabela
        for item in tree.get_children():
            tree.delete(item)

        # Obter oportunidades com filtros
        oportunidades = self.db.get_historico_oportunidades(filters)

        for op in oportunidades:
            # Obter último resultado da oportunidade
            ultimo_resultado = self.db.get_ultimo_resultado_oportunidade(op['id'])

            data_criacao_str = '---'
            if op['data_criacao']:
                try:
                    data_obj = datetime.strptime(op['data_criacao'], '%Y-%m-%d')
                    data_criacao_str = data_obj.strftime('%d/%m/%Y')
                except (ValueError, TypeError):
                    data_criacao_str = op['data_criacao']


            tree.insert('', 'end',
                       values=(
                           op['titulo'],
                           op['nome_empresa'],
                           op['estagio_nome'],
                           format_currency(op['valor']),
                           data_criacao_str,
                           ultimo_resultado or '---'
                       ),
                       tags=(str(op['id']),))  # Armazenar ID nas tags

    def show_opportunity_form(self, op_id=None, client_to_prefill=None):
        form_win = Toplevel(self.root)
        form_win.title("Nova Oportunidade" if not op_id else "Editar Oportunidade")
        form_win.geometry("1100x800") # Aumentado para melhor visualização
        form_win.configure(bg=DOLP_COLORS['white'])

        # --- Estrutura Principal da Janela ---
        # Notebook para as abas (ocupa a maior parte da janela)
        notebook = ttk.Notebook(form_win)
        notebook.pack(fill='both', expand=True, padx=10, pady=(10, 0))

        # Botões de Ação (sempre visíveis na parte inferior)
        buttons_frame = ttk.Frame(form_win, padding=(20, 10, 20, 20))
        buttons_frame.pack(side='bottom', fill='x')
        buttons_frame.columnconfigure(0, weight=1) # Spacer para empurrar botões para a direita

        # --- Criação das Abas com Rolagem ---
        # Aba 1: Análise Prévia de Viabilidade
        analise_frame = self._create_scrollable_tab(notebook, '  Análise Prévia de Viabilidade  ')

        # Aba 2: Sumário Executivo
        sumario_frame = self._create_scrollable_tab(notebook, '  Sumário Executivo  ')

        # Preparar dados
        clients = self.db.get_all_clients()
        client_map = {c['nome_empresa']: c['id'] for c in clients}
        estagios = self.db.get_pipeline_data()[0]
        estagio_map = {e['nome']: e['id'] for e in estagios}
        servicos = self.db.get_all_servicos()
        servico_map = {s['nome']: s['id'] for s in servicos}
        servico_names = [t['nome'] for t in servicos]

        entries = {}

        # === ANÁLISE PRÉVIA DE VIABILIDADE (CONTEÚDO DENTRO DA ABA DE ROLAGEM) ===
        analise_frame.columnconfigure(1, weight=1)

        # Informações Básicas
        info_basicas = ttk.LabelFrame(analise_frame, text="Informações Básicas", padding=15, style='White.TLabelframe')
        info_basicas.pack(fill='x', pady=(0, 10))
        info_basicas.columnconfigure(1, weight=1)

        basic_fields = [
            ("Título:*", "titulo", "entry"),
            ("Cliente:*", "cliente_id", "combobox", [c['nome_empresa'] for c in clients]),
            ("Estágio:*", "estagio_id", "combobox", [e['nome'] for e in estagios if e['nome'] != 'Histórico']),
            ("Valor Estimado (R$):", "valor", "entry")
        ]

        for i, field_info in enumerate(basic_fields):
            text, key = field_info[0], field_info[1]
            ttk.Label(info_basicas, text=text, style='TLabel').grid(row=i, column=0, sticky='w', pady=5, padx=5)

            if len(field_info) > 3:  # combobox com valores
                widget = ttk.Combobox(info_basicas, values=field_info[3], state='readonly')
            elif field_info[2] == "combobox":
                widget = ttk.Combobox(info_basicas, state='readonly')
            else:
                widget = ttk.Entry(info_basicas)

            widget.grid(row=i, column=1, sticky='ew', pady=5, padx=5)
            entries[key] = widget

        # Checklist de Qualificação de Oportunidade
        checklist_frame = ttk.LabelFrame(analise_frame, text="Checklist de Qualificação de Oportunidade", padding=15, style='White.TLabelframe')
        checklist_frame.pack(fill='x', pady=(0, 10))
        checklist_frame.columnconfigure(1, weight=1)

        # --- Seção de Configuração de Serviços e Equipes (Lógica Nova) ---

        # Frame para as configurações dinâmicas de serviço/equipe
        servicos_config_frame = ttk.Frame(analise_frame, padding=(0, 10))
        servicos_config_frame.pack(fill='x', expand=True)

        # Dicionários para manter o estado da UI dinâmica
        servico_frames = {}
        servico_equipes_data = {}
        entries['servicos_data'] = servico_equipes_data

        def _add_equipe_row(servico_id, servico_nome, container):
            row_frame = ttk.Frame(container, padding=(0, 5))
            row_frame.pack(fill='x', expand=True, pady=2)

            row_widgets = {}

            team_types = self.db.get_team_types_for_service(servico_id)
            team_type_names = [t['nome'] for t in team_types]

            base_widgets = entries.get('bases_nomes_widgets', [])
            base_names = [b.get().strip() for b in base_widgets if b.get().strip()]

            # Widgets da linha
            ttk.Label(row_frame, text="Tipo de Equipe:").pack(side='left', padx=(0,5))
            tipo_combo = ttk.Combobox(row_frame, values=team_type_names, state='readonly', width=20)
            tipo_combo.pack(side='left', padx=5)
            row_widgets['tipo_combo'] = tipo_combo

            ttk.Label(row_frame, text="Qtd:").pack(side='left', padx=(5,0))
            qtd_entry = ttk.Entry(row_frame, width=5)
            qtd_entry.pack(side='left', padx=5)
            row_widgets['qtd_entry'] = qtd_entry

            ttk.Label(row_frame, text="Volumetria:").pack(side='left', padx=(5,0))
            vol_entry = ttk.Entry(row_frame, width=8)
            vol_entry.pack(side='left', padx=5)
            row_widgets['vol_entry'] = vol_entry

            ttk.Label(row_frame, text="Base:").pack(side='left', padx=(5,0))
            base_combo = ttk.Combobox(row_frame, values=base_names, state='readonly', width=15)
            base_combo.pack(side='left', padx=5)
            row_widgets['base_combo'] = base_combo

            def remove_row():
                servico_equipes_data[servico_nome].remove(row_widgets)
                row_frame.destroy()

            remove_button = ttk.Button(row_frame, text="X", command=remove_row, style='Danger.TButton', width=3)
            remove_button.pack(side='right', padx=5)

            servico_equipes_data[servico_nome].append(row_widgets)

        def _update_servicos_ui():
            for servico_nome, var in tipos_vars.items():
                if var.get():
                    if servico_nome not in servico_frames:
                        servico_id = servico_map[servico_nome]

                        frame = ttk.LabelFrame(servicos_config_frame, text=f"Configuração para: {servico_nome}", padding=10)
                        frame.pack(fill='x', expand=True, pady=5, padx=5)
                        servico_frames[servico_nome] = frame

                        equipes_container = ttk.Frame(frame)
                        equipes_container.pack(fill='x', expand=True)

                        add_button = ttk.Button(frame, text="Adicionar Equipe", style='Success.TButton',
                                                command=lambda s_id=servico_id, s_nome=servico_nome, c=equipes_container: _add_equipe_row(s_id, s_nome, c))
                        add_button.pack(pady=5, anchor='w')

                        servico_equipes_data[servico_nome] = []
                else:
                    if servico_nome in servico_frames:
                        servico_frames[servico_nome].destroy()
                        del servico_frames[servico_nome]
                        if servico_nome in servico_equipes_data:
                            del servico_equipes_data[servico_nome]

        # --- Início da UI do Checklist ---

        # Tipos de Serviço (Checkboxes)
        ttk.Label(checklist_frame, text="Tipos de Serviço:", font=('Segoe UI', 10, 'bold')).grid(row=0, column=0, sticky='nw', pady=5, padx=5)
        tipos_frame = ttk.Frame(checklist_frame)
        tipos_frame.grid(row=0, column=1, sticky='ew', pady=5, padx=5)
        tipos_vars = {}
        col_count = 3
        row = 0
        col = 0
        for name in servico_names:
            var = tk.BooleanVar()
            tipos_vars[name] = var
            cb = ttk.Checkbutton(tipos_frame, text=name, variable=var, command=_update_servicos_ui)
            cb.grid(row=row, column=col, sticky='w', padx=5, pady=2)
            col += 1
            if col >= col_count:
                col = 0
                row += 1
        entries['tipos_servico_vars'] = tipos_vars
        start_row_after_services = row + 1

        # Outros campos do checklist
        checklist_fields = [("Tempo de Contrato (meses):", "tempo_contrato_meses", "entry"), ("Regional:", "regional", "entry"), ("Polo:", "polo", "entry")]
        for i, (text, key, widget_type) in enumerate(checklist_fields):
            ttk.Label(checklist_frame, text=text).grid(row=start_row_after_services + i, column=0, sticky='w', pady=5, padx=5)
            entry = ttk.Entry(checklist_frame)
            entry.grid(row=start_row_after_services + i, column=1, sticky='ew', pady=5, padx=5)
            entries[key] = entry

        # Empresa Referência
        empresa_row = start_row_after_services + len(checklist_fields)
        ttk.Label(checklist_frame, text="Empresa Referência:", font=('Segoe UI', 10, 'bold')).grid(row=empresa_row, column=0, sticky='w', pady=5, padx=5)
        empresas_ref = self.db.get_all_empresas_referencia()
        empresa_names = sorted(list(set([emp['nome_empresa'] for emp in empresas_ref])))
        empresa_combo = ttk.Combobox(checklist_frame, values=empresa_names, state='readonly')
        empresa_combo.grid(row=empresa_row, column=1, sticky='ew', pady=5, padx=5)
        entries['empresa_referencia'] = empresa_combo

        # Quantidade de Bases
        bases_row = empresa_row + 1
        ttk.Label(checklist_frame, text="Quantidade de Bases:", font=('Segoe UI', 10, 'bold')).grid(row=bases_row, column=0, sticky='w', pady=5, padx=5)
        bases_input_frame = ttk.Frame(checklist_frame)
        bases_input_frame.grid(row=bases_row, column=1, sticky='ew', pady=5, padx=5)
        bases_fields_frame = ttk.Frame(checklist_frame)
        bases_fields_frame.grid(row=bases_row + 1, column=0, columnspan=2, sticky='ew', pady=5, padx=5)

        base_name_entries = []
        entries['bases_nomes_widgets'] = base_name_entries

        def _update_base_fields_ui():
            for widget in bases_fields_frame.winfo_children():
                widget.destroy()
            base_name_entries.clear()
            try:
                num_bases = int(bases_spinbox.get())
            except (ValueError, tk.TclError):
                num_bases = 0

            for i in range(num_bases):
                base_frame = ttk.Frame(bases_fields_frame)
                base_frame.pack(fill='x', pady=2, padx=5)
                ttk.Label(base_frame, text=f"Base {i+1}:", width=15).pack(side='left')
                entry = ttk.Entry(base_frame, width=30)
                entry.pack(side='left', fill='x', expand=True, padx=5)
                base_name_entries.append(entry)

        def _update_base_fields_ui_and_combos():
            _update_base_fields_ui()
            # Atualizar comboboxes de base em todas as linhas de equipe existentes
            base_widgets = entries.get('bases_nomes_widgets', [])
            base_names = [b.get().strip() for b in base_widgets if b.get().strip()]
            for service, equipe_rows in servico_equipes_data.items():
                for row in equipe_rows:
                    if 'base_combo' in row:
                        row['base_combo']['values'] = base_names

        bases_spinbox = ttk.Spinbox(bases_input_frame, from_=0, to=50, width=10, command=_update_base_fields_ui_and_combos)
        bases_spinbox.pack(side='left')
        entries['quantidade_bases'] = bases_spinbox


        # === SUMÁRIO EXECUTIVO (CONTEÚDO DENTRO DA ABA DE ROLAGEM) ===
        sumario_frame.columnconfigure(1, weight=1)

        # Informações do Edital
        edital_frame = ttk.LabelFrame(sumario_frame, text="Informações do Edital", padding=15, style='White.TLabelframe')
        edital_frame.pack(fill='x', pady=(0, 10))
        edital_frame.columnconfigure(1, weight=1)

        edital_fields = [
            ("Número do Edital:", "numero_edital", "entry"),
            ("Data de Abertura:", "data_abertura", "date"),
            ("Modalidade:", "modalidade", "entry"),
            ("Contato Principal:", "contato_principal", "entry"),
            ("Link da Pasta de Documentos:", "link_documentos", "entry")
        ]

        for i, (text, key, widget_type) in enumerate(edital_fields):
            ttk.Label(edital_frame, text=text).grid(row=i, column=0, sticky='w', pady=5, padx=5)
            if widget_type == 'date':
                entry = DateEntry(edital_frame, date_pattern='dd/mm/yyyy', width=20)
            else:
                entry = ttk.Entry(edital_frame)
            entry.grid(row=i, column=1, sticky='ew', pady=5, padx=5)
            entries[key] = entry

        # Informações de Cotação
        cotacao_frame = ttk.LabelFrame(sumario_frame, text="Informações de Cotação", padding=15, style='White.TLabelframe')
        cotacao_frame.pack(fill='x', pady=(0, 10))
        cotacao_frame.columnconfigure(1, weight=1)

        cotacao_fields = [
            ("Faturamento Estimado (R$):", "faturamento_estimado", "entry"),
            ("Duração do Contrato (meses):", "duracao_contrato", "entry"),
            ("MOD (Mão de Obra Direta):", "mod", "entry"),
            ("MOI (Mão de Obra Indireta):", "moi", "entry"),
            ("Total de Pessoas:", "total_pessoas", "entry"),
            ("Margem de Contribuição (%):", "margem_contribuicao", "entry"),
        ]

        # Função para calcular total de pessoas automaticamente
        def calcular_total_pessoas():
            try:
                mod = float(entries['mod'].get() or '0')
                moi = float(entries['moi'].get() or '0')
                total = mod + moi
                entries['total_pessoas'].delete(0, 'end')
                entries['total_pessoas'].insert(0, str(int(total)))
            except ValueError:
                pass

        for i, (text, key, widget_type) in enumerate(cotacao_fields):
            ttk.Label(cotacao_frame, text=text).grid(row=i, column=0, sticky='w', pady=5, padx=5)
            if widget_type == 'date':
                entry = DateEntry(cotacao_frame, date_pattern='dd/mm/yyyy', width=20)
            else:
                entry = ttk.Entry(cotacao_frame)
                # Bind para calcular total automaticamente quando MOD ou MOI mudarem
                if key in ['mod', 'moi']:
                    entry.bind('<KeyRelease>', lambda e: calcular_total_pessoas())
                    entry.bind('<FocusOut>', lambda e: calcular_total_pessoas())
            entry.grid(row=i, column=1, sticky='ew', pady=5, padx=5)
            entries[key] = entry

        # Detalhes dos Serviços e Preços (Automático baseado na Análise Prévia)
        servicos_frame = ttk.LabelFrame(sumario_frame, text="Detalhes dos Serviços e Preços (Calculado Automaticamente)", padding=15, style='White.TLabelframe')
        servicos_frame.pack(fill='x', pady=(0, 10))

        # Informações sobre cálculo
        info_calculo = ttk.Label(servicos_frame, text="Os preços são calculados com base na Análise Prévia. Clique no botão para recalcular.", font=('Segoe UI', 9, 'italic'), foreground=DOLP_COLORS['medium_gray'])
        info_calculo.pack(pady=(0, 10))

        # Botão para calcular preços
        calculo_frame = ttk.Frame(servicos_frame)
        calculo_frame.pack(fill='x', pady=(0, 10))
        ttk.Button(calculo_frame, text="Calcular Preços Automaticamente", command=lambda: calcular_precos_automaticos(), style='Primary.TButton').pack(side='left', padx=5)

        # Lista de serviços calculados
        servicos_tree = ttk.Treeview(servicos_frame, columns=('servico', 'quantidade', 'volumetria', 'preco_unitario', 'preco_total'), show='headings', height=6)
        servicos_tree.heading('servico', text='Serviço')
        servicos_tree.heading('quantidade', text='Qtd Equipes')
        servicos_tree.heading('volumetria', text='Volumetria')
        servicos_tree.heading('preco_unitario', text='Preço Unit. (R$)')
        servicos_tree.heading('preco_total', text='Preço Total (R$)')
        servicos_tree.column('servico', width=200)
        servicos_tree.column('quantidade', width=80, anchor='center')
        servicos_tree.column('volumetria', width=100, anchor='center')
        servicos_tree.column('preco_unitario', width=120, anchor='center')
        servicos_tree.column('preco_total', width=120, anchor='center')
        servicos_tree.pack(fill='x', pady=5)
        entries['servicos_tree'] = servicos_tree

        def calcular_precos_automaticos():
            # TODO: Refatorar esta função para usar a nova estrutura de `servicos_data`.
            # A lógica atual está quebrada devido à refatoração do formulário.
            messagebox.showinfo("Em Desenvolvimento",
                                "O cálculo automático de preços está sendo reformulado para a nova estrutura de dados.",
                                parent=form_win)


        # Descrição Detalhada
        desc_frame = ttk.LabelFrame(sumario_frame, text="Descrição Detalhada", padding=15, style='White.TLabelframe')
        desc_frame.pack(fill='both', expand=True, pady=(0, 10))

        desc_text = tk.Text(desc_frame, height=8, wrap='word', bg='white', font=('Segoe UI', 10))
        desc_scrollbar = ttk.Scrollbar(desc_frame, orient="vertical", command=desc_text.yview)
        desc_text.configure(yscrollcommand=desc_scrollbar.set)
        desc_text.pack(side="left", fill="both", expand=True)
        desc_scrollbar.pack(side="right", fill="y")
        entries['descricao_detalhada'] = desc_text


        # Função para carregar dados automaticamente quando a aba for selecionada
        def on_tab_changed(event):
            selected_tab = event.widget.tab('current')['text']
            if 'Sumário Executivo' in selected_tab:
                form_win.after(100, lambda: auto_load_analise_data())

        def auto_load_analise_data():
            """Carrega automaticamente dados da análise prévia no sumário executivo"""
            try:
                # Carregar tempo de contrato se disponível
                if entries['tempo_contrato_meses'].get():
                    entries['duracao_contrato'].delete(0, 'end')
                    entries['duracao_contrato'].insert(0, entries['tempo_contrato_meses'].get())

                tipos_selecionados = [tipo for tipo, var in tipos_vars.items() if var.get()]
                if tipos_selecionados and entries['empresa_referencia'].get():
                    calcular_precos_automaticos()
            except:
                pass

        # Carregar dados se editando oportunidade existente
        if op_id:
            op_data = self.db.get_opportunity_details(op_id)
            if op_data:
                # Carregar dados básicos
                entries['titulo'].insert(0, op_data['titulo'] or '')
                entries['valor'].insert(0, op_data['valor'] or '')

                for client in clients:
                    if client['id'] == op_data['cliente_id']:
                        entries['cliente_id'].set(client['nome_empresa'])
                        break

                for estagio in estagios:
                    if estagio['id'] == op_data['estagio_id']:
                        entries['estagio_id'].set(estagio['nome'])
                        break

                # Carregar dados da análise prévia
                entries['tempo_contrato_meses'].insert(0, op_data['tempo_contrato_meses'] or '')
                entries['regional'].insert(0, op_data['regional'] or '')
                entries['polo'].insert(0, op_data['polo'] or '')
                entries['empresa_referencia'].set(op_data['empresa_referencia'] or '')

                if op_data.get('quantidade_bases') is not None:
                    bases_spinbox.set(op_data['quantidade_bases'])
                    _update_base_fields_ui()

                # Carregar dados do sumário executivo
                entries['numero_edital'].insert(0, op_data['numero_edital'] or '')
                if op_data['data_abertura']:
                    try:
                        date_obj = datetime.strptime(op_data['data_abertura'], '%d/%m/%Y').date()
                        entries['data_abertura'].set_date(date_obj)
                    except (ValueError, TypeError):
                        pass # Ignora se o formato da data for inválido
                entries['modalidade'].insert(0, op_data['modalidade'] or '')
                entries['contato_principal'].insert(0, op_data['contato_principal'] or '')
                entries['link_documentos'].insert(0, op_data['link_documentos'] or '')
                entries['faturamento_estimado'].insert(0, op_data['faturamento_estimado'] or '')
                entries['duracao_contrato'].insert(0, op_data['duracao_contrato'] or '')
                entries['total_pessoas'].insert(0, op_data['total_pessoas'] or '')
                entries['margem_contribuicao'].insert(0, op_data['margem_contribuicao'] or '')

                if op_data['descricao_detalhada']:
                    entries['descricao_detalhada'].insert('1.e...

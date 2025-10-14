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
from tkinter import ttk, messagebox, Toplevel, font, filedialog
from PIL import Image, ImageTk
import requests
from io import BytesIO
import sqlite3
import os
import webbrowser
from tkcalendar import DateEntry
from datetime import datetime, timedelta
import json
import google.generativeai as genai
from ddgs import DDGS
from bs4 import BeautifulSoup
import re
import threading
import time
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, KeepTogether
from reportlab.lib.utils import ImageReader
from reportlab.lib import colors
from reportlab.lib.units import inch
import locale
import pandas as pd
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg


# --- 1. CONFIGURAÇÕES GERAIS ---
LAST_FETCH_FILE = 'last_fetch.log'
FETCH_INTERVAL_HOURS = 4
DB_NAME = 'dolp_crm_final.db'
LOGO_PATH = "dolp_logo.png"
LOGO_URL = "https://mcusercontent.com/cfa43b95eeae85d65cf1366fb/images/a68e98a6-1595-5add-0b79-2e541e7faefa.png"

DOLP_COLORS = {
    'primary_blue': '#004b87', 'secondary_blue': "#4887ec", 'light_blue': '#eff6ff',
    'success_green': '#10b981', 'warning_orange': "#d02a2e", 'danger_red': '#ef4444',
    'white': '#ffffff', 'light_gray': '#ffffff', 'medium_gray': '#004b87',
    'dark_gray': '#004b87', 'very_light_gray': '#ffffff', 'dolp_cyan': '#06b6d4',
    'gradient_start': '#1e40af', 'gradient_end': '#3b82f6', 'border_color': '#4887ec'
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
INITIAL_SETORES = sorted(list(set(["Distribuição", "Geração", "Transmissão", "Comercialização", "Industrial", "Corporativo", "Energia Elétrica", "Infraestrutura"])))
INITIAL_SEGMENTOS = sorted(list(set(["Utilities", "Energia Renovável", "Óleo & Gás", "Manutenção Industrial", "Infraestrutura Elétrica", "Telecomunicações", "Distribuição", "Geração, Distribuição, Transmissão e Comercialização", "Geração e Transmissão", "Transmissão", "Concessão Rodoviária"])))
CLIENT_STATUS_OPTIONS = ["Playbook e não cadastrado", "Playbook e cadastrado", "Cadastrado", "Não cadastrado"]

QUALIFICATION_CHECKLIST = {
    "Aderência Estratégica e ao Perfil de Cliente Ideal": [
        "O serviço está alinhado ao nosso core business de engenharia elétrica (construção, manutenção, serviços comerciais)?",
        "A localização geográfica é estratégica para a DOLP?",
        "O cliente possui reputação e solidez compatíveis com os valores da empresa, como a credibilidade e a ética?"
    ],
    "Capacidade Técnica e Operacional": [
        "O escopo técnico exigido está 100% contido no escopo do nosso SGI?",
        "Possuímos ou temos acesso rápido ao pessoal, equipamentos e frota necessários para atender à demanda sem comprometer os contratos vigentes?",
        "Os requisitos de Segurança do Trabalho e Meio Ambiente do edital são compatíveis com nossas certificações e práticas?"
    ],
    "Viabilidade Econômico-Financeira": [
        "O valor estimado do contrato está acima do mínimo definido pela diretoria?",
        "As exigências de garantias contratuais e capacidade financeira são atendíveis pela empresa?",
        "A margem potencial do negócio está alinhada com as metas financeiras estratégicas de lucro e rentabilidade?"
    ],
    "Análise Concorrencial e de Riscos": [
        "Quais são nossos diferenciais competitivos claros para esta oportunidade específica?",
        "Quais os principais riscos (técnicos, logísticos, regulatórios, políticos) associados ao projeto?"
    ],
    "Análise de Interesse da Diretoria": [
        "O investimento de tempo e recursos na elaboração de uma análise previa de viabilidade, é justificável?"
    ]
}

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

def parse_brazilian_currency(value_str: str) -> float:
    """Converts a Brazilian currency string (e.g., '1.234,56') to a float."""
    if not isinstance(value_str, str) or not value_str.strip():
        return 0.0
    try:
        # Remove thousands separator, then replace decimal comma with a dot
        return float(value_str.replace('.', '').replace(',', '.'))
    except (ValueError, TypeError):
        return 0.0

def format_brazilian_currency_for_entry(value) -> str:
    """Formats a number into a Brazilian currency string for an Entry widget."""
    try:
        return locale.format_string('%.2f', float(value), grouping=True)
    except (ValueError, TypeError):
        return "0,00"

def open_link(url):
    try:
        if url and url != "---" and url.startswith(('http://', 'https://')):
            webbrowser.open(url)
        else:
            messagebox.showwarning("Link Inválido", "O link fornecido não é um endereço web válido (deve começar com http ou https).")
    except Exception as e:
        messagebox.showerror("Erro", f"Não foi possível abrir o link: {e}")

def strip_cnpj(cnpj):
    """Remove todos os caracteres não numéricos de um CNPJ."""
    if not isinstance(cnpj, str):
        return ""
    return "".join(c for c in cnpj if c.isdigit())

def format_cnpj(cnpj):
    """Formata uma string de 14 dígitos de CNPJ para XX.XXX.XXX/XXXX-XX."""
    cnpj_digits = strip_cnpj(cnpj)
    if len(cnpj_digits) == 14:
        return f"{cnpj_digits[:2]}.{cnpj_digits[2:5]}.{cnpj_digits[5:8]}/{cnpj_digits[8:12]}-{cnpj_digits[12:]}"
    return cnpj # Retorna o original (ou o que sobrou) se não tiver 14 dígitos

# --- 3. GERENCIADOR DE BANCO DE DADOS ---
class DatabaseManager:
    def __init__(self, db_name):
        self.db_name = db_name
        self._initialize_database()
        self._run_migrations()

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
                                status TEXT,
                                resumo_atuacao TEXT
                           )''')

            cursor.execute('CREATE TABLE IF NOT EXISTS pipeline_estagios (id INTEGER PRIMARY KEY, nome TEXT UNIQUE NOT NULL, ordem INTEGER)')

            # Tabela para Tipos de Serviço
            cursor.execute('''CREATE TABLE IF NOT EXISTS crm_servicos (
                                id INTEGER PRIMARY KEY,
                                nome TEXT UNIQUE NOT NULL,
                                descricao TEXT,
                                categoria TEXT,
                                ativa INTEGER DEFAULT 1
                           )''')

            # Nova tabela para Tipos de Equipe
            cursor.execute('''CREATE TABLE IF NOT EXISTS crm_tipos_equipe (
                                id INTEGER PRIMARY KEY,
                                nome TEXT NOT NULL,
                                servico_id INTEGER NOT NULL,
                                ativa INTEGER DEFAULT 1,
                                FOREIGN KEY (servico_id) REFERENCES crm_servicos(id) ON DELETE CASCADE
                           )''')

            # Tabela de Oportunidades Refatorada
            cursor.execute('''CREATE TABLE IF NOT EXISTS oportunidades (
                                id INTEGER PRIMARY KEY,
                                numero_oportunidade TEXT UNIQUE,
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
                                bases_nomes TEXT,
                                servicos_data TEXT,
                                empresa_referencia TEXT,
                                -- Campos do Sumário Executivo
                                numero_edital TEXT,
                                data_abertura TEXT,
                                modalidade TEXT,
                                contato_principal TEXT,
                                link_documentos TEXT,
                                faturamento_estimado REAL,
                                duracao_contrato INTEGER,
                                mod REAL,
                                moi REAL,
                                total_pessoas INTEGER,
                                margem_contribuicao REAL,
                                descricao_detalhada TEXT,
                                qualificacao_data TEXT,
                                diferenciais_competitivos TEXT,
                                principais_riscos TEXT,
                                FOREIGN KEY (cliente_id) REFERENCES clientes(id) ON DELETE CASCADE,
                                FOREIGN KEY (estagio_id) REFERENCES pipeline_estagios(id)
                           )''')

            cursor.execute('''CREATE TABLE IF NOT EXISTS crm_interacoes (id INTEGER PRIMARY KEY, oportunidade_id INTEGER NOT NULL, data_interacao TEXT, tipo TEXT, resumo TEXT, usuario TEXT,
                            FOREIGN KEY (oportunidade_id) REFERENCES oportunidades(id) ON DELETE CASCADE)''')
            cursor.execute('''CREATE TABLE IF NOT EXISTS crm_task_categories (
                                id INTEGER PRIMARY KEY,
                                nome TEXT UNIQUE NOT NULL
                           )''')
            cursor.execute('''CREATE TABLE IF NOT EXISTS crm_tarefas (id INTEGER PRIMARY KEY, oportunidade_id INTEGER NOT NULL, descricao TEXT, data_criacao TEXT, data_vencimento TEXT, responsavel TEXT, status TEXT, category_id INTEGER,
                            FOREIGN KEY (oportunidade_id) REFERENCES oportunidades(id) ON DELETE CASCADE,
                            FOREIGN KEY (category_id) REFERENCES crm_task_categories(id)
                            )''')
            cursor.execute('''CREATE TABLE IF NOT EXISTS crm_bases_alocadas (id INTEGER PRIMARY KEY, oportunidade_id INTEGER NOT NULL, nome_base TEXT, equipes_alocadas TEXT,
                            FOREIGN KEY (oportunidade_id) REFERENCES oportunidades(id) ON DELETE CASCADE)''')
            cursor.execute('''CREATE TABLE IF NOT EXISTS crm_empresas_referencia (
                                id INTEGER PRIMARY KEY,
                                nome_empresa TEXT NOT NULL,
                                tipo_servico TEXT NOT NULL,
                                tipo_equipe_id INTEGER,
                                valor_mensal REAL NOT NULL,
                                volumetria_minima REAL NOT NULL,
                                valor_por_pessoa REAL NOT NULL,
                                valor_us_ups_upe_ponto REAL,
                                ativa INTEGER DEFAULT 1,
                                data_criacao TEXT DEFAULT CURRENT_TIMESTAMP,
                                estado TEXT,
                                concessionaria TEXT,
                                ano_referencia TEXT,
                                observacoes TEXT,
                                FOREIGN KEY (tipo_equipe_id) REFERENCES crm_tipos_equipe(id)
                           )''')
            cursor.execute('CREATE TABLE IF NOT EXISTS crm_setores (id INTEGER PRIMARY KEY, nome TEXT UNIQUE NOT NULL)')
            cursor.execute('CREATE TABLE IF NOT EXISTS crm_segmentos (id INTEGER PRIMARY KEY, nome TEXT UNIQUE NOT NULL)')

            # Tabela para Notícias
            cursor.execute('''CREATE TABLE IF NOT EXISTS crm_news (
                                id INTEGER PRIMARY KEY,
                                title TEXT NOT NULL,
                                url TEXT NOT NULL UNIQUE,
                                source TEXT,
                                content_summary TEXT,
                                published_date TEXT,
                                saved INTEGER DEFAULT 0
                           )''')

            self._populate_initial_data(cursor)

    def _run_migrations(self):
        """
        Aplica migrações de schema de forma robusta no banco de dados existente,
        adicionando colunas faltantes para garantir retrocompatibilidade.
        """
        conn = self._connect()
        cursor = conn.cursor()

        try:
            # Adicionar coluna 'resumo_atuacao' na tabela 'clientes'
            cursor.execute("PRAGMA table_info(clientes)")
            client_columns = [row['name'] for row in cursor.fetchall()]
            if 'resumo_atuacao' not in client_columns:
                print("Aplicando migração: Adicionando coluna 'resumo_atuacao' em clientes...")
                cursor.execute("ALTER TABLE clientes ADD COLUMN resumo_atuacao TEXT")
                print("Coluna 'resumo_atuacao' adicionada.")

                # Populate data for existing clients
                client_summaries = {
                    'CPFL (RS)': 'Atuação principal no estado do Rio Grande do Sul (RS).',
                    'CPFL (SP) - Paulista': 'Atuação principal no estado de São Paulo (SP).',
                    'CPFL (SP) - Piratininga': 'Atuação principal no estado de São Paulo (SP).',
                    'Energisa (PB)': 'Atuação principal no estado da Paraíba (PB).',
                    'EDP Distribuição (ES)': 'Atuação principal no estado do Espírito Santo (ES).',
                    'EDP Transmissão': 'Atuação em transmissão de energia em múltiplos estados.',
                    'Cemig': 'Atuação principal no estado de Minas Gerais (MG).',
                    'TAESA Transmissão': 'Grande transmissora de energia com presença nacional.',
                    'State Grid Transmissão': 'Grande transmissora de energia com presença nacional.',
                    'Eletrobrás Transmissão': 'Grande transmissora de energia com presença nacional.',
                    'Eletrobrás Transmissão (Subsidiária)': 'Grande transmissora de energia com presença nacional.',
                    'Engie Transmissão': 'Grande transmissora de energia com presença nacional.',
                    'Ecovias': 'Concessão rodoviária no estado de São Paulo (SP).',
                    'Consórcio Rota Verde': 'Concessão rodoviária no estado de Goiás (GO).'
                }
                for name, summary in client_summaries.items():
                    cursor.execute("UPDATE clientes SET resumo_atuacao = ? WHERE nome_empresa = ?", (summary, name))
                print(f"{len(client_summaries)} resumos de atuação de clientes foram pré-preenchidos.")

            # Etapa 1: Garantir que todas as colunas da tabela 'oportunidades' existam
            cursor.execute("PRAGMA table_info(oportunidades)")
            existing_columns = [row['name'] for row in cursor.fetchall()]

            required_columns = {
                "numero_oportunidade": "TEXT",
                "tempo_contrato_meses": "INTEGER", "regional": "TEXT", "polo": "TEXT",
                "quantidade_bases": "INTEGER", "bases_nomes": "TEXT", "servicos_data": "TEXT",
                "empresa_referencia": "TEXT", "numero_edital": "TEXT", "data_abertura": "TEXT",
                "modalidade": "TEXT", "contato_principal": "TEXT", "link_documentos": "TEXT",
                "faturamento_estimado": "REAL", "duracao_contrato": "INTEGER", "mod": "REAL",
                "moi": "REAL", "total_pessoas": "INTEGER", "margem_contribuicao": "REAL",
                "descricao_detalhada": "TEXT", "qualificacao_data": "TEXT",
                "diferenciais_competitivos": "TEXT", "principais_riscos": "TEXT"
            }

            for col_name, col_type in required_columns.items():
                if col_name not in existing_columns:
                    print(f"Aplicando migração: Adicionando coluna '{col_name}'...")
                    cursor.execute(f"ALTER TABLE oportunidades ADD COLUMN {col_name} {col_type}")
                    print(f"Coluna '{col_name}' adicionada.")

            # Commit das alterações de schema antes de manipular os dados
            conn.commit()

            # Etapa 2: Preencher 'numero_oportunidade' e criar índice UNIQUE
            ops_to_update = cursor.execute("SELECT id FROM oportunidades WHERE numero_oportunidade IS NULL").fetchall()
            if ops_to_update:
                print(f"Aplicando migração: Preenchendo {len(ops_to_update)} IDs de oportunidade...")
                for op in ops_to_update:
                    new_op_id = f"OPP-{op['id']:05d}"
                    cursor.execute("UPDATE oportunidades SET numero_oportunidade = ? WHERE id = ?", (new_op_id, op['id']))
                print("Preenchimento de IDs concluído.")

            # A criação do índice UNIQUE deve vir após o preenchimento para evitar erros
            cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_numero_oportunidade ON oportunidades(numero_oportunidade)")

            # Migração para crm_empresas_referencia
            cursor.execute("PRAGMA table_info(crm_empresas_referencia)")
            empresa_ref_columns = [row['name'] for row in cursor.fetchall()]
            new_empresa_ref_columns = {
                "estado": "TEXT",
                "concessionaria": "TEXT",
                "ano_referencia": "TEXT",
                "observacoes": "TEXT",
                "valor_us_ups_upe_ponto": "REAL",
                "tipo_equipe_id": "INTEGER"
            }

            for col_name, col_type in new_empresa_ref_columns.items():
                if col_name not in empresa_ref_columns:
                    print(f"Aplicando migração: Adicionando coluna '{col_name}' em crm_empresas_referencia...")
                    cursor.execute(f"ALTER TABLE crm_empresas_referencia ADD COLUMN {col_name} {col_type}")
                    print(f"Coluna '{col_name}' adicionada.")

            # Migração para crm_tarefas
            cursor.execute("PRAGMA table_info(crm_tarefas)")
            task_columns = [row['name'] for row in cursor.fetchall()]
            if 'category_id' not in task_columns:
                print("Aplicando migração: Adicionando coluna 'category_id' em crm_tarefas...")
                cursor.execute("ALTER TABLE crm_tarefas ADD COLUMN category_id INTEGER REFERENCES crm_task_categories(id)")
                print("Coluna 'category_id' adicionada.")

            # Commit final de todas as alterações de dados e índice
            conn.commit()

        except sqlite3.Error as e:
            print(f"Erro CRÍTICO durante a migração do banco de dados: {e}")
            conn.rollback()
        finally:
            conn.close()


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
                cursor.execute("INSERT OR IGNORE INTO crm_segmentos (nome) VALUES (?)", (segmento,))

        # Popula a nova tabela crm_task_categories
        if cursor.execute("SELECT count(*) FROM crm_task_categories").fetchone()[0] == 0:
            initial_categories = ["Reclamação", "Sugestão", "Elogio", "Oportunidade de Melhoria"]
            for category in initial_categories:
                cursor.execute("INSERT INTO crm_task_categories (nome) VALUES (?)", (category,))

        # Adicionar clientes específicos
        new_clients = [
            {'nome_empresa': 'CPFL (RS)', 'cnpj': '02.016.440/0001-62', 'cidade': 'São Leopoldo', 'estado': 'RS', 'setor_atuacao': 'Energia Elétrica', 'segmento_atuacao': 'Distribuição'},
            {'nome_empresa': 'CPFL (SP) - Paulista', 'cnpj': '33.050.196/0001-88', 'cidade': 'Campinas', 'estado': 'SP', 'setor_atuacao': 'Energia Elétrica', 'segmento_atuacao': 'Distribuição'},
            {'nome_empresa': 'CPFL (SP) - Piratininga', 'cnpj': '04.172.213/0001-51', 'cidade': 'Campinas', 'estado': 'SP', 'setor_atuacao': 'Energia Elétrica', 'segmento_atuacao': 'Distribuição'},
            {'nome_empresa': 'Energisa (PB)', 'cnpj': '09.095.183/0001-40', 'cidade': 'João Pessoa', 'estado': 'PB', 'setor_atuacao': 'Energia Elétrica', 'segmento_atuacao': 'Distribuição'},
            {'nome_empresa': 'EDP Distribuição (ES)', 'cnpj': '28.152.650/0001-90', 'cidade': 'Vitória', 'estado': 'ES', 'setor_atuacao': 'Energia Elétrica', 'segmento_atuacao': 'Distribuição'},
            {'nome_empresa': 'EDP Transmissão', 'cnpj': '03.983.431/0001-03', 'cidade': 'São Paulo', 'estado': 'SP', 'setor_atuacao': 'Energia Elétrica', 'segmento_atuacao': 'Geração, Distribuição, Transmissão e Comercialização'},
            {'nome_empresa': 'Cemig', 'cnpj': '17.155.730/0001-64', 'cidade': 'Belo Horizonte', 'estado': 'MG', 'setor_atuacao': 'Energia Elétrica', 'segmento_atuacao': 'Geração, Transmissão, Distribuição e Comercialização'},
            {'nome_empresa': 'TAESA Transmissão', 'cnpj': '07.859.971/0001-30', 'cidade': 'Rio de Janeiro', 'estado': 'RJ', 'setor_atuacao': 'Energia Elétrica', 'segmento_atuacao': 'Transmissão'},
            {'nome_empresa': 'State Grid Transmissão', 'cnpj': '11.938.558/0001-39', 'cidade': 'Rio de Janeiro', 'estado': 'RJ', 'setor_atuacao': 'Energia Elétrica', 'segmento_atuacao': 'Transmissão'},
            {'nome_empresa': 'Eletrobrás Transmissão', 'cnpj': '00.001.180/0001-26', 'cidade': 'Rio de Janeiro', 'estado': 'RJ', 'setor_atuacao': 'Energia Elétrica', 'segmento_atuacao': 'Geração, Transmissão e Comercialização'},
            {'nome_empresa': 'Eletrobrás Transmissão (Subsidiária)', 'cnpj': '02.016.507/0001-69', 'cidade': 'Florianópolis', 'estado': 'SC', 'setor_atuacao': 'Energia Elétrica', 'segmento_atuacao': 'Geração e Transmissão'},
            {'nome_empresa': 'Engie Transmissão', 'cnpj': '02.474.103/0001-19', 'cidade': 'Florianópolis', 'estado': 'SC', 'setor_atuacao': 'Energia Elétrica', 'segmento_atuacao': 'Geração e Transmissão'},
            {'nome_empresa': 'Ecovias', 'cnpj': '02.509.491/0001-26', 'cidade': 'São Bernardo do Campo', 'estado': 'SP', 'setor_atuacao': 'Infraestrutura', 'segmento_atuacao': 'Concessão Rodoviária'},
            {'nome_empresa': 'Consórcio Rota Verde', 'cnpj': '59.354.202/0001-84', 'cidade': 'Goiânia', 'estado': 'GO', 'setor_atuacao': 'Infraestrutura', 'segmento_atuacao': 'Concessão Rodoviária'},
        ]

        for client in new_clients:
            # Usar INSERT OR IGNORE para não dar erro se o cliente (baseado no CNPJ) já existir
            cursor.execute("""INSERT OR IGNORE INTO clientes
                              (nome_empresa, cnpj, cidade, estado, setor_atuacao, segmento_atuacao, data_atualizacao, status)
                              VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                           (client['nome_empresa'], strip_cnpj(client['cnpj']), client['cidade'], client['estado'],
                            client['setor_atuacao'], client['segmento_atuacao'], datetime.now().strftime('%d/%m/%Y'), 'Cadastrado'))

    # Métodos de Clientes
    def get_all_clients(self, setor=None, segmento=None):
        with self._connect() as conn:
            base_query = "SELECT * FROM clientes"
            conditions = []
            params = []

            if setor and setor != 'Todos':
                conditions.append("setor_atuacao = ?")
                params.append(setor)

            if segmento and segmento != 'Todos':
                conditions.append("segmento_atuacao = ?")
                params.append(segmento)

            if conditions:
                base_query += " WHERE " + " AND ".join(conditions)

            base_query += " ORDER BY nome_empresa"
            return conn.execute(base_query, params).fetchall()

    def get_client_by_id(self, client_id):
        with self._connect() as conn:
            return conn.execute("SELECT * FROM clientes WHERE id = ?", (client_id,)).fetchone()

    def add_client(self, data):
        with self._connect() as conn:
            conn.execute("INSERT INTO clientes (nome_empresa, cnpj, cidade, estado, setor_atuacao, segmento_atuacao, data_atualizacao, link_portal, status, resumo_atuacao) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (data['nome_empresa'], data['cnpj'], data['cidade'], data['estado'], data['setor_atuacao'], data['segmento_atuacao'], data['data_atualizacao'], data['link_portal'], data['status'], data.get('resumo_atuacao')))

    def update_client(self, client_id, data):
        with self._connect() as conn:
            conn.execute("UPDATE clientes SET nome_empresa=?, cnpj=?, cidade=?, estado=?, setor_atuacao=?, segmento_atuacao=?, data_atualizacao=?, link_portal=?, status=?, resumo_atuacao=? WHERE id=?", (data['nome_empresa'], data['cnpj'], data['cidade'], data['estado'], data['setor_atuacao'], data['segmento_atuacao'], data['data_atualizacao'], data['link_portal'], data['status'], data.get('resumo_atuacao'), client_id))

    # Métodos de Pipeline
    def get_pipeline_data(self, setor=None, segmento=None):
        with self._connect() as conn:
            estagios = conn.execute("SELECT * FROM pipeline_estagios ORDER BY ordem").fetchall()

            base_query = "SELECT o.id, o.titulo, o.valor, o.cliente_id, o.estagio_id, c.nome_empresa FROM oportunidades o JOIN clientes c ON o.cliente_id = c.id"
            conditions = []
            params = []

            if setor and setor != 'Todos':
                conditions.append("c.setor_atuacao = ?")
                params.append(setor)

            if segmento and segmento != 'Todos':
                conditions.append("c.segmento_atuacao = ?")
                params.append(segmento)

            if conditions:
                base_query += " WHERE " + " AND ".join(conditions)

            oportunidades = conn.execute(base_query, params).fetchall()
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
                        faturamento_estimado, duracao_contrato, mod, moi, total_pessoas, margem_contribuicao, descricao_detalhada, qualificacao_data,
                        diferenciais_competitivos, principais_riscos)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
            params = (data['titulo'], data['valor'], data['cliente_id'], data['estagio_id'], datetime.now().date(),
                      data.get('tempo_contrato_meses'), data.get('regional'), data.get('polo'), data.get('quantidade_bases'),
                      data.get('bases_nomes'), data.get('servicos_data'), data.get('empresa_referencia'),
                      data.get('numero_edital'), data.get('data_abertura'), data.get('modalidade'), data.get('contato_principal'),
                      data.get('link_documentos'), data.get('faturamento_estimado'), data.get('duracao_contrato'),
                      data.get('mod'), data.get('moi'), data.get('total_pessoas'),
                      data.get('margem_contribuicao'), data.get('descricao_detalhada'), data.get('qualificacao_data'),
                      data.get('diferenciais_competitivos'), data.get('principais_riscos'))

            cursor = conn.cursor()
            cursor.execute(query, params)

            # Gerar e salvar o número único da oportunidade
            new_id = cursor.lastrowid
            numero_oportunidade = f"OPP-{new_id:05d}"
            conn.execute("UPDATE oportunidades SET numero_oportunidade = ? WHERE id = ?", (numero_oportunidade, new_id))

    def update_opportunity(self, op_id, data):
        with self._connect() as conn:
            query = '''UPDATE oportunidades SET titulo=?, valor=?, cliente_id=?, estagio_id=?,
                        tempo_contrato_meses=?, regional=?, polo=?, quantidade_bases=?, bases_nomes=?, servicos_data=?, empresa_referencia=?,
                        numero_edital=?, data_abertura=?, modalidade=?, contato_principal=?, link_documentos=?,
                        faturamento_estimado=?, duracao_contrato=?, mod=?, moi=?, total_pessoas=?, margem_contribuicao=?, descricao_detalhada=?, qualificacao_data=?,
                        diferenciais_competitivos=?, principais_riscos=?
                         WHERE id=?'''
            params = (data['titulo'], data['valor'], data['cliente_id'], data['estagio_id'],
                      data.get('tempo_contrato_meses'), data.get('regional'), data.get('polo'), data.get('quantidade_bases'),
                      data.get('bases_nomes'), data.get('servicos_data'), data.get('empresa_referencia'),
                      data.get('numero_edital'), data.get('data_abertura'), data.get('modalidade'), data.get('contato_principal'),
                      data.get('link_documentos'), data.get('faturamento_estimado'), data.get('duracao_contrato'),
                      data.get('mod'), data.get('moi'), data.get('total_pessoas'),
                      data.get('margem_contribuicao'), data.get('descricao_detalhada'), data.get('qualificacao_data'),
                      data.get('diferenciais_competitivos'), data.get('principais_riscos'),
                      op_id)
            conn.execute(query, params)

    def update_opportunity_stage(self, op_id, new_stage_id):
        with self._connect() as conn:
            conn.execute("UPDATE oportunidades SET estagio_id = ? WHERE id = ?", (new_stage_id, op_id))

    def get_historico_oportunidades(self, filters=None):
        with self._connect() as conn:
            base_query = "SELECT o.id, o.numero_oportunidade, o.titulo, o.valor, o.data_criacao, c.nome_empresa, p.nome as estagio_nome FROM oportunidades o JOIN clientes c ON o.cliente_id = c.id JOIN pipeline_estagios p ON o.estagio_id = p.id"
            conditions = []
            params = []

            if filters:
                if filters.get('numero_oportunidade'):
                    conditions.append("o.numero_oportunidade LIKE ?")
                    params.append(f"%{filters['numero_oportunidade']}%")
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
    def get_interaction_types(self):
        with self._connect() as conn:
            return [row['tipo'] for row in conn.execute("SELECT DISTINCT tipo FROM crm_interacoes ORDER BY tipo").fetchall()]

    def get_interactions_for_opportunity(self, op_id, tipo=None, start_date_str=None, end_date_str=None):
        with self._connect() as conn:
            base_query = "SELECT * FROM crm_interacoes WHERE oportunidade_id = ?"
            params = [op_id]

            if tipo and tipo != 'Todos':
                base_query += " AND tipo = ?"
                params.append(tipo)

            if start_date_str:
                try:
                    start_date_obj = datetime.strptime(start_date_str, '%d/%m/%Y')
                    base_query += " AND substr(data_interacao, 7, 4) || '-' || substr(data_interacao, 4, 2) || '-' || substr(data_interacao, 1, 2) >= ?"
                    params.append(start_date_obj.strftime('%Y-%m-%d'))
                except ValueError:
                    pass

            if end_date_str:
                try:
                    end_date_obj = datetime.strptime(end_date_str, '%d/%m/%Y')
                    base_query += " AND substr(data_interacao, 7, 4) || '-' || substr(data_interacao, 4, 2) || '-' || substr(data_interacao, 1, 2) <= ?"
                    params.append(end_date_obj.strftime('%Y-%m-%d'))
                except ValueError:
                    pass

            base_query += " ORDER BY substr(data_interacao, 7, 4) DESC, substr(data_interacao, 4, 2) DESC, substr(data_interacao, 1, 2) DESC, substr(data_interacao, 12) DESC"
            return conn.execute(base_query, params).fetchall()

    def add_interaction(self, data):
        conn = None
        try:
            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute("INSERT INTO crm_interacoes (oportunidade_id, data_interacao, tipo, resumo, usuario) VALUES (?, ?, ?, ?, ?)", (data['oportunidade_id'], data['data_interacao'], data['tipo'], data['resumo'], data['usuario']))
            conn.commit()
        except sqlite3.Error as e:
            print(f"Database error in add_interaction: {e}")
            if conn:
                conn.rollback()
            raise e
        finally:
            if conn:
                conn.close()

    # Métodos de Tarefas
    def get_task_responsibles(self, op_id):
        with self._connect() as conn:
            return [row['responsavel'] for row in conn.execute("SELECT DISTINCT responsavel FROM crm_tarefas WHERE oportunidade_id = ? ORDER BY responsavel", (op_id,)).fetchall()]

    def get_tasks_for_opportunity(self, op_id, status=None, responsavel=None, category_id=None, start_date_str=None, end_date_str=None):
        with self._connect() as conn:
            base_query = "SELECT t.*, c.nome as category_name FROM crm_tarefas t LEFT JOIN crm_task_categories c ON t.category_id = c.id WHERE t.oportunidade_id = ?"
            params = [op_id]

            if status and status != 'Todos':
                base_query += " AND t.status = ?"
                params.append(status)

            if responsavel and responsavel != 'Todos':
                base_query += " AND t.responsavel = ?"
                params.append(responsavel)

            if category_id:
                base_query += " AND t.category_id = ?"
                params.append(category_id)

            if start_date_str:
                try:
                    start_date_obj = datetime.strptime(start_date_str, '%d/%m/%Y')
                    base_query += " AND substr(data_vencimento, 7, 4) || '-' || substr(data_vencimento, 4, 2) || '-' || substr(data_vencimento, 1, 2) >= ?"
                    params.append(start_date_obj.strftime('%Y-%m-%d'))
                except ValueError:
                    pass

            if end_date_str:
                try:
                    end_date_obj = datetime.strptime(end_date_str, '%d/%m/%Y')
                    base_query += " AND substr(data_vencimento, 7, 4) || '-' || substr(data_vencimento, 4, 2) || '-' || substr(data_vencimento, 1, 2) <= ?"
                    params.append(end_date_obj.strftime('%Y-%m-%d'))
                except ValueError:
                    pass

            base_query += " ORDER BY status, data_vencimento"
            return conn.execute(base_query, params).fetchall()

    def add_task(self, data):
        with self._connect() as conn:
            conn.execute("INSERT INTO crm_tarefas (oportunidade_id, descricao, data_criacao, data_vencimento, responsavel, status, category_id) VALUES (?, ?, ?, ?, ?, ?, ?)",(data['oportunidade_id'], data['descricao'], data['data_criacao'], data['data_vencimento'], data['responsavel'], data['status'], data.get('category_id')))

    def update_task_status(self, task_id, status):
        with self._connect() as conn:
            conn.execute("UPDATE crm_tarefas SET status = ? WHERE id = ?", (status, task_id))

    def update_task(self, task_id, data):
        with self._connect() as conn:
            conn.execute("UPDATE crm_tarefas SET descricao=?, data_vencimento=?, responsavel=?, status=?, category_id=? WHERE id=?",
                         (data['descricao'], data['data_vencimento'], data['responsavel'], data['status'], data.get('category_id'), task_id))

    def delete_task(self, task_id):
        with self._connect() as conn:
            conn.execute("DELETE FROM crm_tarefas WHERE id = ?", (task_id,))

    # --- Métodos para o Dashboard ---
    def get_opportunity_stats_by_client(self):
        query = """
            SELECT
                c.nome_empresa,
                COUNT(o.id) as opportunity_count,
                SUM(o.valor) as total_value
            FROM clientes c
            JOIN oportunidades o ON c.id = o.cliente_id
            GROUP BY c.nome_empresa
            ORDER BY opportunity_count DESC
        """
        with self._connect() as conn:
            return conn.execute(query).fetchall()

    def get_client_count_by_setor(self):
        query = """
            SELECT setor_atuacao, COUNT(id) as client_count
            FROM clientes
            WHERE setor_atuacao IS NOT NULL AND setor_atuacao != ''
            GROUP BY setor_atuacao
            ORDER BY client_count DESC
        """
        with self._connect() as conn:
            return conn.execute(query).fetchall()

    def get_client_count_by_segmento(self):
        query = """
            SELECT segmento_atuacao, COUNT(id) as client_count
            FROM clientes
            WHERE segmento_atuacao IS NOT NULL AND segmento_atuacao != ''
            GROUP BY segmento_atuacao
            ORDER BY client_count DESC
        """
        with self._connect() as conn:
            return conn.execute(query).fetchall()

    def get_opportunity_count_by_stage(self):
        query = """
            SELECT
                p.nome,
                COUNT(o.id) as opportunity_count
            FROM pipeline_estagios p
            LEFT JOIN oportunidades o ON p.id = o.estagio_id
            WHERE p.nome != 'Histórico' AND p.nome != 'Clientes e Segmentos definidos (Playbook)'
            GROUP BY p.nome
            ORDER BY p.ordem
        """
        with self._connect() as conn:
            return conn.execute(query).fetchall()

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

    # Métodos de Categorias de Tarefas
    def get_all_task_categories(self):
        with self._connect() as conn:
            return conn.execute("SELECT * FROM crm_task_categories ORDER BY nome").fetchall()

    def add_task_category(self, nome):
        with self._connect() as conn:
            conn.execute("INSERT INTO crm_task_categories (nome) VALUES (?)", (nome,))

    def delete_task_category(self, category_id):
        with self._connect() as conn:
            # Opcional: Desassociar tarefas antes de apagar a categoria
            conn.execute("UPDATE crm_tarefas SET category_id = NULL WHERE category_id = ?", (category_id,))
            conn.execute("DELETE FROM crm_task_categories WHERE id = ?", (category_id,))


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
    def get_all_empresas_referencia(self, estado=None, tipo_servico=None, concessionaria=None):
        with self._connect() as conn:
            base_query = """
                SELECT er.*, te.nome as tipo_equipe_nome
                FROM crm_empresas_referencia er
                LEFT JOIN crm_tipos_equipe te ON er.tipo_equipe_id = te.id
            """
            conditions = []
            params = []

            if estado and estado != 'Todos':
                conditions.append("er.estado = ?")
                params.append(estado)
            if tipo_servico and tipo_servico != 'Todos':
                conditions.append("er.tipo_servico = ?")
                params.append(tipo_servico)
            if concessionaria and concessionaria != 'Todos':
                conditions.append("er.concessionaria = ?")
                params.append(concessionaria)

            if conditions:
                base_query += " WHERE " + " AND ".join(conditions)

            base_query += " ORDER BY er.nome_empresa, er.tipo_servico"
            return conn.execute(base_query, params).fetchall()

    def get_empresa_referencia_by_id(self, empresa_id):
        with self._connect() as conn:
            return conn.execute("SELECT * FROM crm_empresas_referencia WHERE id = ?", (empresa_id,)).fetchone()

    def add_empresa_referencia(self, data):
        with self._connect() as conn:
            conn.execute("""
                INSERT INTO crm_empresas_referencia
                (nome_empresa, tipo_servico, tipo_equipe_id, valor_mensal, volumetria_minima, valor_por_pessoa, valor_us_ups_upe_ponto, ativa, estado, concessionaria, ano_referencia, observacoes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                data['nome_empresa'], data['tipo_servico'], data.get('tipo_equipe_id'), data['valor_mensal'],
                data['volumetria_minima'], data['valor_por_pessoa'], data['valor_us_ups_upe_ponto'], data['ativa'],
                data.get('estado'), data.get('concessionaria'), data.get('ano_referencia'), data.get('observacoes')
            ))

    def update_empresa_referencia(self, empresa_id, data):
        with self._connect() as conn:
            conn.execute("""
                UPDATE crm_empresas_referencia SET
                nome_empresa=?, tipo_servico=?, tipo_equipe_id=?, valor_mensal=?, volumetria_minima=?, valor_por_pessoa=?, valor_us_ups_upe_ponto=?, ativa=?,
                estado=?, concessionaria=?, ano_referencia=?, observacoes=?
                WHERE id=?
            """, (
                data['nome_empresa'], data['tipo_servico'], data.get('tipo_equipe_id'), data['valor_mensal'],
                data['volumetria_minima'], data['valor_por_pessoa'], data['valor_us_ups_upe_ponto'], data['ativa'],
                data.get('estado'), data.get('concessionaria'), data.get('ano_referencia'), data.get('observacoes'),
                empresa_id
            ))

    def get_empresa_referencia_by_tipo(self, tipo_servico):
        with self._connect() as conn:
            return conn.execute("SELECT * FROM crm_empresas_referencia WHERE tipo_servico = ? AND ativa = 1", (tipo_servico,)).fetchone()

    def get_empresa_referencia_by_nome_e_tipo(self, nome_empresa, tipo_servico):
        with self._connect() as conn:
            return conn.execute("SELECT * FROM crm_empresas_referencia WHERE nome_empresa = ? AND tipo_servico = ? AND ativa = 1", (nome_empresa, tipo_servico)).fetchone()

    # Métodos para Notícias
    def add_news_article(self, article_data):
        with self._connect() as conn:
            conn.execute("INSERT OR IGNORE INTO crm_news (title, url, source, content_summary, published_date, saved) VALUES (?, ?, ?, ?, ?, ?)",
                         (article_data['title'], article_data['url'], article_data.get('source'), article_data.get('content_summary'), article_data.get('published_date'), 0))

    def get_latest_news(self, limit=10):
        with self._connect() as conn:
            return conn.execute("SELECT * FROM crm_news ORDER BY id DESC LIMIT ?", (limit,)).fetchall()

    def get_saved_news(self):
        with self._connect() as conn:
            return conn.execute("SELECT * FROM crm_news WHERE saved = 1 ORDER BY id DESC").fetchall()

    def set_news_saved_status(self, news_id, saved):
        with self._connect() as conn:
            conn.execute("UPDATE crm_news SET saved = ? WHERE id = ?", (1 if saved else 0, news_id))

    def delete_old_unsaved_news(self, days_old=7):
        with self._connect() as conn:
            try:
                date_limit = (datetime.now() - timedelta(days=days_old)).strftime('%Y-%m-%d')
                conn.execute("DELETE FROM crm_news WHERE saved = 0 AND published_date < ?", (date_limit,))
            except sqlite3.Error as e:
                print(f"Could not delete old news: {e}")

# --- 4. SERVIÇO DE NOTÍCIAS ---
class NewsService:
    def __init__(self, db_manager):
        self.db = db_manager
        self.gemini_api_key = os.environ.get('GEMINI_API_KEY')
        if self.gemini_api_key:
            genai.configure(api_key=self.gemini_api_key)
            self.model = genai.GenerativeModel('gemini-pro')
        else:
            self.model = None
            print("AVISO: Chave da API do Gemini não configurada.")

    def _search_news(self):
        """Busca notícias usando DuckDuckGo."""
        queries = [
            "notícias setor elétrico brasileiro",
            "programas governo federal energia elétrica",
            "fatos relevantes concessionárias energia Brasil",
            "ANEEL últimas notícias",
            "leilão de transmissão energia"
        ]
        results = []
        with DDGS() as ddgs:
            for query in queries:
                search_results = [r for r in ddgs.news(query, region='br-pt', timelimit='m', max_results=5)]
                results.extend(search_results)
                time.sleep(2)
        unique_results = {result['url']: result for result in results}.values()
        return list(unique_results)

    def _get_article_text(self, url):
        """Extrai o texto principal de uma página web."""
        try:
            response = requests.get(url, timeout=10, headers={'User-Agent': 'Mozilla/5.0'})
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'lxml')
            for script_or_style in soup(["script", "style", "header", "footer", "nav"]):
                script_or_style.decompose()
            text = soup.get_text()
            lines = (line.strip() for line in text.splitlines())
            chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
            text = '\n'.join(chunk for chunk in chunks if chunk)
            return text
        except Exception as e:
            print(f"Erro ao buscar conteúdo de {url}: {e}")
            return None

    def _check_relevance_batch(self, news_items):
        """Verifica a relevância de uma lista de notícias em uma única chamada de API."""
        if not self.model or not news_items:
            return []

        titles_with_indices = [f'{i}: {item["title"]}' for i, item in enumerate(news_items)]
        titles_text = "\n".join(titles_with_indices)

        prompt = f'''
        Você é um analista do setor de energia. Abaixo está uma lista de títulos de notícias, cada um com um índice. Sua tarefa é identificar quais notícias são relevantes para uma empresa de engenharia elétrica no Brasil.

        **Critérios de Relevância:**
        - Relevante: Notícias sobre leilões de energia, construção/manutenção de linhas de transmissão ou distribuição, investimentos, mudanças regulatórias da ANEEL, ou sobre grandes concessionárias (Neoenergia, CPFL, Eletrobras, etc.).
        - Não Relevante: Notícias genéricas de economia, política não relacionada, ou outros setores.

        **Lista de Títulos:**
        {titles_text}

        **Sua Tarefa:**
        Responda APENAS com um objeto JSON contendo uma única chave "indices_relevantes", que é uma lista de números (inteiros) correspondendo aos índices dos títulos que você considerou relevantes.
        Exemplo de Resposta:
        {{
          "indices_relevantes": [0, 4, 15]
        }}
        '''
        try:
            response = self.model.generate_content(prompt)
            # Extrair o JSON da resposta
            if '```json' in response.text:
                json_str = response.text.split('```json')[1].split('```')[0].strip()
            else:
                json_str = response.text

            data = json.loads(json_str)
            return data.get("indices_relevantes", [])
        except (json.JSONDecodeError, KeyError, Exception) as e:
            print(f"Erro ao checar relevância em lote: {e}")
            return []

    def _get_summary(self, text, title):
        """Gera o resumo para um único artigo já considerado relevante."""
        if not self.model:
            return "Resumo não disponível (API não configurada)."

        prompt = f'''
        Resuma a seguinte notícia em 2 a 3 frases, focando no impacto para o setor de engenharia elétrica.

        Título: "{title}"
        Conteúdo: "{text[:10000]}"
        '''
        try:
            response = self.model.generate_content(prompt)
            return response.text.strip()
        except Exception as e:
            print(f"Erro ao gerar resumo para '{title}': {e}")
            return "Não foi possível gerar o resumo."

    def fetch_and_store_news(self):
        """Orquestra o processo de busca e armazenamento de notícias de forma eficiente."""
        print("Buscando novas notícias...")
        news_items = self._search_news()
        if not news_items:
            print("Nenhuma notícia encontrada.")
            return

        print(f"Verificando relevância de {len(news_items)} notícias...")
        relevant_indices = self._check_relevance_batch(news_items)
        if not relevant_indices:
            print("Nenhuma notícia relevante encontrada na checagem inicial.")
            self.db.delete_old_unsaved_news()
            print("Busca de notícias concluída.")
            return

        relevant_articles = [news_items[i] for i in relevant_indices if i < len(news_items)]
        print(f"Encontradas {len(relevant_articles)} notícias relevantes. Buscando resumos...")

        for item in relevant_articles:
            url = item.get('url')
            title = item.get('title')
            print(f"Processando resumo para: {title}")

            article_text = self._get_article_text(url)
            if not article_text:
                time.sleep(4)
                continue

            summary = self._get_summary(article_text, title)

            article_data = {
                'title': title,
                'url': url,
                'source': item.get('source'),
                'content_summary': summary,
                'published_date': ''
            }
            if item.get('date'):
                try:
                    dt_obj = datetime.fromisoformat(item['date'].replace('Z', '+00:00'))
                    article_data['published_date'] = dt_obj.strftime('%Y-%m-%d')
                except ValueError:
                    pass

            self.db.add_news_article(article_data)
            print(f"  -> Notícia '{title}' salva no banco de dados.")
            time.sleep(4) # Respeitar o limite de RPM da API

        self.db.delete_old_unsaved_news()
        print("Busca de notícias concluída.")


# --- 5. APLICAÇÃO PRINCIPAL ---
class CRMApp:
    def __init__(self, root):
        self.root = root
        self.db = DatabaseManager(DB_NAME)
        self.news_service = NewsService(self.db)
        self.root.title("CRM Dolp Engenharia")
        self.root.geometry("1600x900")
        self.root.minsize(1280, 720)
        self.root.configure(bg=DOLP_COLORS['white'])
        self.logo_image = load_logo_image()

        # Inicializar estado dos filtros
        self.kanban_setor_filter = 'Todos'
        self.kanban_segmento_filter = 'Todos'

        self._configure_styles()
        self._create_main_container()
        self.show_main_menu()
        self.fetch_news_thread()

    def _configure_styles(self):
        style = ttk.Style(self.root)
        style.theme_use('clam')

        # Estilos base modernos
        style.configure('TFrame', background=DOLP_COLORS['white'])
        style.configure('Header.TFrame', background=DOLP_COLORS['white'], relief='flat', borderwidth=0)

        # Botões modernos com gradiente visual
        style.configure('TButton', font=('Segoe UI', 11, 'normal'), padding=(15, 12), relief='flat', borderwidth=0)
        style.configure('Primary.TButton', background=DOLP_COLORS['primary_blue'], foreground='white', font=('Segoe UI', 11, 'bold'))
        style.map('Primary.TButton', background=[('active', DOLP_COLORS['secondary_blue']), ('pressed', DOLP_COLORS['gradient_start'])])

        style.configure('Success.TButton', background=DOLP_COLORS['success_green'], foreground='white', font=('Segoe UI', 11, 'bold'))
        style.map('Success.TButton', background=[('active', '#059669'), ('pressed', '#047857')])

        style.configure('Warning.TButton', background=DOLP_COLORS['warning_orange'], foreground='white', font=('Segoe UI', 11, 'bold'))
        style.map('Warning.TButton', background=[('active', '#d97706'), ('pressed', '#b45309')])

        style.configure('Danger.TButton', background=DOLP_COLORS['danger_red'], foreground='white', font=('Segoe UI', 11, 'bold'))
        style.map('Danger.TButton', background=[('active', '#dc2626'), ('pressed', '#b91c1c')])

        # Estilos para botões do menu principal
        style.configure('MainMenu.Primary.TButton', background=DOLP_COLORS['primary_blue'], foreground='white', font=('Segoe UI', 12, 'bold'), padding=(30, 18))
        style.map('MainMenu.Primary.TButton', background=[('active', DOLP_COLORS['secondary_blue']), ('pressed', DOLP_COLORS['gradient_start'])])
        style.configure('MainMenu.Warning.TButton', background=DOLP_COLORS['warning_orange'], foreground='white', font=('Segoe UI', 12, 'bold'), padding=(30, 18))
        style.map('MainMenu.Warning.TButton', background=[('active', '#d97706'), ('pressed', '#b45309')])

        # Labels e outros elementos
        style.configure('TLabel', foreground='#000000', font=('Segoe UI', 10), background=DOLP_COLORS['white'])
        style.configure('Header.TLabel', foreground=DOLP_COLORS['primary_blue'], font=('Segoe UI', 24, 'bold'), background=DOLP_COLORS['white'])
        style.configure('Title.TLabel', foreground=DOLP_COLORS['dark_gray'], font=('Segoe UI', 14, 'bold'), background=DOLP_COLORS['white'])

        # Estilos para LabelFrames
        style.configure('White.TLabelframe', background=DOLP_COLORS['white'], borderwidth=1, relief='solid')
        style.configure('White.TLabelframe.Label', foreground=DOLP_COLORS['primary_blue'], font=('Segoe UI', 11, 'bold'))

        # Estilos para Labels específicos
        style.configure('Metric.White.TLabel', foreground=DOLP_COLORS['primary_blue'], font=('Segoe UI', 12, 'bold'), background=DOLP_COLORS['white'])
        style.configure('Value.White.TLabel', foreground='#000000', font=('Segoe UI', 11), background=DOLP_COLORS['white'])
        style.configure('Link.White.TLabel', foreground=DOLP_COLORS['secondary_blue'], font=('Segoe UI', 10, 'underline'), background=DOLP_COLORS['white'])

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
        style.configure('Card.TLabel', foreground=DOLP_COLORS['dark_gray'], font=('Segoe UI', 10))
        style.configure('Card.Title.TLabel', foreground=DOLP_COLORS['dark_gray'], font=('Segoe UI', 11, 'bold'))
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

        title_label = ttk.Label(header_frame, text="Customer Relationship Management (CRM) - Dolp Engenharia", style='Header.TLabel')
        title_label.pack(side='left')

        version_label = ttk.Label(header_frame, text="v59", font=('Segoe UI', 9, 'italic'), foreground=DOLP_COLORS['medium_gray'], style='TLabel')
        version_label.pack(side='right', padx=(10, 0), anchor='s', pady=(0, 4))

        # Área de conteúdo
        self.content_frame = ttk.Frame(self.main_container, style='TFrame')
        self.content_frame.pack(fill='both', expand=True, padx=20, pady=(0, 20))

    def _create_scrollable_tab(self, parent_notebook, tab_text):
        tab_main_frame = ttk.Frame(parent_notebook)
        parent_notebook.add(tab_main_frame, text=tab_text)

        # Configure grid layout for the tab's main frame to make the canvas expand
        tab_main_frame.rowconfigure(0, weight=1)
        tab_main_frame.columnconfigure(0, weight=1)

        canvas = tk.Canvas(tab_main_frame, bg=DOLP_COLORS['white'], highlightthickness=0)
        scrollbar = ttk.Scrollbar(tab_main_frame, orient="vertical", command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas, style='TFrame', padding=20)

        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )

        window_id = canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")

        def _on_canvas_configure(event):
            canvas.itemconfig(window_id, width=event.width)
        canvas.bind("<Configure>", _on_canvas_configure)

        canvas.configure(yscrollcommand=scrollbar.set)

        def _on_mousewheel(event):
            canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")

        # This binding is more robust. It activates when the mouse enters the tab area.
        tab_main_frame.bind('<Enter>', lambda e: self.root.bind_all("<MouseWheel>", _on_mousewheel))
        tab_main_frame.bind('<Leave>', lambda e: self.root.unbind_all("<MouseWheel>"))

        canvas.grid(row=0, column=0, sticky='nsew')
        scrollbar.grid(row=0, column=1, sticky='ns')

        # This inner frame will host the actual content and will expand
        # to fill the scrollable_frame, ensuring its children can expand horizontally.
        content_host = ttk.Frame(scrollable_frame, style='TFrame')
        content_host.pack(fill='both', expand=True)

        return content_host

    def clear_content(self):
        # Limpar quaisquer eventos globais para evitar erros de widgets destruídos
        self.root.unbind_all("<MouseWheel>")
        for widget in self.content_frame.winfo_children():
            widget.destroy()

    def should_fetch_news(self):
        if not os.path.exists(LAST_FETCH_FILE):
            return True
        try:
            with open(LAST_FETCH_FILE, 'r') as f:
                last_fetch_time = datetime.fromisoformat(f.read().strip())
            if (datetime.now() - last_fetch_time) > timedelta(hours=FETCH_INTERVAL_HOURS):
                return True
        except (IOError, ValueError):
            return True
        return False

    def update_last_fetch_time(self):
        try:
            with open(LAST_FETCH_FILE, 'w') as f:
                f.write(datetime.now().isoformat())
        except IOError:
            print("Aviso: Não foi possível atualizar o horário da última busca de notícias.")

    def fetch_news_thread(self):
        """Inicia a busca de notícias em uma thread separada para não bloquear a UI."""
        def run_fetch():
            if not self.should_fetch_news():
                print("Busca de notícias recente já realizada. Pulando.")
                return

            self.news_service.fetch_and_store_news()
            self.update_last_fetch_time()

            try:
                if self.content_frame.winfo_exists() and self.content_frame.winfo_children() and isinstance(self.content_frame.winfo_children()[0], ttk.Label) and self.content_frame.winfo_children()[0].cget("text") == "Menu Principal":
                     self.root.after(0, self.show_main_menu)
            except tk.TclError:
                pass

        thread = threading.Thread(target=run_fetch, daemon=True)
        thread.start()

    def show_main_menu(self):
        self.clear_content()

        # Título da seção
        title_label = ttk.Label(self.content_frame, text="Menu Principal", style='Title.TLabel')
        title_label.pack(pady=(0, 20))

        # Main layout frame
        main_layout_frame = ttk.Frame(self.content_frame, style='TFrame')
        main_layout_frame.pack(fill='both', expand=True)

        # News frame on the left (more prominent)
        news_lf = ttk.LabelFrame(main_layout_frame, text="Últimas Notícias do Setor", padding=15, style='White.TLabelframe')
        news_lf.pack(side='left', fill='both', expand=True, padx=(0, 20))

        # Container for buttons on the right
        buttons_frame = ttk.Frame(main_layout_frame, style='TFrame')
        buttons_frame.pack(side='right', fill='y', padx=(20, 0))


        # Botões do menu principal
        menu_buttons = [
            ("📊 Funil de Vendas", self.show_kanban_view, 'MainMenu.Primary.TButton'),
            ("👥 Clientes", self.show_clients_view, 'MainMenu.Primary.TButton'),
            ("🔖 Notícias Salvas", self.show_saved_news_view, 'MainMenu.Primary.TButton'),
            ("⚙️ Configurações do CRM", self.show_crm_settings, 'MainMenu.Warning.TButton')
        ]

        for i, (text, command, style) in enumerate(menu_buttons):
            btn = ttk.Button(buttons_frame, text=text, command=command, style=style, width=28)
            btn.pack(pady=10, anchor='n', fill='x')

        # --- Scrollable area for news ---
        news_canvas = tk.Canvas(news_lf, bg=DOLP_COLORS['white'], highlightthickness=0)
        news_scrollbar = ttk.Scrollbar(news_lf, orient="vertical", command=news_canvas.yview)
        scrollable_news_frame = ttk.Frame(news_canvas, style='TFrame')
        scrollable_news_frame.columnconfigure(0, weight=1)

        scrollable_news_frame.bind("<Configure>", lambda e: news_canvas.configure(scrollregion=news_canvas.bbox("all")))

        # Align the frame to the top-left and make it resize with the canvas
        news_window = news_canvas.create_window((0, 0), window=scrollable_news_frame, anchor="nw")
        def _resize_news_frame(event):
            news_canvas.itemconfig(news_window, width=event.width)
        news_canvas.bind("<Configure>", _resize_news_frame)

        news_canvas.configure(yscrollcommand=news_scrollbar.set)

        def _on_mousewheel(event):
            news_canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")

        news_lf.bind('<Enter>', lambda e: self.root.bind_all("<MouseWheel>", _on_mousewheel))
        news_lf.bind('<Leave>', lambda e: self.root.unbind_all("<MouseWheel>"))

        news_canvas.pack(side="left", fill="both", expand=True)
        news_scrollbar.pack(side="right", fill="y")
        # ------------------------------------

        latest_news = self.db.get_latest_news()

        if not latest_news:
            ttk.Label(scrollable_news_frame, text="Buscando notícias relevantes...", style='Value.White.TLabel', font=('Segoe UI', 10, 'italic')).pack(pady=20)
        else:
            for news_item in latest_news:
                self.create_news_card(scrollable_news_frame, news_item, self.show_main_menu)

    def create_news_card(self, parent, news_item, refresh_callback):
        """Cria um card para uma notícia."""
        card = ttk.Frame(parent, style='Card.TFrame', padding=15, relief='solid', borderwidth=1)
        card.grid(sticky='ew', pady=5, padx=5)

        # Top frame for title and buttons
        top_frame = ttk.Frame(card, style='Card.TFrame')
        top_frame.pack(fill='x')

        # Title
        title_label = ttk.Label(top_frame, text=news_item['title'], style='Card.Title.TLabel', cursor="hand2", wraplength=750)
        title_label.pack(side='left', anchor='w', expand=True, fill='x')
        title_label.bind("<Button-1>", lambda e, url=news_item['url']: open_link(url))

        # Action buttons
        actions_frame = ttk.Frame(top_frame, style='Card.TFrame')
        actions_frame.pack(side='right', anchor='ne')

        def toggle_save():
            new_status = not bool(news_item['saved'])
            self.db.set_news_saved_status(news_item['id'], new_status)
            refresh_callback()

        save_button = ttk.Button(actions_frame, command=toggle_save)
        if news_item['saved']:
            save_button.config(text="Salvo ✓", style='Success.TButton')
        else:
            save_button.config(text="Salvar", style='Primary.TButton')
        save_button.pack()

        # Info line
        info_text = f"Fonte: {news_item['source'] or 'N/A'} | Data: {news_item['published_date'] or 'N/A'}"
        ttk.Label(card, text=info_text, style='Card.TLabel', font=('Segoe UI', 9, 'italic')).pack(anchor='w', pady=(5,0))

        # Summary
        ttk.Label(card, text=news_item['content_summary'] or 'Sem resumo.', style='Card.TLabel', wraplength=800, justify='left').pack(anchor='w', pady=(5,0))


    def show_saved_news_view(self):
        self.clear_content()

        title_frame = ttk.Frame(self.content_frame, style='TFrame')
        title_frame.pack(fill='x', pady=(0, 20))

        ttk.Label(title_frame, text="Notícias Salvas", style='Title.TLabel').pack(side='left')
        ttk.Button(title_frame, text="← Voltar", command=self.show_main_menu, style='TButton').pack(side='right')

        # Scrollable area
        main_canvas = tk.Canvas(self.content_frame, bg=DOLP_COLORS['white'], highlightthickness=0)
        main_scrollbar = ttk.Scrollbar(self.content_frame, orient="vertical", command=main_canvas.yview)
        scrollable_frame = ttk.Frame(main_canvas, style='TFrame', padding=20)
        scrollable_frame.columnconfigure(0, weight=1)

        scrollable_frame.bind("<Configure>", lambda e: main_canvas.configure(scrollregion=main_canvas.bbox("all")))

        # Align the frame to the top-left and make it resize with the canvas
        saved_news_window = main_canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        def _resize_saved_news_frame(event):
            main_canvas.itemconfig(saved_news_window, width=event.width)
        main_canvas.bind("<Configure>", _resize_saved_news_frame)

        main_canvas.configure(yscrollcommand=main_scrollbar.set)

        def _on_mousewheel(event):
            main_canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")

        self.content_frame.bind('<Enter>', lambda e: self.root.bind_all("<MouseWheel>", _on_mousewheel))
        self.content_frame.bind('<Leave>', lambda e: self.root.unbind_all("<MouseWheel>"))

        main_canvas.pack(side="left", fill="both", expand=True)
        main_scrollbar.pack(side="right", fill="y")

        saved_news = self.db.get_saved_news()

        if not saved_news:
            ttk.Label(scrollable_frame, text="Nenhuma notícia salva ainda.", style='Value.White.TLabel', font=('Segoe UI', 10, 'italic')).pack(pady=20)
        else:
            for news_item in saved_news:
                self.create_news_card(scrollable_frame, news_item, self.show_saved_news_view)

    def _apply_kanban_filters(self):
        """Salva o estado atual dos filtros e recarrega a visão do kanban."""
        if hasattr(self, 'setor_filter') and hasattr(self, 'segmento_filter'):
            self.kanban_setor_filter = self.setor_filter.get()
            self.kanban_segmento_filter = self.segmento_filter.get()
        self.show_kanban_view()

    def _show_summary_popup(self, summary_text, event):
        """Exibe um popup com o resumo de atuação do cliente."""
        if not summary_text or not summary_text.strip():
            summary_text = "Nenhum resumo de atuação disponível."

        popup = Toplevel(self.root)
        popup.title("Resumo de Atuação")

        # Posiciona o popup perto do cursor
        popup_x = event.x_root
        popup_y = event.y_root + 10
        popup.geometry(f"400x150+{popup_x}+{popup_y}")
        popup.configure(bg=DOLP_COLORS['white'])
        popup.resizable(False, False)

        main_frame = ttk.Frame(popup, padding=15, style='TFrame')
        main_frame.pack(fill='both', expand=True)

        summary_label = ttk.Label(main_frame, text=summary_text, wraplength=370, justify='left', style='Value.White.TLabel')
        summary_label.pack(fill='both', expand=True, pady=(0, 10))

        close_button = ttk.Button(main_frame, text="Fechar", command=popup.destroy, style='Primary.TButton')
        close_button.pack()

        popup.transient(self.root)
        popup.grab_set()
        self.root.wait_window(popup)

    def show_kanban_view(self):
        self.clear_content()

        # Título e botões
        title_frame = ttk.Frame(self.content_frame, style='TFrame')
        title_frame.pack(fill='x', pady=(0, 20))

        ttk.Label(title_frame, text="Funil de Vendas", style='Title.TLabel').pack(side='left')
        ttk.Button(title_frame, text="📈 Dashboard", command=self.show_dashboard_view, style='Primary.TButton').pack(side='right', padx=(0, 10))
        ttk.Button(title_frame, text="Histórico", command=self.show_historico_view, style='Warning.TButton').pack(side='right', padx=(0, 10))
        ttk.Button(title_frame, text="Nova Oportunidade", command=lambda: self.show_opportunity_form(back_callback=self.show_kanban_view), style='Success.TButton').pack(side='right', padx=(0, 10))
        ttk.Button(title_frame, text="← Voltar", command=self.show_main_menu, style='TButton').pack(side='right', padx=(0, 10))

        # Frame de Filtros
        filters_frame = ttk.LabelFrame(self.content_frame, text="Filtros", padding=15, style='White.TLabelframe')
        filters_frame.pack(fill='x', pady=(5, 20))

        # Setor
        ttk.Label(filters_frame, text="Setor:", style='TLabel').grid(row=0, column=0, sticky='w', padx=(0, 5))
        self.setor_filter = ttk.Combobox(filters_frame, values=['Todos'] + self.db.get_all_setores(), width=25)
        self.setor_filter.set(self.kanban_setor_filter)
        self.setor_filter.grid(row=0, column=1, padx=(0, 20))

        # Segmento
        ttk.Label(filters_frame, text="Segmento:", style='TLabel').grid(row=0, column=2, sticky='w', padx=(0, 5))
        self.segmento_filter = ttk.Combobox(filters_frame, values=['Todos'] + self.db.get_all_segmentos(), width=25)
        self.segmento_filter.set(self.kanban_segmento_filter)
        self.segmento_filter.grid(row=0, column=3, padx=(0, 20))

        # Botão de Aplicar
        apply_btn = ttk.Button(filters_frame, text="🔍 Aplicar Filtros", style='Primary.TButton',
                               command=self._apply_kanban_filters)
        apply_btn.grid(row=0, column=4, padx=(20, 0))


        # Frame principal com scrollbar vertical
        main_frame = ttk.Frame(self.content_frame, style='TFrame')
        main_frame.pack(fill='both', expand=True)

        # Canvas e scrollbar para o funil vertical
        canvas = tk.Canvas(main_frame, bg=DOLP_COLORS['white'], highlightthickness=0)
        v_scrollbar = ttk.Scrollbar(main_frame, orient="vertical", command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas, style='TFrame')

        scrollable_frame.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))

        window_id = canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")

        def on_canvas_configure(event):
            canvas.itemconfig(window_id, width=event.width)

        canvas.bind("<Configure>", on_canvas_configure)
        canvas.configure(yscrollcommand=v_scrollbar.set)

        # Posicionar canvas e scrollbar
        canvas.pack(side="left", fill="both", expand=True)
        v_scrollbar.pack(side="right", fill="y")

        # Obter dados do pipeline com base nos filtros
        estagios_todos, oportunidades = self.db.get_pipeline_data(
            setor=self.kanban_setor_filter,
            segmento=self.kanban_segmento_filter
        )
        estagios = [e for e in estagios_todos if e['nome'] != 'Histórico']
        clients = self.db.get_all_clients(
            setor=self.kanban_setor_filter,
            segmento=self.kanban_segmento_filter
        )

        # Container para o funil, que se expande para preencher o espaço
        funil_container = ttk.Frame(scrollable_frame, style='TFrame')
        funil_container.pack(fill='x', expand=True, pady=20)

        # --- Lógica do Funil Visual ---
        self.root.update_idletasks() # Garante que as dimensões da janela estão atualizadas
        num_stages = len(estagios)

        # Define o padding (margem) mínimo e máximo para criar o efeito de funil
        # O padding é aplicado em ambos os lados, então o encolhimento visual é o dobro do padding
        min_padx = 20
        # A largura máxima do conteúdo será a largura do container - 2*min_padx
        # A largura mínima será a largura do container - 2*max_padx
        max_padx = (self.content_frame.winfo_width() * 0.4) # Deixa a última etapa com 20% da largura

        if num_stages > 1:
            padx_step = (max_padx - min_padx) / (num_stages - 1)
        else:
            padx_step = 0
        # -----------------------------

        # Criar estágios do funil verticalmente
        for i, estagio in enumerate(estagios):
            # Calcula o padding para o estágio atual
            current_padx = int(min_padx + (i * padx_step))

            # Frame para cada estágio
            stage_frame = ttk.Frame(funil_container, style='White.TLabelframe', padding=15)
            # Usa fill='x' e o padx dinâmico para criar o efeito de funil centrado
            stage_frame.pack(fill='x', pady=5, padx=current_padx)

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

                    client_card = ttk.Frame(clients_frame, style='TFrame', padding=10, cursor="hand2")
                    client_card.grid(row=row, column=col, padx=5, pady=5, sticky='ew')
                    client_card.configure(relief='solid', borderwidth=1)

                    # --- Lógica do clique para exibir o popup de resumo ---
                    summary = client['resumo_atuacao'] if 'resumo_atuacao' in client.keys() else ''
                    click_handler = lambda e, s=summary: self._show_summary_popup(s, e)
                    client_card.bind("<Button-1>", click_handler)
                    # ----------------------------------------------------

                    # Nome da empresa
                    nome_label = ttk.Label(client_card, text=client['nome_empresa'], style='Value.White.TLabel',
                                           font=('Segoe UI', 10, 'bold'))
                    nome_label.pack(anchor='w')
                    nome_label.bind("<Button-1>", click_handler)

                    # Status
                    status = client['status'] or 'Não cadastrado'
                    status_label = ttk.Label(client_card, text=f"Status: {status}", style='Value.White.TLabel')
                    status_label.pack(anchor='w')
                    status_label.bind("<Button-1>", click_handler)

                    # Setor
                    if client['setor_atuacao']:
                        setor_label = ttk.Label(client_card, text=f"Setor: {client['setor_atuacao']}", style='Value.White.TLabel')
                        setor_label.pack(anchor='w')
                        setor_label.bind("<Button-1>", click_handler)

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
                            self.show_opportunity_details(op_id, self.show_kanban_view)

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

        # Bind scroll do mouse de forma mais robusta
        def on_mousewheel(event):
            if canvas.winfo_exists():
                canvas.yview_scroll(int(-1*(event.delta/120)), "units")

        def _bind_scroll(event):
            self.root.bind_all("<MouseWheel>", on_mousewheel)

        def _unbind_scroll(event):
            self.root.unbind_all("<MouseWheel>")

        main_frame.bind('<Enter>', _bind_scroll)
        main_frame.bind('<Leave>', _unbind_scroll)

    def show_dashboard_view(self):
        self.clear_content()

        # --- Título e Botão Voltar ---
        title_frame = ttk.Frame(self.content_frame, style='TFrame')
        title_frame.pack(fill='x', pady=(0, 20))
        ttk.Label(title_frame, text="Dashboard de Análise", style='Title.TLabel').pack(side='left')
        ttk.Button(title_frame, text="← Voltar ao Funil", command=self.show_kanban_view, style='TButton').pack(side='right')

        # --- Frame Principal com Rolagem ---
        main_canvas = tk.Canvas(self.content_frame, bg=DOLP_COLORS['white'], highlightthickness=0)
        main_scrollbar = ttk.Scrollbar(self.content_frame, orient="vertical", command=main_canvas.yview)
        scrollable_frame = ttk.Frame(main_canvas, style='TFrame', padding=20)

        scrollable_frame.bind("<Configure>", lambda e: main_canvas.configure(scrollregion=main_canvas.bbox("all")))
        window_id = main_canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")

        def _on_canvas_configure(event):
            main_canvas.itemconfig(window_id, width=event.width)
        main_canvas.bind("<Configure>", _on_canvas_configure)

        main_canvas.configure(yscrollcommand=main_scrollbar.set)

        main_canvas.pack(side="left", fill="both", expand=True)
        main_scrollbar.pack(side="right", fill="y")

        def _on_mousewheel(event):
            main_canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")

        main_canvas.bind_all("<MouseWheel>", _on_mousewheel)

        # --- Layout dos Gráficos ---
        # Os gráficos serão adicionados aqui, em um grid
        scrollable_frame.columnconfigure(0, weight=1)
        scrollable_frame.columnconfigure(1, weight=1)

        # Adicionar os gráficos (a lógica de criação vem na próxima etapa)
        self.add_opportunities_by_client_chart(scrollable_frame, 0, 0)
        self.add_value_by_client_chart(scrollable_frame, 0, 1)
        self.add_clients_by_setor_chart(scrollable_frame, 1, 0)
        self.add_clients_by_segmento_chart(scrollable_frame, 1, 1)
        self.add_opportunities_by_stage_chart(scrollable_frame, 2, 0, 2)


    def _create_chart_frame(self, parent, title):
        """Cria um contêiner padronizado para um gráfico."""
        chart_lf = ttk.LabelFrame(parent, text=title, padding=15, style='White.TLabelframe')
        chart_lf.grid(padx=10, pady=10, sticky='nsew')
        return chart_lf

    def add_opportunities_by_client_chart(self, parent, row, col):
        chart_frame = self._create_chart_frame(parent, "Quantidade de Oportunidades por Cliente")
        chart_frame.grid(row=row, column=col)

        data = self.db.get_opportunity_stats_by_client()
        if not data:
            ttk.Label(chart_frame, text="Não há dados suficientes.").pack()
            return

        df = pd.DataFrame(data, columns=['nome_empresa', 'opportunity_count', 'total_value'])

        fig = Figure(figsize=(6, 4), dpi=100)
        ax = fig.add_subplot(111)

        df.plot(kind='bar', x='nome_empresa', y='opportunity_count', ax=ax, color=DOLP_COLORS['primary_blue'], legend=False)
        ax.set_title("Oportunidades por Cliente", fontsize=12)
        ax.set_ylabel("Quantidade")
        ax.set_xlabel("")
        fig.autofmt_xdate()

        canvas = FigureCanvasTkAgg(fig, master=chart_frame)
        canvas.draw()
        canvas.get_tk_widget().pack(fill='both', expand=True)

    def add_value_by_client_chart(self, parent, row, col):
        chart_frame = self._create_chart_frame(parent, "Valor Global (R$) por Cliente")
        chart_frame.grid(row=row, column=col)

        data = self.db.get_opportunity_stats_by_client()
        if not data:
            ttk.Label(chart_frame, text="Não há dados suficientes.").pack()
            return

        df = pd.DataFrame(data, columns=['nome_empresa', 'opportunity_count', 'total_value'])
        df = df[df['total_value'] > 0] # Apenas clientes com valor

        fig = Figure(figsize=(6, 4), dpi=100)
        ax = fig.add_subplot(111)

        df.plot(kind='bar', x='nome_empresa', y='total_value', ax=ax, color=DOLP_COLORS['success_green'], legend=False)
        ax.set_title("Valor Total por Cliente", fontsize=12)
        ax.set_ylabel("Valor (R$)")
        ax.set_xlabel("")
        ax.get_yaxis().set_major_formatter(tk.FuncFormatter(lambda x, p: f'R$ {x:,.0f}'))
        fig.autofmt_xdate()

        canvas = FigureCanvasTkAgg(fig, master=chart_frame)
        canvas.draw()
        canvas.get_tk_widget().pack(fill='both', expand=True)

    def add_clients_by_setor_chart(self, parent, row, col):
        chart_frame = self._create_chart_frame(parent, "Clientes por Setor de Atuação")
        chart_frame.grid(row=row, column=col)

        data = self.db.get_client_count_by_setor()
        if not data:
            ttk.Label(chart_frame, text="Não há dados suficientes.").pack()
            return

        df = pd.DataFrame(data, columns=['setor_atuacao', 'client_count'])

        fig = Figure(figsize=(6, 4), dpi=100)
        ax = fig.add_subplot(111)

        df.plot(kind='pie', y='client_count', labels=df['setor_atuacao'], ax=ax, autopct='%1.1f%%', startangle=90, legend=False)
        ax.set_title("Distribuição de Clientes por Setor", fontsize=12)
        ax.set_ylabel('') # Esconde o label do eixo y

        canvas = FigureCanvasTkAgg(fig, master=chart_frame)
        canvas.draw()
        canvas.get_tk_widget().pack(fill='both', expand=True)

    def add_clients_by_segmento_chart(self, parent, row, col):
        chart_frame = self._create_chart_frame(parent, "Clientes por Segmento de Atuação")
        chart_frame.grid(row=row, column=col)

        data = self.db.get_client_count_by_segmento()
        if not data:
            ttk.Label(chart_frame, text="Não há dados suficientes.").pack()
            return

        df = pd.DataFrame(data, columns=['segmento_atuacao', 'client_count'])

        fig = Figure(figsize=(6, 4), dpi=100)
        ax = fig.add_subplot(111)

        df.plot(kind='pie', y='client_count', labels=df['segmento_atuacao'], ax=ax, autopct='%1.1f%%', startangle=90, legend=False)
        ax.set_title("Distribuição de Clientes por Segmento", fontsize=12)
        ax.set_ylabel('')

        canvas = FigureCanvasTkAgg(fig, master=chart_frame)
        canvas.draw()
        canvas.get_tk_widget().pack(fill='both', expand=True)

    def add_opportunities_by_stage_chart(self, parent, row, col, colspan):
        chart_frame = self._create_chart_frame(parent, "Oportunidades por Etapa do Funil")
        chart_frame.grid(row=row, column=col, columnspan=colspan)

        data = self.db.get_opportunity_count_by_stage()
        if not data:
            ttk.Label(chart_frame, text="Não há dados suficientes.").pack()
            return

        df = pd.DataFrame(data, columns=['nome', 'opportunity_count'])

        fig = Figure(figsize=(12, 5), dpi=100)
        ax = fig.add_subplot(111)

        df.plot(kind='barh', x='nome', y='opportunity_count', ax=ax, color=DOLP_COLORS['dolp_cyan'], legend=False)
        ax.set_title("Contagem de Oportunidades por Etapa", fontsize=12)
        ax.set_xlabel("Quantidade")
        ax.set_ylabel("Etapa do Funil")

        # Adicionar contagem no final de cada barra
        for index, value in enumerate(df['opportunity_count']):
            ax.text(value, index, f' {value}')

        fig.tight_layout()

        canvas = FigureCanvasTkAgg(fig, master=chart_frame)
        canvas.draw()
        canvas.get_tk_widget().pack(fill='both', expand=True)

    def show_resultado_dialog(self, op_id, current_stage_id):
        """Mostra tela para aprovar ou reprovar oportunidade"""
        self.clear_content()

        title_frame = ttk.Frame(self.content_frame, style='TFrame')
        title_frame.pack(fill='x', pady=(0, 20))
        ttk.Label(title_frame, text="Resultado da Avaliação", style='Title.TLabel').pack(side='left')
        ttk.Button(title_frame, text="← Voltar", command=self.show_kanban_view, style='TButton').pack(side='right')

        main_frame = ttk.Frame(self.content_frame, padding=20, style='TFrame')
        main_frame.pack(fill='both', expand=True)

        op_data = self.db.get_opportunity_details(op_id)
        ttk.Label(main_frame, text=f"Oportunidade: {op_data['titulo']}", style='Title.TLabel').pack(pady=(0, 10))
        ttk.Label(main_frame, text=f"Estágio Atual: {op_data['estagio_nome']}", style='Value.White.TLabel').pack(pady=(0, 20))
        ttk.Label(main_frame, text="Qual o resultado desta avaliação?", style='TLabel').pack(pady=(0, 20))

        buttons_frame = ttk.Frame(main_frame, style='TFrame')
        buttons_frame.pack(fill='x')

        def aprovar():
            estagios = self.db.get_pipeline_data()[0]
            current_order = next((e['ordem'] for e in estagios if e['id'] == current_stage_id), None)
            if current_order is not None:
                next_stage = next((e for e in estagios if e['ordem'] == current_order + 1), None)
                if next_stage:
                    self.db.update_opportunity_stage(op_id, next_stage['id'])
                    self.add_movement_record(op_id, op_data['estagio_nome'], next_stage['nome'], "Aprovado")
                    messagebox.showinfo("Sucesso", f"Oportunidade aprovada e movida para: {next_stage['nome']}", parent=self.root)
                else:
                    messagebox.showinfo("Informação", "Esta oportunidade já está no último estágio.", parent=self.root)
            self.show_kanban_view()

        def reprovar():
            historico_stage = next((e for e in self.db.get_pipeline_data()[0] if e['nome'] == "Histórico"), None)
            if historico_stage:
                self.db.update_opportunity_stage(op_id, historico_stage['id'])
                self.add_movement_record(op_id, op_data['estagio_nome'], "Histórico", "Reprovado")
                messagebox.showinfo("Sucesso", "Oportunidade reprovada e movida para o Histórico.", parent=self.root)
            self.show_kanban_view()

        ttk.Button(buttons_frame, text="✓ Aprovado", command=aprovar, style='Success.TButton').pack(side='left', padx=(0, 10))
        ttk.Button(buttons_frame, text="✗ Reprovado", command=reprovar, style='Danger.TButton').pack(side='left')

    def add_movement_record(self, op_id, from_stage, to_stage, result):
        """Adiciona registro de movimentação no histórico"""
        data = {
            'oportunidade_id': op_id,
            'data_interacao': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
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

        # Número da Oportunidade
        ttk.Label(filter_row1, text="Nº Oportunidade:", style='TLabel').grid(row=0, column=0, sticky='w', padx=(0, 5))
        num_op_filter = ttk.Entry(filter_row1, width=15)
        num_op_filter.grid(row=0, column=1, padx=(0, 20))

        # Cliente
        ttk.Label(filter_row1, text="Cliente:", style='TLabel').grid(row=0, column=2, sticky='w', padx=(0, 5))
        client_filter = ttk.Combobox(filter_row1, values=['Todos'] + [c['nome_empresa'] for c in self.db.get_all_clients()], width=20)
        client_filter.set('Todos')
        client_filter.grid(row=0, column=3, padx=(0, 20))

        # Estágio
        ttk.Label(filter_row1, text="Estágio:", style='TLabel').grid(row=0, column=4, sticky='w', padx=(0, 5))
        stage_filter = ttk.Combobox(filter_row1, values=['Todos'] + [e['nome'] for e in self.db.get_pipeline_data()[0]], width=25)
        stage_filter.set('Todos')
        stage_filter.grid(row=0, column=5, padx=(0, 20))

        # Resultado
        ttk.Label(filter_row1, text="Resultado:", style='TLabel').grid(row=0, column=6, sticky='w', padx=(0, 5))
        result_filter = ttk.Combobox(filter_row1, values=['Todos', 'Aprovado', 'Reprovado'], width=15)
        result_filter.set('Todos')
        result_filter.grid(row=0, column=7)

        # Segunda linha de filtros
        filter_row2 = ttk.Frame(filters_frame, style='TFrame')
        filter_row2.pack(fill='x', pady=(10, 10))

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
                               command=lambda: self.apply_historico_filters(num_op_filter, client_filter, stage_filter, result_filter, period_filter, min_value_filter, results_tree))
        search_btn.grid(row=0, column=4, padx=(20, 0))

        # Tabela de resultados
        results_frame = ttk.Frame(self.content_frame, style='TFrame')
        results_frame.pack(fill='both', expand=True)

        # Treeview para mostrar oportunidades
        columns = ('num_op', 'titulo', 'cliente', 'estagio', 'valor', 'data_criacao', 'ultimo_resultado')
        results_tree = ttk.Treeview(results_frame, columns=columns, show='headings', height=15)

        # Cabeçalhos
        results_tree.heading('num_op', text='Nº Oport.')
        results_tree.heading('titulo', text='Título')
        results_tree.heading('cliente', text='Cliente')
        results_tree.heading('estagio', text='Estágio Atual')
        results_tree.heading('valor', text='Valor (R$)')
        results_tree.heading('data_criacao', text='Data Criação')
        results_tree.heading('ultimo_resultado', text='Último Resultado')

        # Larguras das colunas
        results_tree.column('num_op', width=100, anchor='center')
        results_tree.column('titulo', width=250)
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
                self.show_opportunity_details(op_id, self.show_historico_view)

        results_tree.bind('<Double-1>', on_item_double_click)

        # Carregar todas as oportunidades inicialmente
        self.load_historico_data(results_tree)

    def apply_historico_filters(self, num_op_filter, client_filter, stage_filter, result_filter, period_filter, min_value_filter, results_tree):
        """Aplica filtros e atualiza a tabela de histórico"""
        filters = {
            'numero_oportunidade': num_op_filter.get().strip() if num_op_filter.get().strip() else None,
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
                           op['numero_oportunidade'] if 'numero_oportunidade' in op.keys() else '---',
                           op['titulo'],
                           op['nome_empresa'],
                           op['estagio_nome'],
                           format_currency(op['valor']),
                           data_criacao_str,
                           ultimo_resultado or '---'
                       ),
                       tags=(str(op['id']),))  # Armazenar ID nas tags

    def show_opportunity_form(self, op_id=None, client_to_prefill=None, back_callback=None):
        self.clear_content()
        form_title = "Nova Oportunidade" if not op_id else "Editar Oportunidade"

        # Configure grid layout for the main content frame to allow vertical expansion
        self.content_frame.rowconfigure(1, weight=1)
        self.content_frame.columnconfigure(0, weight=1)

        # --- Header with Back button ---
        header_frame = ttk.Frame(self.content_frame, style='TFrame')
        header_frame.grid(row=0, column=0, sticky='ew', pady=(0, 10))
        ttk.Label(header_frame, text=form_title, style='Title.TLabel').pack(side='left')
        if back_callback:
            ttk.Button(header_frame, text="← Voltar", command=back_callback, style='TButton').pack(side='right')

        # --- Notebook for the tabs ---
        notebook = ttk.Notebook(self.content_frame)
        notebook.grid(row=1, column=0, sticky='nsew', pady=(0, 10))

        # --- Action buttons at the bottom ---
        buttons_frame = ttk.Frame(self.content_frame, padding=(10, 15, 10, 0))
        buttons_frame.grid(row=2, column=0, sticky='ew')

        analise_frame = self._create_scrollable_tab(notebook, '  Análise Prévia de Viabilidade  ')
        sumario_frame = self._create_scrollable_tab(notebook, '  Sumário Executivo  ')

        clients = self.db.get_all_clients()
        client_map = {c['nome_empresa']: c['id'] for c in clients}
        estagios = self.db.get_pipeline_data()[0]
        estagio_map = {e['nome']: e['id'] for e in estagios}
        servicos = self.db.get_all_servicos()
        servico_map = {s['nome']: s['id'] for s in servicos}
        servico_names = [t['nome'] for t in servicos]
        entries = {}

        analise_frame.columnconfigure(1, weight=1)
        info_basicas = ttk.LabelFrame(analise_frame, text="Informações Básicas", padding=15, style='White.TLabelframe')
        info_basicas.pack(fill='x', pady=(0, 10))
        info_basicas.columnconfigure(1, weight=1)
        basic_fields = [
            ("Título:*", "titulo", "entry"), ("Cliente:*", "cliente_id", "combobox", [c['nome_empresa'] for c in clients]),
            ("Estágio:*", "estagio_id", "combobox", [e['nome'] for e in estagios if e['nome'] != 'Histórico']),
            ("Valor Estimado (R$):", "valor", "entry")
        ]
        for i, field_info in enumerate(basic_fields):
            text, key = field_info[0], field_info[1]
            ttk.Label(info_basicas, text=text, style='TLabel').grid(row=i, column=0, sticky='w', pady=5, padx=5)
            if len(field_info) > 3: widget = ttk.Combobox(info_basicas, values=field_info[3], state='readonly')
            elif field_info[2] == "combobox": widget = ttk.Combobox(info_basicas, state='readonly')
            else: widget = ttk.Entry(info_basicas)
            widget.grid(row=i, column=1, sticky='ew', pady=5, padx=5)
            entries[key] = widget

        qualificacao_frame = ttk.LabelFrame(analise_frame, text="Formulário de Análise de Qualificação da Oportunidade", padding=15, style='White.TLabelframe')
        qualificacao_frame.pack(fill='x', pady=(0, 10))
        qualificacao_vars = {}
        entries['qualificacao_data'] = qualificacao_vars
        q_diferenciais = "Quais são nossos diferenciais competitivos claros para esta oportunidade específica?"
        q_riscos = "Quais os principais riscos (técnicos, logísticos, regulatórios, políticos) associados ao projeto?"
        question_counter = 1
        for section, questions in QUALIFICATION_CHECKLIST.items():
            section_frame = ttk.LabelFrame(qualificacao_frame, text=section, padding=10, style='White.TLabelframe')
            section_frame.pack(fill='x', expand=True, pady=5)
            section_frame.columnconfigure(0, weight=1)
            row_idx = 0
            for question in questions:
                numbered_question = f"{question_counter}. {question}"
                if question == q_diferenciais:
                    q_label = ttk.Label(section_frame, text=numbered_question, wraplength=800, justify='left')
                    q_label.grid(row=row_idx, column=0, columnspan=2, sticky='w', pady=(5,2))
                    diferenciais_text = tk.Text(section_frame, height=4, wrap='word', bg='white', font=('Segoe UI', 10))
                    diferenciais_text.grid(row=row_idx + 1, column=0, columnspan=2, sticky='ew', pady=(0, 10), padx=5)
                    entries['diferenciais_competitivos'] = diferenciais_text
                    row_idx += 2
                elif question == q_riscos:
                    q_label = ttk.Label(section_frame, text=numbered_question, wraplength=800, justify='left')
                    q_label.grid(row=row_idx, column=0, columnspan=2, sticky='w', pady=(5,2))
                    riscos_text = tk.Text(section_frame, height=4, wrap='word', bg='white', font=('Segoe UI', 10))
                    riscos_text.grid(row=row_idx + 1, column=0, columnspan=2, sticky='ew', pady=(0, 10), padx=5)
                    entries['principais_riscos'] = riscos_text
                    row_idx += 2
                else:
                    q_var = tk.StringVar(value="")
                    qualificacao_vars[question] = q_var
                    q_label = ttk.Label(section_frame, text=numbered_question, wraplength=800, justify='left')
                    q_label.grid(row=row_idx, column=0, sticky='w', pady=(5,0))
                    radio_frame = ttk.Frame(section_frame)
                    radio_frame.grid(row=row_idx, column=1, sticky='e', padx=10)
                    rb_sim = ttk.Radiobutton(radio_frame, text="Sim", variable=q_var, value="Sim")
                    rb_nao = ttk.Radiobutton(radio_frame, text="Não", variable=q_var, value="Não")
                    rb_sim.pack(side='left')
                    rb_nao.pack(side='left', padx=10)
                    row_idx += 1
                question_counter += 1

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
            ttk.Label(row_frame, text="Tipo de Equipe:").pack(side='left', padx=(0,5))
            tipo_combo = ttk.Combobox(row_frame, values=team_type_names, state='readonly', width=40)
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

        servicos_lf = ttk.LabelFrame(analise_frame, text="Configuração de Serviços e Equipes", padding=15, style='White.TLabelframe')
        servicos_lf.pack(fill='both', expand=True, pady=(0, 10))
        servicos_lf.columnconfigure(1, weight=1)
        servicos_config_frame = ttk.Frame(servicos_lf, padding=(0, 10))
        ttk.Label(servicos_lf, text="Tipos de Serviço:", font=('Segoe UI', 10, 'bold')).grid(row=0, column=0, sticky='nw', pady=5, padx=5)
        tipos_frame = ttk.Frame(servicos_lf)
        tipos_frame.grid(row=0, column=1, sticky='ew', pady=5, padx=5)
        tipos_vars = {}
        col_count = 3
        row, col = 0, 0
        for name in servico_names:
            var = tk.BooleanVar()
            tipos_vars[name] = var
            cb = ttk.Checkbutton(tipos_frame, text=name, variable=var, command=_update_servicos_ui)
            cb.grid(row=row, column=col, sticky='w', padx=5, pady=2)
            col += 1
            if col >= col_count: col, row = 0, row + 1
        entries['tipos_servico_vars'] = tipos_vars
        start_row_after_services = 1
        checklist_fields = [("Tempo de Contrato (meses):", "tempo_contrato_meses", "entry"), ("Regional:", "regional", "entry"), ("Polo:", "polo", "entry")]
        for i, (text, key, widget_type) in enumerate(checklist_fields):
            ttk.Label(servicos_lf, text=text).grid(row=start_row_after_services + i, column=0, sticky='w', pady=5, padx=5)
            entry = ttk.Entry(servicos_lf)
            entry.grid(row=start_row_after_services + i, column=1, sticky='ew', pady=5, padx=5)
            entries[key] = entry
        empresa_row = start_row_after_services + len(checklist_fields)
        ttk.Label(servicos_lf, text="Empresa Referência:", font=('Segoe UI', 10, 'bold')).grid(row=empresa_row, column=0, sticky='w', pady=5, padx=5)
        empresas_ref = self.db.get_all_empresas_referencia()
        empresa_names = sorted(list(set([emp['nome_empresa'] for emp in empresas_ref])))
        empresa_combo = ttk.Combobox(servicos_lf, values=empresa_names, state='readonly')
        empresa_combo.grid(row=empresa_row, column=1, sticky='ew', pady=5, padx=5)
        entries['empresa_referencia'] = empresa_combo
        bases_row = empresa_row + 1
        ttk.Label(servicos_lf, text="Quantidade de Bases:", font=('Segoe UI', 10, 'bold')).grid(row=bases_row, column=0, sticky='w', pady=5, padx=5)
        bases_input_frame = ttk.Frame(servicos_lf)
        bases_input_frame.grid(row=bases_row, column=1, sticky='ew', pady=5, padx=5)
        bases_fields_frame = ttk.Frame(servicos_lf)
        bases_fields_frame.grid(row=bases_row + 1, column=0, columnspan=2, sticky='ew', pady=5, padx=5)
        next_row_for_dynamic_services = bases_row + 2
        servicos_config_frame.grid(row=next_row_for_dynamic_services, column=0, columnspan=2, sticky='ew', pady=(10, 0))
        base_name_entries = []
        entries['bases_nomes_widgets'] = base_name_entries
        def _update_base_fields_ui():
            for widget in bases_fields_frame.winfo_children(): widget.destroy()
            base_name_entries.clear()
            try: num_bases = int(bases_spinbox.get())
            except (ValueError, tk.TclError): num_bases = 0
            for i in range(num_bases):
                base_frame = ttk.Frame(bases_fields_frame)
                base_frame.pack(fill='x', pady=2, padx=5)
                ttk.Label(base_frame, text=f"Base {i+1}:", width=15).pack(side='left')
                entry = ttk.Entry(base_frame, width=30)
                entry.pack(side='left', fill='x', expand=True, padx=5)
                base_name_entries.append(entry)
        def _update_base_fields_ui_and_combos():
            _update_base_fields_ui()
            base_widgets = entries.get('bases_nomes_widgets', [])
            base_names = [b.get().strip() for b in base_widgets if b.get().strip()]
            for service, equipe_rows in servico_equipes_data.items():
                for row in equipe_rows:
                    if 'base_combo' in row: row['base_combo']['values'] = base_names
        bases_spinbox = ttk.Spinbox(bases_input_frame, from_=0, to=50, width=10, command=_update_base_fields_ui_and_combos)
        bases_spinbox.pack(side='left')
        entries['quantidade_bases'] = bases_spinbox

        sumario_frame.columnconfigure(1, weight=1)
        edital_frame = ttk.LabelFrame(sumario_frame, text="Informações do Edital", padding=15, style='White.TLabelframe')
        edital_frame.pack(fill='x', pady=(0, 10))
        edital_frame.columnconfigure(1, weight=1)
        edital_fields = [
            ("Número do Edital:", "numero_edital", "entry"), ("Data de Abertura:", "data_abertura", "date"),
            ("Modalidade:", "modalidade", "entry"), ("Contato Principal:", "contato_principal", "entry"),
            ("Link da Pasta de Documentos:", "link_documentos", "entry")
        ]
        for i, (text, key, widget_type) in enumerate(edital_fields):
            ttk.Label(edital_frame, text=text).grid(row=i, column=0, sticky='w', pady=5, padx=5)
            if widget_type == 'date': entry = DateEntry(edital_frame, date_pattern='dd/mm/yyyy', width=20)
            else: entry = ttk.Entry(edital_frame)
            entry.grid(row=i, column=1, sticky='ew', pady=5, padx=5)
            entries[key] = entry

        cotacao_frame = ttk.LabelFrame(sumario_frame, text="Informações de Cotação", padding=15, style='White.TLabelframe')
        cotacao_frame.pack(fill='x', pady=(0, 10))
        cotacao_frame.columnconfigure(1, weight=1)
        cotacao_fields = [
            ("Faturamento Estimado (R$):", "faturamento_estimado", "entry"), ("Duração do Contrato (meses):", "duracao_contrato", "entry"),
            ("MOD (Mão de Obra Direta):", "mod", "entry"), ("MOI (Mão de Obra Indireta):", "moi", "entry"),
            ("Total de Pessoas:", "total_pessoas", "entry"), ("Margem de Contribuição (%):", "margem_contribuicao", "entry"),
        ]
        def calcular_total_pessoas():
            try:
                mod = float(entries['mod'].get() or '0')
                moi = float(entries['moi'].get() or '0')
                total = mod + moi
                entries['total_pessoas'].delete(0, 'end')
                entries['total_pessoas'].insert(0, str(int(total)))
            except ValueError: pass
        for i, (text, key, widget_type) in enumerate(cotacao_fields):
            ttk.Label(cotacao_frame, text=text).grid(row=i, column=0, sticky='w', pady=5, padx=5)
            if widget_type == 'date': entry = DateEntry(cotacao_frame, date_pattern='dd/mm/yyyy', width=20)
            else:
                entry = ttk.Entry(cotacao_frame)
                if key in ['mod', 'moi']:
                    entry.bind('<KeyRelease>', lambda e: calcular_total_pessoas())
                    entry.bind('<FocusOut>', lambda e: calcular_total_pessoas())
            entry.grid(row=i, column=1, sticky='ew', pady=5, padx=5)
            entries[key] = entry

        servicos_frame = ttk.LabelFrame(sumario_frame, text="Detalhes dos Serviços e Preços (Calculado Automaticamente)", padding=15, style='White.TLabelframe')
        servicos_frame.pack(fill='x', pady=(0, 10))
        info_calculo = ttk.Label(servicos_frame, text="Os preços são calculados com base na Análise Prévia. Clique no botão para recalcular.", font=('Segoe UI', 9, 'italic'), foreground=DOLP_COLORS['medium_gray'])
        info_calculo.pack(pady=(0, 10))
        calculo_frame = ttk.Frame(servicos_frame)
        calculo_frame.pack(fill='x', pady=(0, 10))
        ttk.Button(calculo_frame, text="Calcular Preços Automaticamente", command=lambda: calcular_precos_automaticos(), style='Primary.TButton').pack(side='left', padx=5)
        servicos_tree = ttk.Treeview(servicos_frame, columns=('servico', 'quantidade', 'volumetria', 'preco_unitario', 'preco_total'), show='headings', height=6)
        servicos_tree.heading('servico', text='Serviço'); servicos_tree.heading('quantidade', text='Qtd Equipes'); servicos_tree.heading('volumetria', text='Volumetria'); servicos_tree.heading('preco_unitario', text='Preço Unit. (R$)'); servicos_tree.heading('preco_total', text='Preço Total (R$)')
        servicos_tree.column('servico', width=200); servicos_tree.column('quantidade', width=80, anchor='center'); servicos_tree.column('volumetria', width=100, anchor='center'); servicos_tree.column('preco_unitario', width=120, anchor='center'); servicos_tree.column('preco_total', width=120, anchor='center')
        servicos_tree.pack(fill='x', pady=5)
        entries['servicos_tree'] = servicos_tree
        def calcular_precos_automaticos():
            empresa_nome = entries['empresa_referencia'].get()
            if not empresa_nome:
                messagebox.showwarning("Aviso", "Por favor, selecione uma Empresa Referência na aba 'Análise Prévia' primeiro.", parent=self.root)
                return
            for item in servicos_tree.get_children(): servicos_tree.delete(item)
            faturamento_total = 0.0
            servico_equipes_data = entries.get('servicos_data', {})
            tipos_servico_vars = entries.get('tipos_servico_vars', {})
            for servico_nome, equipe_rows in servico_equipes_data.items():
                if not (tipos_servico_vars.get(servico_nome) and tipos_servico_vars[servico_nome].get()): continue
                ref_data = self.db.get_empresa_referencia_by_nome_e_tipo(empresa_nome, servico_nome)
                if not ref_data:
                    servicos_tree.insert('', 'end', values=(servico_nome, '---', '---', 'N/A', 'Ref. não encontrada'))
                    continue
                preco_unitario = ref_data['valor_mensal']
                total_qtd_equipes, total_volumetria = 0, 0.0
                for row_widgets in equipe_rows:
                    try:
                        total_qtd_equipes += int(row_widgets['qtd_entry'].get() or 0)
                        total_volumetria += float(row_widgets['vol_entry'].get().replace(',', '.') or 0)
                    except (ValueError, TypeError):
                        messagebox.showerror("Erro de Formato", f"Verifique os valores de Quantidade e Volumetria para o serviço '{servico_nome}'. Devem ser números.", parent=self.root)
                        return
                preco_total_servico = total_qtd_equipes * preco_unitario
                faturamento_total += preco_total_servico
                servicos_tree.insert('', 'end', values=(servico_nome, total_qtd_equipes, f"{total_volumetria:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."), format_currency(preco_unitario), format_currency(preco_total_servico)))
            entries['faturamento_estimado'].delete(0, 'end')
            faturamento_estimado_str = f"{faturamento_total:.2f}".replace('.', ',')
            entries['faturamento_estimado'].insert(0, faturamento_estimado_str)
            messagebox.showinfo("Sucesso", "Cálculo de preços concluído e Faturamento Estimado atualizado.", parent=self.root)

        desc_frame = ttk.LabelFrame(sumario_frame, text="Descrição Detalhada", padding=15, style='White.TLabelframe')
        desc_frame.pack(fill='both', expand=True, pady=(0, 10))
        desc_text = tk.Text(desc_frame, height=8, wrap='word', bg='white', font=('Segoe UI', 10))
        desc_scrollbar = ttk.Scrollbar(desc_frame, orient="vertical", command=desc_text.yview)
        desc_text.configure(yscrollcommand=desc_scrollbar.set)
        desc_text.pack(side="left", fill="both", expand=True)
        desc_scrollbar.pack(side="right", fill="y")
        entries['descricao_detalhada'] = desc_text

        if op_id:
            try:
                op_data = self.db.get_opportunity_details(op_id)
                if not op_data:
                    messagebox.showerror("Erro", f"Não foi possível encontrar os dados para a oportunidade com ID {op_id}.", parent=self.root)
                    return
                op_keys = op_data.keys()
                entries['titulo'].insert(0, str(op_data['titulo']) if 'titulo' in op_keys and op_data['titulo'] is not None else '')
                entries['valor'].insert(0, format_brazilian_currency_for_entry(op_data['valor']) if 'valor' in op_keys and op_data['valor'] is not None else '0,00')
                cliente_id_val = op_data['cliente_id'] if 'cliente_id' in op_keys else None
                for client in clients:
                    if client['id'] == cliente_id_val: entries['cliente_id'].set(client['nome_empresa']); break
                estagio_id_val = op_data['estagio_id'] if 'estagio_id' in op_keys else None
                for estagio in estagios:
                    if estagio['id'] == estagio_id_val: entries['estagio_id'].set(estagio['nome']); break
                entries['tempo_contrato_meses'].insert(0, str(op_data['tempo_contrato_meses']) if 'tempo_contrato_meses' in op_keys and op_data['tempo_contrato_meses'] is not None else '')
                entries['regional'].insert(0, str(op_data['regional']) if 'regional' in op_keys and op_data['regional'] is not None else '')
                entries['polo'].insert(0, str(op_data['polo']) if 'polo' in op_keys and op_data['polo'] is not None else '')
                entries['empresa_referencia'].set(str(op_data['empresa_referencia']) if 'empresa_referencia' in op_keys and op_data['empresa_referencia'] is not None else '')
                entries['numero_edital'].insert(0, str(op_data['numero_edital']) if 'numero_edital' in op_keys and op_data['numero_edital'] is not None else '')
                data_abertura_str = op_data['data_abertura'] if 'data_abertura' in op_keys else None
                if data_abertura_str:
                    try:
                        date_obj = datetime.strptime(data_abertura_str, '%d/%m/%Y').date()
                        entries['data_abertura'].set_date(date_obj)
                    except (ValueError, TypeError): pass
                entries['modalidade'].insert(0, str(op_data['modalidade']) if 'modalidade' in op_keys and op_data['modalidade'] is not None else '')
                entries['contato_principal'].insert(0, str(op_data['contato_principal']) if 'contato_principal' in op_keys and op_data['contato_principal'] is not None else '')
                entries['link_documentos'].insert(0, str(op_data['link_documentos']) if 'link_documentos' in op_keys and op_data['link_documentos'] is not None else '')
                entries['faturamento_estimado'].insert(0, format_brazilian_currency_for_entry(op_data['faturamento_estimado']) if 'faturamento_estimado' in op_keys and op_data['faturamento_estimado'] is not None else '0,00')
                entries['duracao_contrato'].insert(0, str(op_data['duracao_contrato']) if 'duracao_contrato' in op_keys and op_data['duracao_contrato'] is not None else '')
                entries['mod'].insert(0, format_brazilian_currency_for_entry(op_data['mod']) if 'mod' in op_keys and op_data['mod'] is not None else '0,00')
                entries['moi'].insert(0, format_brazilian_currency_for_entry(op_data['moi']) if 'moi' in op_keys and op_data['moi'] is not None else '0,00')
                entries['total_pessoas'].insert(0, str(op_data['total_pessoas']) if 'total_pessoas' in op_keys and op_data['total_pessoas'] is not None else '')
                entries['margem_contribuicao'].insert(0, format_brazilian_currency_for_entry(op_data['margem_contribuicao']) if 'margem_contribuicao' in op_keys and op_data['margem_contribuicao'] is not None else '0,00')
                descricao_detalhada = op_data['descricao_detalhada'] if 'descricao_detalhada' in op_keys else None
                if descricao_detalhada: entries['descricao_detalhada'].insert('1.0', str(descricao_detalhada))
                analise_frame.update_idletasks()
                num_bases = op_data['quantidade_bases'] if 'quantidade_bases' in op_keys else None
                if num_bases is not None:
                    bases_spinbox.set(num_bases)
                    _update_base_fields_ui()
                    bases_nomes_json = op_data['bases_nomes'] if 'bases_nomes' in op_keys else None
                    if bases_nomes_json:
                        try:
                            bases_nomes_data = json.loads(bases_nomes_json)
                            base_widgets = entries.get('bases_nomes_widgets', [])
                            for i, nome in enumerate(bases_nomes_data):
                                if i < len(base_widgets): base_widgets[i].insert(0, nome)
                        except (json.JSONDecodeError, TypeError): print(f"Alerta: Falha ao carregar nomes de bases: {bases_nomes_json}")
                analise_frame.update_idletasks()
                servicos_data_json_str = op_data['servicos_data'] if 'servicos_data' in op_keys else None
                if servicos_data_json_str:
                    try:
                        servicos_data_json = json.loads(servicos_data_json_str)
                        tipos_servico_vars = entries.get('tipos_servico_vars', {})
                        for servico_info in servicos_data_json:
                            servico_nome = servico_info.get('servico_nome')
                            if servico_nome in tipos_servico_vars: tipos_servico_vars[servico_nome].set(True)
                        _update_servicos_ui()
                        analise_frame.update_idletasks()
                        for servico_info in servicos_data_json:
                            servico_nome = servico_info.get('servico_nome')
                            equipes_data = servico_info.get('equipes', [])
                            if servico_nome in servico_frames:
                                servico_id = servico_map.get(servico_nome)
                                container = next((w for w in servico_frames[servico_nome].winfo_children() if isinstance(w, ttk.Frame)), None)
                                if container and servico_id:
                                    for equipe_info in equipes_data:
                                        _add_equipe_row(servico_id, servico_nome, container)
                                        new_row_widgets = servico_equipes_data[servico_nome][-1]
                                        new_row_widgets['tipo_combo'].set(equipe_info.get('tipo_equipe', ''))
                                        new_row_widgets['qtd_entry'].insert(0, equipe_info.get('quantidade', ''))
                                        new_row_widgets['vol_entry'].insert(0, equipe_info.get('volumetria', ''))
                                        new_row_widgets['base_combo'].set(equipe_info.get('base', ''))
                    except (json.JSONDecodeError, TypeError) as e:
                        print(f"Erro ao carregar dados de serviços: {e}")
                        messagebox.showwarning("Alerta de Carregamento", "Não foi possível carregar os detalhes de serviços e equipes. Os dados podem estar corrompidos.", parent=self.root)
                qualificacao_data_json = op_data['qualificacao_data'] if 'qualificacao_data' in op_keys else None
                if qualificacao_data_json:
                    try:
                        qualificacao_answers = json.loads(qualificacao_data_json)
                        qualificacao_vars = entries.get('qualificacao_data', {})
                        for question, answer in qualificacao_answers.items():
                            if question in qualificacao_vars: qualificacao_vars[question].set(answer)
                    except (json.JSONDecodeError, TypeError) as e: print(f"Erro ao carregar dados de qualificação: {e}")
                if 'diferenciais_competitivos' in entries and 'diferenciais_competitivos' in op_keys and op_data['diferenciais_competitivos']:
                    entries['diferenciais_competitivos'].insert('1.0', op_data['diferenciais_competitivos'])
                if 'principais_riscos' in entries and 'principais_riscos' in op_keys and op_data['principais_riscos']:
                    entries['principais_riscos'].insert('1.0', op_data['principais_riscos'])
            except Exception as e:
                import traceback
                traceback.print_exc()
                messagebox.showerror("Erro Crítico ao Carregar Dados", f"Ocorreu um erro inesperado ao carregar os dados da oportunidade.\n\nErro: {str(e)}\n\nO formulário pode não exibir todos os dados corretamente.", parent=self.root)

        if client_to_prefill:
            entries['cliente_id'].set(client_to_prefill)
            entries['estagio_id'].set("Oportunidades")

        def on_save():
            try:
                data = {}
                data['titulo'] = entries['titulo'].get().strip()
                data['valor'] = parse_brazilian_currency(entries['valor'].get())
                data['cliente_id'] = client_map.get(entries['cliente_id'].get())
                data['estagio_id'] = estagio_map.get(entries['estagio_id'].get())
                if not data['titulo'] or not data['cliente_id'] or not data['estagio_id']:
                    messagebox.showerror("Erro", "Título, Cliente e Estágio são obrigatórios!", parent=self.root)
                    return
                data['tempo_contrato_meses'] = entries['tempo_contrato_meses'].get().strip()
                data['regional'] = entries['regional'].get().strip()
                data['polo'] = entries['polo'].get().strip()
                data['quantidade_bases'] = entries['quantidade_bases'].get()
                data['empresa_referencia'] = entries['empresa_referencia'].get()
                base_widgets = entries.get('bases_nomes_widgets', [])
                data['bases_nomes'] = json.dumps([entry.get().strip() for entry in base_widgets if entry.get().strip()])
                servicos_data_to_save = []
                tipos_servico_vars = entries.get('tipos_servico_vars', {})
                servico_equipes_data = entries.get('servicos_data', {})
                for servico_nome, equipe_rows in servico_equipes_data.items():
                    if tipos_servico_vars.get(servico_nome) and tipos_servico_vars[servico_nome].get():
                        equipes_to_save = []
                        for row_widgets in equipe_rows:
                            equipe_data = {"tipo_equipe": row_widgets['tipo_combo'].get(), "quantidade": row_widgets['qtd_entry'].get(), "volumetria": row_widgets['vol_entry'].get(), "base": row_widgets['base_combo'].get()}
                            equipes_to_save.append(equipe_data)
                        servico_entry = { "servico_nome": servico_nome, "equipes": equipes_to_save }
                        servicos_data_to_save.append(servico_entry)
                data['servicos_data'] = json.dumps(servicos_data_to_save)
                qualificacao_answers = {}
                qualificacao_vars = entries.get('qualificacao_data', {})
                for question, var in qualificacao_vars.items(): qualificacao_answers[question] = var.get()
                data['qualificacao_data'] = json.dumps(qualificacao_answers)
                if 'diferenciais_competitivos' in entries: data['diferenciais_competitivos'] = entries['diferenciais_competitivos'].get('1.0', 'end-1c').strip()
                if 'principais_riscos' in entries: data['principais_riscos'] = entries['principais_riscos'].get('1.0', 'end-1c').strip()
                data['numero_edital'] = entries['numero_edital'].get().strip()
                data['data_abertura'] = entries['data_abertura'].get() if hasattr(entries['data_abertura'], 'get') else ''
                data['modalidade'] = entries['modalidade'].get().strip()
                data['contato_principal'] = entries['contato_principal'].get().strip()
                data['link_documentos'] = entries['link_documentos'].get().strip()
                data['faturamento_estimado'] = parse_brazilian_currency(entries['faturamento_estimado'].get())
                data['duracao_contrato'] = entries['duracao_contrato'].get().strip()
                data['mod'] = parse_brazilian_currency(entries['mod'].get())
                data['moi'] = parse_brazilian_currency(entries['moi'].get())
                data['total_pessoas'] = entries['total_pessoas'].get().strip()
                data['margem_contribuicao'] = parse_brazilian_currency(entries['margem_contribuicao'].get())
                data['descricao_detalhada'] = entries['descricao_detalhada'].get('1.0', 'end-1c')
                if op_id:
                    self.db.update_opportunity(op_id, data)
                    messagebox.showinfo("Sucesso", "Oportunidade atualizada com sucesso!", parent=self.root)
                else:
                    self.db.add_opportunity(data)
                    messagebox.showinfo("Sucesso", "Oportunidade criada com sucesso!", parent=self.root)
                if back_callback:
                    back_callback()
            except sqlite3.Error as e:
                 messagebox.showerror("Erro de Banco de Dados", f"Erro ao salvar: {str(e)}", parent=self.root)
            except Exception as e:
                messagebox.showerror("Erro Inesperado", f"Ocorreu um erro: {str(e)}", parent=self.root)

        ttk.Button(buttons_frame, text="Salvar Alterações" if op_id else "Criar Oportunidade", command=on_save, style='Success.TButton').pack(side='right')

    def show_opportunity_details(self, op_id, back_callback):
        self.clear_content()

        op_data = self.db.get_opportunity_details(op_id)
        if not op_data:
            messagebox.showerror("Erro", "Oportunidade não encontrada!", parent=self.root)
            back_callback()
            return

        op_keys = op_data.keys()

        header_frame = ttk.Frame(self.content_frame, padding=20, style='TFrame')
        header_frame.pack(fill='x')

        title_text = f"{op_data['numero_oportunidade'] if 'numero_oportunidade' in op_keys else 'OPP-?????'}: {op_data['titulo'] if 'titulo' in op_keys else 'Sem Título'}"
        ttk.Label(header_frame, text=title_text, style='Title.TLabel').pack(side='left')
        ttk.Button(header_frame, text="Editar Detalhes", command=lambda: self.show_opportunity_form(op_id, back_callback=back_callback), style='Primary.TButton').pack(side='right')
        ttk.Button(header_frame, text="← Voltar", command=back_callback, style='TButton').pack(side='right', padx=(0, 10))

        notebook = ttk.Notebook(self.content_frame, padding=10)
        notebook.pack(fill='both', expand=True, padx=20, pady=(0, 20))

        # --- Refatoração para usar abas com rolagem ---

        # Aba 1: Análise Prévia de Viabilidade
        analise_tab = self._create_scrollable_tab(notebook, '  Análise Prévia de Viabilidade  ')

        # Botão de Exportar
        export_analise_btn = ttk.Button(analise_tab, text="Exportar para PDF", command=lambda: self.export_analise_previa_pdf(op_id), style='Primary.TButton')
        export_analise_btn.pack(anchor='ne', pady=(0, 10))

        info_frame = ttk.LabelFrame(analise_tab, text="Informações Básicas", padding=15, style='White.TLabelframe')
        info_frame.pack(fill='x', pady=(0, 10))

        basic_info = [
            ("Cliente:", op_data['nome_empresa'] if 'nome_empresa' in op_keys else '---'),
            ("Estágio:", op_data['estagio_nome'] if 'estagio_nome' in op_keys else '---'),
            ("Valor Estimado:", format_currency(op_data['valor'] if 'valor' in op_keys else 0)),
            ("Tempo de Contrato:", f"{op_data['tempo_contrato_meses']} meses" if 'tempo_contrato_meses' in op_keys and op_data['tempo_contrato_meses'] else "---"),
            ("Regional:", op_data['regional'] if 'regional' in op_keys else '---'),
            ("Polo:", op_data['polo'] if 'polo' in op_keys else '---'),
            ("Empresa Referência:", op_data['empresa_referencia'] if 'empresa_referencia' in op_keys else '---')
        ]

        info_frame.columnconfigure(1, weight=1)
        for i, (label, value) in enumerate(basic_info):
            ttk.Label(info_frame, text=label, style='Metric.White.TLabel').grid(row=i, column=0, sticky='w', pady=2)
            ttk.Label(info_frame, text=str(value), style='Value.White.TLabel', wraplength=400).grid(row=i, column=1, sticky='w', pady=2, padx=(10,0))

        bases_nomes_json = op_data['bases_nomes'] if 'bases_nomes' in op_keys else None
        if bases_nomes_json:
            try:
                bases_nomes = json.loads(bases_nomes_json)
                if bases_nomes:
                    bases_frame = ttk.LabelFrame(analise_tab, text="Bases Alocadas", padding=15, style='White.TLabelframe')
                    bases_frame.pack(fill='x', pady=(10, 0))
                    for i, base in enumerate(bases_nomes, 1):
                        base_frame = ttk.Frame(bases_frame)
                        base_frame.pack(fill='x', pady=2)
                        ttk.Label(base_frame, text=f"Base {i}:", style='Metric.White.TLabel', width=10).pack(side='left')
                        ttk.Label(base_frame, text=base, style='Value.White.TLabel').pack(side='left', padx=(10, 0))
            except (json.JSONDecodeError, TypeError):
                print(f"Alerta: Falha ao carregar nomes de bases na tela de detalhes: {bases_nomes_json}")

        qual_frame = ttk.LabelFrame(analise_tab, text="Formulário de Análise de Qualificação da Oportunidade", padding=15, style='White.TLabelframe')
        qual_frame.pack(fill='x', pady=(10, 0))
        qualificacao_data_json = op_data['qualificacao_data'] if 'qualificacao_data' in op_keys else None
        qualificacao_answers = {}
        if qualificacao_data_json:
            try:
                qualificacao_answers = json.loads(qualificacao_data_json)
            except (json.JSONDecodeError, TypeError):
                pass
        q_diferenciais = "Quais são nossos diferenciais competitivos claros para esta oportunidade específica?"
        q_riscos = "Quais os principais riscos (técnicos, logísticos, regulatórios, políticos) associados ao projeto?"
        question_counter = 1
        for section, questions in QUALIFICATION_CHECKLIST.items():
            section_frame = ttk.LabelFrame(qual_frame, text=section, padding=10, style='White.TLabelframe')
            section_frame.pack(fill='x', expand=True, pady=5)
            section_frame.columnconfigure(1, weight=1)
            row_idx = 0
            for question in questions:
                numbered_question = f"{question_counter}. {question}"
                if question == q_diferenciais:
                    ttk.Label(section_frame, text=numbered_question, style='Metric.White.TLabel').grid(row=row_idx, column=0, sticky='w', pady=2)
                    diferenciais_text = (op_data['diferenciais_competitivos'] if 'diferenciais_competitivos' in op_keys and op_data['diferenciais_competitivos'] else "---")
                    ttk.Label(section_frame, text=diferenciais_text, style='Value.White.TLabel', wraplength=600).grid(row=row_idx, column=1, sticky='w', pady=2, padx=(10,0))
                    row_idx +=1
                elif question == q_riscos:
                    ttk.Label(section_frame, text=numbered_question, style='Metric.White.TLabel').grid(row=row_idx, column=0, sticky='w', pady=2)
                    riscos_text = (op_data['principais_riscos'] if 'principais_riscos' in op_keys and op_data['principais_riscos'] else "---")
                    ttk.Label(section_frame, text=riscos_text, style='Value.White.TLabel', wraplength=600).grid(row=row_idx, column=1, sticky='w', pady=2, padx=(10,0))
                    row_idx +=1
                elif question in qualificacao_answers:
                    answer = qualificacao_answers.get(question)
                    is_special_question = question_counter <= 9 or question_counter == 12

                    ttk.Label(section_frame, text=numbered_question, wraplength=600, justify='left', style='Value.White.TLabel').grid(row=row_idx, column=0, sticky='w')

                    if is_special_question and answer in ["Sim", "Não"]:
                        icon = "✓" if answer == "Sim" else "✗"
                        color = DOLP_COLORS['success_green'] if answer == "Sim" else DOLP_COLORS['danger_red']
                        answer_label = ttk.Label(section_frame, text=icon, style='Value.White.TLabel', font=('Segoe UI', 12, 'bold'), foreground=color)
                    else:
                        display_text = answer or "Não respondido"
                        answer_label = ttk.Label(section_frame, text=display_text, style='Value.White.TLabel')

                    answer_label.grid(row=row_idx, column=1, sticky='e', padx=10)
                    row_idx += 1
                question_counter += 1

        # Aba 2: Sumário Executivo
        sumario_tab = self._create_scrollable_tab(notebook, '  Sumário Executivo  ')

        export_sumario_btn = ttk.Button(sumario_tab, text="Exportar para PDF", command=lambda: self.export_sumario_executivo_pdf(op_id), style='Primary.TButton')
        export_sumario_btn.pack(anchor='ne', pady=(0, 10))
        edital_frame = ttk.LabelFrame(sumario_tab, text="Informações do Edital", padding=15, style='White.TLabelframe')
        edital_frame.pack(fill='x', pady=(0, 10))
        edital_info = [
            ("Número do Edital:", op_data['numero_edital'] if 'numero_edital' in op_keys else '---'),
            ("Data de Abertura:", op_data['data_abertura'] if 'data_abertura' in op_keys else '---'),
            ("Modalidade:", op_data['modalidade'] if 'modalidade' in op_keys else '---'),
            ("Contato Principal:", op_data['contato_principal'] if 'contato_principal' in op_keys else '---')
        ]
        edital_frame.columnconfigure(1, weight=1)
        for i, (label, value) in enumerate(edital_info):
            ttk.Label(edital_frame, text=label, style='Metric.White.TLabel').grid(row=i, column=0, sticky='w', pady=2)
            ttk.Label(edital_frame, text=str(value), style='Value.White.TLabel').grid(row=i, column=1, sticky='w', pady=2, padx=(10,0))
        link_docs = op_data['link_documentos'] if 'link_documentos' in op_keys else None
        if link_docs:
            row_index = len(edital_info)
            ttk.Label(edital_frame, text="Pasta de Documentos:", style='Metric.White.TLabel').grid(row=row_index, column=0, sticky='w', pady=2)
            link_label = ttk.Label(edital_frame, text="Abrir Pasta", style='Link.White.TLabel', cursor="hand2")
            link_label.grid(row=row_index, column=1, sticky='w', pady=2, padx=(10,0))
            link_label.bind("<Button-1>", lambda e, url=link_docs: open_link(url))
        financeiro_frame = ttk.LabelFrame(sumario_tab, text="Informações Financeiras e de Pessoal", padding=15, style='White.TLabelframe')
        financeiro_frame.pack(fill='x', pady=(10, 10))
        financeiro_info = [
            ("Faturamento Estimado:", format_currency(op_data['faturamento_estimado'] if 'faturamento_estimado' in op_keys else 0)),
            ("Duração do Contrato:", f"{op_data['duracao_contrato']} meses" if 'duracao_contrato' in op_keys and op_data['duracao_contrato'] else "---"),
            ("MOD (Mão de Obra Direta):", op_data['mod'] if 'mod' in op_keys else '---'),
            ("MOI (Mão de Obra Indireta):", op_data['moi'] if 'moi' in op_keys else '---'),
            ("Total de Pessoas:", op_data['total_pessoas'] if 'total_pessoas' in op_keys else '---'),
            ("Margem de Contribuição:", f"{op_data['margem_contribuicao']}%" if 'margem_contribuicao' in op_keys and op_data['margem_contribuicao'] else "---")
        ]
        financeiro_frame.columnconfigure(1, weight=1)
        for i, (label, value) in enumerate(financeiro_info):
            ttk.Label(financeiro_frame, text=label, style='Metric.White.TLabel').grid(row=i, column=0, sticky='w', pady=2)
            ttk.Label(financeiro_frame, text=str(value), style='Value.White.TLabel').grid(row=i, column=1, sticky='w', pady=2, padx=(10,0))
        servicos_data_json_str = op_data['servicos_data'] if 'servicos_data' in op_keys else None
        if servicos_data_json_str:
            try:
                servicos_data = json.loads(servicos_data_json_str)
                if servicos_data:
                    servicos_frame = ttk.LabelFrame(sumario_tab, text="Serviços e Equipes Configurados", padding=15, style='White.TLabelframe')
                    servicos_frame.pack(fill='x', pady=(10,0))
                    for servico_info in servicos_data:
                        servico_nome = servico_info.get("servico_nome", "N/A")
                        equipes = servico_info.get("equipes", [])
                        ttk.Label(servicos_frame, text=servico_nome, style='Metric.White.TLabel', font=('Segoe UI', 11, 'bold')).pack(anchor='w', pady=(5,2))
                        if not equipes:
                            ttk.Label(servicos_frame, text="  - Nenhuma equipe configurada", style='Value.White.TLabel').pack(anchor='w', padx=(15,0))
                        else:
                            for equipe in equipes:
                                equipe_nome = equipe.get('tipo_equipe', 'N/A')
                                qtd = equipe.get('quantidade', 'N/A')
                                vol = equipe.get('volumetria', 'N/A')
                                base = equipe.get('base', 'N/A')
                                info_text = f"  - Equipe: {equipe_nome} | Qtd: {qtd} | Volumetria: {vol} | Base: {base}"
                                ttk.Label(servicos_frame, text=info_text, style='Value.White.TLabel').pack(anchor='w', padx=(15,0))
            except (json.JSONDecodeError, TypeError):
                print(f"Alerta: Falha ao carregar dados de serviço na tela de detalhes: {servicos_data_json_str}")
        descricao_detalhada = op_data['descricao_detalhada'] if 'descricao_detalhada' in op_keys else None
        if descricao_detalhada:
            desc_frame = ttk.LabelFrame(sumario_tab, text="Descrição Detalhada", padding=15, style='White.TLabelframe')
            desc_frame.pack(fill='both', expand=True, pady=(10, 0))
            desc_text = tk.Text(desc_frame, height=5, wrap='word', bg='white', font=('Segoe UI', 10), state='disabled')
            desc_scrollbar = ttk.Scrollbar(desc_frame, orient="vertical", command=desc_text.yview)
            desc_text.configure(yscrollcommand=desc_scrollbar.set)
            desc_text.pack(side="left", fill="both", expand=True)
            desc_scrollbar.pack(side="right", fill="y")
            desc_text.config(state='normal')
            desc_text.insert('1.0', descricao_detalhada)
            desc_text.config(state='disabled')

        # Aba 3: Histórico de Interações
        interacoes_tab = self._create_scrollable_tab(notebook, '  Histórico de Interações  ')

        # --- Filtros para Interações ---
        filters_interactions_frame = ttk.LabelFrame(interacoes_tab, text="Filtros", padding=15, style='White.TLabelframe')
        filters_interactions_frame.pack(fill='x', pady=(0, 10))

        # Tipo
        ttk.Label(filters_interactions_frame, text="Tipo:", style='TLabel').grid(row=0, column=0, sticky='w', padx=(0, 5))
        interaction_types = ['Todos'] + self.db.get_interaction_types()
        tipo_int_filter = ttk.Combobox(filters_interactions_frame, values=interaction_types, state='readonly')
        tipo_int_filter.set('Todos')
        tipo_int_filter.grid(row=0, column=1, padx=(0, 20))

        # Data Início
        ttk.Label(filters_interactions_frame, text="De:", style='TLabel').grid(row=0, column=2, sticky='w', padx=(0, 5))
        start_date_int_filter = DateEntry(filters_interactions_frame, date_pattern='dd/mm/yyyy', width=12)
        start_date_int_filter.delete(0, 'end') # Limpar campo inicial
        start_date_int_filter.grid(row=0, column=3, padx=(0, 20))

        # Data Fim
        ttk.Label(filters_interactions_frame, text="Até:", style='TLabel').grid(row=0, column=4, sticky='w', padx=(0, 5))
        end_date_int_filter = DateEntry(filters_interactions_frame, date_pattern='dd/mm/yyyy', width=12)
        end_date_int_filter.delete(0, 'end') # Limpar campo inicial
        end_date_int_filter.grid(row=0, column=5, padx=(0, 20))

        # Container para os resultados
        interactions_results_frame = ttk.Frame(interacoes_tab, style='TFrame')
        interactions_results_frame.pack(fill='both', expand=True, pady=(10,0))

        def _refilter_interactions():
            # Limpar resultados antigos
            for widget in interactions_results_frame.winfo_children():
                widget.destroy()

            # Obter valores dos filtros
            tipo = tipo_int_filter.get()
            start_date = start_date_int_filter.get()
            end_date = end_date_int_filter.get()

            interacoes = self.db.get_interactions_for_opportunity(op_id, tipo, start_date, end_date)

            if interacoes:
                for interacao in interacoes:
                    int_frame = ttk.LabelFrame(interactions_results_frame, text=f"{interacao['tipo']} - {interacao['data_interacao']}", padding=10, style='White.TLabelframe')
                    int_frame.pack(fill='x', pady=5)
                    ttk.Label(int_frame, text=f"Usuário: {interacao['usuario']}", style='Metric.White.TLabel').pack(anchor='w')
                    ttk.Label(int_frame, text=interacao['resumo'], style='Value.White.TLabel', wraplength=750, justify='left').pack(anchor='w', pady=(5, 0))
            else:
                ttk.Label(interactions_results_frame, text="Nenhuma interação encontrada para os filtros selecionados.", style='Value.White.TLabel').pack(pady=20)

        # Botão de Filtrar
        ttk.Button(filters_interactions_frame, text="🔍 Filtrar", command=_refilter_interactions, style='Primary.TButton').grid(row=0, column=6, padx=(20, 0))

        # Botão de Nova Interação
        ttk.Button(filters_interactions_frame, text="Nova Interação", command=lambda: self.add_interaction_dialog(op_id, lambda: self.show_opportunity_details(op_id, back_callback)), style='Success.TButton').grid(row=0, column=7, padx=(10,0))

        # Carregar interações iniciais
        _refilter_interactions()

        # Aba 4: Tarefas
        tarefas_tab = self._create_scrollable_tab(notebook, '  Tarefas  ')

        # --- Filtros para Tarefas ---
        filters_tasks_frame = ttk.LabelFrame(tarefas_tab, text="Filtros", padding=15, style='White.TLabelframe')
        filters_tasks_frame.pack(fill='x', pady=(0, 10))
        filters_tasks_frame.columnconfigure(7, weight=1) # Coluna do botão de nova tarefa

        # Status
        ttk.Label(filters_tasks_frame, text="Status:", style='TLabel').grid(row=0, column=0, sticky='w', padx=(0, 5))
        status_task_filter = ttk.Combobox(filters_tasks_frame, values=['Todos', 'Pendente', 'Concluída'], state='readonly')
        status_task_filter.set('Todos')
        status_task_filter.grid(row=0, column=1, padx=(0, 20))

        # Responsável
        ttk.Label(filters_tasks_frame, text="Responsável:", style='TLabel').grid(row=0, column=2, sticky='w', padx=(0, 5))
        responsibles = ['Todos'] + self.db.get_task_responsibles(op_id)
        responsavel_filter = ttk.Combobox(filters_tasks_frame, values=responsibles, state='readonly')
        responsavel_filter.set('Todos')
        responsavel_filter.grid(row=0, column=3, padx=(0, 20))

        # Data Vencimento Início
        ttk.Label(filters_tasks_frame, text="Vencimento de:", style='TLabel').grid(row=0, column=4, sticky='w', padx=(0, 5))
        start_date_task_filter = DateEntry(filters_tasks_frame, date_pattern='dd/mm/yyyy', width=12)
        start_date_task_filter.delete(0, 'end')
        start_date_task_filter.grid(row=0, column=5, padx=(0, 20))

        # Data Vencimento Fim
        ttk.Label(filters_tasks_frame, text="Até:", style='TLabel').grid(row=0, column=6, sticky='w', padx=(0, 5))
        end_date_task_filter = DateEntry(filters_tasks_frame, date_pattern='dd/mm/yyyy', width=12)
        end_date_task_filter.delete(0, 'end')
        end_date_task_filter.grid(row=0, column=7, padx=(0, 20))

        # Categoria
        ttk.Label(filters_tasks_frame, text="Categoria:", style='TLabel').grid(row=1, column=0, sticky='w', padx=(0, 5), pady=(10,0))
        task_categories = self.db.get_all_task_categories()
        task_category_map = {c['nome']: c['id'] for c in task_categories}
        category_filter = ttk.Combobox(filters_tasks_frame, values=['Todos'] + list(task_category_map.keys()), state='readonly')
        category_filter.set('Todos')
        category_filter.grid(row=1, column=1, pady=(10,0))


        # Container para os resultados das tarefas
        tasks_results_frame = ttk.Frame(tarefas_tab, style='TFrame')
        tasks_results_frame.pack(fill='both', expand=True, pady=(10,0))

        def _refilter_tasks():
            for widget in tasks_results_frame.winfo_children():
                widget.destroy()

            status = status_task_filter.get()
            responsavel = responsavel_filter.get()
            start_date = start_date_task_filter.get()
            end_date = end_date_task_filter.get()
            category_name = category_filter.get()
            category_id = task_category_map.get(category_name) if category_name != 'Todos' else None


            tarefas = self.db.get_tasks_for_opportunity(op_id, status, responsavel, category_id, start_date, end_date)

            if tarefas:
                for tarefa in tarefas:
                    category_name_display = tarefa['category_name'] or 'Sem Categoria'
                    task_frame = ttk.LabelFrame(tasks_results_frame, text=f"{category_name_display} - {tarefa['status']}", padding=10, style='White.TLabelframe')
                    task_frame.pack(fill='x', pady=5)

                    # Top frame for description and buttons
                    top_task_frame = ttk.Frame(task_frame)
                    top_task_frame.pack(fill='x')

                    ttk.Label(top_task_frame, text=tarefa['descricao'], style='Value.White.TLabel', wraplength=750, justify='left').pack(side='left', fill='x', expand=True)

                    # Buttons on the right
                    task_buttons_frame = ttk.Frame(top_task_frame)
                    task_buttons_frame.pack(side='right')

                    edit_btn = ttk.Button(task_buttons_frame, text="Editar", style='Primary.TButton', command=lambda t_id=tarefa['id']: self.add_task_dialog(op_id, lambda: self.show_opportunity_details(op_id, back_callback), task_id=t_id))
                    edit_btn.pack(side='left', padx=(0, 5))
                    delete_btn = ttk.Button(task_buttons_frame, text="Excluir", style='Danger.TButton', command=lambda t_id=tarefa['id']: self.delete_task(t_id, lambda: self.show_opportunity_details(op_id, back_callback)))
                    delete_btn.pack(side='left')

                    # Bottom frame for info
                    info_frame = ttk.Frame(task_frame)
                    info_frame.pack(fill='x', pady=(5, 0))
                    ttk.Label(info_frame, text=f"Responsável: {tarefa['responsavel']}", style='Metric.White.TLabel').pack(side='left')
                    ttk.Label(info_frame, text=f"Vencimento: {tarefa['data_vencimento']}", style='Metric.White.TLabel').pack(side='right')

                    if tarefa['status'] != 'Concluída':
                        ttk.Button(task_frame, text="Marcar como Concluída",
                                 command=lambda t_id=tarefa['id']: self.complete_task(t_id, lambda: self.show_opportunity_details(op_id, back_callback)),
                                 style='Success.TButton').pack(anchor='e', pady=(5, 0))
            else:
                ttk.Label(tasks_results_frame, text="Nenhuma tarefa encontrada para os filtros selecionados.", style='Value.White.TLabel').pack(pady=20)

        ttk.Button(filters_tasks_frame, text="🔍 Filtrar", command=_refilter_tasks, style='Primary.TButton').grid(row=1, column=2, padx=(20, 0), pady=(10,0))
        ttk.Button(filters_tasks_frame, text="Nova Tarefa", command=lambda: self.add_task_dialog(op_id, lambda: self.show_opportunity_details(op_id, back_callback)), style='Success.TButton').grid(row=1, column=3, padx=(10,0), pady=(10,0))

        # Carregar tarefas iniciais
        _refilter_tasks()

    def export_analise_previa_pdf(self, op_id):
        op_data = self.db.get_opportunity_details(op_id)
        if not op_data:
            messagebox.showerror("Erro", "Oportunidade não encontrada!")
            return

        op_keys = op_data.keys()

        # Ask for save location
        file_path = filedialog.asksaveasfilename(
            defaultextension=".pdf",
            filetypes=[("PDF files", "*.pdf"), ("All files", "*.*")],
            title="Salvar Análise Prévia de Viabilidade",
            initialfile=f"Analise_Previa_{op_data['numero_oportunidade'] if 'numero_oportunidade' in op_keys else 'NA'}_{op_data['titulo'] if 'titulo' in op_keys else 'SemTitulo'}.pdf".replace(" ", "_")
        )

        if not file_path:
            return

        try:
            doc = SimpleDocTemplate(file_path, pagesize=A4,
                                    rightMargin=72, leftMargin=72,
                                    topMargin=90, bottomMargin=72)
            story = []
            styles = getSampleStyleSheet()
            styles.add(ParagraphStyle(name='Justify', alignment=4))

            story.append(Paragraph("Análise Prévia de Viabilidade", styles['h1']))
            story.append(Spacer(1, 12))
            story.append(Paragraph(f"Oportunidade: {op_data['titulo']}", styles['h2']))
            story.append(Spacer(1, 24))

            # --- Seção de Informações Básicas ---
            basic_info_content = [
                Paragraph("1. Informações Básicas", styles['h3']),
                Spacer(1, 12),
                Table([
                    ['Cliente:', op_data['nome_empresa']],
                    ['Estágio:', op_data['estagio_nome']],
                    ['Valor Estimado:', format_currency(op_data['valor'])],
                    ['Tempo de Contrato:', f"{op_data['tempo_contrato_meses']} meses" if 'tempo_contrato_meses' in op_keys and op_data['tempo_contrato_meses'] else "---"],
                    ['Regional:', op_data['regional'] if 'regional' in op_keys and op_data['regional'] else "---"],
                    ['Polo:', op_data['polo'] if 'polo' in op_keys and op_data['polo'] else "---"],
                    ['Empresa Referência:', op_data['empresa_referencia'] if 'empresa_referencia' in op_keys and op_data['empresa_referencia'] else "---"],
                ], colWidths=[1.5*inch, 4.5*inch])
            ]
            basic_info_content[2].setStyle(TableStyle([
                ('ALIGN', (0,0), (-1,-1), 'LEFT'),
                ('FONTNAME', (0,0), (0,-1), 'Helvetica-Bold'),
                ('BOTTOMPADDING', (0,0), (-1,-1), 6),
            ]))
            story.append(KeepTogether(basic_info_content))
            story.append(Spacer(1, 24))

            story.append(Paragraph("2. Formulário de Análise de Qualificação", styles['h3']))
            story.append(Spacer(1, 12))

            qualificacao_data_json = op_data['qualificacao_data'] if 'qualificacao_data' in op_keys else None
            if qualificacao_data_json:
                try:
                    qualificacao_answers = json.loads(qualificacao_data_json)
                    question_counter = 1
                    for section, questions in QUALIFICATION_CHECKLIST.items():
                        section_content = [Paragraph(f"<b>{section}</b>", styles['h4']), Spacer(1, 6)]

                        if section == "Análise Concorrencial e de Riscos":
                            for question in questions:
                                numbered_question = f"<b>{question_counter}. {question}</b>"
                                section_content.append(Paragraph(numbered_question, styles['BodyText']))
                                section_content.append(Spacer(1, 4))
                                if question == "Quais são nossos diferenciais competitivos claros para esta oportunidade específica?":
                                    answer = op_data['diferenciais_competitivos'] if 'diferenciais_competitivos' in op_keys and op_data['diferenciais_competitivos'] else "---"
                                elif question == "Quais os principais riscos (técnicos, logísticos, regulatórios, políticos) associados ao projeto?":
                                    answer = op_data['principais_riscos'] if 'principais_riscos' in op_keys and op_data['principais_riscos'] else "---"
                                else:
                                    answer = "Não aplicável"
                                section_content.append(Paragraph(answer.replace('\n', '<br/>'), styles['Justify']))
                                section_content.append(Spacer(1, 12))
                                question_counter += 1
                        else:
                            question_data = []
                            for question in questions:
                                numbered_question = f"{question_counter}. {question}"
                                answer_text = qualificacao_answers.get(question, "Não respondido")
                                is_special_question = question_counter <= 9 or question_counter == 12

                                if is_special_question and answer_text in ["Sim", "Não"]:
                                    icon = "✓" if answer_text == "Sim" else "✗"
                                    color = "green" if answer_text == "Sim" else "red"
                                    answer_cell = Paragraph(f'<font color="{color}" size="14">{icon}</font>', styles['BodyText'])
                                else:
                                    answer_cell = Paragraph(answer_text, styles['BodyText'])

                                question_data.append([Paragraph(numbered_question, styles['BodyText']), answer_cell])
                                question_counter += 1

                            question_table = Table(question_data, colWidths=[5*inch, 1*inch])
                            question_table.setStyle(TableStyle([
                                ('ALIGN', (0,0), (-1,-1), 'LEFT'),
                                ('VALIGN', (0,0), (-1,-1), 'TOP'),
                                ('GRID', (0,0), (-1,-1), 0.25, colors.grey),
                                ('LEFTPADDING', (0,0), (-1,-1), 6),
                                ('RIGHTPADDING', (0,0), (-1,-1), 6),
                                ('TOPPADDING', (0,0), (-1,-1), 6),
                                ('BOTTOMPADDING', (0,0), (-1,-1), 6),
                            ]))
                            section_content.append(question_table)
                            section_content.append(Spacer(1, 12))

                        story.append(KeepTogether(section_content))

                except (json.JSONDecodeError, TypeError):
                    story.append(Paragraph("Erro ao carregar dados de qualificação.", styles['BodyText']))
            else:
                story.append(Paragraph("Dados de qualificação não preenchidos.", styles['BodyText']))
            story.append(Spacer(1, 24))

            bases_content = [Paragraph("3. Bases Alocadas", styles['h3']), Spacer(1, 12)]
            bases_nomes_json = op_data['bases_nomes'] if 'bases_nomes' in op_keys else None
            if bases_nomes_json:
                try:
                    bases_nomes = json.loads(bases_nomes_json)
                    if bases_nomes:
                        for base in bases_nomes:
                            bases_content.append(Paragraph(f"- {base}", styles['BodyText']))
                    else:
                        bases_content.append(Paragraph("Nenhuma base alocada.", styles['BodyText']))
                except (json.JSONDecodeError, TypeError):
                    bases_content.append(Paragraph("Erro ao carregar nomes de bases.", styles['BodyText']))
            else:
                bases_content.append(Paragraph("Nenhuma base alocada.", styles['BodyText']))
            story.append(KeepTogether(bases_content))
            story.append(Spacer(1, 24))

            servicos_main_content = [Paragraph("4. Serviços e Equipes", styles['h3']), Spacer(1, 12)]
            servicos_data_json = op_data['servicos_data'] if 'servicos_data' in op_keys else None
            if servicos_data_json:
                try:
                    servicos_data = json.loads(servicos_data_json)
                    if servicos_data:
                        for servico_info in servicos_data:
                            servico_block = [Paragraph(f"<b>Serviço: {servico_info.get('servico_nome', 'N/A')}</b>", styles['h4'])]
                            equipes = servico_info.get('equipes', [])
                            if equipes:
                                equipe_data = [['Tipo de Equipe', 'Qtd', 'Volumetria', 'Base']]
                                for equipe in equipes:
                                    equipe_data.append([
                                        equipe.get('tipo_equipe', 'N/A'),
                                        equipe.get('quantidade', 'N/A'),
                                        equipe.get('volumetria', 'N/A'),
                                        equipe.get('base', 'N/A')
                                    ])
                                equipe_table = Table(equipe_data)
                                equipe_table.setStyle(TableStyle([
                                    ('BACKGROUND', (0,0), (-1,0), colors.grey),
                                    ('TEXTCOLOR', (0,0), (-1,0), colors.whitesmoke),
                                    ('ALIGN', (0,0), (-1,-1), 'CENTER'),
                                    ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
                                    ('BOTTOMPADDING', (0,0), (-1,0), 12),
                                    ('BACKGROUND', (0,1), (-1,-1), colors.beige),
                                    ('GRID', (0,0), (-1,-1), 1, colors.black)
                                ]))
                                servico_block.append(equipe_table)
                                servico_block.append(Spacer(1, 12))
                            else:
                                servico_block.append(Paragraph("Nenhuma equipe configurada para este serviço.", styles['BodyText']))
                            servicos_main_content.append(KeepTogether(servico_block))
                    else:
                        servicos_main_content.append(Paragraph("Nenhum serviço configurado.", styles['BodyText']))
                except (json.JSONDecodeError, TypeError):
                    servicos_main_content.append(Paragraph("Erro ao carregar dados de serviços.", styles['BodyText']))
            else:
                servicos_main_content.append(Paragraph("Nenhum serviço configurado.", styles['BodyText']))
            story.extend(servicos_main_content)

            def header_footer(canvas, doc):
                canvas.saveState()
                if os.path.exists(LOGO_PATH):
                    try:
                        logo = ImageReader(LOGO_PATH)
                        img_width, img_height = logo.getSize()
                        aspect = img_height / float(img_width)
                        display_width = 1.5 * inch
                        display_height = display_width * aspect
                        canvas.drawImage(logo, doc.leftMargin, A4[1] - 1.0 * inch, width=display_width, height=display_height, mask='auto')
                    except Exception as e:
                        print(f"Erro ao desenhar logo no PDF: {e}")

                now = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                canvas.setFont('Helvetica', 9)
                canvas.drawRightString(A4[0] - doc.rightMargin, A4[1] - 0.75 * inch, f"Gerado em: {now}")

                canvas.setFont('Helvetica', 10)
                # Adicionando a seção de aprovação
                canvas.drawString(doc.leftMargin, 1.25 * inch, "Seguiremos com a elaboração do sumário executivo?   (   ) Sim   (   ) Não")
                canvas.drawString(doc.leftMargin, 0.75 * inch, "_________________________________________")
                canvas.drawString(doc.leftMargin, 0.5 * inch, "Assinatura da Diretoria")
                canvas.restoreState()

            doc.build(story, onFirstPage=header_footer, onLaterPages=header_footer)
            messagebox.showinfo("Sucesso", f"PDF 'Análise Prévia de Viabilidade' gerado com sucesso em:\n{file_path}", parent=self.root)

        except Exception as e:
            messagebox.showerror("Erro ao Gerar PDF", f"Ocorreu um erro: {e}", parent=self.root)

    def export_sumario_executivo_pdf(self, op_id):
        op_data = self.db.get_opportunity_details(op_id)
        if not op_data:
            messagebox.showerror("Erro", "Oportunidade não encontrada!")
            return

        op_keys = op_data.keys()

        # Ask for save location
        file_path = filedialog.asksaveasfilename(
            defaultextension=".pdf",
            filetypes=[("PDF files", "*.pdf"), ("All files", "*.*")],
            title="Salvar Sumário Executivo",
            initialfile=f"Sumario_Executivo_{op_data['numero_oportunidade'] if 'numero_oportunidade' in op_keys else 'NA'}_{op_data['titulo'] if 'titulo' in op_keys else 'SemTitulo'}.pdf".replace(" ", "_")
        )

        if not file_path:
            return

        try:
            doc = SimpleDocTemplate(file_path, pagesize=A4,
                                    rightMargin=72, leftMargin=72,
                                    topMargin=90, bottomMargin=72) # Increased margins
            story = []
            styles = getSampleStyleSheet()
            styles.add(ParagraphStyle(name='Justify', alignment=4)) # TA_JUSTIFY

            # Title
            story.append(Paragraph("Sumário Executivo", styles['h1']))
            story.append(Spacer(1, 12))
            story.append(Paragraph(f"Oportunidade: {op_data['titulo'] if 'titulo' in op_keys else 'N/A'}", styles['h2']))
            story.append(Spacer(1, 24))

            # Informações do Edital
            # Section 1: Informações do Edital
            edital_content = [
                Paragraph("1. Informações do Edital", styles['h3']),
                Spacer(1, 12),
                Table([
                    ['Número do Edital:', op_data['numero_edital'] if 'numero_edital' in op_keys and op_data['numero_edital'] else "---"],
                    ['Data de Abertura:', op_data['data_abertura'] if 'data_abertura' in op_keys and op_data['data_abertura'] else "---"],
                    ['Modalidade:', op_data['modalidade'] if 'modalidade' in op_keys and op_data['modalidade'] else "---"],
                    ['Contato Principal:', op_data['contato_principal'] if 'contato_principal' in op_keys and op_data['contato_principal'] else "---"],
                    ['Link dos Documentos:', op_data['link_documentos'] if 'link_documentos' in op_keys and op_data['link_documentos'] else "---"],
                ], colWidths=[1.5*inch, 4.5*inch])
            ]
            edital_content[2].setStyle(TableStyle([
                ('ALIGN', (0,0), (-1,-1), 'LEFT'), ('FONTNAME', (0,0), (0,-1), 'Helvetica-Bold'), ('BOTTOMPADDING', (0,0), (-1,-1), 6),
            ]))
            story.append(KeepTogether(edital_content))
            story.append(Spacer(1, 24))

            # Section 2: Informações Financeiras e de Pessoal
            financeiro_content = [
                Paragraph("2. Informações Financeiras e de Pessoal", styles['h3']),
                Spacer(1, 12),
                Table([
                    ['Faturamento Estimado:', format_currency(op_data['faturamento_estimado'] if 'faturamento_estimado' in op_keys else None)],
                    ['Duração do Contrato:', f"{op_data['duracao_contrato']} meses" if 'duracao_contrato' in op_keys and op_data['duracao_contrato'] else "---"],
                    ['MOD (Mão de Obra Direta):', op_data['mod'] if 'mod' in op_keys and op_data['mod'] else "---"],
                    ['MOI (Mão de Obra Indireta):', op_data['moi'] if 'moi' in op_keys and op_data['moi'] else "---"],
                    ['Total de Pessoas:', op_data['total_pessoas'] if 'total_pessoas' in op_keys and op_data['total_pessoas'] else "---"],
                    ['Margem de Contribuição:', f"{op_data['margem_contribuicao']}%" if 'margem_contribuicao' in op_keys and op_data['margem_contribuicao'] else "---"],
                ], colWidths=[2*inch, 4*inch])
            ]
            financeiro_content[2].setStyle(TableStyle([
                ('ALIGN', (0,0), (-1,-1), 'LEFT'), ('FONTNAME', (0,0), (0,-1), 'Helvetica-Bold'), ('BOTTOMPADDING', (0,0), (-1,-1), 6),
            ]))
            story.append(KeepTogether(financeiro_content))
            story.append(Spacer(1, 24))

            # Section 3: Detalhes de Serviços e Preços
            servicos_main_content = [Paragraph("3. Detalhes de Serviços e Preços", styles['h3']), Spacer(1, 12)]
            servicos_data_json = op_data['servicos_data'] if 'servicos_data' in op_keys else None
            if servicos_data_json:
                try:
                    servicos_data = json.loads(servicos_data_json)
                    if servicos_data:
                        for servico_info in servicos_data:
                            servico_block = [Paragraph(f"<b>Serviço: {servico_info.get('servico_nome', 'N/A')}</b>", styles['h4'])]
                            equipes = servico_info.get('equipes', [])
                            if equipes:
                                equipe_data = [['Tipo de Equipe', 'Qtd', 'Volumetria', 'Base']]
                                for equipe in equipes:
                                    equipe_data.append([
                                        equipe.get('tipo_equipe', 'N/A'), equipe.get('quantidade', 'N/A'),
                                        equipe.get('volumetria', 'N/A'), equipe.get('base', 'N/A')
                                    ])
                                equipe_table = Table(equipe_data)
                                equipe_table.setStyle(TableStyle([
                                    ('BACKGROUND', (0,0), (-1,0), colors.grey), ('TEXTCOLOR', (0,0), (-1,0), colors.whitesmoke),
                                    ('ALIGN', (0,0), (-1,-1), 'CENTER'), ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
                                    ('BOTTOMPADDING', (0,0), (-1,0), 12), ('BACKGROUND', (0,1), (-1,-1), colors.beige),
                                    ('GRID', (0,0), (-1,-1), 1, colors.black)
                                ]))
                                servico_block.append(equipe_table)
                            else:
                                servico_block.append(Paragraph("Nenhuma equipe configurada para este serviço.", styles['BodyText']))
                            servicos_main_content.append(KeepTogether(servico_block))
                            servicos_main_content.append(Spacer(1, 12))
                    else:
                        servicos_main_content.append(Paragraph("Nenhum serviço configurado.", styles['BodyText']))
                except (json.JSONDecodeError, TypeError):
                    servicos_main_content.append(Paragraph("Erro ao carregar dados de serviços.", styles['BodyText']))
            else:
                servicos_main_content.append(Paragraph("Nenhum serviço configurado.", styles['BodyText']))
            story.extend(servicos_main_content)
            story.append(Spacer(1, 24))

            # Section 4: Descrição Detalhada
            descricao_content = [
                Paragraph("4. Descrição Detalhada", styles['h3']),
                Spacer(1, 12),
                Paragraph((op_data['descricao_detalhada'] if 'descricao_detalhada' in op_keys and op_data['descricao_detalhada'] else 'Nenhuma descrição fornecida.').replace('\n', '<br/>'), styles['BodyText'])
            ]
            story.append(KeepTogether(descricao_content))
            story.append(Spacer(1, 48))

            # Header and Footer function
            def header_footer(canvas, doc):
                canvas.saveState()
                # Header
                if os.path.exists(LOGO_PATH):
                    try:
                        logo = ImageReader(LOGO_PATH)
                        img_width, img_height = logo.getSize()
                        aspect = img_height / float(img_width)
                        display_width = 1.5 * inch
                        display_height = display_width * aspect
                        canvas.drawImage(logo, doc.leftMargin, A4[1] - 1.0 * inch, width=display_width, height=display_height, mask='auto')
                    except Exception as e:
                        print(f"Erro ao desenhar logo no PDF: {e}")

                now = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                canvas.setFont('Helvetica', 9)
                canvas.drawRightString(A4[0] - doc.rightMargin, A4[1] - 0.75 * inch, f"Gerado em: {now}")

                # Footer (Signature Line)
                canvas.setFont('Helvetica', 10)
                canvas.drawString(doc.leftMargin, 0.75 * inch, "_________________________________________")
                canvas.drawString(doc.leftMargin, 0.5 * inch, "Assinatura da Diretoria")
                canvas.restoreState()

            doc.build(story, onFirstPage=header_footer, onLaterPages=header_footer)
            messagebox.showinfo("Sucesso", f"PDF 'Sumário Executivo' gerado com sucesso em:\n{file_path}", parent=self.root)

        except Exception as e:
            messagebox.showerror("Erro ao Gerar PDF", f"Ocorreu um erro: {e}", parent=self.root)

    def add_interaction_dialog(self, op_id, back_callback):
        self.clear_content()
        title_frame = ttk.Frame(self.content_frame, style='TFrame')
        title_frame.pack(fill='x', pady=(0, 20), padx=20)
        ttk.Label(title_frame, text="Nova Interação", style='Title.TLabel').pack(side='left')
        ttk.Button(title_frame, text="← Voltar", command=back_callback, style='TButton').pack(side='right')

        main_frame = ttk.Frame(self.content_frame, padding=20, style='TFrame')
        main_frame.pack(fill='both', expand=True, padx=20, pady=0)

        ttk.Label(main_frame, text="Tipo de Interação:", style='TLabel').pack(pady=5)
        tipo_combo = ttk.Combobox(main_frame, values=["Reunião", "Ligação", "E-mail", "Proposta", "Negociação", "Outro"], state='readonly')
        tipo_combo.pack(pady=5, padx=20, fill='x')
        ttk.Label(main_frame, text="Usuário:", style='TLabel').pack(pady=5)
        usuario_entry = ttk.Entry(main_frame)
        usuario_entry.pack(pady=5, padx=20, fill='x')
        ttk.Label(main_frame, text="Resumo:", style='TLabel').pack(pady=5)
        resumo_text = tk.Text(main_frame, height=8, wrap='word', bg='white')
        resumo_text.pack(pady=5, padx=20, fill='both', expand=True)

        def save_interaction():
            data = {
                'oportunidade_id': op_id, 'data_interacao': datetime.now().strftime('%d/%m/%Y %H:%M'),
                'tipo': tipo_combo.get(), 'resumo': resumo_text.get('1.0', 'end-1c'), 'usuario': usuario_entry.get()
            }
            if not data['tipo'] or not data['resumo'] or not data['usuario']:
                messagebox.showerror("Erro", "Todos os campos são obrigatórios!", parent=self.root)
                return
            self.db.add_interaction(data)
            messagebox.showinfo("Sucesso", "Interação adicionada com sucesso!", parent=self.root)
            back_callback()

        buttons_frame = ttk.Frame(main_frame, style='TFrame')
        buttons_frame.pack(pady=10)
        ttk.Button(buttons_frame, text="Salvar", command=save_interaction, style='Success.TButton').pack()

    def add_task_dialog(self, op_id, back_callback, task_id=None):
        self.clear_content()
        form_title = "Nova Tarefa" if not task_id else "Editar Tarefa"

        title_frame = ttk.Frame(self.content_frame, style='TFrame')
        title_frame.pack(fill='x', pady=(0, 20), padx=20)
        ttk.Label(title_frame, text=form_title, style='Title.TLabel').pack(side='left')
        ttk.Button(title_frame, text="← Voltar", command=back_callback, style='TButton').pack(side='right')

        main_frame = ttk.Frame(self.content_frame, padding=20, style='TFrame')
        main_frame.pack(fill='both', expand=True, padx=20, pady=0)

        ttk.Label(main_frame, text="Descrição:", style='TLabel').pack(pady=5)
        desc_text = tk.Text(main_frame, height=6, wrap='word', bg='white')
        desc_text.pack(pady=5, padx=20, fill='both', expand=True)
        ttk.Label(main_frame, text="Responsável:", style='TLabel').pack(pady=5)
        responsavel_entry = ttk.Entry(main_frame)
        responsavel_entry.pack(pady=5, padx=20, fill='x')

        # Categoria
        ttk.Label(main_frame, text="Categoria:", style='TLabel').pack(pady=5)
        categories = self.db.get_all_task_categories()
        category_map = {c['nome']: c['id'] for c in categories}
        category_id_map = {c['id']: c['nome'] for c in categories}
        category_combo = ttk.Combobox(main_frame, values=list(category_map.keys()), state='readonly')
        category_combo.pack(pady=5, padx=20, fill='x')

        ttk.Label(main_frame, text="Data de Vencimento:", style='TLabel').pack(pady=5)
        vencimento_date = DateEntry(main_frame, date_pattern='dd/mm/yyyy')
        vencimento_date.pack(pady=5, padx=20)

        # Status (apenas para edição)
        status_label = ttk.Label(main_frame, text="Status:", style='TLabel')
        status_combo = ttk.Combobox(main_frame, values=['Pendente', 'Concluída'], state='readonly')

        if task_id:
            # Carregar dados da tarefa para edição
            task_data = self.db.get_tasks_for_opportunity(op_id, status='Todos') # Simplificado para encontrar a tarefa
            current_task = next((t for t in task_data if t['id'] == task_id), None)
            if current_task:
                desc_text.insert('1.0', current_task['descricao'])
                responsavel_entry.insert(0, current_task['responsavel'])
                if current_task['category_id']:
                    category_combo.set(category_id_map.get(current_task['category_id'], ''))
                try:
                    vencimento_date.set_date(datetime.strptime(current_task['data_vencimento'], '%d/%m/%Y').date())
                except (ValueError, TypeError):
                    pass
                status_label.pack(pady=5)
                status_combo.pack(pady=5, padx=20, fill='x')
                status_combo.set(current_task['status'])

        def save_task():
            data = {
                'descricao': desc_text.get('1.0', 'end-1c'),
                'data_vencimento': vencimento_date.get(),
                'responsavel': responsavel_entry.get(),
                'status': status_combo.get() if task_id else 'Pendente',
                'category_id': category_map.get(category_combo.get())
            }
            if not data['descricao'] or not data['responsavel'] or not data['category_id']:
                messagebox.showerror("Erro", "Descrição, Responsável e Categoria são obrigatórios!", parent=self.root)
                return

            try:
                if task_id:
                    self.db.update_task(task_id, data)
                    messagebox.showinfo("Sucesso", "Tarefa atualizada com sucesso!", parent=self.root)
                else:
                    data['oportunidade_id'] = op_id
                    data['data_criacao'] = datetime.now().strftime('%d/%m/%Y')
                    self.db.add_task(data)
                    messagebox.showinfo("Sucesso", "Tarefa adicionada com sucesso!", parent=self.root)
                back_callback()
            except Exception as e:
                messagebox.showerror("Erro", f"Ocorreu um erro ao salvar a tarefa: {e}", parent=self.root)

        buttons_frame = ttk.Frame(main_frame, style='TFrame')
        buttons_frame.pack(pady=10)
        ttk.Button(buttons_frame, text="Salvar", command=save_task, style='Success.TButton').pack()

    def complete_task(self, task_id, refresh_callback):
        self.db.update_task_status(task_id, 'Concluída')
        messagebox.showinfo("Sucesso", "Tarefa marcada como concluída!", parent=self.root)
        refresh_callback()

    def delete_task(self, task_id, refresh_callback):
        if messagebox.askyesno("Confirmar Exclusão", "Tem certeza de que deseja excluir esta tarefa?", parent=self.root):
            try:
                self.db.delete_task(task_id)
                messagebox.showinfo("Sucesso", "Tarefa excluída com sucesso!", parent=self.root)
                refresh_callback()
            except Exception as e:
                messagebox.showerror("Erro", f"Ocorreu um erro ao excluir a tarefa: {e}", parent=self.root)

    def show_clients_view(self):
        self.clear_content()

        title_frame = ttk.Frame(self.content_frame, style='TFrame')
        title_frame.pack(fill='x', pady=(0, 20))

        ttk.Label(title_frame, text="Clientes", style='Title.TLabel').pack(side='left')
        ttk.Button(title_frame, text="Novo Cliente", command=self.show_client_form, style='Success.TButton').pack(side='right')
        ttk.Button(title_frame, text="← Voltar", command=self.show_main_menu, style='TButton').pack(side='right', padx=(0, 10))

        clients_frame = ttk.Frame(self.content_frame, style='TFrame')
        clients_frame.pack(fill='both', expand=True)

        columns = ('id', 'nome_empresa', 'cnpj', 'cidade', 'estado', 'status')
        tree = ttk.Treeview(clients_frame, columns=columns, show='headings', height=15)

        tree.heading('id', text='ID')
        tree.heading('nome_empresa', text='Empresa')
        tree.heading('cnpj', text='CNPJ')
        tree.heading('cidade', text='Cidade')
        tree.heading('estado', text='Estado')
        tree.heading('status', text='Status')

        tree.column('id', width=50, anchor='center')
        tree.column('nome_empresa', width=300)
        tree.column('cnpj', width=150)
        tree.column('cidade', width=150)
        tree.column('estado', width=80, anchor='center')
        tree.column('status', width=200)

        scrollbar = ttk.Scrollbar(clients_frame, orient='vertical', command=tree.yview)
        tree.configure(yscrollcommand=scrollbar.set)

        tree.pack(side='left', fill='both', expand=True)
        scrollbar.pack(side='right', fill='y')

        clients = self.db.get_all_clients()
        for client in clients:
            status = client['status'] or 'Não cadastrado'
            if client['data_atualizacao']:
                try:
                    data_atualizacao = datetime.strptime(client['data_atualizacao'], '%d/%m/%Y')
                    if (datetime.now() - data_atualizacao).days > 365:
                         status = 'Cadastro Desatualizado'
                except (ValueError, TypeError):
                    pass

            tree.insert('', 'end', values=(
                client['id'],
                client['nome_empresa'],
                format_cnpj(client['cnpj']) or '---',
                client['cidade'] or '---',
                client['estado'] or '---',
                status
            ))

        def on_double_click(event):
            selection = tree.selection()
            if selection:
                item = tree.item(selection[0])
                client_id = item['values'][0]
                self.show_client_form(client_id)

        tree.bind('<Double-1>', on_double_click)

        def show_context_menu(event):
            selection = tree.selection()
            if selection:
                context_menu = tk.Menu(self.root, tearoff=0)
                context_menu.add_command(label="Editar Cliente", command=lambda: self.show_client_form(tree.item(selection[0])['values'][0]))
                context_menu.add_command(label="Nova Oportunidade", command=lambda: self.show_opportunity_form(client_to_prefill=tree.item(selection[0])['values'][1], back_callback=self.show_clients_view))
                context_menu.tk_popup(event.x_root, event.y_root)

        tree.bind('<Button-3>', show_context_menu)

    def show_client_form(self, client_id=None):
        self.clear_content()
        form_title = "Novo Cliente" if not client_id else "Editar Cliente"

        title_frame = ttk.Frame(self.content_frame, style='TFrame')
        title_frame.pack(fill='x', pady=(0, 20))
        ttk.Label(title_frame, text=form_title, style='Title.TLabel').pack(side='left')
        ttk.Button(title_frame, text="← Voltar", command=self.show_clients_view, style='TButton').pack(side='right')

        # --- Scrollable Frame ---
        canvas = tk.Canvas(self.content_frame, bg=DOLP_COLORS['white'], highlightthickness=0)
        scrollbar = ttk.Scrollbar(self.content_frame, orient="vertical", command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas, style='TFrame', padding=20)
        scrollable_frame.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        def _on_mousewheel(event):
            canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
        self.content_frame.bind('<Enter>', lambda e: self.root.bind_all("<MouseWheel>", _on_mousewheel))
        self.content_frame.bind('<Leave>', lambda e: self.root.unbind_all("<MouseWheel>"))
        # ------------------------

        entries = {}
        fields = [
            ("Nome da Empresa:*", "nome_empresa", "entry"), ("CNPJ:", "cnpj", "entry"),
            ("Cidade:", "cidade", "entry"), ("Estado:", "estado", "combobox", BRAZILIAN_STATES),
            ("Setor de Atuação:", "setor_atuacao", "combobox", self.db.get_all_setores()),
            ("Segmento de Atuação:", "segmento_atuacao", "combobox", self.db.get_all_segmentos()),
            ("Data de Atualização:", "data_atualizacao", "date"), ("Link do Portal:", "link_portal", "entry"),
            ("Status:", "status", "combobox", CLIENT_STATUS_OPTIONS)
        ]

        for i, field_info in enumerate(fields):
            text, key = field_info[0], field_info[1]
            field_frame = ttk.Frame(scrollable_frame, style='TFrame')
            field_frame.pack(fill='x', pady=5)
            ttk.Label(field_frame, text=text, style='TLabel', width=25).pack(side='left')

            if len(field_info) > 3: widget = ttk.Combobox(field_frame, values=field_info[3], state='readonly')
            elif field_info[2] == "combobox": widget = ttk.Combobox(field_frame, state='readonly')
            elif field_info[2] == "date": widget = DateEntry(field_frame, date_pattern='dd/mm/yyyy', width=20)
            else: widget = ttk.Entry(field_frame, width=40)
            widget.pack(side='left', padx=(10, 0), fill='x', expand=True)
            entries[key] = widget

        resumo_frame = ttk.Frame(scrollable_frame, style='TFrame')
        resumo_frame.pack(fill='both', pady=5, expand=True)
        ttk.Label(resumo_frame, text="Resumo de Atuação:", style='TLabel', width=25).pack(side='left', anchor='n', pady=(5,0))
        resumo_text = tk.Text(resumo_frame, height=5, wrap='word', bg='white', font=('Segoe UI', 10), borderwidth=1, relief='solid')
        resumo_text.pack(side='left', padx=(10, 0), fill='both', expand=True)
        entries['resumo_atuacao'] = resumo_text

        if client_id:
            client_data = self.db.get_client_by_id(client_id)
            if client_data:
                for key, widget in entries.items():
                    if key == 'resumo_atuacao':
                        if key in client_data.keys() and client_data[key]: widget.insert('1.0', client_data[key])
                        continue
                    value = client_data[key] if key in client_data.keys() else ""
                    value = value or ""
                    if key == 'cnpj': value = format_cnpj(value)
                    if hasattr(widget, 'set'): widget.set(value)
                    else:
                        widget.delete(0, 'end')
                        widget.insert(0, value)
        else:
            entries['data_atualizacao'].set_date(datetime.now().date())

        buttons_frame = ttk.Frame(scrollable_frame, style='TFrame')
        buttons_frame.pack(fill='x', pady=(20, 0))

        def save_client():
            try:
                data = {}
                for key, widget in entries.items():
                    if key == 'resumo_atuacao': data[key] = widget.get('1.0', 'end-1c').strip()
                    else: data[key] = widget.get().strip()
                if 'cnpj' in data: data['cnpj'] = strip_cnpj(data['cnpj'])
                if not data['nome_empresa']:
                    messagebox.showerror("Erro", "Nome da empresa é obrigatório!", parent=self.root)
                    return
                if client_id:
                    self.db.update_client(client_id, data)
                    messagebox.showinfo("Sucesso", "Cliente atualizado com sucesso!", parent=self.root)
                else:
                    self.db.add_client(data)
                    messagebox.showinfo("Sucesso", "Cliente criado com sucesso!", parent=self.root)
                self.show_clients_view()
            except sqlite3.IntegrityError as e:
                error_message = str(e).lower()
                if 'clientes.cnpj' in error_message: messagebox.showerror("Erro de Duplicidade", "O CNPJ informado já está cadastrado para outro cliente.", parent=self.root)
                elif 'clientes.nome_empresa' in error_message: messagebox.showerror("Erro de Duplicidade", "O Nome da Empresa informado já está cadastrado para outro cliente.", parent=self.root)
                else: messagebox.showerror("Erro de Banco de Dados", f"Erro de integridade ao salvar: {str(e)}", parent=self.root)
            except Exception as e:
                messagebox.showerror("Erro Inesperado", f"Ocorreu um erro inesperado ao salvar: {str(e)}", parent=self.root)

        ttk.Button(buttons_frame, text="Salvar", command=save_client, style='Success.TButton').pack(side='right')

    def show_crm_settings(self):
        self.clear_content()

        title_frame = ttk.Frame(self.content_frame, style='TFrame')
        title_frame.pack(fill='x', pady=(0, 20))

        ttk.Label(title_frame, text="Configurações do CRM", style='Title.TLabel').pack(side='left')
        ttk.Button(title_frame, text="← Voltar", command=self.show_main_menu, style='TButton').pack(side='right')

        settings_frame = ttk.Frame(self.content_frame, style='TFrame')
        settings_frame.pack(expand=True)

        config_buttons = [
            ("Tipos de Serviço", self.show_servicos_view, 'Primary.TButton'),
            ("Tipos de Equipe", self.show_team_types_view, 'Primary.TButton'),
            ("Empresas Referência", self.show_empresa_referencia_view, 'Primary.TButton'),
            ("Setores de Atuação", lambda: self.show_list_manager("Setores", self.db.get_all_setores, self.db.add_setor, self.db.delete_setor, "nome"), 'Warning.TButton'),
            ("Segmentos de Atuação", lambda: self.show_list_manager("Segmentos", self.db.get_all_segmentos, self.db.add_segmento, self.db.delete_segmento, "nome"), 'Warning.TButton'),
            ("Categorias de Tarefas", lambda: self.show_list_manager("Categorias de Tarefas", self.db.get_all_task_categories, self.db.add_task_category, self.db.delete_task_category, "id"), 'Warning.TButton')
        ]

        for i, (text, command, style) in enumerate(config_buttons):
            btn = ttk.Button(settings_frame, text=text, command=command, style=style, width=25)
            btn.pack(pady=10)

    def show_servicos_view(self):
        self.clear_content()

        title_frame = ttk.Frame(self.content_frame, style='TFrame')
        title_frame.pack(fill='x', pady=(0, 20))

        ttk.Label(title_frame, text="Tipos de Serviço", style='Title.TLabel').pack(side='left')
        ttk.Button(title_frame, text="Novo Serviço", command=self.show_servico_form, style='Success.TButton').pack(side='right')
        ttk.Button(title_frame, text="← Voltar", command=self.show_crm_settings, style='TButton').pack(side='right', padx=(0, 10))

        types_frame = ttk.Frame(self.content_frame, style='TFrame')
        types_frame.pack(fill='both', expand=True)

        columns = ('id', 'nome', 'categoria', 'ativa')
        tree = ttk.Treeview(types_frame, columns=columns, show='headings', height=15)

        tree.heading('id', text='ID')
        tree.heading('nome', text='Nome')
        tree.heading('categoria', text='Categoria')
        tree.heading('ativa', text='Ativa')

        tree.column('id', width=50, anchor='center')
        tree.column('nome', width=300)
        tree.column('categoria', width=200)
        tree.column('ativa', width=80, anchor='center')

        scrollbar = ttk.Scrollbar(types_frame, orient='vertical', command=tree.yview)
        tree.configure(yscrollcommand=scrollbar.set)

        tree.pack(side='left', fill='both', expand=True)
        scrollbar.pack(side='right', fill='y')

        servicos = self.db.get_all_servicos()
        for servico in servicos:
            tree.insert('', 'end', values=(
                servico['id'],
                servico['nome'],
                servico['categoria'] or '---',
                'Sim' if servico['ativa'] else 'Não'
            ))

        def on_double_click(event):
            selection = tree.selection()
            if selection:
                item = tree.item(selection[0])
                servico_id = item['values'][0]
                self.show_servico_form(servico_id)

        tree.bind('<Double-1>', on_double_click)

    def show_servico_form(self, servico_id=None):
        self.clear_content()
        form_title = "Novo Tipo de Serviço" if not servico_id else "Editar Tipo de Serviço"

        title_frame = ttk.Frame(self.content_frame, style='TFrame')
        title_frame.pack(fill='x', pady=(0, 20))
        ttk.Label(title_frame, text=form_title, style='Title.TLabel').pack(side='left')
        ttk.Button(title_frame, text="← Voltar", command=self.show_servicos_view, style='TButton').pack(side='right')

        main_frame = ttk.Frame(self.content_frame, padding=20, style='TFrame')
        main_frame.pack(fill='both', expand=True)

        entries = {}
        fields = [("Nome:*", "nome", "entry"), ("Categoria:", "categoria", "entry"), ("Descrição:", "descricao", "text"), ("Ativa:", "ativa", "checkbox")]

        for text, key, widget_type in fields:
            field_frame = ttk.Frame(main_frame, style='TFrame')
            field_frame.pack(fill='x', pady=5)
            ttk.Label(field_frame, text=text, style='TLabel', width=15).pack(side='left')

            if widget_type == "text":
                widget = tk.Text(field_frame, height=4, wrap='word', bg='white')
            elif widget_type == "checkbox":
                widget = tk.BooleanVar()
                cb = ttk.Checkbutton(field_frame, variable=widget)
                cb.pack(side='left', padx=(10, 0))
            else:
                widget = ttk.Entry(field_frame, width=40)

            if widget_type != "checkbox":
                widget.pack(side='left', padx=(10, 0), fill='x', expand=True)
            entries[key] = widget

        if servico_id:
            servico_data = self.db.get_servico_by_id(servico_id)
            if servico_data:
                entries['nome'].insert(0, servico_data['nome'] or '')
                entries['categoria'].insert(0, servico_data['categoria'] or '')
                if servico_data['descricao']:
                    entries['descricao'].insert('1.0', servico_data['descricao'])
                entries['ativa'].set(bool(servico_data['ativa']))
        else:
            entries['ativa'].set(True)

        buttons_frame = ttk.Frame(main_frame, style='TFrame')
        buttons_frame.pack(fill='x', pady=(20, 0))

        def save_servico():
            try:
                data = {
                    'nome': entries['nome'].get().strip(),
                    'categoria': entries['categoria'].get().strip(),
                    'descricao': entries['descricao'].get('1.0', 'end-1c').strip(),
                    'ativa': 1 if entries['ativa'].get() else 0
                }
                if not data['nome']:
                    messagebox.showerror("Erro", "Nome é obrigatório!", parent=self.root)
                    return

                if servico_id:
                    self.db.update_servico(servico_id, data)
                    messagebox.showinfo("Sucesso", "Tipo de serviço atualizado com sucesso!", parent=self.root)
                else:
                    self.db.add_servico(data)
                    messagebox.showinfo("Sucesso", "Tipo de serviço criado com sucesso!", parent=self.root)
                self.show_servicos_view()
            except Exception as e:
                messagebox.showerror("Erro de Banco de Dados", f"Erro ao salvar: {str(e)}", parent=self.root)

        ttk.Button(buttons_frame, text="Salvar", command=save_servico, style='Success.TButton').pack(side='right')

    def show_team_types_view(self):
        self.clear_content()
        title_frame = ttk.Frame(self.content_frame, style='TFrame')
        title_frame.pack(fill='x', pady=(0, 20))
        ttk.Label(title_frame, text="Tipos de Equipe", style='Title.TLabel').pack(side='left')
        ttk.Button(title_frame, text="Novo Tipo de Equipe", command=self.show_team_type_form, style='Success.TButton').pack(side='right')
        ttk.Button(title_frame, text="← Voltar", command=self.show_crm_settings, style='TButton').pack(side='right', padx=(0, 10))

        view_frame = ttk.Frame(self.content_frame, style='TFrame')
        view_frame.pack(fill='both', expand=True)
        columns = ('id', 'nome', 'servico_nome', 'ativa')
        tree = ttk.Treeview(view_frame, columns=columns, show='headings', height=15)
        tree.heading('id', text='ID')
        tree.heading('nome', text='Nome da Equipe')
        tree.heading('servico_nome', text='Tipo de Serviço Associado')
        tree.heading('ativa', text='Ativa')
        tree.column('id', width=50, anchor='center')
        tree.column('nome', width=300)
        tree.column('servico_nome', width=300)
        tree.column('ativa', width=80, anchor='center')

        scrollbar = ttk.Scrollbar(view_frame, orient='vertical', command=tree.yview)
        tree.configure(yscrollcommand=scrollbar.set)
        tree.pack(side='left', fill='both', expand=True)
        scrollbar.pack(side='right', fill='y')

        team_types = self.db.get_all_team_types()
        for tt in team_types:
            tree.insert('', 'end', values=(tt['id'], tt['nome'], tt['servico_nome'], 'Sim' if tt['ativa'] else 'Não'))

        def on_double_click(event):
            selection = tree.selection()
            if selection:
                item = tree.item(selection[0])
                team_id = item['values'][0]
                self.show_team_type_form(team_id)
        tree.bind('<Double-1>', on_double_click)

    def show_team_type_form(self, team_id=None):
        self.clear_content()
        form_title = "Novo Tipo de Equipe" if not team_id else "Editar Tipo de Equipe"

        title_frame = ttk.Frame(self.content_frame, style='TFrame')
        title_frame.pack(fill='x', pady=(0, 20))
        ttk.Label(title_frame, text=form_title, style='Title.TLabel').pack(side='left')
        ttk.Button(title_frame, text="← Voltar", command=self.show_team_types_view, style='TButton').pack(side='right')

        main_frame = ttk.Frame(self.content_frame, padding=20, style='TFrame')
        main_frame.pack(fill='both', expand=True)

        entries = {}
        servicos = self.db.get_all_servicos()
        servico_map = {s['nome']: s['id'] for s in servicos}
        servico_names = list(servico_map.keys())

        # Nome
        nome_frame = ttk.Frame(main_frame, style='TFrame')
        nome_frame.pack(fill='x', pady=5)
        ttk.Label(nome_frame, text="Nome da Equipe:*", style='TLabel', width=20).pack(side='left')
        entries['nome'] = ttk.Entry(nome_frame, width=40)
        entries['nome'].pack(side='left', padx=(10,0), fill='x', expand=True)

        # Tipo de Serviço
        servico_frame = ttk.Frame(main_frame, style='TFrame')
        servico_frame.pack(fill='x', pady=5)
        ttk.Label(servico_frame, text="Tipo de Serviço:*", style='TLabel', width=20).pack(side='left')
        entries['servico_id'] = ttk.Combobox(servico_frame, values=servico_names, state='readonly')
        entries['servico_id'].pack(side='left', padx=(10,0), fill='x', expand=True)

        # Ativa
        ativa_frame = ttk.Frame(main_frame, style='TFrame')
        ativa_frame.pack(fill='x', pady=5)
        ttk.Label(ativa_frame, text="Ativa:", style='TLabel', width=20).pack(side='left')
        entries['ativa'] = tk.BooleanVar()
        ttk.Checkbutton(ativa_frame, variable=entries['ativa']).pack(side='left', padx=(10,0))

        if team_id:
            team_data = self.db.get_team_type_by_id(team_id)
            if team_data:
                entries['nome'].insert(0, team_data['nome'] or '')
                servico_id = team_data['servico_id']
                for s_name, s_id in servico_map.items():
                    if s_id == servico_id:
                        entries['servico_id'].set(s_name)
                        break
                entries['ativa'].set(bool(team_data['ativa']))
        else:
            entries['ativa'].set(True)

        buttons_frame = ttk.Frame(main_frame, style='TFrame')
        buttons_frame.pack(fill='x', pady=(20, 0))

        def save_team_type():
            try:
                servico_nome = entries['servico_id'].get()
                if not servico_nome:
                    messagebox.showerror("Erro", "Tipo de Serviço é obrigatório!", parent=self.root)
                    return

                data = {
                    'nome': entries['nome'].get().strip(),
                    'servico_id': servico_map[servico_nome],
                    'ativa': 1 if entries['ativa'].get() else 0
                }
                if not data['nome']:
                    messagebox.showerror("Erro", "Nome da equipe é obrigatório!", parent=self.root)
                    return

                if team_id:
                    self.db.update_team_type(team_id, data)
                    messagebox.showinfo("Sucesso", "Tipo de equipe atualizado com sucesso!", parent=self.root)
                else:
                    self.db.add_team_type(data)
                    messagebox.showinfo("Sucesso", "Tipo de equipe criado com sucesso!", parent=self.root)
                self.show_team_types_view()
            except Exception as e:
                messagebox.showerror("Erro de Banco de Dados", f"Erro ao salvar: {str(e)}", parent=self.root)

        ttk.Button(buttons_frame, text="Salvar", command=save_team_type, style='Success.TButton').pack(side='right')

    def show_empresa_referencia_view(self):
        self.clear_content()

        # --- Título e Botões de Ação ---
        title_frame = ttk.Frame(self.content_frame, style='TFrame')
        title_frame.pack(fill='x', pady=(0, 10))
        ttk.Label(title_frame, text="Empresas Referência", style='Title.TLabel').pack(side='left')
        ttk.Button(title_frame, text="Nova Empresa", command=self.show_empresa_referencia_form, style='Success.TButton').pack(side='right')
        ttk.Button(title_frame, text="← Voltar", command=self.show_crm_settings, style='TButton').pack(side='right', padx=(0, 10))

        # --- Frame de Filtros ---
        filters_frame = ttk.LabelFrame(self.content_frame, text="Filtros", padding=15, style='White.TLabelframe')
        filters_frame.pack(fill='x', pady=(10, 20))

        # Dados para os filtros
        servicos = self.db.get_all_servicos()
        service_names = ['Todos'] + [s['nome'] for s in servicos if s['ativa']]
        clients = self.db.get_all_clients()
        concessionaria_names = ['Todos'] + [c['nome_empresa'] for c in clients]

        # Filtro Estado
        ttk.Label(filters_frame, text="Estado:", style='TLabel').grid(row=0, column=0, padx=(0, 5), pady=5)
        estado_filter = ttk.Combobox(filters_frame, values=['Todos'] + BRAZILIAN_STATES, state='readonly', width=15)
        estado_filter.set('Todos')
        estado_filter.grid(row=0, column=1, padx=(0, 20))

        # Filtro Tipo de Serviço
        ttk.Label(filters_frame, text="Tipo de Serviço:", style='TLabel').grid(row=0, column=2, padx=(0, 5), pady=5)
        servico_filter = ttk.Combobox(filters_frame, values=service_names, state='readonly', width=25)
        servico_filter.set('Todos')
        servico_filter.grid(row=0, column=3, padx=(0, 20))

        # Filtro Concessionária
        ttk.Label(filters_frame, text="Concessionária:", style='TLabel').grid(row=0, column=4, padx=(0, 5), pady=5)
        concessionaria_filter = ttk.Combobox(filters_frame, values=concessionaria_names, state='readonly', width=25)
        concessionaria_filter.set('Todos')
        concessionaria_filter.grid(row=0, column=5, padx=(0, 20))

        # --- Frame da Tabela de Resultados ---
        empresas_frame = ttk.Frame(self.content_frame, style='TFrame')
        empresas_frame.pack(fill='both', expand=True)
        empresas_frame.columnconfigure(0, weight=1)
        empresas_frame.rowconfigure(0, weight=1)

        columns = ('id', 'nome_empresa', 'tipo_servico', 'tipo_equipe', 'estado', 'concessionaria', 'ano_referencia', 'valor_mensal', 'volumetria_minima', 'valor_por_pessoa', 'valor_us_ups_upe_ponto', 'ativa', 'observacoes')
        tree = ttk.Treeview(empresas_frame, columns=columns, show='headings', height=15)

        # Configuração dos Cabeçalhos
        headings = {
            'id': ('ID', 50), 'nome_empresa': ('Empresa', 180), 'tipo_servico': ('Tipo de Serviço', 180), 'tipo_equipe': ('Tipo de Equipe', 180),
            'estado': ('UF', 50), 'concessionaria': ('Concessionária', 150), 'ano_referencia': ('Ano Ref.', 80),
            'valor_mensal': ('Valor Mensal', 120), 'volumetria_minima': ('Vol. Mínima', 100),
            'valor_por_pessoa': ('Valor/Pessoa', 120), 'valor_us_ups_upe_ponto': ('Valor US/UPS/UPE/Ponto', 150), 'ativa': ('Ativa', 60), 'observacoes': ('Obs.', 200)
        }
        for col, (text, width) in headings.items():
            tree.heading(col, text=text)
            tree.column(col, width=width, anchor='center')

        # Scrollbars
        v_scrollbar = ttk.Scrollbar(empresas_frame, orient='vertical', command=tree.yview)
        h_scrollbar = ttk.Scrollbar(empresas_frame, orient='horizontal', command=tree.xview)
        tree.configure(yscrollcommand=v_scrollbar.set, xscrollcommand=h_scrollbar.set)

        tree.grid(row=0, column=0, sticky='nsew')
        v_scrollbar.grid(row=0, column=1, sticky='ns')
        h_scrollbar.grid(row=1, column=0, sticky='ew')

        # --- Lógica de Carregamento e Filtragem ---
        def load_data(estado=None, tipo_servico=None, concessionaria=None):
            # Limpar a tabela
            for item in tree.get_children():
                tree.delete(item)

            # Buscar dados com filtros
            empresas = self.db.get_all_empresas_referencia(estado, tipo_servico, concessionaria)
            for empresa in empresas:
                tree.insert('', 'end', values=(
                    empresa['id'],
                    empresa['nome_empresa'],
                    empresa['tipo_servico'],
                    empresa['tipo_equipe_nome'] if 'tipo_equipe_nome' in empresa.keys() and empresa['tipo_equipe_nome'] is not None else '---',
                    empresa['estado'] if 'estado' in empresa.keys() and empresa['estado'] is not None else '---',
                    empresa['concessionaria'] if 'concessionaria' in empresa.keys() and empresa['concessionaria'] is not None else '---',
                    empresa['ano_referencia'] if 'ano_referencia' in empresa.keys() and empresa['ano_referencia'] is not None else '---',
                    format_currency(empresa['valor_mensal']),
                    f"{empresa['volumetria_minima']:,.0f}" if empresa['volumetria_minima'] else '---',
                    format_currency(empresa['valor_por_pessoa']),
                    format_currency(empresa['valor_us_ups_upe_ponto']) if 'valor_us_ups_upe_ponto' in empresa.keys() and empresa['valor_us_ups_upe_ponto'] is not None else '---',
                    'Sim' if empresa['ativa'] else 'Não',
                    empresa['observacoes'] if 'observacoes' in empresa.keys() and empresa['observacoes'] is not None else ''
                ))

        def apply_filters():
            estado = estado_filter.get()
            tipo_servico = servico_filter.get()
            concessionaria = concessionaria_filter.get()
            load_data(estado, tipo_servico, concessionaria)

        # Botão de Filtrar
        ttk.Button(filters_frame, text="🔍 Filtrar", style='Primary.TButton', command=apply_filters).grid(row=0, column=6, padx=(20, 0), pady=5)

        # Carregar dados iniciais (sem filtros)
        load_data()

        def on_double_click(event):
            selection = tree.selection()
            if selection:
                item = tree.item(selection[0])
                empresa_id = item['values'][0]
                self.show_empresa_referencia_form(empresa_id)

        tree.bind('<Double-1>', on_double_click)

    def show_empresa_referencia_form(self, empresa_id=None):
        self.clear_content()
        form_title = "Nova Empresa Referência" if not empresa_id else "Editar Empresa Referência"

        # --- Título e Botão Voltar ---
        title_frame = ttk.Frame(self.content_frame, style='TFrame')
        title_frame.pack(fill='x', pady=(0, 10))
        ttk.Label(title_frame, text=form_title, style='Title.TLabel').pack(side='left')
        ttk.Button(title_frame, text="← Voltar", command=self.show_empresa_referencia_view, style='TButton').pack(side='right')

        # --- Frame Principal com Rolagem ---
        form_canvas = tk.Canvas(self.content_frame, bg=DOLP_COLORS['white'], highlightthickness=0)
        form_scrollbar = ttk.Scrollbar(self.content_frame, orient="vertical", command=form_canvas.yview)
        scrollable_form_frame = ttk.Frame(form_canvas, style='TFrame', padding=20)

        scrollable_form_frame.bind("<Configure>", lambda e: form_canvas.configure(scrollregion=form_canvas.bbox("all")))
        form_canvas.create_window((0, 0), window=scrollable_form_frame, anchor="nw")
        form_canvas.configure(yscrollcommand=form_scrollbar.set)

        form_canvas.pack(side="left", fill="both", expand=True)
        form_scrollbar.pack(side="right", fill="y")

        # Bind da rolagem do mouse
        def _on_mousewheel(event):
            form_canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
        form_canvas.bind_all("<MouseWheel>", _on_mousewheel)

        entries = {}
        # Dados para os comboboxes
        servicos = self.db.get_all_servicos()
        service_names = [s['nome'] for s in servicos if s['ativa']]
        team_types = self.db.get_all_team_types()
        team_type_map = {tt['nome']: tt['id'] for tt in team_types}
        team_type_names = list(team_type_map.keys())
        clients = self.db.get_all_clients()
        concessionaria_names = [c['nome_empresa'] for c in clients]

        # Definição dos campos do formulário
        fields = [
            ("Nome da Empresa:*", "nome_empresa", "entry"), ("Tipo de Serviço:*", "tipo_servico", "combobox", service_names),
            ("Tipo de Equipe:", "tipo_equipe_id", "combobox", team_type_names), ("Estado:", "estado", "combobox", BRAZILIAN_STATES),
            ("Concessionária:", "concessionaria", "combobox", concessionaria_names), ("Ano de Referência:", "ano_referencia", "entry"),
            ("Valor Mensal (R$):*", "valor_mensal", "entry"), ("Volumetria Mínima:*", "volumetria_minima", "entry"),
            ("Valor por Pessoa (R$):*", "valor_por_pessoa", "entry"), ("Valor US/UPS/UPE/Ponto (R$):", "valor_us_ups_upe_ponto", "entry"),
            ("Ativa:", "ativa", "checkbox"), ("Observações:", "observacoes", "text")
        ]

        for text, key, widget_type, *args in fields:
            field_frame = ttk.Frame(scrollable_form_frame, style='TFrame')
            field_frame.pack(fill='x', pady=5)
            ttk.Label(field_frame, text=text, style='TLabel', width=20).pack(side='left', anchor='n' if widget_type == 'text' else 'w')

            if widget_type == "combobox":
                widget = ttk.Combobox(field_frame, values=args[0], state='readonly', width=40)
            elif widget_type == "checkbox":
                widget = tk.BooleanVar()
                cb = ttk.Checkbutton(field_frame, variable=widget)
                cb.pack(side='left', padx=(10, 0))
            elif widget_type == "text":
                text_frame = ttk.Frame(field_frame)
                widget = tk.Text(text_frame, height=5, wrap='word', bg='white', font=('Segoe UI', 10))
                scrollbar = ttk.Scrollbar(text_frame, orient="vertical", command=widget.yview)
                widget.configure(yscrollcommand=scrollbar.set)
                widget.pack(side="left", fill="both", expand=True)
                scrollbar.pack(side="right", fill="y")
                text_frame.pack(side='left', padx=(10, 0), fill='x', expand=True)
            else:
                widget = ttk.Entry(field_frame, width=40)

            if widget_type not in ["checkbox", "text"]:
                widget.pack(side='left', padx=(10, 0), fill='x', expand=True)
            entries[key] = widget

        if empresa_id:
            empresa_data = self.db.get_empresa_referencia_by_id(empresa_id)
            if empresa_data:
                op_keys = empresa_data.keys()
                entries['nome_empresa'].insert(0, empresa_data['nome_empresa'] or '')
                entries['tipo_servico'].set(empresa_data['tipo_servico'] or '')
                if 'tipo_equipe_id' in op_keys and empresa_data['tipo_equipe_id']:
                    for name, id in team_type_map.items():
                        if id == empresa_data['tipo_equipe_id']:
                            entries['tipo_equipe_id'].set(name)
                            break
                entries['valor_mensal'].insert(0, format_brazilian_currency_for_entry(empresa_data['valor_mensal']))
                entries['volumetria_minima'].insert(0, format_brazilian_currency_for_entry(empresa_data['volumetria_minima']))
                entries['valor_por_pessoa'].insert(0, format_brazilian_currency_for_entry(empresa_data['valor_por_pessoa']))
                entries['ativa'].set(bool(empresa_data['ativa']))
                if 'estado' in op_keys: entries['estado'].set(empresa_data['estado'] or '')
                if 'concessionaria' in op_keys: entries['concessionaria'].set(empresa_data['concessionaria'] or '')
                if 'ano_referencia' in op_keys: entries['ano_referencia'].insert(0, empresa_data['ano_referencia'] or '')
                if 'valor_us_ups_upe_ponto' in op_keys: entries['valor_us_ups_upe_ponto'].insert(0, format_brazilian_currency_for_entry(empresa_data['valor_us_ups_upe_ponto']))
                if 'observacoes' in op_keys and empresa_data['observacoes']:
                    entries['observacoes'].insert('1.0', empresa_data['observacoes'])
        else:
            entries['ativa'].set(True)

        buttons_frame = ttk.Frame(scrollable_form_frame, style='TFrame')
        buttons_frame.pack(fill='x', pady=(20, 0))

        def save_empresa():
            try:
                selected_team_type_name = entries['tipo_equipe_id'].get()
                team_type_id = team_type_map.get(selected_team_type_name)
                data = {
                    'nome_empresa': entries['nome_empresa'].get().strip(), 'tipo_servico': entries['tipo_servico'].get(),
                    'tipo_equipe_id': team_type_id, 'valor_mensal': parse_brazilian_currency(entries['valor_mensal'].get()),
                    'volumetria_minima': parse_brazilian_currency(entries['volumetria_minima'].get()), 'valor_por_pessoa': parse_brazilian_currency(entries['valor_por_pessoa'].get()),
                    'valor_us_ups_upe_ponto': parse_brazilian_currency(entries['valor_us_ups_upe_ponto'].get()), 'ativa': 1 if entries['ativa'].get() else 0,
                    'estado': entries['estado'].get(), 'concessionaria': entries['concessionaria'].get(), 'ano_referencia': entries['ano_referencia'].get().strip(),
                    'observacoes': entries['observacoes'].get('1.0', 'end-1c').strip()
                }
                if not data['nome_empresa'] or not data['tipo_servico']:
                    messagebox.showerror("Erro", "Nome da empresa e tipo de serviço são obrigatórios!", parent=self.root)
                    return
                if empresa_id:
                    self.db.update_empresa_referencia(empresa_id, data)
                    messagebox.showinfo("Sucesso", "Empresa referência atualizada com sucesso!", parent=self.root)
                else:
                    self.db.add_empresa_referencia(data)
                    messagebox.showinfo("Sucesso", "Empresa referência criada com sucesso!", parent=self.root)
                self.root.after(50, self.show_empresa_referencia_view)
            except ValueError:
                messagebox.showerror("Erro", "Valores numéricos inválidos!", parent=self.root)
            except Exception as e:
                messagebox.showerror("Erro de Banco de Dados", f"Erro ao salvar: {str(e)}", parent=self.root)

        ttk.Button(buttons_frame, text="Salvar", command=save_empresa, style='Success.TButton').pack(side='right')

    def show_list_manager(self, title, get_func, add_func, delete_func, key_for_delete):
        self.clear_content()

        title_frame = ttk.Frame(self.content_frame, style='TFrame')
        title_frame.pack(fill='x', pady=(0, 20))
        ttk.Label(title_frame, text=f"Gerenciar {title}", style='Title.TLabel').pack(side='left')
        ttk.Button(title_frame, text="← Voltar", command=self.show_crm_settings, style='TButton').pack(side='right')

        main_frame = ttk.Frame(self.content_frame, padding=20, style='TFrame')
        main_frame.pack(fill='both', expand=True)

        # Usar um Treeview para lidar com dados mais complexos (ID e Nome)
        list_frame = ttk.Frame(main_frame, style='TFrame')
        list_frame.pack(fill='both', expand=True, pady=(0, 10))

        tree = ttk.Treeview(list_frame, columns=('id', 'nome'), show='headings', height=15)
        tree.heading('id', text='ID')
        tree.heading('nome', text='Nome')
        tree.column('id', width=50, anchor='center')
        tree.column('nome', width=300)

        scrollbar_list = ttk.Scrollbar(list_frame, orient='vertical', command=tree.yview)
        tree.configure(yscrollcommand=scrollbar_list.set)
        tree.pack(side='left', fill='both', expand=True)
        scrollbar_list.pack(side='right', fill='y')

        # Armazenar os dados completos para referência
        item_data_map = {}

        def refresh_list():
            for i in tree.get_children():
                tree.delete(i)
            item_data_map.clear()
            items = get_func()
            for item in items:
                # Lidar com dicionários (sqlite3.Row) e strings simples
                if isinstance(item, str):
                    item_id = item
                    item_name = item
                else: # Assumir que é um objeto tipo dicionário (sqlite3.Row)
                    item_id = item['id']
                    item_name = item['nome']

                item_data_map[item_name] = item
                tree.insert('', 'end', values=(item_id, item_name))
        refresh_list()

        add_frame = ttk.Frame(main_frame, style='TFrame')
        add_frame.pack(fill='x', pady=(0, 10))

        singular_title = title[:-1] if title.endswith('s') else title
        ttk.Label(add_frame, text=f"Novo {singular_title}:", style='TLabel').pack(side='left')
        new_entry = ttk.Entry(add_frame, width=30)
        new_entry.pack(side='left', padx=(10, 0), fill='x', expand=True)

        def add_item():
            new_item_name = new_entry.get().strip()
            if new_item_name:
                try:
                    add_func(new_item_name)
                    new_entry.delete(0, 'end')
                    refresh_list()
                except Exception as e:
                    messagebox.showerror("Erro", f"Erro ao adicionar: {str(e)}", parent=self.root)

        ttk.Button(add_frame, text="Adicionar", command=add_item, style='Success.TButton').pack(side='right', padx=(10, 0))

        buttons_frame = ttk.Frame(main_frame, style='TFrame')
        buttons_frame.pack(fill='x')

        def delete_selected():
            selection = tree.selection()
            if selection:
                selected_item_values = tree.item(selection[0])['values']
                item_name = selected_item_values[1] # O nome é a segunda coluna

                if messagebox.askyesno("Confirmar", f"Deseja excluir '{item_name}'?", parent=self.root):
                    try:
                        # Obter o item completo do mapa para encontrar a chave de exclusão
                        full_item = item_data_map.get(item_name)
                        if full_item:
                            # O valor a ser deletado pode ser o nome ou o ID
                            value_to_delete = full_item if isinstance(full_item, str) else full_item[key_for_delete]
                            delete_func(value_to_delete)
                            refresh_list()
                        else:
                            messagebox.showerror("Erro", "Não foi possível encontrar os dados do item para exclusão.", parent=self.root)
                    except Exception as e:
                        messagebox.showerror("Erro", f"Erro ao excluir: {str(e)}", parent=self.root)

        ttk.Button(buttons_frame, text="Excluir Selecionado", command=delete_selected, style='Danger.TButton').pack(side='left')

# --- 5. EXECUÇÃO PRINCIPAL ---
def main():
    try:
        # Define o locale para pt_BR para formatação de moeda correta
        locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
    except locale.Error:
        try:
            # Fallback para o locale padrão do sistema
            locale.setlocale(locale.LC_ALL, '')
            print("Aviso: Locale 'pt_BR.UTF-8' não encontrado. Usando o locale padrão do sistema.")
        except locale.Error:
            # Fallback final se nenhum locale puder ser definido
            print("Aviso CRÍTICO: Não foi possível definir nenhum locale. A formatação de moeda pode estar incorreta.")

    root = tk.Tk()
    app = CRMApp(root)
    root.mainloop()

if __name__ == "__main__":
    main()

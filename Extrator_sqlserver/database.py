import concurrent.futures
import gc
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import List, Dict, Tuple, Set, Optional
import pyarrow.parquet as pq
import logging
import os
import sys
from datetime import datetime
from typing import List, Dict
import pandas as pd
import polars as pl
import pytz
import sqlalchemy
from pymssql import OperationalError
from pymysql import Connection
from sqlalchemy import create_engine
import pyodbc
from config import DATABASE_CONFIG, GENERAL_CONFIG, STORAGE_CONFIG
from dicionario_dados import obter_dicionario_tipos, ajustar_tipos_dados
from storage import enviar_resultados


def obter_versao_sqlserver(host: str, port: int, database: str, user: str, password: str) -> Optional[str]:
    """
    Obtém a versão do SQL Server conectando-se via ODBC.
    Tenta usar o primeiro driver disponível que contenha "SQL Server".

    Args:
        host (str): Endereço do servidor.
        port (int): Porta do servidor.
        database (str): Nome do banco de dados.
        user (str): Nome de usuário.
        password (str): Senha de acesso.

    Returns:
        Optional[str]: Versão do SQL Server (ex.: "14.0.1000.169") ou None se não for possível obter.
    """
    drivers = [driver for driver in pyodbc.drivers() if "SQL Server" in driver]
    if not drivers:
        logging.error("Nenhum driver ODBC do SQL Server foi encontrado para obter a versão.")
        return None
    # Usa o primeiro driver disponível para tentar obter a versão
    driver = drivers[0]
    dsn = (
        f"DRIVER={{{driver}}};"
        f"SERVER={host},{port};"
        f"DATABASE={database};"
        f"UID={user};"
        f"PWD={password};"
        "Connection Timeout=30;"
    )
    try:
        conn = pyodbc.connect(dsn, autocommit=True)
        cursor = conn.cursor()
        cursor.execute("SELECT CAST(SERVERPROPERTY('ProductVersion') AS varchar(50));")
        result = cursor.fetchone()
        version = result[0] if result else None
        cursor.close()
        conn.close()
        logging.info(f"Versão do SQL Server detectada: {version}")
        return version
    except Exception as e:
        logging.error(f"Erro ao obter a versão do SQL Server: {e}")
        return None

def detectar_driver_sqlserver(host: str, port: int, database: str, user: str, password: str,
                                driver_especifico: Optional[str] = None) -> Optional[str]:
    """
    Detecta e seleciona o driver ODBC mais adequado para SQL Server disponível no sistema,
    utilizando a versão do servidor (obtida via obter_versao_sqlserver) para recomendar o driver ideal.
    Se um driver específico for informado e estiver disponível, ele será utilizado.

    Versões mais usadas de drivers ODBC para SQL Server:
      - ODBC Driver 17 for SQL Server
      - ODBC Driver 13 for SQL Server
      - SQL Server Native Client 11.0
      - SQL Server Native Client 10.0

    Args:
        host (str): Endereço do servidor.
        port (int): Porta.
        database (str): Nome do banco.
        user (str): Nome de usuário.
        password (str): Senha.
        driver_especifico (Optional[str]): Driver desejado, se fornecido.

    Returns:
        Optional[str]: Nome do driver adequado ou None.
    """
    versao_banco = obter_versao_sqlserver(host, port, database, user, password)
    if versao_banco:
        try:
            major_version = int(versao_banco.split('.')[0])
        except Exception as e:
            logging.error(f"Erro ao interpretar a versão do SQL Server: {versao_banco}. Erro: {e}")
            major_version = None
    else:
        major_version = None

    # Lógica de recomendação:
    # - Se a versão for obtida e major_version >= 14 (SQL Server 2017 ou superior), recomenda "ODBC Driver 17 for SQL Server"
    # - Se a versão for obtida e major_version >= 10, recomenda "SQL Server Native Client 11.0"
    # - Caso contrário, recomenda "SQL Server Native Client 10.0"
    if major_version is None:
        recommended = None
    elif major_version >= 14:
        recommended = "ODBC Driver 17 for SQL Server"
    elif major_version >= 10:
        recommended = "SQL Server Native Client 11.0"
    else:
        recommended = "SQL Server Native Client 10.0"

    logging.info(f"Driver recomendado para SQL Server versão {versao_banco if versao_banco else 'desconhecida'}: {recommended}")

    # Obtém a lista dos drivers instalados que contenham "SQL Server"
    drivers = [driver for driver in pyodbc.drivers() if "SQL Server" in driver]
    drivers.sort(reverse=True)

    if driver_especifico and driver_especifico in drivers:
        logging.info(f"Usando driver específico solicitado: {driver_especifico}")
        return driver_especifico

    if recommended and recommended in drivers:
        logging.info(f"O driver recomendado {recommended} está disponível e será utilizado.")
        return recommended
    else:
        if drivers:
            logging.error(f"O driver recomendado {recommended} não está disponível. "
                          f"Utilizando o driver mais recente disponível: {drivers[0]}.")
            return drivers[0]
        else:
            logging.error("Nenhum driver ODBC do SQL Server foi encontrado.")
            return None

# ------------------------------------------------------------------------------
# Classe wrapper que cria uma nova conexão para cada cursor via ODBC.
# Essa abordagem garante que a conexão retorne um cursor iterável, compatível com pandas.
# ------------------------------------------------------------------------------
class MultiplexConnection:
    def __init__(self, dsn: str, autocommit: bool):
        self._dsn = dsn
        self._autocommit = autocommit

    def cursor(self):
        # Cria uma nova conexão ODBC a cada chamada de cursor() e retorna seu cursor.
        return pyodbc.connect(self._dsn, autocommit=self._autocommit).cursor()

    def close(self):
        # Como cada cursor abre sua própria conexão, não há conexão persistente para fechar.
        pass

    def commit(self):
        pass

    def rollback(self):
        pass

def conectar_ao_banco(host: str, port: int, database: str, user: str, password: str,
                        driver_odbc: Optional[str] = None, autocommit: bool = False) -> Optional[object]:
    """
    Conecta a um banco de dados SQL Server utilizando ODBC ou SQLAlchemy.
    Tenta primeiramente via ODBC; se falhar, tenta via SQLAlchemy.

    Args:
        host (str): Endereço do servidor.
        port (int): Porta.
        database (str): Nome do banco.
        user (str): Nome de usuário.
        password (str): Senha.
        driver_odbc (Optional[str]): Driver desejado, se fornecido.
        autocommit (bool): Define se a conexão ODBC usará autocommit (default: False).

    Returns:
        Optional[object]: Objeto de conexão se bem-sucedido; caso contrário, None.
    """
    try:
        logging.info("Tentando conectar ao banco de dados SQL Server via ODBC...")
        driver = detectar_driver_sqlserver(host, port, database, user, password, driver_especifico=driver_odbc)
        if driver:
            dsn = (
                f"DRIVER={{{driver}}};"
                f"SERVER={host},{port};"
                f"DATABASE={database};"
                f"UID={user};"
                f"PWD={password};"
                "MARS_Connection=Yes;"                      # Habilita múltiplos resultados ativos
                "MultipleActiveResultSets=True;"            # Reforça a opção, se suportado
                "Connection Timeout=30;"                     # Timeout para evitar conexões pendentes
                "Pooling=False;"                             # Desabilita o pooling, conforme o modelo
            )
            logging.info(f"Conexão multiplexada com o banco de dados SQL Server via ODBC ({driver}) estabelecida com sucesso.")
            return MultiplexConnection(dsn, autocommit)
        else:
            logging.warning("Falha na conexão via ODBC. Tentando via SQLAlchemy...")
    except pyodbc.Error as e:
        logging.warning(f"Erro ao conectar via ODBC: {e}. Tentando via SQLAlchemy...")

    try:
        url_conexao = f"mssql+pymssql://{user}:{password}@{host}:{port}/{database}"
        engine = create_engine(url_conexao, pool_pre_ping=True, pool_recycle=3600)
        conexao = engine.connect()
        logging.info("Conexão com o banco de dados SQL Server via SQLAlchemy estabelecida com sucesso.")
        return conexao
    except sqlalchemy.exc.OperationalError as e:
        logging.error(f"Erro SQLAlchemy: {e}")
    except Exception as e:
        logging.error(f"Erro inesperado ao conectar ao banco de dados: {e}")
    return None

def fechar_conexao(conexao: Connection):
    """
    Fecha a conexão com o banco de dados.

    Args:
        conexao (Connection): Conexão ativa com o banco de dados.
    """
    try:
        conexao.close()
        logging.info("Conexão com o banco de dados fechada.")
    except Exception as e:
        logging.error(f"Erro ao fechar a conexão: {e}")
# ===================================================
# EXECUÇÃO DE CONSULTAS
# ===================================================
def executar_consultas(
    conexoes_config: dict,
    consultas: List[Dict[str, str]],
    pasta_temp: str,
    paralela: bool = False,
    workers: int = 4,
) -> Tuple[Dict[str, str], Dict[str, Set[str]]]:
    """
    Executa as consultas no banco de dados de forma paralela ou sequencial.

    - Se paralela for False, uma única conexão é criada e reutilizada.
    - Se paralela for True, cada thread abre e fecha sua própria conexão.

    Retorna:
      - Um dicionário com o caminho final dos arquivos processados para cada consulta.
      - Um dicionário com os conjuntos de partições criadas.
    """
    resultados = {}
    particoes_criadas = {}
    os.makedirs(pasta_temp, exist_ok=True)

    conexao_persistente = conectar_ao_banco(**conexoes_config)

    def processa_consulta(consulta: Dict[str, str]) -> Tuple[str, str, Set[str]]:
        nome_consulta = consulta.get("name", "").replace(" ", "")
        query = consulta.get("query")
        try:
            inicio = time.time()
            pasta_consulta, particoes = executar_consulta(conexao_persistente, nome_consulta, query, pasta_temp)
            duracao = time.time() - inicio
            logging.info(f"Consulta '{nome_consulta}' processada em {duracao:.2f} segundos.")
            return nome_consulta, pasta_consulta, particoes
        except Exception as e:
            logging.error(f"Erro ao processar consulta '{nome_consulta}': {e}")
            return nome_consulta, None, set()


    try:
        if paralela:
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futuros = {executor.submit(processa_consulta, consulta): consulta for consulta in consultas}
                for futuro in concurrent.futures.as_completed(futuros):
                    nome_consulta, pasta_consulta, particoes = futuro.result()
                    if pasta_consulta:
                        resultados[nome_consulta] = pasta_consulta
                        particoes_criadas[nome_consulta] = particoes
        else:
            for consulta in consultas:
                nome_consulta, pasta_consulta, particoes = processa_consulta(consulta)
                if pasta_consulta:
                    resultados[nome_consulta] = pasta_consulta
                    particoes_criadas[nome_consulta] = particoes
    except Exception as e:
        logging.error(f"Erro na execução das consultas: {e}")
    finally:
        if conexao_persistente:
            fechar_conexao(conexao_persistente)

    return resultados, particoes_criadas

def executar_consulta(conexao, nome: str, query: str, pasta_temp: str) -> Tuple[str, Set[str]]:
    """
    Executa uma consulta SQL e retorna o caminho da pasta com os arquivos particionados e as partições criadas.

    Se a consulta retornar um DataFrame vazio, retorna uma string vazia e um conjunto vazio.
    """
    retries = 5
    for tentativa in range(retries):
        try:
            logging.info(f"Executando consulta: {nome}...")
            df_pandas = pd.read_sql(query, con=conexao)
            if df_pandas.empty:
                logging.warning(f"Consulta '{nome}' retornou um DataFrame vazio.")
                return "", set()
            total_registros = len(df_pandas)
            logging.info(f"Consulta '{nome}' finalizada. Total de registros: {total_registros}")
            return processar_dados(df_pandas, nome, pasta_temp)
        except OperationalError as e:
            logging.warning(f"Erro de conexão na consulta '{nome}', tentativa {tentativa+1}/{retries}: {e}")
            time.sleep(5)
        except Exception as e:
            logging.error(f"Erro ao executar a consulta '{nome}': {e}")
            return "", set()
    logging.error(f"Consulta '{nome}' falhou após {retries} tentativas.")
    return "", set()

def processar_dados(df_pandas: pd.DataFrame, nome: str, pasta_temp: str) -> Tuple[str, Set[str]]:
    """
    Processes a pandas DataFrame by applying transformations, converting it to a Polars DataFrame,
    and saving the data in a partitioned Parquet format. Additionally, it handles specific adjustments
    based on the data context (e.g., sales or purchases) and ensures certain columns are correctly
    formatted or present. The function creates a temporary directory for saving files and logs
    information about the process flow.

    Parameters:
        df_pandas (pd.DataFrame): Input pandas DataFrame to be processed.
        nome (str): Name of the dataset, used for context-specific column handling.
        pasta_temp (str): Path to the temporary folder where files will be saved.

    Returns:
        Tuple[str, Set[str]]: A tuple where the first element is the path to the generated dataset,
        and the second element is a set containing the paths of the created partitions.

    Raises:
        ValueError: If the required 'idEmpresa' column is missing from the processed Polars DataFrame.
    """
    try:
        os.makedirs(pasta_temp, exist_ok=True)
        pasta_consulta = os.path.join(pasta_temp, nome)
        logging.info(f"Processando dados da consulta '{nome}'...")
        coluna_data = None
        if nome == "Vendas" and "DataVenda" in df_pandas.columns:
            coluna_data = "DataVenda"
        elif nome == "Compras" and "DataEmissaoNF" in df_pandas.columns:
            coluna_data = "DataEmissaoNF"

        if "HoraVenda" in df_pandas.columns:
            if pd.api.types.is_timedelta64_dtype(df_pandas["HoraVenda"]):
                df_pandas["HoraVenda"] = df_pandas["HoraVenda"].apply(lambda x: str(x).split()[-1] if not pd.isna(x) else "00:00:00")
            elif df_pandas["HoraVenda"].dtype == "object":
                df_pandas["HoraVenda"] = df_pandas["HoraVenda"].astype(str).str.extract(r"(\d{2}:\d{2}:\d{2})")[0].fillna("00:00:00")

        # Conversão para Polars – utilizando a função from_pandas (ou from_arrow se for vantajoso)
        df_polars = pl.from_pandas(df_pandas).with_columns([
            pl.lit(datetime.now(pytz.timezone("America/Sao_Paulo")).strftime("%d/%m/%Y %H:%M:%S")).alias("DataHoraAtualizacao"),
            pl.lit(STORAGE_CONFIG["idemp"]).alias("idEmpresa"),
            pl.lit(STORAGE_CONFIG["idemp"]).alias("idEmp")
        ])

        if coluna_data:
            df_polars = df_polars.with_columns(pl.col(coluna_data).cast(pl.Utf8))
            df_polars = df_polars.with_columns([
                pl.col(coluna_data).str.slice(0, 4).alias("Ano"),
                pl.col(coluna_data).str.slice(5, 2).alias("Mes")
            ])
         #   amostra_particoes = df_polars.select(["Ano", "Mes", "Dia"]).unique().head(5)
         #   logging.info(f"Amostra das partições para '{nome}':\n{amostra_particoes.to_pandas().to_string(index=False)}")

        df_polars = ajustar_tipos_dados(df_polars, nome)
        if 'idEmpresa' not in df_polars.schema:
            raise ValueError("A coluna 'idEmpresa' é obrigatória para particionamento.")

        logging.info(f"Salvando '{nome}' em formato particionado...")
        partition_cols = ["idEmpresa"] + (["Ano", "Mes"] if coluna_data else [])
        pq.write_to_dataset(
            df_polars.to_arrow(),
            root_path=pasta_consulta,
            partition_cols=partition_cols,
            compression="snappy",
            use_dictionary=True,
            row_group_size=500_000
        )
        logging.info(f"Salvamento concluído para '{nome}'. Arquivos disponíveis em: {pasta_consulta}")

        particoes_criadas = {os.path.join(pasta_consulta, d) for d in os.listdir(pasta_consulta)}
        return pasta_consulta, particoes_criadas

    except Exception as e:
        logging.error(f"Erro ao processar dados da consulta '{nome}': {e}")
        return "", set()

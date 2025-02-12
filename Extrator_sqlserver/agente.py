import logging
import os
import socket
import subprocess
import sys
import threading
import time
from datetime import datetime
import runpy
import main
if False:
    import main
if True:
    import main
import psutil
import pytz
import requests
from flask import Flask, jsonify

from logging_config import LoggingConfigurator  # Configuração de logging personalizada

# ----------------------------------------------------------------------
# Se o script for chamado com o argumento "main.py", executa o main.py
# e encerra para evitar que o agente se reinicie.
#
# Usamos os.path.basename para comparar apenas o nome do arquivo,
# assim, mesmo que o caminho completo seja passado (por exemplo, em modo empacotado),
# a verificação funcionará corretamente.
# ----------------------------------------------------------------------
if len(sys.argv) > 1 and os.path.basename(sys.argv[1]) == "main.py":
    # Executa o main.py utilizando runpy e encerra o processo
    runpy.run_path(sys.argv[1], run_name="__main__")
    sys.exit(0)

# ----------------------------------------------------------------------
# Configuração de logging para o agente
# ----------------------------------------------------------------------
configurador = LoggingConfigurator(base_log_dir="logs/agente")
configurador.configurar_logging()

app = Flask(__name__)


load_dotenv(dotenv_path="config/config.env")

# Identificação e endereço do servidor
ID_MAQUINA = os.getenv("ID_MAQUINA")
URL_SERVIDOR = os.getenv("URL_SERVIDOR")

# Estado do sistema controlado pelo agente
status_sistema = {"status": "parado", "id": ID_MAQUINA}
sistema_executando = False
processo_sistema = None


def obter_horario_local():
    """Retorna o horário local para 'America/Sao_Paulo'."""
    timezone = pytz.timezone("America/Sao_Paulo")
    return datetime.now(timezone)


def processo_esta_rodando(pid):
    """Verifica se um processo está em execução pelo PID."""
    try:
        processo = psutil.Process(pid)
        return processo.is_running()
    except psutil.NoSuchProcess:
        return False


def exibir_logs_processo(processo):
    """Captura a saída do processo e exibe no terminal."""
    for linha in iter(processo.stdout.readline, ''):
        sys.stdout.write(linha)
        sys.stdout.flush()
    processo.stdout.close()


def enviar_status():
    """Envia o status da máquina ao servidor."""
    try:
        dados = {"id": ID_MAQUINA, "status": status_sistema["status"]}
        resposta = requests.post(f"{URL_SERVIDOR}/status", json=dados, timeout=5)
        if resposta.status_code == 200:
            logging.info(f"Status atualizado no servidor: {status_sistema['status']}")
        else:
            logging.warning(f"Erro ao atualizar status: {resposta.text}")
    except Exception as e:
        logging.error(f"Erro ao conectar no servidor para atualizar status: {e}")


def registrar_status():
    """Verifica se a máquina deve estar rodando e ajusta o estado inicial."""
    global sistema_executando
    try:
        resposta = requests.get(f"{URL_SERVIDOR}/machines", timeout=5)
        if resposta.status_code == 200:
            maquinas = resposta.json()
            if ID_MAQUINA in maquinas and maquinas[ID_MAQUINA]["status"] == "rodando":
                sistema_executando = True
                status_sistema["status"] = "rodando"
            else:
                sistema_executando = False
                status_sistema["status"] = "parado"
    except Exception as e:
        logging.error(f"Erro ao registrar status: {e}")
    enviar_status()


@app.route("/status", methods=["GET"])
def obter_status():
    """Endpoint que retorna o status atual do agente."""
    return jsonify(status_sistema)


@app.route("/start", methods=["POST"])
def iniciar_sistema():
    """
    Endpoint para iniciar o sistema principal.
    Ao receber o comando, inicia o main.py em um subprocesso.
    """
    global sistema_executando, processo_sistema
    if sistema_executando:
        if processo_sistema and processo_esta_rodando(processo_sistema.pid):
            return jsonify({"message": "Sistema já rodando."}), 400
        else:
            logging.warning("Processo foi finalizado inesperadamente. Reiniciando...")
            sistema_executando = False
            processo_sistema = None

    inicio = obter_horario_local()
    logging.info(f"Comando de INÍCIO recebido. Iniciado às: {inicio.strftime('%d/%m/%Y %H:%M:%S')}")

    sistema_executando = True
    status_sistema["status"] = "rodando"
    enviar_status()

    try:
        # Define o caminho para o main.py
        # Em modo empacotado, main.py deve ter sido incluído via --add-data "main.py;."
        if getattr(sys, 'frozen', False):
            caminho_main = os.path.join(sys._MEIPASS, "main.py")
        else:
            caminho_main = "main.py"

        # Define o comando para executar main.py.
        # Ao passar "main.py" como argumento, o bloco inicial deste script detecta e executa main.py.
        comando = [sys.executable, caminho_main]

        # Inicia o subprocesso que executa main.py
        processo_sistema = subprocess.Popen(
            comando,
            stdout=sys.stdout,
            stderr=sys.stderr,
            universal_newlines=True
        )
        logging.info(f"Sistema iniciado com PID {processo_sistema.pid}.")
        threading.Thread(target=monitorar_sistema, args=(inicio,), daemon=True).start()
    except Exception as e:
        logging.error(f"Erro ao iniciar sistema: {e}")
        sistema_executando = False
        status_sistema["status"] = "parado"
        enviar_status()
        return jsonify({"message": "Falha ao iniciar."}), 500

    return jsonify({"message": f"Sistema iniciado na máquina {ID_MAQUINA}."})


@app.route("/stop", methods=["POST"])
def parar_sistema():
    """Endpoint para parar o sistema principal e atualizar o status."""
    global sistema_executando, processo_sistema
    if not sistema_executando:
        return jsonify({"message": "Sistema já parado."}), 400

    logging.info("Comando de PARADA recebido.")

    if processo_sistema:
        try:
            processo_sistema.terminate()
            processo_sistema.wait(timeout=10)
            logging.info("Sistema principal parado com sucesso.")
        except Exception as e:
            logging.error(f"Erro ao parar o sistema: {e}")
            return jsonify({"message": "Erro ao parar o sistema."}), 500
        finally:
            processo_sistema = None

    sistema_executando = False
    status_sistema["status"] = "parado"
    enviar_status()

    fim = obter_horario_local()
    logging.info(f"Finalizado às: {fim.strftime('%d/%m/%Y %H:%M:%S')}")

    return jsonify({"message": f"Sistema parado na máquina {ID_MAQUINA}."})


def monitorar_sistema(inicio):
    """
    Monitora o subprocesso que executa o sistema principal.
    Quando o subprocesso termina, atualiza o status e registra o horário de finalização.
    """
    global sistema_executando, processo_sistema
    if processo_sistema:
        processo_sistema.wait()
        fim = obter_horario_local()
        logging.info(f"Sistema finalizado com código {processo_sistema.returncode}.")

        sistema_executando = False
        status_sistema["status"] = "parado"
        enviar_status()
        processo_sistema = None


def verificar_comandos():
    """
    Faz polling para verificar se há comandos para iniciar ou parar o sistema.
    Se houver comando e o status atual não corresponder à ação, executa a ação e limpa o comando.
    """
    while True:
        try:
            resposta = requests.get(f"{URL_SERVIDOR}/machines", timeout=5)
            if resposta.status_code == 200:
                maquinas = resposta.json()
                if ID_MAQUINA in maquinas:
                    comando = maquinas[ID_MAQUINA].get("command")
                    if comando:
                        if comando == "start":
                            if status_sistema["status"] != "rodando":
                                logging.info("Processando comando start.")
                                requests.post("http://127.0.0.1:5001/start", timeout=5)
                            else:
                                logging.info("Comando start recebido, mas o sistema já está rodando. Ignorando.")
                        elif comando == "stop":
                            if status_sistema["status"] != "parado":
                                logging.info("Processando comando stop.")
                                requests.post("http://127.0.0.1:5001/stop", timeout=5)
                            else:
                                logging.info("Comando stop recebido, mas o sistema já está parado. Ignorando.")
                        # Limpa o comando no servidor
                        requests.post(f"{URL_SERVIDOR}/command", json={"id": ID_MAQUINA, "command": None}, timeout=5)
        except Exception as e:
            logging.error(f"Erro ao buscar comandos: {e}")
        time.sleep(5) #Verificar a cada 5 segundos 


def kill_existing_agents():
    """
    Mata instâncias duplicadas do agente ao iniciar, sem matar o próprio processo.
    Evita múltiplas instâncias simultâneas.
    """
    pid_atual = os.getpid()
    cmdline_atual = psutil.Process(pid_atual).cmdline()

    for proc in psutil.process_iter(["pid", "name", "cmdline"]):
        try:
            cmdline = proc.info["cmdline"]
            if cmdline and isinstance(cmdline, list):
                if "agente" in " ".join(cmdline) and proc.info["pid"] != pid_atual:
                    if cmdline == cmdline_atual:
                        logging.warning(f"Evitando autoencerramento: {cmdline}")
                        continue
                    logging.info(f"Matando processo duplicado: PID {proc.info['pid']}")
                    proc.kill()
        except (psutil.NoSuchProcess, psutil.AccessDenied, TypeError) as e:
            logging.warning(f"Erro ao processar processo {proc.info.get('pid')}: {e}")


def iniciar_agente():
    """Inicia o agente Flask e a verificação de comandos."""
    kill_existing_agents()
    registrar_status()
    logging.info(f"Agente iniciado em http://127.0.0.1:5001")
    threading.Thread(target=verificar_comandos, daemon=True).start()
    app.run(host="0.0.0.0", port=5001, debug=False, threaded=True, use_reloader=False)


if __name__ == "__main__":
    iniciar_agente()

"""
DMS (Dealer Management System) RPA Otomasyonu
Genişletilmiş Kod Dosyası — Full Pipeline
- SQL Server loglama
- UiPath Orchestrator API tetikleme (mock + gerçek endpoint yapısı)
- BPMN 2.0 parser (extended)
- Python ön-işleme modülleri
- Config yönetimi
- Retry mekanizması
- JSON tabanlı süreç parametreleri

Bu dosya gerçek RPA mimarisine yakın, genişletilmiş bir örnek projedir.
"""

import pyodbc
import logging
import requests
import time
import json
import importlib
from datetime import datetime

# -----------------------------------------------------------
# 1. CONFIG YÖNETİMİ (config.json üzerinden)
# -----------------------------------------------------------

def load_config():
    with open("config.json", "r", encoding="utf-8") as f:
        return json.load(f)

CONFIG = load_config()

# -----------------------------------------------------------
# 2. SQL SERVER BAĞLANTISI + LOG TABLOSU
# -----------------------------------------------------------

def get_sql_connection():
    try:
        conn = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={CONFIG['sql']['server']};"
            f"DATABASE={CONFIG['sql']['database']};"
            f"Trusted_Connection=yes;"
        )
        return conn
    except Exception as e:
        print("SQL bağlantı hatası:", e)
        return None


def write_sql_log(level, process, message):
    conn = get_sql_connection()
    if not conn:
        return
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO Logs (Level, ProcessName, Message, LogDate) VALUES (?, ?, ?, ?)",
        (level, process, message, datetime.now())
    )
    conn.commit()
    conn.close()

# -----------------------------------------------------------
# 3. DOSYA + SQL + KONSOL LOGGING
# -----------------------------------------------------------
logging.basicConfig(
    filename="rpa_log.txt",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def log(level, process, message):
    print(message)
    logging.log(level, f"{process} | {message}")
    write_sql_log(level, process, message)

# -----------------------------------------------------------
# 4. RETRY MEKANİZMASI
# -----------------------------------------------------------

def retry(func, retries=3, delay=2):
    for attempt in range(1, retries + 1):
        try:
            return func()
        except Exception as e:
            log(logging.ERROR, "Retry", f"Deneme {attempt} hata: {e}")
            time.sleep(delay)
    raise Exception("Tüm retry denemeleri başarısız oldu.")

# -----------------------------------------------------------
# 5. BPMN 2.0 PARSER (GENİŞLETİLMİŞ)
# -----------------------------------------------------------

def load_bpmn(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)


def execute_bpmn_flow(flow):
    steps = flow.get("steps", [])
    for step in steps:
        name = step.get("name", "UnknownStep")
        action = step.get("action")
        params = step.get("params", {})

        log(logging.INFO, name, f"Adım başlatıldı: {name}")

        try:
            if action == "uipath":
                trigger_uipath_bot(step["bot_name"], params)

            elif action == "python":
                run_python_module(step["module"], params)

            elif action == "wait":
                time.sleep(params.get("seconds", 1))

            elif action == "condition":
                run_conditional_flow(step)

        except Exception as e:
            log(logging.ERROR, name, f"Adım hatası: {e}")

        log(logging.INFO, name, f"Adım tamamlandı: {name}")

# -----------------------------------------------------------
# 6. UiPath Orchestrator API (gerçek endpoint yapısı + mock)
# -----------------------------------------------------------

def orchestrator_auth():
    return {
        "Authorization": f"Bearer {CONFIG['uipath']['token']}"
    }


def trigger_uipath_bot(bot_name, params):
    url = CONFIG['uipath']['orchestrator_url'] + "/jobs/start"
    payload = {
        "bot": bot_name,
        "parameters": params
    }

    # MOCK MODE
    if CONFIG['uipath']['mock']:
        log(logging.INFO, "UiPath", f"MOCK: Bot tetiklendi: {bot_name}")
        return True

    # REAL API MODE
    response = requests.post(url, json=payload, headers=orchestrator_auth())
    if response.status_code == 200:
        log(logging.INFO, "UiPath", f"Bot tetiklendi: {bot_name}")
    else:
        raise Exception(f"UiPath API hatası: {response.text}")

# -----------------------------------------------------------
# 7. PYTHON ÖN-İŞLEME MODÜLLERİ (dinamik yükleme)
# -----------------------------------------------------------

def run_python_module(module_name, params):
    log(logging.INFO, "PythonModule", f"Modül çağrılıyor: {module_name}")
    module = importlib.import_module(module_name)

    if hasattr(module, "run"):
        return module.run(params)
    else:
        raise Exception(f"Modülde 'run' fonksiyonu yok: {module_name}")

# -----------------------------------------------------------
# 8. KOŞULLU BPMN ADIMI
# -----------------------------------------------------------

def run_conditional_flow(step):
    condition = step.get("condition")
    true_flow = step.get("true_flow")
    false_flow = step.get("false_flow")

    if eval(condition):
        log(logging.INFO, "Condition", "Şart sağlandı → True Flow")
        execute_bpmn_flow(true_flow)
    else:
        log(logging.INFO, "Condition", "Şart sağlanmadı → False Flow")
        execute_bpmn_flow(false_flow)

# -----------------------------------------------------------
# 9. ANA ÇALIŞTIRMA
# -----------------------------------------------------------
if __name__ == "__main__":
    try:
        log(logging.INFO, "Main", "DMS RPA Otomasyon Başlatıldı")
        bpmn_flow = load_bpmn("process_flow.json")
        execute_bpmn_flow(bpmn_flow)
        log(logging.INFO, "Main", "Tüm süreç tamamlandı")

    except Exception as e:
        log(logging.ERROR, "Main", f"Kritik hata: {e}")

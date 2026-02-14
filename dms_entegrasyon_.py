import os
import sys
from pathlib import Path
import json
import logging
from datetime import datetime

# proje kök dizini
ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "config" / "config.json"
PROCESS_FLOW = ROOT / "config" / "process_flow.json"
LOG_DIR = ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)

# logging
logging.basicConfig(
    filename=str(LOG_DIR / "rpa_runtime.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# lokal modülleri yükleyebilmek için src yoluna ekle
sys.path.append(str(ROOT / "src"))

# helper fonksiyon
def load_json(p):
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)

config = load_json(CONFIG_PATH)
process_flow = load_json(PROCESS_FLOW)

# basit SQL logger (pyodbc kullandım)
try:
    import pyodbc
except Exception:
    pyodbc = None


def write_sql_log(level, process, message):
    if not config.get("sql", {}).get("enabled", False):
        return
    if pyodbc is None:
        logging.warning("pyodbc bulunamadı, SQL yazma atlandı")
        return

    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={config['sql']['server']};"
        f"DATABASE={config['sql']['database']};"
    )
    try:
        conn = pyodbc.connect(conn_str, autocommit=False)
        cur = conn.cursor()
        cur.execute(
            "IF OBJECT_ID('dbo.Logs','U') IS NULL
CREATE TABLE dbo.Logs (Id INT IDENTITY(1,1) PRIMARY KEY, LogDate DATETIME, Level NVARCHAR(20), ProcessName NVARCHAR(200), Message NVARCHAR(MAX))"
        )
        cur.execute(
            "INSERT INTO dbo.Logs (LogDate, Level, ProcessName, Message) VALUES (?, ?, ?, ?)",
            (datetime.now(), level, process, message)
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logging.error(f"SQL yazma esnasında hata: {e}")


def app_log(level, process, message):
    print(f"[{level}] {process} - {message}")
    logging.log(level, f"{process} | {message}")
    write_sql_log(level, process, message)

# BPMN akış çalıştırıcısı ama just basic

def execute_step(step):
    action = step.get("action")
    name = step.get("name", "Unnamed")
    app_log(logging.INFO, name, f"Çalıştırılıyor: {action}")

    try:
        if action == "uipath":
            from integrations.uipath_integration import trigger_uipath
            trigger_uipath(step.get("bot_name"), step.get("parameters", {}), config)

        elif action == "python":
            module = step.get("module")
            from modules import runner as module_runner
            module_runner.run(module, step.get("parameters", {}))

        elif action == "wait":
            import time
            time.sleep(step.get("seconds", 1))

        elif action == "condition":
            cond = step.get("condition")
            # güvenlik kaynaklı  eval yerine sınırlı bir eval func kullanıyorum 
            # ama basic demo için eval kullanıyorum burda
            if eval(cond):
                for s in step.get("true_flow", {}).get("steps", []):
                    execute_step(s)
            else:
                for s in step.get("false_flow", {}).get("steps", []):
                    execute_step(s)

        else:
            app_log(logging.WARNING, name, f"Bilinmeyen action: {action}")

    except Exception as e:
        app_log(logging.ERROR, name, f"Hata: {e}")


def main():
    app_log(logging.INFO, "Main", "DMS RPA Otomasyon Başlatıldı")
    for step in process_flow.get("steps", []):
        execute_step(step)
    app_log(logging.INFO, "Main", "Tüm süreç tamamlandı")


##----------------------------------------------------------------------------

import requests
import logging


def get_token(config):
    """OAuth2 authentication (UiPath Cloud)"""
    url = config['uipath']['auth_url']
    payload = {
        "grant_type": "refresh_token",
        "client_id": config['uipath']['client_id'],
        "refresh_token": config['uipath']['refresh_token']
    }
    r = requests.post(url, data=payload)
    if r.status_code != 200:
        raise Exception(f"Token alınamadı: {r.text}")
    return r.json()['access_token']


def get_release_key(bot_name, config, token):
    """ReleaseKey'i bot adına göre çeker"""
    url = f"{config['uipath']['orchestrator_url']}/odata/Releases?$filter=Name eq '{bot_name}'"
    headers = {"Authorization": f"Bearer {token}"}

    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        raise Exception(f"Release key alınamadı: {r.text}")

    data = r.json()
    if not data['value']:
        raise Exception(f"Bot bulunamadı: {bot_name}")

    return data['value'][0]['Key']


def trigger_uipath(bot_name, parameters, config):
    """Gerçek job başlatma"""
    if config['uipath'].get('mock', True):
        logging.info(f"MOCK UiPath (gerçek çağrı kapalı): {bot_name} {parameters}")
        return {"status": "mocked"}

    # token al
    token = get_token(config)

    # releasekey al
    release_key = get_release_key(bot_name, config, token)

    # job start 
    url = f"{config['uipath']['orchestrator_url']}/odata/Jobs/UiPath.Server.Configuration.OData.StartJobs"

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    body = {
        "startInfo": {
            "ReleaseKey": release_key,
            "Strategy": "ModernJobs",
            "MachineLogicalName": config['uipath']['machine'],
            "RunAsUser": config['uipath']['run_as_user'],
            "InputArguments": parameters
        }
    }

    r = requests.post(url, json=body, headers=headers)

    if r.status_code not in (200, 201):
        raise Exception(f"UiPath job başlatılamadı: {r.status_code} {r.text}")

    logging.info(f"UiPath job tetiklendi: {bot_name}")
    return r.json()

"""
Basit UiPath Orchestrator entegrasyonu kullanımdan önce config.json içindeki
ayarları düzgün  doldr ve API spec e göre genişlet
"""
import requests
import logging


def trigger_uipath(bot_name, parameters, config):
    if config['uipath'].get('mock', True):
        logging.info(f"MOCK UiPath: {bot_name} param={parameters}")
        return {"status": "mocked"}

    url = config['uipath']['orchestrator_url'].rstrip('/') + '/odata/Jobs/UiPath.Server.Configuration.OData.StartJobs'
    headers = {"Authorization": f"Bearer {config['uipath']['token']}", "Content-Type": "application/json"}

    body = {
        "startInfo": {
            # Orchestrator sürümüne göre değişken knk kontrol et
            "ReleaseKey": config['uipath']['release_key_for_' + bot_name],
            "Strategy": "Specific",
            "RobotIds": config['uipath'].get('robot_ids', []),
            "InputArguments": parameters
        }
    }

    resp = requests.post(url, json=body, headers=headers)
    if resp.status_code not in (200, 201):
        raise Exception(f"UiPath API hata: {resp.status_code} {resp.text}")
    return resp.json()

=== FILE: src/modules/runner.py ===

"""
 src/modules içindeki modülleri dinamik çalıştırır
her modüll `run(params: dict)` fonksiyonumu expose etmek zorunda 
"""
import importlib
import logging


def run(module_name, params):
    logging.info(f"Module runner: {module_name} params={params}")
    mod = importlib.import_module(f"modules.{module_name}")
    if hasattr(mod, 'run'):
        return mod.run(params)
    else:
        raise Exception("Modülde run fonksiyonu bulunamadı")

##ex.:ön işleme modülü müşteri datasını normalize edip doğrula r

def run(params):
    # params örneği: {"customer": {"name":" ... ", "phone": "..."}}
    customer = params.get('customer', {})
    name = customer.get('name', '').strip().title()
    phone = customer.get('phone', '')
    # basit temizleme
    phone = ''.join([c for c in phone if c.isdigit()])

    # örnek dönüş
    return {"name": name, "phone": phone}

=== FILE: config/config.json ===
```json
{
  "sql": {
    "enabled": false,
    "server": "localhost",
    "database": "DMS_LOGS"
  },
  "uipath": {
    "mock": true,
    "orchestrator_url": "https://platform.uipath.com",
    "token": "REPLACE_WITH_TOKEN"
  }
}

=== FILE: config/process_flow.json ===
```json
{
  "name": "DMS Service Flow",
  "steps": [
    {
      "name": "Preprocess Customer",
      "action": "python",
      "module": "preprocess_customer",
      "parameters": {"customer": {"name": "ali", "phone": "+90 (555) 123 45 67"}}
    },
    {
      "name": "Trigger UiPath CreateService",
      "action": "uipath",
      "bot_name": "CreateServiceJob",
      "parameters": {"input": "{""example"": 1}"}
    },
    {
      "name": "Wait Short",
      "action": "wait",
      "seconds": 2
    }
  ]
}

=== FILE: sql/create_tables.sql ===
```sql
-- SQL Server: Log tabloları
CREATE TABLE dbo.Logs (
    Id INT IDENTITY(1,1) PRIMARY KEY,
    LogDate DATETIME NOT NULL DEFAULT GETDATE(),
    Level NVARCHAR(20),
    ProcessName NVARCHAR(200),
    Message NVARCHAR(MAX)
);

CREATE TABLE dbo.ErrorLogs (
    Id INT IDENTITY(1,1) PRIMARY KEY,
    ProcessName NVARCHAR(200),
    ErrorMessage NVARCHAR(MAX),
    LogDate DATETIME NOT NULL DEFAULT GETDATE()
);

=== FILE: requirements.txt ==
pyodbc
requests

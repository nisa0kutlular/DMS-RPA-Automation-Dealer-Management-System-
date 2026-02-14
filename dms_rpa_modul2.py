import os
import sys
from pathlib import Path
import json
import logging
from datetime import datetime


ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "config" / "config.json"
PROCESS_FLOW = ROOT / "config" / "process_flow.json"
LOG_DIR = ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    filename=str(LOG_DIR / "rpa_runtime.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


sys.path.append(str(ROOT / "src"))


def load_json(p):
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)

config = load_json(CONFIG_PATH)
process_flow = load_json(PROCESS_FLOW)

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

if __name__ == "__main__":
    main()

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

import importlib
import logging


def run(module_name, params):
    logging.info(f"Module runner: {module_name} params={params}")
    mod = importlib.import_module(f"modules.{module_name}")
    if hasattr(mod, 'run'):
        return mod.run(params)
    else:
        raise Exception("Modülde run fonksiyonu bulunamadı")


def run(params):

    customer = params.get('customer', {})
    name = customer.get('name', '').strip().title()
    phone = customer.get('phone', '')

    phone = ''.join([c for c in phone if c.isdigit()])

    return {"name": name, "phone": phone}

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

```
pyodbc
requests

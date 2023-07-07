# Fix privileges and expiration
import json
import sys
import os

from pathlib import Path

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)) + "'/../..")
from common.lib.database import Database
from common.lib.logger import Logger

log = Logger(output=True)

import configparser
ini = configparser.ConfigParser()
ini.read(Path(__file__).parent.parent.parent.resolve().joinpath("config/config.ini"))
db_config = ini["DATABASE"]

db = Database(logger=log, dbname=db_config["db_name"], user=db_config["db_user"], password=db_config["db_password"],
              host=db_config["db_host"], port=db_config["db_port"], appname="4cat-migrate")

print("  Adding default privileges for the 'admin' tag...")
privileges = ("privileges.admin.can_manipulate_all_datasets", "privileges.admin.can_manipulate_notifications")
for privilege in privileges:
    print(f"  ...{privilege}")
    db.insert("settings", data={"name": privilege, "value": "true", "tag": "admin"}, safe=True)

# expiration was a mistake
# or at least the initial implementation was
print("  Harmonising dataset expiration dates...")
warning_datasets = False
warning_expires = False
datasets = db.fetchall("SELECT * FROM datasets where parameters::json->>'expires-after' > 0 AND parameters::json->>'keep' IS NULL")
if datasets:
    warning_datasets = True

expires = ("SELECT * FROM settings WHERE name = 'expires.timeout'")["value"]
try:
    expires = int(expires)
except (TypeError, ValueError):
    expires = 0
if expires:
    warning_expires = True

if any(warning_expires, warning_datasets):
    print("  /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ ")
    print("                        WARNING!                      ")
    if warning_datasets:
        print("  Some datasets were explicitly marked for deletion. Because"
              "  it cannot unambiguously be determined whether these should"
              "  be deleted, they have been unmarked (i.e. they will not "
              "  automatically be deleted). Use the data source expiration "
              "  settings to make sure they expire correctly.")
    if warning_expires:
        print("  It is no longer possible to set global expiration timeouts. "
              "  Instead, these need to be configured per data source. You "
              "  can do this in the settings panel.")
    print("")
    print("  For this reason, all expiration settings have been reset."
          "  Please re-configure via the settings panel and the new "
          "  dataset bulk management interface in the web interface.")
    print("")
    print("  See the release notes for version 1.35 for more "
          "  information.")
    print("  /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ ")

    # reset timeouts for all current expiration settings, for all tags
    db.delete("settings", where={"name": "datasources.expiration"})
    datasources = db.fetchall("SELECT * FROM settings WHERE name = 'datasources.enabled'")
    for setting in datasources:
        db.delete("settings", where=setting)
        try:
            # reset timeouts
            enabled = json.loads(setting["value"])
            db.insert("settings", data={"name": "datasources.expiration", "value": json.dumps({
                datasource: {
                    "enabled": True,
                    "timeout": 0,
                    "allow_optout": False
                } for datasource in enabled
            }), "tag": setting["tag"]})
        except json.JSONDecodeError:
            # invalid setting, deleting is enough
            pass



import json
import re
from helpers.MariaDb import MariaDb
from helpers.YamlReader import YamlReader

config = YamlReader.ymlConfig("conf/config.yml")
tableName = config["tables"]["experiment"]["tablename"]
fields = config["tables"]["experiment"]["fields"]
fileLocation = config["files"]["location"]
fileName = config["files"]["file1"]
with open(fileLocation+fileName, "r") as logs:
    for log in logs:
        try:
            data = json.loads(log)
            if "control" in data["msg"].lower():
                date = data["time"][:10]
                group = "control"
                experiment = re.search("\sMS-\d*", data["msg"], re.IGNORECASE).group().strip()
                values = (date, group, experiment)
                MariaDb.insertRecord(tableName, fields, values)
            elif "test" in data["msg"].lower():
                date = data["time"][:10]
                group = "test"
                experiment = re.search("\sMS-\d*", data["msg"], re.IGNORECASE).group().strip()
                values = (date, group, experiment)
                MariaDb.insertRecord(tableName, fields, values)
            else:
                # These records are not ingested in the assumption that if the key "msg" doesn't have the keywords "control" or "test" in its value, they're not part of the visitor assignment
                print("[Not Ingested]: {}".format(log))
        except:
            # These records are not ingested in the assumption that if the key "msg" doesn't have the keywords "control" or "test" in its value, they're not part of the visitor assignment
            print("[Not Ingested]: {}".format(log))

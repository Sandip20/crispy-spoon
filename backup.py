import pymongo
import subprocess
import os
from dotenv import load_dotenv
import certifi
ca = certifi.where()
load_dotenv()

# Local MongoDB configuration
local_host = "localhost"
local_port = 27017
local_db = "nse_historical"
local_url=f"mongodb://localhost:27018/{os.environ['MONGO_INITDB_DATABASE']}"

# Cloud MongoDB configuration
cloud_uri = "mongodb+srv://python_user:YWbhivgSl7EMQKGF@cluster0.dcluk.mongodb.net/?retryWrites=true&w=majority"
# Backup the local MongoDB database
backup_dir = "mongo"
backup_filename = "mongo.dump"
backup_path = os.path.join(os.getcwd(),backup_dir, backup_filename)
print(backup_path)
# 
# working url
# mongorestore --uri "mongodb+srv://python_user:YWbhivgSl7EMQKGF@cluster0.dcluk.mongodb.net/?retryWrites=true&w=majority" --nsInclude "nse_historical.*" --archive < "C:/projects/python/crispy-spoon/mongo/mongodb.dump"
# subprocess.call(["docker", "exec", "-it", "analytics-mongodb-1" ,"/usr/bin/mongodump", 
#  "--username", "root", 
#  "--password","rootpassword",
#  "--authenticationDatabase","admin",
#  "--db","nse_historical",
#  "--archive",">",backup_path])
# subprocess.call(["docker exec -i analytics-mongodb-1 /usr/bin/mongodump --username root --password rootpassword --authenticationDatabase admin --db nse_historical --archive > C:\\Users\\sandip.pawar\\Desktop\\mongodb_data\\mongo.dump"])
# dump_path="C:/Users/sandip.pawar/Desktop/mongodb_data/"
# Restore the backup to the cloud MongoDB database
subprocess.call([ "mongorestore", "--uri", cloud_uri, 
                 "--nsInclude=nse_historical.*","--drop",backup_path])


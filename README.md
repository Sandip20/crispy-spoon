Cookies that nse apis are depens on:
bm_sv ,ak_bmsc
target is to get these cookies and call the apis to get data










requirements
we need to have mongodb instance running either it will be locally or on remote server

install all the required packages to run this script 
download strikes information: https://archives.nseindia.com/content/fo/sos_scheme.xls
Download lots information: https://archives.nseindia.com/content/fo/fo_mktlots.csv
Buying opportunity:
Look for cheapeast as well as  wait for the move if
first standard deviation jaise hi move aati hai we need to enter 
first_standard_deviation =Volatility(IV) / 16.2
for example stocks iv is around 30
wait for 1.5% or 2% then buy and hold the stock


todo:
1) create UI to see the top n cheapest straddles
2) dockerize the script
3) create web server to host  it as application

http://127.0.0.1:5000/api/v1/options?symbol=ABCAPITAL&from_date=2022-12-30&to_date=2023-02-23&weeks_to_expiry=week5
http://127.0.0.1:5000/api/v1/straddles?symbol=ABCAPITAL&from_date=2022-12-30&to_date=2023-02-23&weeks_to_expiry=week5
http://127.0.0.1:5000/api/v1/straddles?symbol=JKCEMENT&from_date=2022-12-30&to_date=2023-02-23&weeks_to_expiry=week5
http://127.0.0.1:5000/api/v1/futures?symbol=JKCEMENT&from_date=2022-12-30&to_date=2023-02-23&weeks_to_expiry=week5

steps to take backup of mongo db running in docker instance-------------

docker ps
Use the docker cp command to copy the MongoDB data to your local system. Replace <container_id> with the container ID you found in step 1, and <local_path> with the path on your local system where you want to save the MongoDB data.

ruby
Copy code
docker cp <container_id>:/data/db <local_path>
For example, if you want to save the MongoDB data to a folder named "mongodb_data" on your Desktop, you can use the following command:

ruby
Copy code
docker cp <container_id>:/data/db C:\Users\YourUserName\Desktop\mongodb_data
Note that you may need to create the folder on your local system before running the command.

-------------------------------------------------
steps to restore the mongodb dump on mongo instance running in docker:

o restore the MongoDB dump data from your local system to the MongoDB instance running in Docker, you can follow these steps:

Open a new command prompt or terminal window.

Navigate to the directory where you have saved the dump data using the cd command.

Start the MongoDB container in Docker using the following command:

docker start <container_name>
Use the docker cp command to copy the dump data to the running container. For example:

php
Copy code
docker cp <path_to_dump_data> <container_name>:<path_inside_container>
In this command, replace <path_to_dump_data> with the path to the dump data directory on your local system and <path_inside_container> with the path where you want to copy the data inside the container. For example, if you want to copy the data to the /data/db directory inside the container, you can use the following command:

ruby
Copy code
docker cp <path_to_dump_data> <container_name>:/data/db
Once the data is copied, you can use the mongorestore command to restore the data in the MongoDB instance running inside the container. For example:

php
Copy code
docker exec <container_name> mongorestore --db <database_name> <path_to_dump_data>
In this command, replace <database_name> with the name of the database that you want to restore and <path_to_dump_data> with the path to the dump data directory inside the container.

For example, if you want to restore the data for a database named mydb and the dump data is located in the /data/db directory inside the container, you can use the following command:

bash
Copy code
docker exec <container_name> mongorestore --db mydb /data/db
mongorestore --uri "mongodb+srv://python_user:YWbhivgSl7EMQKGF@cluster0.dcluk.mongodb.net/?retryWrites=true&w=majority" --nsInclude "nse_historical.*" --archive < "C:/projects/python/crispy-spoon/mongo/mongodb.dump"
This will restore the data to the mydb database in the MongoDB instance running inside the container.

That's it! You have now restored the MongoDB dump data from your local system to the MongoDB instance running in Docker.




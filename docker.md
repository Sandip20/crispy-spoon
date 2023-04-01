1) 
docker hands -on 

docker build
docker push - to push the image  to the registry 
docker pull it will pull the image 
docker run  my-app
docker container inspect:  give mata data of the container in json format 
docker conainer stats  give live performance data of the containers
docker container top 

whats the image:
app binaries and dependencies
metadata about image data 

docker image prune (to clean up just "dangling" images)
docker system prune (will clean up everything)
docker image prune -a (which will remove all images you're not using)

docker tag sourceImage newImage:tag 
UFS union file system
Volumnes : ma

it will create the image from source image  with same image id 

docker push imagename:tag

it will create the image to the repo
if  we create one more tag from our image and try to push it will only push the tag not image

docker volume ls 

docker volume insptect mysql-db
docker 


list the mounted data on the system 


docker compose
    maitaine dependencies
    docker swarm high level
2) aws certified cloud practitioner

GCP Cloud Associate 
3) serverless production 
AWS Certified Developer 
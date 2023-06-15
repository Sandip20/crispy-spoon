#latest ubuntu version
FROM continuumio/miniconda3
# Set the working directory
WORKDIR /crispy-spoon
COPY . /crispy-spoon
#information of author
LABEL AUTHOR="SANDIP" 
# add bash script
ADD install.sh /
RUN chmod +x /install.sh
#run the bash script
RUN /install.sh
# RUN pip install -r requirements.txt
RUN pip install python-dotenv
RUN pip install ratelimit
RUN pip install "pymongo[srv]"
# ENV PATH /root/miniconda3/bin:$PATH
ENV PATH /opt/conda/envs/crispy-spoon/bin:$PATH
CMD [ "python","start_engine.py" ]
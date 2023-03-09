#latest ubuntu version
FROM ubuntu:latest
#information of author
LABEL AUTHOR="SANDIP" 
# add bash script
ADD install.sh /
#run the bash script
RUN /install.sh
ENV PATH /root/miniconda3/bin:$PATH
COPY options_wizard.ipynb options_wizard.ipynb
CMD ["ipython"]



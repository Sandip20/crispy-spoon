#latest ubuntu version
FROM continuumio/miniconda3
#information of author
LABEL AUTHOR="SANDIP" 
# add bash script
ADD install.sh /
RUN chmod +x /install.sh
#run the bash script
RUN /install.sh
ENV PATH /root/miniconda3/bin:$PATH
COPY options_wizard.ipynb options_wizard.ipynb
COPY magic_engine_v2.py magic_engine_v2.py
CMD [ "python","magic_engine_v2.py" ]
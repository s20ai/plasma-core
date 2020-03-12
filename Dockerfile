# Base Image ubuntu
FROM ubuntu:18.04

# Update software repositories
RUN apt-get -y update

# Install nginx redis and mongo db
RUN apt-get -y install nginx redis mongodb python3 python3-pip

# move plasma-core to container
ADD plasmad /opt/plasmad
ADD requirements.txt /opt/
RUN cd /opt/ && pip3 install -r requirements.txt
ADD data/starter.sh /opt/

# Move production UI code to container
ADD data/build/ /opt/plasma-ui/

# Move nginx config file to container
COPY data/nginx_config_plasma /etc/nginx/sites-enabled/default

# Start services 

RUN service nginx start
RUN service mongodb start
#RUN service redis-server start | redis-bug : can not assign address 
RUN redis-server &


# Start plasma daemon 
CMD bash opt/starter.sh


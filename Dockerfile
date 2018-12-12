FROM imqs/ubuntu-base

COPY bin/cmd /opt/search
ENV IMQS_CONTAINER=true
EXPOSE 80
ENTRYPOINT ["wait-for-nc.sh", "config:80", "--", "wait-for-postgres.sh", "db", "/opt/search", "run"]


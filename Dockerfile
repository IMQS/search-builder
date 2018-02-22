FROM imqs/ubuntu-base

COPY bin/cmd /opt/search
ENV IMQS_CONTAINER=true
EXPOSE 80
ENTRYPOINT ["/opt/search", "run"]


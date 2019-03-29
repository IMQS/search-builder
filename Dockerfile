##################################
# Builder image
##################################
FROM golang:1.11 as builder

ARG netrc
ARG ssh_pvt_key
ARG ssh_pub_key

RUN mkdir /build

# Authorize SSH Host
RUN mkdir -p /root/.ssh && \
    chmod 0700 /root/.ssh && \
    ssh-keyscan github.com > /root/.ssh/known_hosts

COPY ./ /build
RUN git version
# Add the keys and set permissions
RUN echo "$netrc" > /root/.netrc && \
    echo "$ssh_pvt_key" > /root/.ssh/id_rsa && \
    echo "$ssh_pub_key" > /root/.ssh/id_rsa.pub && \
    chmod 600 /root/.ssh/id_rsa && \
    chmod 600 /root/.ssh/id_rsa.pub

RUN cat /root/.ssh/id_rsa.pub

RUN git config --global url."git@github.com:".insteadOf "https://github.com/"
WORKDIR /build/
RUN go build

####################################
# Deployed image
####################################
FROM imqs/ubuntu-base
COPY --from=builder /build/search /opt/search
EXPOSE 80
ENTRYPOINT ["wait-for-nc.sh", "config:80", "--", "wait-for-postgres.sh", "db", "/opt/search", "run"]


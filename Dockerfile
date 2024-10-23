# docker build -t imqs/search:master --build-arg SSH_KEY="`cat ~/.ssh/id_rsa`" .

##################################
# Builder image
##################################
FROM golang:1.22 AS builder

ARG SSH_KEY

RUN mkdir /build

# Authorize SSH Host
RUN mkdir -p /root/.ssh && \
	chmod 0700 /root/.ssh && \
	ssh-keyscan github.com > /root/.ssh/known_hosts

COPY ./ /build

# Authorize SSH Host
RUN mkdir -p /root/.ssh && \
	chmod 0700 /root/.ssh && \
	ssh-keyscan github.com > /root/.ssh/known_hosts


RUN echo "$SSH_KEY" > /root/.ssh/id_rsa && \
	chmod 600 /root/.ssh/id_rsa

RUN git config --global url."git@github.com:".insteadOf "https://github.com/"
WORKDIR /build/
RUN go env -w GOPRIVATE="github.com/IMQS/*"
RUN go build

####################################
# Deployed image
####################################
FROM imqs/ubuntu-base:24.04

COPY --from=builder /build/search-builder /opt/search

RUN chmod +x /opt/search

EXPOSE 80

ENTRYPOINT ["wait-for-nc.sh", "config:80", "--", "wait-for-postgres.sh", "db", "/opt/search", "run"]

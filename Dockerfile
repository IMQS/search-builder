# docker build -t imqs/search:master --build-arg ssh_pvt_key="`cat ~/.ssh/id_rsa`" .

##################################
# Builder image
##################################
FROM golang:1.14 as builder

ARG ssh_pvt_key

RUN mkdir /build

# Authorize SSH Host
RUN mkdir -p /root/.ssh && \
	chmod 0700 /root/.ssh && \
	ssh-keyscan github.com > /root/.ssh/known_hosts

COPY ./ /build
RUN git version

# Authorize SSH Host
RUN mkdir -p /root/.ssh && \
	chmod 0700 /root/.ssh && \
	ssh-keyscan github.com > /root/.ssh/known_hosts


RUN echo "$ssh_pvt_key" > /root/.ssh/id_rsa && \
	chmod 600 /root/.ssh/id_rsa

RUN git config --global url."git@github.com:".insteadOf "https://github.com/"
WORKDIR /build/
RUN go env -w GOPRIVATE="github.com/IMQS/*"
RUN go build

####################################
# Deployed image
####################################
FROM imqs/ubuntu-base
COPY --from=builder /build/search /opt/search
EXPOSE 80
ENTRYPOINT ["wait-for-nc.sh", "config:80", "--", "wait-for-postgres.sh", "db", "/opt/search", "run"]

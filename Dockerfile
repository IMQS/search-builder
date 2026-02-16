# docker build -t imqs/search:latest --ssh default .

##################################
# Builder image
##################################
FROM golang:1.22 AS builder

RUN mkdir /build

# Authorize SSH Host
RUN mkdir -p /root/.ssh && \
	chmod 0700 /root/.ssh && \
	ssh-keyscan github.com > /root/.ssh/known_hosts

COPY ./ /build

RUN --mount=type=ssh \
	git config --global url."git@github.com:".insteadOf "https://github.com/"

WORKDIR /build/
RUN go env -w GOPRIVATE="github.com/IMQS/*"
RUN --mount=type=ssh \
	go build

####################################
# Deployed image
####################################
FROM imqs/ubuntu-base:24.04

COPY --from=builder /build/search-builder /opt/search

RUN chmod +x /opt/search

EXPOSE 80

ENTRYPOINT ["wait-for-nc.sh", "config:80", "--", "wait-for-postgres.sh", "db", "/opt/search", "run"]

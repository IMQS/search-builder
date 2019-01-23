##################################
# builder image
##################################
FROM golang:1.8 as builder
RUN mkdir /build
COPY src/ /build/src
ENV GOPATH /build
WORKDIR /build/
RUN go install github.com/IMQS/search/cmd

####################################
# deployed image
####################################
FROM imqs/ubuntu-base
COPY --from=builder /build/bin/cmd /opt/search
EXPOSE 80
ENTRYPOINT ["wait-for-nc.sh", "config:80", "--", "wait-for-postgres.sh", "db", "/opt/search", "run"]


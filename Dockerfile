##################################
# builder image
##################################
FROM golang:1.8 as builder
RUN mkdir /build
COPY ./ /build
WORKDIR /build/
RUN go build

####################################
# deployed image
####################################
FROM imqs/ubuntu-base
COPY --from=builder /build/search /opt/search
EXPOSE 80
ENTRYPOINT ["wait-for-nc.sh", "config:80", "--", "wait-for-postgres.sh", "db", "/opt/search", "run"]


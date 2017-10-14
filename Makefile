.PHONY: kafka-pg-adapter kluster-client-golang build start stop clean cleanCluster cleanAll

kafka-pg-adapter:
	cd kafka-pg-adapter && CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o main .

kluster-client-golang:
	cd kluster-client-golang && CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o main .

build: kafka-pg-adapter kluster-client-golang

start:
	cd kluster && docker-compose up --build --abort-on-container-exit

stop: 
	cd kluster && docker-compose down

clean: 
	cd kafka-pg-adapter && rm -f main && cd ../kluster-client-golang && rm -f main

cleanCluster:
	cd ../kluster && docker-compose rm -f
	
cleanAll: clean cleanCluster

run: build start

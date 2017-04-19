TAG=$(shell git rev-parse --short HEAD)

all: operator wordcount

operator:
	CGO_ENABLED=0 go build -o cmd/operator/bin/operator cmd/operator/main.go
	cp /etc/ssl/certs/ca-certificates.crt cmd/operator/bin/ #Because otherwise x509 wont work in scratch image
	docker build -t $(PREFIX)kubemr-operator cmd/operator/
ifneq ("$(PREFIX)","")
	docker push $(PREFIX)kubemr-operator:latest
	docker tag $(PREFIX)kubemr-operator $(PREFIX)kubemr-operator:$(TAG)
	docker push $(PREFIX)kubemr-operator:$(TAG)
endif

wordcount:
	CGO_ENABLED=0 go build -o cmd/wordcount/bin/wordcount cmd/wordcount/main.go
	docker build -t $(PREFIX)kubemr-wordcount cmd/wordcount/
ifneq ("$(PREFIX)","")
	docker push $(PREFIX)kubemr-wordcount:latest
	docker tag $(PREFIX)kubemr-wordcount $(PREFIX)kubemr-wordcount:$(TAG)
	docker push $(PREFIX)kubemr-wordcount:$(TAG)
endif

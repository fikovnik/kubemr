TAG=$(shell git rev-parse --short HEAD)
BRANCH=$(shell git symbolic-ref --short -q HEAD)
ifeq ("$(BRANCH)","master")
	BRANCH="latest"
endif

export CGO_ENABLED=0

all: wordcount wordcountexec



wordcount:
	go build -o cmd/wordcount/bin/wordcount cmd/wordcount/main.go
	docker build -t $(PREFIX)kubemr-wordcount cmd/wordcount/
ifneq ("$(PREFIX)","")
	docker tag $(PREFIX)kubemr-wordcount $(PREFIX)kubemr-wordcount:$(BRANCH)
	docker push $(PREFIX)kubemr-wordcount:$(BRANCH)
	docker tag $(PREFIX)kubemr-wordcount $(PREFIX)kubemr-wordcount:$(TAG)
	docker push $(PREFIX)kubemr-wordcount:$(TAG)
endif

wordcountexec:
	go build -o cmd/wordcountexec/bin/wordcountexec cmd/wordcountexec/main.go
	docker build -t $(PREFIX)kubemr-wordcountexec cmd/wordcountexec/
ifneq ("$(PREFIX)","")
	docker tag $(PREFIX)kubemr-wordcountexec $(PREFIX)kubemr-wordcountexec:$(BRANCH)
	docker push $(PREFIX)kubemr-wordcountexec:$(BRANCH)
	docker tag $(PREFIX)kubemr-wordcountexec:$(BRANCH) $(PREFIX)kubemr-wordcountexec:$(TAG)
	docker push $(PREFIX)kubemr-wordcountexec:$(TAG)
endif


test:
	go test -cover github.com/turbobytes/kubemr/pkg/worker
	go test -cover github.com/turbobytes/kubemr/pkg/job
	go test -cover github.com/turbobytes/kubemr/pkg/k8s

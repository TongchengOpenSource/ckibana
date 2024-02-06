JAVAC := javac
JAVA_VERSION := 17

repo ?= docker.io/ting001/ckibana
tag ?= latest
platform ?= linux/arm64
env ?= test

all: run

clean:
	@mvn clean

compile:
	@mvn compile

package:
	@mvn package

run: package
	@java -jar target/*.jar

image-build:
	@rm -rf tmp
	@docker rmi "$(repo):$(tag)" --force
	mkdir tmp && \
		cp Dockerfile tmp && \
		cp target/*.jar tmp && cd tmp && \
		docker build  --build-arg env=$(env) -f Dockerfile -t "$(repo):$(tag)" .
	@rm -rf tmp

image-run: image-build
	docker run --rm -it -p 8080:8080 --name es2ck "$(repo):$(tag)"

image-push: image-build
	@docker push "$(repo):$(tag)"

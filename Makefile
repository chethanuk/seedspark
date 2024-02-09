SHELL := /bin/bash

# Define all targets that are not associated with files
.PHONY: all install test clean install-pyenv install-pyenv-version install-kubectl install-helm clean-ubuntu install-dependencies install-sdkman source-sdkman install-java install-flink install-spark install-hadoop clean-pyenv

# The default target
all: install test install-pyenv install-pyenv-version install-kubectl install-helm clean-ubuntu install-dependencies install-sdkman source-sdkman install-java install-flink install-spark install-hadoop clean-pyenv

# Define variables
CC = gcc
CFLAGS = -I.
SDKMAN_URL = https://get.sdkman.io
JAVA_VERSION = 11.0.22-zulu
SCALA_VERSION = 2.13.12
FLINK_VERSION = 1.18.0
SPARK_VERSION = 3.5.0
HADOOP_VERSION = 3.3.5
PYTHON_VERSION = 3.10.13

# Check if poetry is installed and install it if not
# This is a prerequisite for the install target
POETRY := $(shell command -v poetry 2> /dev/null)
SDKMAN := source "$$HOME/.sdkman/bin/sdkman-init.sh" && sdk

$(POETRY):
	@echo "Installing Poetry..."
	@curl -sSL https://install.python-poetry.org | python3 -

# Install the project dependencies using poetry only if they are not already installed
# This creates a .venv directory in the project root
.PHONY: install
install: $(POETRY)
	@echo "Checking dependencies..."
	@if [ ! -d .venv ]; then $(POETRY) install; fi

# Create a virtual environment using poetry and activate it
# This allows you to run commands in the project context
.PHONY: shell
shell: install
	@echo "Creating virtual environment..."
	@$(POETRY) shell

# Run the tests using poetry
# This ensures that the tests are run with the correct dependencies
.PHONY: test
test: install
	@echo "Running tests..."
	@$(POETRY) run pytest

# Clean the project
.PHONY: clean
clean:
	@echo "Cleaning project..."
	@# Add clean commands here if needed

# Install pyenv
.PHONY: install-pyenv
install-pyenv:
	@echo "Installing pyenv..."
	@curl -L https://github.com/pyenv/pyenv-installer/raw/master/bin/pyenv-installer | bash
	@echo 'export PATH="$$HOME/.pyenv/bin:$$PATH"' >> ~/.bashrc
	@source ~/.bashrc

# Install a specific Python version using pyenv
.PHONY: install-pyenv-version
install-pyenv-version: install-pyenv
	@echo "Installing Python version $(PYTHON_VERSION)"
	@pyenv install $(PYTHON_VERSION)
	@pyenv global $(PYTHON_VERSION)0

# Install kubectl
.PHONY: install-kubectl
install-kubectl:
	@echo "Installing kubectl..."
	@curl -fsSLo /usr/share/keyrings/kubernetes-archive-keyring.gpg https://packages.cloud.google.com/apt/doc/apt-key.gpg && \
	echo "deb [signed-by=/usr/share/keyrings/kubernetes-archive-keyring.gpg] https://apt.kubernetes.io/ kubernetes-xenial main" | tee /etc/apt/sources.list.d/kubernetes.list && \
	apt-get update && \
	apt-get install -y kubectl

# Install Helm
.PHONY: install-helm
install-helm: install-kubectl
	@echo "Installing Helm..."
	@curl -sS https://baltocdn.com/helm/signing.asc | apt-key add - && \
	echo "deb https://baltocdn.com/helm/stable/debian/ all main" | tee /etc/apt/sources.list.d/helm-stable-debian.list && \
	# TODO Add Instalation for Mac
	apt-get update && \
	apt-get install -y helm && \
	helm version && \
	kubectl version --client

# Clean Ubuntu environment
.PHONY: clean-ubuntu
clean-ubuntu:
	@echo "Cleaning Ubuntu environment..."
	@bash -c "$$(curl -fsSL https://gist.githubusercontent.com/chethanuk/9410c4bebe2ee6a0c21da13131466031/raw/f090103e21d5ea6d8d521ffe570e497b181ee13c/clean_ubuntu.sh)"

# Install dependencies
.PHONY: install-dependencies
install-dependencies:
	@echo "Installing dependencies..."
	@apt-get update && \
	apt-get install -yq software-properties-common && \
	add-apt-repository -y ppa:git-core/ppa && \
	apt-get update && \
	apt-get install -yq --no-install-recommends zip \
	unzip \
	bash-completion \
	build-essential \
	ninja-build \
	htop \
	iputils-ping \
	jq \
	less \
	locales \
	man-db \
	nano \
	ripgrep \
	sudo \
	stow \
	time \
	emacs-nox \
	vim \
	multitail \
	lsof \
	ssl-cert \
	fish \
	zsh \
	git \
	git-lfs \
	curl \
	podman \
	# LLVM world \
	cmake ninja-build clang \
	# pyenv dependency \
	make build-essential libssl-dev zlib1g-dev \
	libbz2-dev libreadline-dev libsqlite3-dev wget curl \
	libncursesw5-dev xz-utils libxml2-dev libffi-dev liblzma-dev \
	# Lib tools \
	libtool gettext flex bison gdb pkg-config \
	# build tools \
	make autoconf automake fakeroot

# Install sdkman
.PHONY: install-sdkman
install-sdkman:
	@echo "Installing sdkman..."
	@curl -s $(SDKMAN_URL) | bash

# Source sdkman-init.sh
.PHONY: source-sdkman
source-sdkman: install-sdkman
	@echo "Setting up SDKMAN..."
	@bash -c '$(SDKMAN) version'

# Install Java SDK
.PHONY: install-scala
install-scala: source-sdkman
	@echo "Installing Scala SDK - $(SCALA_VERSION)"
	$(SDKMAN) install scala $(SCALA_VERSION)

# Install Java SDK
.PHONY: install-java
install-java: source-sdkman
	@echo "Installing Java SDK..."
	$(SDKMAN) install java $(JAVA_VERSION)

# Install Hadoop
.PHONY: install-hadoop
install-hadoop: install-java install-scala
	@echo "Installing Hadoop..."
	$(SDKMAN) install hadoop $(HADOOP_VERSION)
	# Set Global version
	$(SDKMAN) default hadoop $(HADOOP_VERSION)

# Install Flink
.PHONY: install-flink
install-flink: install-java
	@echo "Installing Flink..."
	$(SDKMAN) install flink $(FLINK_VERSION)
	# Set Global version
	$(SDKMAN) default flink $(FLINK_VERSION)

# Install Spark
.PHONY: install-spark
install-spark: install-hadoop
	@echo "Installing Spark..."
	$(SDKMAN) install spark $(SPARK_VERSION)
	# Set Global version
	$(SDKMAN) default spark $(SPARK_VERSION)

# Clean pyenv
.PHONY: clean-pyenv
clean-pyenv:
	@echo "Cleaning pyenv..."
	@sed -i '/export PATH="$$HOME\/.pyenv\/bin:$$PATH"/d' ~/.bashrc
	@rm -rf ~/.pyenv

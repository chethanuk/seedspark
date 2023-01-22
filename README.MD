# SeedSpark

**SeedSpark** is an open-source is an Extensible PySpark utility package to create production spark pipelines and dev-test them in dev environments or to perform end to end tests. The goal is to enable rapid development of Spark pipelines via PySpark on Spark clusters and locally test the pipeline by using various utilities.

## TODO

1. Move logwrap [on top of loguru] extension out as a seperate package.
1. Add Test containers for [amundsen](https://www.amundsen.io/amundsen/), etc..

## Getting Started

1. Setup [SDKMAN](#setup-sdkman)
1. Setup [Java](#setup-java)
1. Setup [Apache Spark](#setup-apache-spark)
1. Install [Poetry](#poetry)
1. Install Pre-commit and [follow instruction in here](PreCommit.MD)
1. Run [tests locally](#running-tests-locally)

### Setup SDKMAN

SDKMAN is a tool for managing parallel Versions of multiple Software Development Kits on any Unix based system. It provides a convenient command line interface for installing, switching, removing and listing Candidates.
SDKMAN! installs smoothly on Mac OSX, Linux, WSL, Cygwin, etc... Support Bash and ZSH shells.
See documentation on the [SDKMAN! website](https://sdkman.io).

Open your favourite terminal and enter the following:

```shell
$ curl -s https://get.sdkman.io | bash
If the environment needs tweaking for SDKMAN to be installed,
the installer will prompt you accordingly and ask you to restart.

Next, open a new terminal or enter:

$ source "$HOME/.sdkman/bin/sdkman-init.sh"

Lastly, run the following code snippet to ensure that installation succeeded:

$ sdk version
```


### Setup Java

Install Java Now open favourite terminal and enter the following:

```shell
List the AdoptOpenJDK OpenJDK versions
$ sdk list java

To install For Java 11
$ sdk install java 11.0.10.hs-adpt

To install For Java 11
$ sdk install java 8.0.292.hs-adpt
```

### Setup Apache Spark

Install Java Now open favourite terminal and enter the following:

```bash
List the Apache Spark versions:
$ sdk list spark

To install For Spark 3
$ sdk install spark 3.0.2

To install For Spark 3.1
$ sdk install spark 3.0.2
```

### Poetry

Poetry [Commands](https://python-poetry.org/docs/cli/#search)

```bash
poetry install

poetry update

# --tree: List the dependencies as a tree.
# --latest (-l): Show the latest version.
# --outdated (-o): Show the latest version but only for packages that are outdated.
poetry show -o
```

## Running Tests Locally

Take a look at tests in `tests/dataquality` and `tests/jobs`

```bash
$ poetry run pytest
Ran 95 tests in 96.95s
```

NOTE: It's just curated stuff in this repo for personal usage.

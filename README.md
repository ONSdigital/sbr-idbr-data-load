# sbr-control-api
[![license](https://img.shields.io/github/license/mashape/apistatus.svg)]()
[![Dependency Status](https://www.versioneye.com/user/projects/58e23bf2d6c98d00417476cc/badge.svg?style=flat-square)](https://www.versioneye.com/user/projects/58e23bf2d6c98d00417476cc)

### What is this repository?
sbr-control-api is a Play Framework application written predominantly in Scala. This api, sbr-control-api, extends [sbr-api](https://github.com/ONSdigital/sbr-api) and acts as an intermediary that handles requests (from sbr-api) to query HBase. Its current privileges to the data are both read and write.

### Prerequisites

* Java 8 or higher
* SBT ([Download](http://www.scala-sbt.org/))

### Development Setup (MacOS)

To install SBT quickly you can use Homebrew ([Brew](http://brew.sh)):
```shell
brew install sbt
```
Similarly we can get Scala (for development purposes) using brew:
```shell
brew install scala
```
Install HBase locally with brew by using:
```shell
brew install hbase
```

### Running the App

Firstly you must change src/main/scala/spark/SparkSessionManager to be able to run spark session locally rather than via cluster mode

##### HBase

HBase can be started locally by:
```shell
start-hbase.sh
```

Now that HBase has started, we can open the shell and create the namespace and tables.
```sbtshell
hbase shell
create_namespace 'ons'
create 'ons:ENTERPRISE', 'd'
create 'ons:LINKS', 'l'
```

To compile, build and run the application use the following command:
```shell
sbt run
```

The application should have built, created some Hfiles and used them to populate the HBase table. The Hfiles should be located at src/main/resources and should be directories titled enterprises and links.

You can then query the HBase tables to see if they populated correctly, using these commands in the HBase shell:
```sbtshell
scan "ons:LINKS", {LIMIT => 200}
scan "ons:ENTERPRISE", {LIMIT => 200}
```

### License

Copyright © 2017, Office for National Statistics (https://www.ons.gov.uk)

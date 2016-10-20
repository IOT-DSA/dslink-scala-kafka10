#!/usr/bin/env bash
sbt clean compile dist
cp target/universal/dslink-scala-kafka10-0.1.0-SNAPSHOT.zip ../../files/dslink-scala-kafka10.zip

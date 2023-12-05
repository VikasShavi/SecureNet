#!/bin/zsh

/opt/homebrew/bin/kafka-topics --delete --topic logs --bootstrap-server localhost:9092
/opt/homebrew/bin/kafka-topics --delete --topic logsprocessed --bootstrap-server localhost:9092
/opt/homebrew/bin/kafka-topics --delete --topic logslabelled --bootstrap-server localhost:9092
/opt/homebrew/bin/kafka-topics --create --topic logs --bootstrap-server localhost:9092
/opt/homebrew/bin/kafka-topics --create --topic logsprocessed --bootstrap-server localhost:9092
/opt/homebrew/bin/kafka-topics --create --topic logslabelled --bootstrap-server localhost:9092
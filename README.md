## 1. start zookeeper and broker in containers
    cd ../src/main/resources
    docker-compose up

## 2. Start the StreamApp

## 3. Log into the 'broker' container
start the Bash shell session 'bash' in the broker container 'broker' ('exec -it' interactive terminal)
    
    docker exec -it broker bash
    
instantiate the Producer (producing messages to the topic 'source'): 

    kafka-console-producer --broker-list localhost:9092 --topic source

If want to pass a key also (when the logic is working with Key, need to add property w/ key separator and allow for parse = `--property "key.separator=-" --property "parse.key=true`) 
then Producer will accept `key-Value` 

    kafka-console-producer --broker-list localhost:9092 --topic source --property "key.separator=-" --property "parse.key=true"


## 4. Log into the broker container in other terminal window: docker exec -it broker bash
start the Bash shell session 'bash' in the broker container 'broker' ('exec -it' interactive terminal)

    docker exec -it broker bash
    
instantiate the Consumer (to read from the 'destination' topic): 

    kafka-console-consumer --bootstrap-server localhost:9092 --topic destination

All what is passed in the Producer is transformed and read by the Consumer, ex.: test -> test_PROCESSED

# Troubleshooting

List all topics:

    kafka-topics --list --bootstrap-server localhost:9092

Delete single topic name 'source':

    kafka-topics --delete --bootstrap-server localhost:9092 --topic source

Mass delete all user topics:

    kafka-topics --delete --bootstrap-server localhost:9092 --topic '.*'

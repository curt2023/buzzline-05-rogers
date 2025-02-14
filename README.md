# buzzline-05-rogers
In this example project, we incorporate a relational data store. 
We use SQLite, but the example could be altered to work with MySQL, PostgreSQL, or MongoDB.

## VS Code Extensions

- Black Formatter by Microsoft
- Markdown All in One by Yu Zhang
- PowerShell by Microsoft (on Windows Machines)
- Pylance by Microsoft
- Python by Microsoft
- Python Debugger by Microsoft
- Ruff by Astral Software (Linter)
- SQLite Viewer by Florian Klampfer
- WSL by Microsoft (on Windows Machines)

## Task 1. Create and activate a .venv
Follow the instructions in [MANAGE-VENV.md](https://github.com/denisecase/buzzline-01-case/docs/MANAGE-VENV.md) to:
1. Create a .venv
2. Activate .venv
3. Install the required dependencies using requirements.txt.


## Task 2. Start Zookeeper and Kafka (Takes 2 Terminals)

If Zookeeper and Kafka are not already running, you'll need to restart them.
See instructions at [SETUP-KAFKA.md] to:

1. Start Zookeeper Service ([link](https://github.com/denisecase/buzzline-02-case/blob/main/docs/SETUP-KAFKA.md#step-7-start-zookeeper-service-terminal-1))
2. Start Kafka Service ([link](https://github.com/denisecase/buzzline-02-case/blob/main/docs/SETUP-KAFKA.md#step-8-start-kafka-terminal-2))

---

## Task 3. Start a New Streaming Application

This will take two more terminals:

1. One to run the producer which writes messages. 
2. Another to run the consumer which reads messages, processes them, and writes them to a data store. 

### Custom Consumer

The goal of my custom consumer is to use Kafka to take the data presented in the producer_case.py file and process it into an SQLite database. 

This data will be stored as streamed messages table, and a sentiment_per_category table. 

The sentiment_per_category table will be used to feed a visual that will display the average sentiment for each category.

Follow the steps below to execute the code and see if it works.

### Producer (Terminal 3) 

Start the producer to generate the messages. 
The existing producer writes messages to a live data file in the data folder.
If Zookeeper and Kafka services are running, it will try to write them to a Kafka topic as well.
For configuration details, see the .env file. 

In VS Code, open a NEW terminal.
Use the commands below to activate .venv, and start the producer. 

Windows:

```shell
.venv\Scripts\activate
py -m producers.producer_case
```

Mac/Linux:
```zsh
source .venv/bin/activate
python3 -m producers.producer_case
```

The producer will still work if Kafka is not available.

### Consumer (Terminal 4) 

Start an associated consumer. 
You have two options. 
1. Start the consumer that reads from the live data file.
2. OR Start the consumer that reads from the Kafka topic.

In VS Code, open a NEW terminal in your root project folder. 
Use the commands below to activate .venv, and start the consumer. 

Windows:
```shell
.venv\Scripts\activate
py -m consumers.kafka_consumer_rogers
OR
py -m consumers.file_consumer_rogers
```

Mac/Linux:
```zsh
source .venv/bin/activate
python3 -m consumers.kafka_consumer_case
OR
python3 -m consumers.file_consumer_case
```

---

## Summary

This project was a fun an challenging project to work on. My main goal was to use the Kafka streaming data to create a visual showing the average sentiment per category. 
I was able to accomplish this. During execution of this project I also worked towards storing the data in a Mongo DB database. 
I was also able to complete this task but it was significantly harder with the streaming data. I am not exactly sure why. It will take more practice.

All in all i enjoyed this project!!
 

## License
This project is licensed under the MIT License as an example project. 
You are encouraged to fork, copy, explore, and modify the code as you like. 
See the [LICENSE](LICENSE.txt) file for more.

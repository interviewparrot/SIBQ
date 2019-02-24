# SIBQ (Streaming Insert into Big Query)
[Design Document](https://github.com/interviewparrot/SIBQ/wiki)

### Google cloud requirements
1. BigQuery Tables
2. PubSub Topic
3. Google Cloud function

### How to use it
1. Create a message payload which extends BaseMessage class. Ensure that it has all the fields which are specified into the table.
2. Autowire DWPublisher into the class where you want the messages to be published from.
3. Configure a message class to table name. If nothing is configured it would make the className prefixed by dw_


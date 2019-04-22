# DDos Dectector using Kafka and Spark Structured Streaming
This simple program uses Spark Structured Streaming to consume Apache log format strings from a Kafka topic, count the number of requests per IP in 1 minute windows, then write to a MySql table the IPs that exceeds a threshold.
 
## Tested with

sbt version 1.2.8

spark-2.3.3-bin-hadoop2.7

confluent-5.2.1

MySQL version 8.0.11
  
## Instructions

### Setup MySQL
1. Start local MySQL server at default port
	(code assumes port=127.0.0.1:3306 user=root *no password)
2. Create database and table:

	```sql
	CREATE DATABASE IF NOT EXISTS ddos;
	CREATE TABLE IF NOT EXISTS ddos.attacks (
		attack_id INT AUTO_INCREMENT,
		start TIMESTAMP,
		end TIMESTAMP,
		ip varchar(39),
		count INT,
		PRIMARY KEY (attack_id)
	);
	```
### Setup project
1. git clone https://github.com/XiongVang/ddos
2. Fill in required paths for all `.sh` files in `scripts` directory
3. From project root folder:
  	1. Run `./scripts/create-topic.sh`
  	2. Run `./scripts/run-ddos-detector.sh`
  	3. Run `./scripts/load-access-log.sh`
4. Run `SELECT * FROM ddos.attacks;` in MySQL for results.

## Future Improvements
-  Use Kafka connect to ingest logs into `access-log` topic
-  Write to Kudu instead of MySQL
-  Use Machine Learning to detector DDOS attacks
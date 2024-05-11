java -DQUEUE_HOST=localhost \
     -DQUEUE_NAME=book_author \
     -DDB_USER=system \
     -DDB_PWD=SysPassword1 \
     -DJDBC_URL=jdbc:oracle:thin:@localhost:1521/XEPDB1 \
     -jar target/light-rabbitmq-jdbc-consumer-1.0-SNAPSHOT.jar
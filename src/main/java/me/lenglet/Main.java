package me.lenglet;

import com.alibaba.fastjson2.JSON;
import com.rabbitmq.client.*;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger("Consumer");

    public static void main(String[] agrs) throws Exception {
        LOGGER.info("Initializing");

        final var connectionFactory = new ConnectionFactory();
        connectionFactory.setHost(System.getProperty("QUEUE_HOST"));
        connectionFactory.setThreadFactory(new NamedThreadFactory("AMQP-Connection-"));
        //connectionFactory.setPassword();
        final var connection = connectionFactory.newConnection(Executors.newSingleThreadExecutor(new NamedThreadFactory("AMQP-Consumer-")));
        final var channel = connection.createChannel();

        final var queueName = System.getProperty("QUEUE_NAME");
        channel.queueDeclare(queueName, true, false, false, null);

        final var dataSource = new HikariDataSource();
        dataSource.setUsername(System.getProperty("DB_USER"));
        dataSource.setPassword(System.getProperty("DB_PWD"));
        dataSource.setJdbcUrl(System.getProperty("JDBC_URL"));
        dataSource.setAutoCommit(false);
        dataSource.setTransactionIsolation("TRANSACTION_READ_COMMITTED");
        dataSource.setMaximumPoolSize(1);
        dataSource.setPoolName("hikari-pool");

        final var dsProperties = new Properties();
        dsProperties.setProperty("oracle.jdbc.implicitStatementCacheSize", "5");
        dataSource.setDataSourceProperties(dsProperties);

        final var consumer = new Consumer(channel, dataSource);
        channel.basicConsume(queueName, false, consumer, consumerTag -> {
        });

        LOGGER.info("Starting to consume");
    }

    private static class NamedThreadFactory implements ThreadFactory {

        private final AtomicInteger threadCount = new AtomicInteger(1);
        private final String prefix;

        public NamedThreadFactory(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, this.prefix + this.threadCount.getAndIncrement());
        }
    }

    private static class Consumer implements DeliverCallback {

        private final Channel channel;
        private final DataSource dataSource;

        private Consumer(
                Channel channel,
                DataSource dataSource
        ) {
            this.channel = channel;
            this.dataSource = dataSource;
        }

        @Override
        public void handle(String consumerTag, Delivery message) throws IOException {
            final var deliveryTag = message.getEnvelope().getDeliveryTag();

            try (
                    final var dbConnection = this.dataSource.getConnection();
                    final var updateBookStatement = dbConnection.prepareStatement("""
                            UPDATE books
                            SET author = ?
                            WHERE id = ?
                            """);
            ) {
                final var event = JSON.parseObject(message.getBody(), Event.class);
                LOGGER.info("Consuming {}", event);

                updateBookStatement.setString(1, event.newAuthorName());
                updateBookStatement.setLong(2, event.bookId());
                final var result = updateBookStatement.executeUpdate();
                if (result == 0) {
                    throw new SQLException("No row updated");
                }

                dbConnection.commit();

                this.channel.basicAck(deliveryTag, false);

            } catch (SQLException e) {
                LOGGER.error("Exception occurred", e);
                this.channel.basicReject(deliveryTag, !message.getEnvelope().isRedeliver());
            }
        }
    }

    public record Event(String newAuthorName, Long bookId) {
    }
}
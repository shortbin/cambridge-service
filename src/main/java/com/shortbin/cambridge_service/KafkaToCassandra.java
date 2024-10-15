package com.shortbin.cambridge_service;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.mapping.Mapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.shortbin.cambridge_service.model.Click;
import com.shortbin.cambridge_service.model.ClickEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;

import java.net.InetSocketAddress;


public class KafkaToCassandra {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.enableCheckpointing(5000L);
//        env.setParallelism(4);

        String brokers = "ampere_2:9092";
        ObjectMapper objectMapper = new ObjectMapper();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics("click_events")
                .setGroupId("clickstream_consumers")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> stream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        );

        DataStream<Click> enhancedStream = stream
                .flatMap((FlatMapFunction<String, Click>) (value, out) -> {
                    ClickEvent clickEvent = objectMapper.readValue(value, ClickEvent.class);
                    out.collect(new Click(
                            clickEvent.getShortID(),
                            new java.util.Date(),
                            java.util.UUID.randomUUID(),
                            clickEvent.getShortCreatedBy(),
                            clickEvent.getLongUrl(),
                            "IN",
                            clickEvent.getIpAddress(),
                            clickEvent.getUserAgent(),
                            clickEvent.getReferer(),
//                            clickEvent.getXForwardedFor(),
                            clickEvent.getRequestHost()
                    ));
                })
                // using returns to avoid lambda serialization (explicitly specifying type)
                .returns(Click.class);

        ClusterBuilder cassClusterBuilder = new ClusterBuilder() {
            @Override
            protected Cluster buildCluster(Cluster.Builder builder) {
                return builder.addContactPointsWithPorts(new InetSocketAddress("ampere_2", 9042))
                        .withQueryOptions(new QueryOptions())
                        .withSocketOptions(new SocketOptions()
                                .setConnectTimeoutMillis(50000)
                                .setReadTimeoutMillis(50000))
                        .withCredentials("cassandra", "cassandra")
                        .build();
            }
        };

        CassandraSink.addSink(enhancedStream)
                .setClusterBuilder(cassClusterBuilder)
                .setMapperOptions(() -> new Mapper.Option[]{Mapper.Option.saveNullFields(true)})
                .build()
                .name("Cassandra Sink");


        env.execute("KafkaToCassandra");
    }
}
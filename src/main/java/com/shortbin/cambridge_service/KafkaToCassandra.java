package com.shortbin.cambridge_service;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.mapping.Mapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.shortbin.cambridge_service.model.Click;
import com.shortbin.cambridge_service.model.ClickAggregate;
import com.shortbin.cambridge_service.model.ClickEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;

import java.net.InetSocketAddress;
import java.sql.PreparedStatement;


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

        DataStream<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

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

        DataStream<ClickAggregate> aggregatedCounts = enhancedStream
                .keyBy(Click::getShortID)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .apply((WindowFunction<Click, ClickAggregate, String, TimeWindow>) (shortId, window, input, out) -> {
                    int count = 0;
//                    Map<String, Integer> countryCounter = new HashMap<>();
//                    Map<String, Integer> referrerCounter = new HashMap<>();

                    for (Click event : input) {
                        count++;
//                        countryCounter.merge(event.getCountry(), 1, Integer::sum);
//                        referrerCounter.merge(event.getReferer(), 1, Integer::sum);
                    }
                    out.collect(new ClickAggregate(shortId, count));
                })
                .returns(ClickAggregate.class)
                .name("Aggregated Clicks");

        aggregatedCounts.addSink(JdbcSink.sink(
                        "INSERT INTO click_analytics (short_id, count) VALUES (?, ?) " +
                                "ON CONFLICT (short_id) DO UPDATE SET " +
                                "count = click_analytics.count + EXCLUDED.count;",
                        (PreparedStatement ps, ClickAggregate clickAggregate) -> {
                            ps.setString(1, clickAggregate.getShortId());
                            ps.setInt(2, clickAggregate.getCount());
//                            ps.setString(3, clickAggregate.getCountriesJson());
//                            ps.setString(4, clickAggregate.getReferrersJson());
                            System.out.println("Inserting into click_analytics: " + clickAggregate.getShortId() + " " + clickAggregate.getCount());
                        },
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:postgresql://ampere_2:5432/shortbin_dev")
                                .withDriverName("org.postgresql.Driver")
                                .withUsername("dev_user")
                                .withPassword("jw8s0F4B5a")
                                .build()
                ))
                .name("JDBC Sink");

        env.execute("KafkaToCassandra");
    }
}
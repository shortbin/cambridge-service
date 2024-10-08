package com.shortbin.cambridge_service;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.mapping.Mapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;

import java.net.InetSocketAddress;


public class KafkaToKafka {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.enableCheckpointing(5000L);
//        env.setParallelism(4);

        String brokers = "ampere_2:9092";
        String sinkTopic = "click-sink-test";

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics("click_events")
                .setGroupId("clickstream_consumers")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(sinkTopic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
//                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .build();

        stream.sinkTo(sink);

        DataStream<WordCount> result = stream
                .flatMap((FlatMapFunction<String, WordCount>) (value, out) -> {
                    // normalize and split the line
                    String[] words = value.toLowerCase().split("\\s");

                    // emit the pairs
                    for (String word : words) {
                        if (!word.isEmpty()) {
                            //Do not accept empty word, since word is defined as primary key in C* table
                            out.collect(new WordCount(word, 1L));
                        }
                    }
                })
                .keyBy(WordCount::getWord)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<WordCount>() {
                    @Override
                    public WordCount reduce(WordCount a, WordCount b) {
                        return new WordCount(a.getWord(), a.getCount() + b.getCount());
                    }
                });

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

        CassandraSink.addSink(result)
                .setClusterBuilder(cassClusterBuilder)
                .setMapperOptions(() -> new Mapper.Option[]{Mapper.Option.saveNullFields(true)})
                .build()
                .name("Cassandra Sink");

        env.execute("KafkaToKafka");
    }
}

package com.hnust.kafkastreaming;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.Properties;

//这个程序会将topic为“log”的信息流获取来做处理，并以“recommender”为新的topic转发出去。
public class Application {

    public static void main(String[] args) {
        String brokers = "192.168.177.102:9092";
        String zookeepers = "192.168.177.102:2181";

        // 定义输入和输出的topic
        String from = "log";
        String to = "recommender";

        // 定义kafka streaming的配置
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "logFilter");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, zookeepers);

        StreamsConfig config = new StreamsConfig(props);

        // 拓扑建构器
        TopologyBuilder builder = new TopologyBuilder();
        // 定义流处理的拓扑结构
        builder.addSource("SOURCE", from)
                .addProcessor("PROCESSOR",() -> new LogProcessor(), "SOURCE")
                .addSink("SINK",to, "PROCESSOR");

        KafkaStreams streams = new KafkaStreams(builder, config);

        streams.start();
    }

}

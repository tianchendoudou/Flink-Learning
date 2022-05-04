package com.imooc.flink.source;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class SourceAPP {

        public static void main(String[] args) throws Exception {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            env.setParallelism(3);
            // 设置开启checkpoint
            // 每 60s 做一次 checkpoint
            env.enableCheckpointing(20 * 1000);

            // checkpoint 语义设置为 EXACTLY_ONCE，这是默认语义
            env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

            // 两次 checkpoint 的间隔时间至少为 1 s，默认是 0，立即进行下一次 checkpoint
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);

            // checkpoint 必须在 60s 内结束，否则被丢弃，默认是 10 分钟
            env.getCheckpointConfig().setCheckpointTimeout(60000);

            // 同一时间只能允许有一个 checkpoint
            env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

            // 最多允许 checkpoint 失败 3 次
            env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);

            // 当 Flink 任务取消时，保留外部保存的 checkpoint 信息
            env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

            // 当有较新的 Savepoint 时，作业也会从 Checkpoint 处恢复
            //env.getCheckpointConfig().setPreferCheckpointForRecovery(true);

            // 允许实验性的功能：非对齐的 checkpoint，以提升性能
            env.getCheckpointConfig().enableUnalignedCheckpoints();

            KafkaSource<String> source = KafkaSource.<String>builder()
                    .setBootstrapServers("localhost:9092")
                    .setTopics("flink")
                    .setGroupId("my-group")
                    .setStartingOffsets(OffsetsInitializer.earliest())
                    .setValueOnlyDeserializer(new SimpleStringSchema())
                    .build();


            DataStreamSource<String> kafka_source = env.fromSource(source,
                    WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)),
                    "Kafka Source");

            System.out.println(kafka_source.getParallelism());
            System.out.println(kafka_source);

            kafka_source.map(m -> m)
                    .print("local");
            kafka_source.print();

            JobExecutionResult execute = env.execute();
        }
    }





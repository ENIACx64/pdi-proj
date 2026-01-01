/*
 * File: AnomalyDetectionJob.java
 * Author: Jindrich Vodak (xvodak06)
 * Date: 31. 12. 2025
 */

package pipeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

public class AnomalyDetectionJob {

    public static void main(String[] args) throws Exception {

        // Parameters
        ParameterTool params = ParameterTool.fromArgs(args);

        String sourceHost = params.get("source-host", "localhost");
        int sourcePort = params.getInt("source-port", 9999);

        int windowSeconds = params.getInt("window-seconds", 10);
        int latenessSeconds = params.getInt("lateness-seconds", 5);
        int watermarkSeconds = params.getInt("watermark-seconds", 2);

        int synThreshold = params.getInt("syn-threshold", 100);
        int scanThreshold = params.getInt("scan-threshold", 20);

        // Environment
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // Source: socket
        DataStream<String> raw =
                env.socketTextStream(sourceHost, sourcePort);

        ObjectMapper mapper = new ObjectMapper();

        DataStream<NetworkEvent> events =
                raw.map((MapFunction<String, NetworkEvent>) value ->
                        mapper.readValue(value, NetworkEvent.class));

        // Watermark config
        WatermarkStrategy<NetworkEvent> wm =
                WatermarkStrategy
                        .<NetworkEvent>forBoundedOutOfOrderness(
                                Duration.ofSeconds(watermarkSeconds))
                        .withTimestampAssigner(
                                (event, ts) -> event.eventTime
                        );

        DataStream<NetworkEvent> timed =
                events.assignTimestampsAndWatermarks(wm);

        // SYN flood detection
        timed
                .filter(e -> e.syn)
                .keyBy(e -> e.dstIp + ":" + e.dstPort)
                .window(TumblingEventTimeWindows.of(Time.seconds(windowSeconds)))
                .allowedLateness(Time.seconds(latenessSeconds))
                .process(new ProcessWindowFunction<NetworkEvent, String, String, TimeWindow>() {

                    @Override
                    public void process(
                            String key,
                            Context context,
                            Iterable<NetworkEvent> elements,
                            Collector<String> out) {

                        int count = 0;
                        for (NetworkEvent e : elements) {
                            count++;
                        }

                        if (count >= synThreshold) {
                            out.collect(
                                    "ALERT SYN_FLOOD target=" + key +
                                            " synCount=" + count +
                                            " window=[" +
                                            context.window().getStart() + "," +
                                            context.window().getEnd() + "]"
                            );
                        }
                    }
                })
                .print();

        // Fast port scanning detection
        timed
                .filter(e -> e.syn)
                .keyBy(e -> e.srcIp + "->" + e.dstIp)
                .window(TumblingEventTimeWindows.of(Time.seconds(windowSeconds)))
                .allowedLateness(Time.seconds(latenessSeconds))
                .process(new ProcessWindowFunction<NetworkEvent, String, String, TimeWindow>() {

                    @Override
                    public void process(
                            String key,
                            Context context,
                            Iterable<NetworkEvent> elements,
                            Collector<String> out) {

                        Set<Integer> ports = new HashSet<>();
                        for (NetworkEvent e : elements) {
                            ports.add(e.dstPort);
                        }

                        if (ports.size() >= scanThreshold) {
                            out.collect(
                                    "ALERT PORT_SCAN attacker=" + key +
                                            " uniquePorts=" + ports.size() +
                                            " window=[" +
                                            context.window().getStart() + "," +
                                            context.window().getEnd() + "]"
                            );
                        }
                    }
                })
                .print();

        env.execute("Java Anomaly Detection Pipeline");
    }
}

package pipeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
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

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // -------------------------
        // Source: socket (Python generator)
        // -------------------------
        DataStream<String> raw =
                env.socketTextStream("localhost", 9999);

        ObjectMapper mapper = new ObjectMapper();

        DataStream<NetworkEvent> events =
                raw.map((MapFunction<String, NetworkEvent>) value ->
                        mapper.readValue(value, NetworkEvent.class));

        // -------------------------
        // Event time + watermarks
        // -------------------------
        WatermarkStrategy<NetworkEvent> wm =
                WatermarkStrategy
                        .<NetworkEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(
                                (event, ts) -> event.eventTime
                        );

        DataStream<NetworkEvent> timed =
                events.assignTimestampsAndWatermarks(wm);

        // =========================================================
        // 1) SYN FLOOD DETECTION
        // =========================================================
        timed
                .filter(e -> e.syn)
                .keyBy(e -> e.dstIp + ":" + e.dstPort)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.seconds(5))
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

                        if (count >= 100) {
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

        // =========================================================
        // 2) PORT SCAN DETECTION
        // =========================================================
        timed
                .filter(e -> e.syn)
                .keyBy(e -> e.srcIp + "->" + e.dstIp)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.seconds(5))
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

                        if (ports.size() >= 20) {
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

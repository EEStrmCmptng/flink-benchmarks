package flink.queries;


import flink.sinks.DummyLatencyCountingSink;
import flink.sources.BidSourceFunction;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class Query5 {

    private static final Logger logger = LoggerFactory.getLogger(Query5.class);

    public static void main(String[] args) throws Exception {
        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);
        final float exchangeRate = params.getFloat("exchange-rate", 0.82F);
        String ratelist = params.getRequired("ratelist");
        long windowlength = params.getLong("windowlength", 60);

        //  --ratelist 5000_300_1000_300
        int[] numbers = Arrays.stream(ratelist.split("_"))
                .mapToInt(Integer::parseInt)
                .toArray();
        System.out.println(Arrays.toString(numbers));
        List<List<Integer>> rates = new ArrayList<>();

        for (int i = 0; i < numbers.length - 1; i += 2) {
            rates.add(Arrays.asList(numbers[i], numbers[i + 1]));
        }

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000);

        if (params.getBoolean("checkpointingenabled", false)) {
            long checkpointingInterval = params.getLong("checkpointinginterval");
            CheckpointingMode checkpointingMode = CheckpointingMode.valueOf(params.get("checkpointingmode", "EXACTLY_ONCE"));
            env.enableCheckpointing(checkpointingInterval, checkpointingMode);
            env.getCheckpointConfig().enableUnalignedCheckpoints();
            env.getCheckpointConfig().setCheckpointTimeout(100000);
        }

        // env.disableOperatorChaining();

        // enable latency tracking
        env.getConfig().setLatencyTrackingInterval(5000);

        //final int srcRate = params.getInt("srcRate", 100000);

        DataStream<Bid> bids = env.addSource(new BidSourceFunction(rates))
                .setParallelism(params.getInt("p-bid-source", 1))
                .name("SourceBid")
                .assignTimestampsAndWatermarks(new TimestampAssigner())
                .name("TimestampAssigner");
              //  .setParallelism(params.getInt("p-watermark", 1));
              //  .slotSharingGroup("watermark");

        // SELECT B1.auction, count(*) AS num
        // FROM Bid [RANGE 60 MINUTE SLIDE 1 MINUTE] B1
        // GROUP BY B1.auction
        DataStream<Tuple2<Long, Long>> windowed = bids.keyBy((KeySelector<Bid, Long>) bid -> bid.auction)
                //.window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(1)))
                .timeWindow(Time.minutes(windowlength), Time.minutes(1))
                .aggregate(new CountBids())
                .name("SlidingWindow")
                .setParallelism(params.getInt("p-window", 1))
                .slotSharingGroup("window");

        GenericTypeInfo<Object> objectTypeInfo = new GenericTypeInfo<>(Object.class);
        windowed.transform("DummyLatencySink", objectTypeInfo, new DummyLatencyCountingSink<>(logger))
                .setParallelism(params.getInt("p-sink", 1))
                .slotSharingGroup("sink");

        // execute program
        env.execute("Nexmark Query5");
    }

    private static final class TimestampAssigner implements AssignerWithPeriodicWatermarks<Bid> {
        private long maxTimestamp = Long.MIN_VALUE;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(maxTimestamp);
        }

        @Override
        public long extractTimestamp(Bid element, long previousElementTimestamp) {
            maxTimestamp = Math.max(maxTimestamp, element.dateTime);
            return element.dateTime;
        }
    }

    private static final class CountBids implements AggregateFunction<Bid, Long, Tuple2<Long, Long>> {

        private long auction = 0L;

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Bid value, Long accumulator) {
            auction = value.auction;
            return accumulator + 1;
        }

        @Override
        public Tuple2<Long, Long> getResult(Long accumulator) {
            return new Tuple2<>(auction, accumulator);
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }
}


package flink.queries;

import flink.sources.ImagesDataGeneratorSource;
import flink.functions.ImgTransformFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import flink.sinks.DummyLatencyCountingSink;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.TimeCharacteristic;
import javax.annotation.Nullable;

import java.util.*;

public class Imgproc{
    private static final Logger logger  = LoggerFactory.getLogger(Imgproc.class);

    public static void main(String[] args) throws Exception {
        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);
        String ratelist = params.get("ratelist", "620_900");
        final long bufftimeout = params.getLong("bufferTimeout", -1);
        final int imgSize = params.getInt("imgSize", 128);
        final int cmpSize = params.getInt("cmpSize", 128);
        final int batchSize = params.getInt("batchSize", 1);
        final int psrc = params.getInt("psrc", 1);
        final int pmap = params.getInt("pmap", 2);
        final int psink = params.getInt("psink", 1);
        final int blurstep = params.getInt("blurstep", 64);
        final int warmUpRequestsNum = params.getInt("warmUpRequestsNum", 0);
        // --ratelist 620_900000 --bufferTimeout -1 --imgSize 128 --batchSize 1 --blurstep 2 --psrc 1 --ptra 2

        int[] numbers = Arrays.stream(ratelist.split("_"))
                .mapToInt(Integer::parseInt)
                .toArray();
        System.out.println(Arrays.toString(numbers));
        List<List<Integer>> rates = new ArrayList<>();
        // The internal list will be [rate, time in ms]
        for (int i = 0; i < numbers.length - 1; i += 2) {
            rates.add(Arrays.asList(numbers[i], numbers[i + 1]));
        }
        final int inputRatePerProducer = rates.get(0).get(0);
        final int experimentTimeInSeconds = rates.get(0).get(1);

        Configuration configuration = new Configuration();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setBufferTimeout(bufftimeout);

//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.getConfig().setAutoWatermarkInterval(1000);

        env.disableOperatorChaining();

        // enable latency tracking
        env.getConfig().setLatencyTrackingInterval(5000);

        // Read the input data stream
        System.out.println("psrc: " + psrc);
        System.out.println("inputRatePerProducer: " + inputRatePerProducer);
        System.out.println("experimentTimeInSeconds: " + experimentTimeInSeconds);
        System.out.println("batchSize: " + batchSize);
        System.out.println("blurstep: " + blurstep);
        System.out.println("imgSize: " + imgSize);
        System.out.println("cmpSize: " + cmpSize);

        DataStream<Tuple2<ArrayList<ArrayList<Float>>, Long>> batches = env
                .addSource(
                        new ImagesDataGeneratorSource(batchSize, experimentTimeInSeconds, warmUpRequestsNum, inputRatePerProducer, imgSize, ratelist))
                .setParallelism(psrc).slotSharingGroup("psrc")
                .name("Bids Source").uid("Bids-Source");
                //.assignTimestampsAndWatermarks(new TimestampAssigner())
                //.name("TimestampAssigner");

        SingleOutputStreamOperator<Tuple2<ArrayList<ArrayList<Float>>, Long>> transformed = batches
                .process(new ImgTransformFunction(blurstep, imgSize, cmpSize))
                .setParallelism(pmap).slotSharingGroup("pmap").name("Mapper");


        GenericTypeInfo<Object> objectTypeInfo = new GenericTypeInfo<>(Object.class);
        transformed.transform("DummyLatencySink", objectTypeInfo, new DummyLatencyCountingSink<>(logger))
                .setParallelism(1)
                .name("Latency Sink")
                .uid("Latency-Sink")
                .setParallelism(psink).slotSharingGroup("psink");

        System.out.println(env.getExecutionPlan());
        env.execute("Imgproc");
    }

private static final class TimestampAssigner implements AssignerWithPeriodicWatermarks<Tuple2<ArrayList<ArrayList<Float>>, Long>> {
        private long maxTimestamp = Long.MIN_VALUE;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(maxTimestamp);
        }

        @Override
        public long extractTimestamp(Tuple2<ArrayList<ArrayList<Float>>, Long> element, long previousElementTimestamp) {
            maxTimestamp = Math.max(maxTimestamp, element.f1);
            return element.f1;
        }
    }
}


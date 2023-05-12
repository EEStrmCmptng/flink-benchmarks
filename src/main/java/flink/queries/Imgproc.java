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
        final int inputRatePerProducer = params.getInt("inputRate", 620);
        final long bufftimeout = params.getLong("bufferTimeout", 10L);
        final int imgSize = params.getInt("imgSize", 128);
        final int batchSize = params.getInt("batchSize", 1);
        final int experimentTimeInSeconds = params.getInt("experimentTimeInSeconds", 1800);
        final int psrc = params.getInt("psrc", 1);
        final int ptra = params.getInt("ptra", 2);
        final int blurstep = params.getInt("blurstep", 2);
        final int warmUpRequestsNum = params.getInt("warmUpRequestsNum", 0);
        // --inputRate 640 --imgSize 128 --batchSize 1 --blurstep 2 --psrc 3 --ptra 6

        Configuration configuration = new Configuration();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setBufferTimeout(bufftimeout);

//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.getConfig().setAutoWatermarkInterval(1000);

        env.disableOperatorChaining();

        // enable latency tracking
        env.getConfig().setLatencyTrackingInterval(5000);

        // Read the input data stream
        System.out.println("Input producers: " + psrc);
        System.out.println("Input rate per producer: " + inputRatePerProducer);

        DataStream<Tuple2<ArrayList<ArrayList<Float>>, Long>> batches = env
                .addSource(
                        new ImagesDataGeneratorSource(batchSize, experimentTimeInSeconds, warmUpRequestsNum, inputRatePerProducer, imgSize))
                .setParallelism(psrc).slotSharingGroup("src")
                .name("Bids Source").uid("Bids-Source");
                //.assignTimestampsAndWatermarks(new TimestampAssigner())
                //.name("TimestampAssigner");

        SingleOutputStreamOperator<Tuple2<ArrayList<ArrayList<Float>>, Long>> transformed = batches
                .process(new ImgTransformFunction(blurstep, imgSize))
                .setParallelism(ptra).slotSharingGroup("tra").name("Mapper");


        GenericTypeInfo<Object> objectTypeInfo = new GenericTypeInfo<>(Object.class);
        transformed.transform("DummyLatencySink", objectTypeInfo, new DummyLatencyCountingSink<>(logger))
                .setParallelism(1)
                .name("Latency Sink")
                .uid("Latency-Sink")
                .slotSharingGroup("sink");

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


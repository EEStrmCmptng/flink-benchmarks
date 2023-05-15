package flink.sources;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ImagesDataGeneratorSource extends RichParallelSourceFunction<Tuple2<ArrayList<ArrayList<Float>>, Long>> {
    private static final Logger LOG = LoggerFactory.getLogger(ImagesDataGeneratorSource.class);
    private final ImagesDataGenerator generator;
    private volatile boolean running = true;
    private long eventsCountSoFar = 0;
    private long experimentTimeInSeconds = 0;
    private final int inputRate;

    public ImagesDataGeneratorSource(int batchSize, int experimentTimeInSeconds, int warmupRequestsNum, int inputRate, int imgSize) {
        this.generator = new ImagesDataGenerator(batchSize, experimentTimeInSeconds, warmupRequestsNum, imgSize);
        this.inputRate=inputRate;
        this.experimentTimeInSeconds=experimentTimeInSeconds;
    }

    @Override
    public void run(SourceContext<Tuple2<ArrayList<ArrayList<Float>>, Long>> sourceContext) throws Exception {

        Date finishTime = new Date();

        while (running && new Date().getTime() - finishTime.getTime() < this.experimentTimeInSeconds*1000) {
            long emitStartTime = System.currentTimeMillis();

            for (int i = 0; i < this.inputRate; i++) {
                sourceContext.collect(this.generator.next());
                eventsCountSoFar++;
            }

            // Sleep for the rest of time slice if needed
            long emitTime = System.currentTimeMillis() - emitStartTime;
            if (emitTime < 1000) {
                Thread.sleep(1000 - emitTime);
            }
        }

    }

    @Override
    public void cancel() {
        running = false;
    }
}
package flink.sources;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class ImagesDataGeneratorSource extends RichParallelSourceFunction<Tuple2<ArrayList<ArrayList<Float>>, Long>> implements CheckpointedFunction {
    private final ImagesDataGenerator generator;
    private volatile boolean running = true;
    private Long eventsCountSoFar;

    private List<List<Integer>> updatedRates = null;
    private List<List<Integer>> srcRates = new ArrayList<>();
    private boolean dynamicRate = false;
    private int experimentTimeInSeconds = 0;       // total running duration
    private ListState<List<List<Integer>>> stateUpdatedRatelist;
    private ListState<Long> stateEventsCountSoFar;
    private Long deployTime;    // timestamp that the job is deployed in each run

    public ImagesDataGeneratorSource(int batchSize, int experimentTimeInSeconds, int warmupRequestsNum, int inputRate, int imgSize, String ratelist) {
        if(ratelist != "0_0") {
            int[] numbers = Arrays.stream(ratelist.split("_")).mapToInt(Integer::parseInt).toArray();
            this.dynamicRate=true;
            for (int i = 0; i < numbers.length - 1; i += 2) {
                this.srcRates.add(Arrays.asList(numbers[i], numbers[i + 1]));
            }
        }
        else{
            this.srcRates.add(Arrays.asList(inputRate, experimentTimeInSeconds));
        }
        this.experimentTimeInSeconds=sumTime();

        System.out.println("src rate:  "+this.srcRates);
        System.out.println("total duration:  "+this.experimentTimeInSeconds);
        this.generator = new ImagesDataGenerator(batchSize, this.experimentTimeInSeconds, warmupRequestsNum, imgSize);
    }

    @Override
    public void run(SourceContext<Tuple2<ArrayList<ArrayList<Float>>, Long>> sourceContext) throws Exception {
        for (List<Integer> rate : this.updatedRates) {
            int currentRate = rate.get(0);
            int currentDuration = rate.get(1);
            Date finishTime = new Date();

            System.out.println(rate.toString() + " start at " + new Long(System.currentTimeMillis()).toString());

            ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
            
            final Runnable beeper1 = new Runnable() {
                public void run() {
                    sourceContext.collect(ImagesDataGeneratorSource.this.generator.next());
                    eventsCountSoFar++;
                }
            };
            
            long eventDelay=1000*1000*1000/currentRate;
            final ScheduledFuture<?> beeperHandle = scheduler.scheduleAtFixedRate(beeper1, 0, eventDelay, TimeUnit.NANOSECONDS);
            scheduler.schedule(new Runnable() {
                public void run() {
                    beeperHandle.cancel(true); 
                    scheduler.shutdown();
                }
            }, currentDuration, TimeUnit.SECONDS);
            
            Thread.sleep(currentDuration*1000);    //Ms

            System.out.println(rate.toString() + "  done at " + new Long(System.currentTimeMillis()).toString());
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    private int sumTime() {
        int result = 0;
        assert this.srcRates != null;
        for (List<Integer> rate : this.srcRates) {
            result += rate.get(1);
        }
        return result;
    }

    private void updateRates() {
        Long curTime = (System.currentTimeMillis() - deployTime) / 1000;    // convert to seconds
        System.out.println("updateRates:  curTime(s)="+curTime);
        int toRemove = 0;
        for (int i = 0; i < this.updatedRates.size(); i++) {
            int currentDuration = this.updatedRates.get(i).get(1);
            if (curTime > currentDuration) {
                curTime -= currentDuration;
                toRemove++;
            } else {
                List<Integer> rateToChange = this.updatedRates.get(i);
                rateToChange.set(1, (int) (rateToChange.get(1) - curTime));
                this.updatedRates.set(i, rateToChange);
                break; // Do this only once
            }
        }
        if (toRemove > 0) {
            this.updatedRates.subList(0, toRemove).clear();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        updateRates();
        stateEventsCountSoFar.clear();
        stateEventsCountSoFar.add(eventsCountSoFar);
        stateUpdatedRatelist.clear();
        stateUpdatedRatelist.add(this.updatedRates);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        stateEventsCountSoFar = context.getOperatorStateStore()
                .getListState(new ListStateDescriptor<>("stateEventsCountSoFar",  Long.class));
        stateUpdatedRatelist = context.getOperatorStateStore()
                .getListState(new ListStateDescriptor<>("stateUpdatedRatelist", new TypeHint<List<List<Integer>>>() {}.getTypeInfo()));

        for (List<List<Integer>> l : stateUpdatedRatelist.get()) {
            this.updatedRates = l;
        }
        if (this.updatedRates == null) {
            this.updatedRates = this.srcRates.stream()
                    .map(ArrayList::new)
                    .collect(Collectors.toList());
            System.out.println("ImagesDataGeneratorSource:  Initialized this.updatedRates = "+this.updatedRates.toString());
        } else {
            System.out.println("ImagesDataGeneratorSource:  Restored this.updatedRates = "+this.updatedRates.toString());
        }

        for (Long l : stateEventsCountSoFar.get()) {
            eventsCountSoFar = l;
        }
        if (eventsCountSoFar == null) {
            eventsCountSoFar=0L;
            System.out.println("ImagesDataGeneratorSource:  Initialized eventsCountSoFar value state.. eventsCountSoFar="+eventsCountSoFar);
        } else {
            System.out.println("ImagesDataGeneratorSource:  Value state recovered.. eventsCountSoFar="+eventsCountSoFar);
        }

        deployTime=System.currentTimeMillis();
    }
}
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package flink.sources;

import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.sources.generator.GeneratorConfig;
import org.apache.beam.sdk.nexmark.sources.generator.model.AuctionGenerator;
import org.apache.beam.sdk.nexmark.sources.generator.model.BidGenerator;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

/**
 * A ParallelSourceFunction that generates Nexmark Person data
 */
public class AuctionSourceFunction extends RichParallelSourceFunction<Auction> implements CheckpointedFunction {

    private static final Logger LOG = LoggerFactory.getLogger(AuctionSourceFunction.class);
    private final GeneratorConfig config = new GeneratorConfig(NexmarkConfiguration.DEFAULT, 1, 1000L, 0, 1);
    private final List<List<Integer>> rates;
    Integer totalTime;
    private volatile boolean running = true;
    private long eventsCountSoFar = 0;
    private Long time;
    private ListState<Long> stateAuction;

    public AuctionSourceFunction(List<List<Integer>> srcRate) {
        this.rates = srcRate;
        totalTime = sumTime();
    }

    private void updateRates() {
        Long curTime = System.currentTimeMillis() - time;
        int toRemove = 0;
        for (int i = 0; i < rates.size(); i++) {
            int currentDuration = this.rates.get(i).get(1);
            if (curTime > currentDuration) {
                curTime -= currentDuration;
                toRemove++;
            } else {
                List<Integer> rateToChange = this.rates.get(i);
                rateToChange.set(1, (int) (rateToChange.get(1) - curTime));
                this.rates.set(i, rateToChange);
                break; // Do this only once
            }
        }
        if (toRemove > 0) {
            this.rates.subList(0, toRemove).clear();
        }
    }

    private int sumTime() {
        int result = 0;
        assert this.rates != null;
        for (List<Integer> rate : this.rates) {
            result += rate.get(1);
        }
        return result;
    }

    @Override
    public void run(SourceContext<Auction> ctx) throws Exception {
        for (List<Integer> rate : this.rates) {
            int currentRate = rate.get(0);
            int currentDuration = rate.get(1);    //sec
            Date finishTime = new Date();
            System.out.println(rate.toString() + " start at " + new Long(System.currentTimeMillis()).toString());

            ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
            
            final Runnable beeper1 = new Runnable() {
                public void run() {
                    long nextId = nextId();
                    Random rnd = new Random(nextId);
                    // When, in event time, we should generate the event. Monotonic.
                    long eventTimestamp = config.timestampAndInterEventDelayUsForEvent(config.nextEventNumber(eventsCountSoFar)).getKey();
                    ctx.collect(AuctionGenerator.nextAuction(eventsCountSoFar, nextId, rnd, eventTimestamp, config));
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

    private long nextId() {
        return config.firstEventId + config.nextAdjustedEventNumber(eventsCountSoFar);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        stateAuction = context.getOperatorStateStore().getListState(new ListStateDescriptor<>(
                "stateAuction",
                Long.class));
        for (Long l : stateAuction.get()) {
            time = l;
        }
        if (time == null) {
            time = System.currentTimeMillis();
            stateAuction.clear();
            stateAuction.add(time);
            LOG.info("Initialized time value state");
        } else {
            LOG.info("Value state recovered");
        }
        updateRates();
        LOG.info("Updated rates, {}", rates.toString());
    }
}
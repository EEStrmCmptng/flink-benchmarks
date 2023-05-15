package flink.functions;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.*;

public class ImgTransformFunction
        extends
        ProcessFunction<Tuple2<ArrayList<ArrayList<Float>>, Long>, Tuple2<ArrayList<ArrayList<Float>>, Long>> {
    private final int blurstep;
    private final int imgSize;
    private Integer InputSize;
    private Integer OutputSize;

    public int getidx(int x, int y, int imgsize){
        int res=x*imgsize+y;
        if(res<0)
            res+=imgsize*imgsize;
        res=res%(imgsize*imgsize);
        return(res);
    }

    public ImgTransformFunction(int blurstep, int imgSize, int cmpSize) {
        this.blurstep=blurstep;
        this.imgSize=imgSize;
        this.InputSize = imgSize;
        this.OutputSize = cmpSize;
    }

    public int get1didx(int x, int y, int imgsize){
        int res=x*imgsize+y;
        if(res<0)
            res+=imgsize*imgsize;
        res=res%(imgsize*imgsize);
        return(res);
    }

    public Tuple2<ArrayList<ArrayList<Float>>, Long> TransformOneBatch(Tuple2<ArrayList<ArrayList<Float>>, Long> inputBatches) {
        ArrayList<ArrayList<Float>> result = new ArrayList<>();
        int inputSize = inputBatches.f0.get(0).size();
        int batchSize = inputBatches.f0.size();

        for(int bi=0; bi<batchSize; bi++){
            ArrayList<Float> inputImg = inputBatches.f0.get(bi);
            for (int i=0;i<this.imgSize;i+=this.blurstep){
                for(int j=0;j<this.imgSize;j+=this.blurstep){
                    ArrayList<Integer> idxavg = new ArrayList<>(Arrays.asList(getidx(i,j,this.imgSize),
                            getidx(i-1,j-1,this.imgSize),
                            getidx(i-1,j,this.imgSize),
                            getidx(i-1,j+1,this.imgSize),
                            getidx(i,j-1,this.imgSize),
                            getidx(i,j+1,this.imgSize),
                            getidx(i+1,j-1,this.imgSize),
                            getidx(i+1,j,this.imgSize),
                            getidx(i+1,j+1,this.imgSize)
                    ));
                    // https://docs.opencv.org/4.x/d4/d13/tutorial_py_filtering.html
                    float res=0.0f;
                    for(int k : idxavg){
                        res+=inputImg.get(k);
                    }
                    res=res/9;
                    inputImg.set(i*this.imgSize+j, res);
                }
            }
            result.add(inputImg);
        }

        return(new Tuple2<>(result, inputBatches.f1));
    }

    public Tuple2<ArrayList<ArrayList<Float>>, Long> CompressOneBatch(Tuple2<ArrayList<ArrayList<Float>>, Long> inputBatches) {
        ArrayList<ArrayList<Float>> result = new ArrayList<>();
        int inputSize = inputBatches.f0.get(0).size();
        int batchSize = inputBatches.f0.size();
        int outputSize = Math.min(OutputSize*OutputSize, inputSize);

        for(int i=0; i<batchSize;i++){
            ArrayList<Float> tmp = new ArrayList<>();
            for(int dx=0; dx<OutputSize; dx++){
                for(int dy=0; dy<OutputSize; dy++){
                    int tx=(int) dx*InputSize/OutputSize;
                    int ty=(int) dy*InputSize/OutputSize;
                    tmp.add(inputBatches.f0.get(i).get(get1didx(tx, ty, InputSize)));
                }
            }
            result.add(tmp);
        }

        return(new Tuple2<>(result, inputBatches.f1));
    }

    @Override
    public void processElement(Tuple2<ArrayList<ArrayList<Float>>, Long> inputBatches, Context context,
                               Collector<Tuple2<ArrayList<ArrayList<Float>>, Long>> collector) throws Exception {
        Tuple2<ArrayList<ArrayList<Float>>, Long> ptra=TransformOneBatch(inputBatches);
        if(!Objects.equals(this.InputSize, this.OutputSize)) {
            Tuple2<ArrayList<ArrayList<Float>>, Long> pcmp = CompressOneBatch(ptra);
            collector.collect(pcmp);
        }
        else {
            collector.collect(ptra);
        }
    }
}

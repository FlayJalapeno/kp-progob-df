package dataframe;

import java.util.*;

public class GroupbyActionsThread implements Runnable {
    private int t;
    private DataFrame df;
    public DataFrame ret;
    private ArrayList<Integer> key_id;
    public GroupbyActionsThread(int optype, DataFrame inputdf, ArrayList<Integer> id) throws IncorrectOperationKey{
        if(optype < 0 || optype > 5){
            throw new IncorrectOperationKey();
        }
        else {
            t = optype;
            df = inputdf;
            key_id = id;
            ret = null;
        }
    }

    @Override
    public void run() {
        LinkedList<Value> key = new LinkedList<>();
        for(int i=0;i<df.width;i++){
            if(key_id.contains(i)){
                key.add(df.cols[i].col.get(0));
            }
        }
        if(t == 0){
            ret = df.groupby().min();
        }
        if(t == 1){
            ret = df.groupby().max();
        }
        if(t == 2){
            ret = df.groupby().sum();
        }
        if(t == 3){
            ret = df.groupby().mean();
        }
        if(t == 4){
            ret = df.groupby().std();
        }
        if(t == 5){
            ret = df.groupby().var();
        }
        for(int i=0;i<df.width;i++){
            if(key_id.contains(i)){
                ret.cols[i].col.set(0,key.getFirst());
            }
        }
    }
}

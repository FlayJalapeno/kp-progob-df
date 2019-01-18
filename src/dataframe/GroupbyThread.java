package dataframe;

public class GroupbyThread implements Runnable {
    private DataFrame df;
    public DataFrame.GroupedDF ret;
    private String[] cnames;

    public GroupbyThread(DataFrame inputdf){
        df = inputdf;
        cnames = null;
        ret = null;
    }
    public GroupbyThread(DataFrame inputdf, String[] c_names){
        df = inputdf;
        cnames = c_names;
        ret = null;
    }

    public void run(){
        if(cnames != null){
            ret = df.groupby(cnames);
        }
        else {
            ret = df.groupby();
        }
    }

}

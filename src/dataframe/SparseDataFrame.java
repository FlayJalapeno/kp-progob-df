package dataframe;

import java.util.List;

public class SparseDataFrame extends DataFrame {
    public SparseColumn colms[];
    public Value hidden;
    public SparseDataFrame(String[] col_names, List<Class<? extends Value>> col_types, Value hide){
        super(col_names,col_types);
        colms = new SparseColumn[width];
        for(int i =0;i<width;i++){
            colms[i] = new SparseColumn(col_names[i], col_types.get(i));
        }
        hidden = hide;
    }

    public SparseDataFrame(DataFrame df, Value hide){
        super(df.cnames,df.ctypes);
        colms = new SparseColumn[width];
        for(int i =0;i<width;i++){
            colms[i] = new SparseColumn(df.cnames[i], df.ctypes.get(i));
        }
        hidden = hide;
        for(int i =0;i<width;i++){
            for(int j = 0; j<df.cols[i].h; j++){
                colms[i].Add(df.cols[i].col.get(j), hidden);
            }
        }
    }

    public DataFrame toDense(){
        DataFrame dfr = new DataFrame(cnames,ctypes);
        int it = 0, itc, itp = 0;
        for(SparseColumn n: colms){
            COOValue tmp = (COOValue)n.col.get(itp);
            itc = tmp.pos;
            itp++;
            for(int i=0;i<n.h;i++){
                if(i == itc){
                    dfr.cols[it].Add(tmp.val);
                    if(itp < n.col.size()) {
                        tmp = (COOValue) n.col.get(itp);
                        itc = tmp.pos;
                        itp++;
                    }
                }else{
                    dfr.cols[it].Add(hidden);
                }
            }
            it++;
            itp = 0;
        }
        dfr.height = dfr.cols[0].h;
        return dfr;
    }

    public DataFrame iloc(int i){
        DataFrame dft = this.toDense();
        DataFrame dfr = dft.iloc(i);
        return new SparseDataFrame(dfr,hidden);
    }

    public DataFrame iloc(int from,int to){
        DataFrame dft = this.toDense();
        DataFrame dfr = dft.iloc(from,to);
        return new SparseDataFrame(dfr,hidden);
    }
}

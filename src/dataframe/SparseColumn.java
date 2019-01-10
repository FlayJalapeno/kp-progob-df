package dataframe;

public class SparseColumn extends Column {
    public SparseColumn(String n,Class<? extends Value> t){
        super(n,t);
    }

    public void Add(Value val, Value hidden){
        try {
            if(val.neq(hidden)){
                COOValue a = new COOValue(h, (Value) val.getVal());
                col.add(a);
            }
        } catch (IncompatibleType incompatibleType) {
            incompatibleType.printStackTrace();
        }
        h++;
    }

}

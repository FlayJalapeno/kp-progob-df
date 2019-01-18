package dataframe;

public class VString extends Value implements Cloneable{
    public String val;
    public VString(String v){
        val = v;
    }
    public VString(){
        val = "";
    }
    @Override
    public String toString() {
        return val;
    }

    @Override
    public Value add(Value v) throws InvalidType {
        throw new InvalidType();
    }

    @Override
    public Value sub(Value v) throws InvalidType {
        throw new InvalidType();
    }

    @Override
    public Value mul(Value v) throws InvalidType {
        throw new InvalidType();
    }

    @Override
    public Value div(Value v) throws InvalidType {
        throw new InvalidType();
    }

    @Override
    public Value pow(Value v) throws InvalidType {
        throw new InvalidType();
    }

    @Override
    public boolean eq(Value v) throws InvalidType {
        if(v instanceof VString){
            if(val.compareTo(((VString) v).val)==0) return true;
            else return false;
        }
        throw new InvalidType();
    }

    @Override
    public boolean gte(Value v) throws InvalidType {
        if(v instanceof VString){
            if(val.compareTo(((VString) v).val)>=0) return true;
            else return false;
        }
        throw new InvalidType();
    }

    @Override
    public boolean lte(Value v) throws InvalidType {
        if(v instanceof VString){
            if(val.compareTo(((VString) v).val)<=0) return true;
            else return false;
        }
        throw new InvalidType();
    }

    @Override
    public boolean gt(Value v) throws InvalidType {
        if(v instanceof VString){
            if(val.compareTo(((VString) v).val)>0) return true;
            else return false;
        }
        throw new InvalidType();
    }

    @Override
    public boolean lt(Value v) throws InvalidType {
        if(v instanceof VString){
            if(val.compareTo(((VString) v).val)<0) return true;
            else return false;
        }
        throw new InvalidType();
    }

    @Override
    public boolean neq(Value v) throws InvalidType {
        if(v instanceof VString){
            if(val.compareTo(((VString) v).val)!=0) return true;
            else return false;
        }
        throw new InvalidType();
    }

    @Override
    public boolean equals(Object other) {
        return other.hashCode() == this.hashCode();
    }

    @Override
    public int hashCode() {
        int code = val.hashCode() * 10 + 4;
        return code;
    }

    @Override
    public VString create(String s) {
        return new VString(s);
    }

    @Override
    public Object clone() throws CloneNotSupportedException{
        Object cln = new VString(val);
        return cln;
    }

    @Override
    public Number toNumber() {
        return null;
    }

    //do tego jebanego konstruktora coovalue w SparseColumn (nie ma dostępu do pola val przy obiekcie Value)\
    @Override
    public Object getVal(){
        return this.val;
    }
}

package dataframe;

import java.text.ParseException;
import java.util.Date;
import java.text.SimpleDateFormat;

public class VDateTime extends Value implements Cloneable{
    public Date val;
    public VDateTime(Date v){
        val = v;
    }
    public VDateTime(){
        val = new Date();
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
    public boolean gt(Value v) throws InvalidType {
        if(v instanceof VDateTime){
            return val.after(((VDateTime) v).val);
        }
        throw new InvalidType();
    }

    @Override
    public boolean gte(Value v) throws InvalidType {
        if(v instanceof VDateTime){
            return val.after(((VDateTime) v).val) || val.equals(((VDateTime) v).val);
        }
        throw new InvalidType();
    }

    @Override
    public boolean lt(Value v) throws InvalidType {
        if(v instanceof VDateTime){
            return val.before(((VDateTime) v).val);
        }
        throw new InvalidType();
    }

    @Override
    public boolean lte(Value v) throws InvalidType {
        if(v instanceof VDateTime){
            return val.before(((VDateTime) v).val) || val.equals(((VDateTime) v).val);
        }
        throw new InvalidType();
    }

    @Override
    public boolean eq(Value v) throws InvalidType {
        if(v instanceof VDateTime){
            return val.equals(((VDateTime) v).val);
        }
        throw new InvalidType();
    }

    @Override
    public boolean neq(Value v) throws InvalidType {
        if(v instanceof VDateTime){
            return  !val.equals(((VDateTime) v).val);
        }
        throw new InvalidType();
    }

    @Override
    public int hashCode() {
        int r = val.hashCode()*10 + 5;
        return r;
    }

    @Override
    public boolean equals(Object other) {
        return this.hashCode()==other.hashCode();
    }

    @Override
    public VDateTime create(String s) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date f = null;
        try {
            f = sdf.parse(s);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        VDateTime vr = new VDateTime(f);
        return vr;
    }

    @Override
    public String toString() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        String ret = sdf.format(val);
        return ret;
    }

    @Override
    public Object clone() throws CloneNotSupportedException{
        Object cln = new VDateTime(val);
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

package dataframe;

public class VInt extends Value implements Cloneable{
    public int val;
    public VInt(int v){
        val = v;
    }
    public VInt(){ val = 0; }

    @Override
    public String toString() {
        return Integer.toString(val);
    }

    @Override
    public Value add(Value v) throws IncompatibleType {
        if(v instanceof VInt){
            val = val + ((VInt) v).val;
            return this;
        }
        else if(v instanceof VDouble){
            val = val + (int)((VDouble) v).val;
            return this;
        }
        else if(v instanceof VFloat){
            val = val + (int) ((VFloat) v).val;
            return this;
        }
        throw new IncompatibleType();
    }

    @Override
    public Value sub(Value v) throws IncompatibleType {
        if(v instanceof VInt){
            val = val - ((VInt) v).val;
            return this;
        }
        else if(v instanceof VDouble){
            val = val - (int)((VDouble) v).val;
            return this;
        }
        else if(v instanceof VFloat){
            val = val - (int) ((VFloat) v).val;
            return this;
        }
        throw new IncompatibleType();
    }

    @Override
    public Value mul(Value v) throws IncompatibleType {
        if(v instanceof VInt){
            val = val * ((VInt) v).val;
            return this;
        }
        else if(v instanceof VDouble){
            val = val * (int)((VDouble) v).val;
            return this;
        }
        else if(v instanceof VFloat){
            val = val * (int) ((VFloat) v).val;
            return this;
        }
        throw new IncompatibleType();
    }

    @Override
    public Value div(Value v) throws DivisionByZero, IncompatibleType {
        if (v instanceof VInt) {
            if (((VInt) v).val == 0) throw new DivisionByZero();
            val = val / ((VInt) v).val;
            return this;
        }
        else if (v instanceof VDouble) {
            if (((VDouble) v).val == 0) throw new DivisionByZero();
            val = val / (int) ((VDouble) v).val;
            return this;
        }
        else if (v instanceof VFloat) {
            if (((VFloat) v).val == 0) throw new DivisionByZero();
            val = val / (int) ((VFloat) v).val;
            return this;
        }
        throw new IncompatibleType();
    }

    @Override
    public Value pow(Value v) throws IncompatibleType {
        if(v instanceof VInt){
            val = DataFrame.powr(val,((VInt) v).val);
            return this;
        }
        else if(v instanceof VDouble){
            val = (int) Math.pow(val,((VDouble) v).val);
            return this;
        }
        else if(v instanceof VFloat){
            val = (int) Math.pow(val,(double) ((VFloat) v).val);
            return this;
        }
        throw new IncompatibleType();
    }

    @Override
    public boolean eq(Value v) throws IncompatibleType {
        if(v instanceof VInt){
            return val == ((VInt) v).val;
        }
        else if(v instanceof VDouble){
            return val == (int) ((VDouble) v).val;
        }
        else if(v instanceof VFloat){
            return val == (int) ((VFloat) v).val;
        }
        throw new IncompatibleType();
    }

    @Override
    public boolean gte(Value v) throws IncompatibleType {
        if(v instanceof VInt){
            return val >= ((VInt) v).val;
        }
        else if(v instanceof VDouble){
            return val >= (int) ((VDouble) v).val;
        }
        else if(v instanceof VFloat){
            return val >= (int) ((VFloat) v).val;
        }
        throw new IncompatibleType();
    }

    @Override
    public boolean lte(Value v) throws IncompatibleType {
        if(v instanceof VInt){
            return val <= ((VInt) v).val;
        }
        else if(v instanceof VDouble) {
            return val <= (int) ((VDouble) v).val;
        }
        else if(v instanceof VFloat){
            return val <= (int) ((VFloat) v).val;
        }
        throw new IncompatibleType();
    }

    @Override
    public boolean gt(Value v) throws IncompatibleType {
        if(v instanceof VInt){
            return val > ((VInt) v).val;
        }
        else if(v instanceof VDouble){
            return val > (int) ((VDouble) v).val;
        }
        else if(v instanceof VFloat){
            return val > (int) ((VFloat) v).val;
        }
        throw new IncompatibleType();
    }

    @Override
    public boolean lt(Value v) throws IncompatibleType {
        if(v instanceof VInt){
            return val < ((VInt) v).val;
        }
        else if(v instanceof VDouble){
            return val < (int) ((VDouble) v).val;
        }
        else if(v instanceof VFloat){
            return val < (int) ((VFloat) v).val;
        }
        throw new IncompatibleType();
    }

    @Override
    public boolean neq(Value v) throws IncompatibleType {
        if(v instanceof VInt){
            return val != ((VInt) v).val;
        }
        else if(v instanceof VDouble){
            return val != (int) ((VDouble) v).val;
        }
        else if(v instanceof VFloat){
            return val != (int) ((VFloat) v).val;
        }
        throw new IncompatibleType();
    }

    @Override
    public boolean equals(Object other) {
        return other.hashCode() == this.hashCode();
    }

    @Override
    public int hashCode() {
        int code = val*10 + 1;
        return code;
    }

    @Override
    public VInt create(String s) {
        int vn = Integer.parseInt(s);
        return new VInt(vn);
    }

    @Override
    public Object clone() throws CloneNotSupportedException{
        Object cln = new VInt(val);
        return cln;
    }

    @Override
    public Number toNumber() {
        return new Number() {
            @Override
            public int intValue() {
                return val;
            }

            @Override
            public long longValue() {
                return (long)val;
            }

            @Override
            public float floatValue() {
                return (float)val;
            }

            @Override
            public double doubleValue() {
                return (double)val;
            }
        };
    }

    //do tego jebanego konstruktora coovalue w SparseColumn (nie ma dostępu do pola val przy obiekcie Value)\
    @Override
    public Object getVal(){
        return this.val;
    }
}

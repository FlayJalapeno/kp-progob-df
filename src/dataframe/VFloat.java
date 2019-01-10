package dataframe;

public class VFloat extends Value implements Cloneable{
    public float val;
    public VFloat(float v){ val = v; }
    public VFloat(){
        val = 0;
    }

    @Override
    public String toString() {
        return Float.toString(val);
    }

    @Override
    public Value add(Value v) throws IncompatibleType {
        if(v instanceof VFloat){
            val = val+((VFloat) v).val;
            return this;
        }
        if(v instanceof VInt){
            val = val+ (float) ((VInt) v).val;
            return this;
        }
        if(v instanceof VDouble){
            val = val+ (float) ((VDouble) v).val;
            return this;
        }
        throw new IncompatibleType();
    }

    @Override
    public Value sub(Value v) throws IncompatibleType {
        if(v instanceof VFloat){
            val = val-((VFloat) v).val;
            return this;
        }
        if(v instanceof VInt){
            val = val- (float) ((VInt) v).val;
            return this;
        }
        if(v instanceof VDouble){
            val = val- (float) ((VDouble) v).val;
            return this;
        }
        throw new IncompatibleType();
    }

    @Override
    public Value div(Value v) throws DivisionByZero, IncompatibleType {
        if (v instanceof VInt) {
            if (((VInt) v).val == 0) throw new DivisionByZero();
            val = val / (float) ((VInt) v).val;
            return this;
        }
        if (v instanceof VDouble) {
            if (((VDouble) v).val == 0) throw new DivisionByZero();
            val = val / (float) ((VDouble) v).val;
            return this;
        }
        if (v instanceof VFloat) {
            if (((VFloat) v).val == 0) throw new DivisionByZero();
            val = val / ((VFloat) v).val;
            return this;
        }
        throw new IncompatibleType();
    }

    @Override
    public Value mul(Value v) throws IncompatibleType {
        if(v instanceof VFloat){
            val = val*((VFloat) v).val;
            return this;
        }
        if(v instanceof VInt){
            val = val* (float) ((VInt) v).val;
            return this;
        }
        if(v instanceof VDouble){
            val = val* (float) ((VDouble) v).val;
            return this;
        }
        throw new IncompatibleType();
    }

    @Override
    public Value pow(Value v) throws IncompatibleType {
        if(v instanceof VFloat){
            val = (float)Math.pow(val,((VFloat) v).val);
            return this;
        }
        if(v instanceof VInt){
            val = (float)Math.pow(val,((VInt) v).val);
            return this;
        }
        if(v instanceof VDouble){
            val = (float)Math.pow(val,((VDouble) v).val);
            return this;
        }
        throw new IncompatibleType();
    }

    @Override
    public boolean eq(Value v) throws IncompatibleType {
        if(v instanceof VFloat){
            return val == ((VFloat) v).val;
        }
        if(v instanceof VInt){
            return val == (float) ((VInt) v).val;
        }
        if(v instanceof VDouble){
            return val == (float) ((VDouble) v).val;
        }
        throw new IncompatibleType();
    }

    @Override
    public boolean gte(Value v) throws IncompatibleType {
        if(v instanceof VFloat){
            return val >= ((VFloat) v).val;
        }
        if(v instanceof VInt){
            return val >= (float) ((VInt) v).val;
        }
        if(v instanceof VDouble){
            return val >= (float) ((VDouble) v).val;
        }
        throw new IncompatibleType();
    }

    @Override
    public boolean lte(Value v) throws IncompatibleType {
        if(v instanceof VFloat){
            return val <= ((VFloat) v).val;
        }
        if(v instanceof VInt){
            return val <= (float) ((VInt) v).val;
        }
        if(v instanceof VDouble){
            return val <= (float) ((VDouble) v).val;
        }
        throw new IncompatibleType();
    }

    @Override
    public boolean gt(Value v) throws IncompatibleType {
        if(v instanceof VFloat){
            return val > ((VFloat) v).val;
        }
        if(v instanceof VInt){
            return val > (float) ((VInt) v).val;
        }
        if(v instanceof VDouble){
            return val > (float) ((VDouble) v).val;
        }
        throw new IncompatibleType();
    }

    @Override
    public boolean lt(Value v) throws IncompatibleType {
        if(v instanceof VFloat){
            return val < ((VFloat) v).val;
        }
        if(v instanceof VInt){
            return val < (float) ((VInt) v).val;
        }
        if(v instanceof VDouble){
            return val < (float) ((VDouble) v).val;
        }
        throw new IncompatibleType();
    }

    @Override
    public boolean neq(Value v) throws IncompatibleType {
        if(v instanceof VFloat){
            return val != ((VFloat) v).val;
        }
        if(v instanceof VInt){
            return val != (float) ((VInt) v).val;
        }
        if(v instanceof VDouble){
            return val != (float) ((VDouble) v).val;
        }
        throw new IncompatibleType();
    }

    @Override
    public boolean equals(Object other) {
        return this.hashCode() == other.hashCode();
    }

    @Override
    public int hashCode() {
        int r = Float.valueOf(val).hashCode() * 10 + 3;
        return r;
    }

    @Override
    public VFloat create(String s) {
        VFloat vr = new VFloat(Float.parseFloat(s));
        return vr;
    }
    @Override
    public Object clone() throws CloneNotSupportedException{
        Object cln = new VFloat(val);
        return cln;
    }

    @Override
    public Number toNumber() {
        return new Number() {
            @Override
            public int intValue() {
                return (int)val;
            }

            @Override
            public long longValue() {
                return (long)val;
            }

            @Override
            public float floatValue() {
                return val;
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
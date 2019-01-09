package dataframe;

public class CooValue extends Value{
    public int pos;
    public int val;
    public CooValue(int p,int v){
        pos = p;
        val = v;
    }

    @Override
    public String toString() {
        return null;
    }

    @Override
    public Value add(Value v) {
        return null;
    }

    @Override
    public Value sub(Value v) {
        return null;
    }

    @Override
    public Value mul(Value v) {
        return null;
    }

    @Override
    public Value div(Value v) {
        return null;
    }

    @Override
    public Value pow(Value v) {
        return null;
    }

    @Override
    public boolean gt(Value v) {
        return false;
    }

    @Override
    public boolean gte(Value v) {
        return false;
    }

    @Override
    public boolean eq(Value v) {
        return false;
    }

    @Override
    public boolean lt(Value v) {
        return false;
    }

    @Override
    public boolean lte(Value v) {
        return false;
    }

    @Override
    public boolean neq(Value v) {
        return false;
    }

    @Override
    public boolean equals(Object other) {
        return false;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public CooValue create(String s) {
        return null;
    }

    @Override
    public Number toNumber() {
        return null;
    }
}

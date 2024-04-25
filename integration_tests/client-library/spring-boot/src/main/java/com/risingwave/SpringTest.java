package com.risingwave;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;

@Entity
@Table(name="springtest")
public class SpringTest {
    @Id
    private int id;
    private boolean cboolean;
    private short csmallint;
    private int cinteger;
    private long cbigint;
    private float cfloat;
    private double cdouble;
    private String name;
    private OffsetDateTime ctimestamptz;

    protected SpringTest() {}
    public SpringTest(int id, String name) {
        setId(id);
        setBoolean(false);
        setSmallint((short) 0);
        setInteger(0);
        setBigint(0);
        setFloat(0);
        setDouble(0);
        setName(name);
        setTimestampTZ(OffsetDateTime.now(ZoneOffset.UTC));
    }
    public SpringTest(int id, boolean c_boolean, short c_smallint, int c_integer, long c_bigint, float c_float, double c_double, String name, OffsetDateTime ctimestamptz) {
        setId(id);
        setBoolean(c_boolean);
        setSmallint(c_smallint);
        setInteger(c_integer);
        setBigint(c_bigint);
        setFloat(c_float);
        setDouble(c_double);
        setName(name);
        setTimestampTZ(ctimestamptz);
    }

    public int getId() {
        return id;
    }
    public void setId(int id) {
        this.id = id;
    }
    public boolean getBoolean() {
        return cboolean;
    }
    public void setBoolean(boolean c_boolean) {
        this.cboolean = c_boolean;
    }
    public short getSmallint() {
        return csmallint;
    }
    public void setSmallint(short c_smallint) {
        this.csmallint = c_smallint;
    }
    public int getInteger() {
        return cinteger;
    }
    public void setInteger(int c_integer) {
        this.cinteger = c_integer;
    }
    public long getBigint() {
        return cbigint;
    }
    public void setBigint(long c_bigint) {
        this.cbigint = c_bigint;
    }
    public float getFloat() {
        return cfloat;
    }
    public void setFloat(float c_float) {
        this.cfloat = c_float;
    }
    public double getDouble() {
        return cdouble;
    }
    public void setDouble(double c_double) {
        this.cdouble = c_double;
    }
    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    public OffsetDateTime getTimestampTZ() {
        return ctimestamptz;
    }
    public void setTimestampTZ(OffsetDateTime ctimestamptz) {
        this.ctimestamptz = ctimestamptz;
    }
}

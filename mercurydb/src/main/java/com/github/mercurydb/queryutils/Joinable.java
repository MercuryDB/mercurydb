package com.github.mercurydb.queryutils;

public interface Joinable {
    public HgTupleStream joinOn(ValueExtractable fe);
}

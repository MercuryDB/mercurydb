package org.mercurydb.queryutils;

import java.util.HashMap;

/**
 * This class is essentially an alias
 * for HashMap<Class<?>, Object> currently. This
 * should make client code a bit cleaner, as well as
 * our JoinDriver class which also uses this.
 */
public class HgTuple extends HashMap<Class<?>, Object> {
	private static final long serialVersionUID = 3954994964527336275L;
	
	private HgTuple() {
		super();
	}
	
	public HgTuple(HgJoinInput s1, final Object o1, HgJoinInput s2, final Object o2) {
		HgTuple jr1 = makeRecord(s1, o1);
		HgTuple jr2 = makeRecord(s2, o2);
		putAll(jr1);
		putAll(jr2);
	}
	
	public static HgTuple makeRecord(HgJoinInput s, Object o) {
		HgTuple jr;
		if (o instanceof HgTuple) {
			jr = (HgTuple)o;
		} else {
			jr = new HgTuple();
			jr.put(s.getContainerClass(), o);
		}
		
		return jr;
	}
	
	public static HgTuple singleton(HgJoinInput s, Object o) {
		HgTuple t = new HgTuple();
		t.put(s.getContainerClass(), o);
		return t;
	}
}
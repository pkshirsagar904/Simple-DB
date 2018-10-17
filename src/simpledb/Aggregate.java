package simpledb;

import java.util.*;

/**
 * An {@code Aggregate} operator computes an aggregate value (e.g., sum, avg,
 * max, min) over a single column, grouped by a single column.
 */
public class Aggregate extends AbstractDbIterator {

	/**
	 * The child operator.
	 */
	DbIterator child;

	/**
	 * The {@code TupleDesc} associated with this {@code Aggregate}.
	 */
	TupleDesc td;

	/**
	 * The {@code Aggregator} for this {@code Aggregate} operator.
	 */
	Aggregator aggregator;

	/**
	 * The current DbIterator over aggregate results.
	 */
	DbIterator it = null;
	private DbIterator agggregateiter;
	private int afield, gfield;
	private Aggregator.Op aop;

	private Type gfieldtype;

	/**
	 * Constructs an {@code Aggregate}.
	 *
	 * Implementation hint: depending on the type of afield, you will want to
	 * construct an {@code IntAggregator} or {@code StringAggregator} to help you
	 * with your implementation of {@code readNext()}.
	 * 
	 *
	 * @param child
	 *            the {@code DbIterator} that provides {@code Tuple}s.
	 * @param afield
	 *            the column over which we are computing an aggregate.
	 * @param gfield
	 *            the column over which we are grouping the result, or -1 if there
	 *            is no grouping
	 * @param aop
	 *            the {@code Aggregator} operator to use
	 */
	public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
		// some code goes here
		this.child = child;
		this.afield = afield;
		this.gfield = gfield;
		this.aop = aop;
		agggregateiter = null;
		Type[] temp= new Type[] {Type.INT_TYPE,Type.INT_TYPE};
		td= new TupleDesc(temp);
		
		if(agggregateiter == null) {
			if (gfield == Aggregator.NO_GROUPING)
			{	 Type[] t2 = new Type[] {Type.INT_TYPE};
			td= new TupleDesc(t2);
				gfieldtype = null;}
			else
				gfieldtype = child.getTupleDesc().getType(gfield);

			if (child.getTupleDesc().getType(afield).equals(Type.INT_TYPE))
				aggregator = new IntAggregator(gfield, gfieldtype, afield, aop);
			else
				aggregator = new StringAggregator(gfield, gfieldtype, afield, aop);
		
		}
		
	}

	public static String aggName(Aggregator.Op aop) {
		switch (aop) {
		case MIN:
			return "min";
		case MAX:
			return "max";
		case AVG:
			return "avg";
		case SUM:
			return "sum";
		case COUNT:
			return "count";
		}
		return "";
	}

	/**
	 * Returns the {@code TupleDesc} of this {@code Aggregate}. If there is no group
	 * by field, this will have one field - the aggregate column. If there is a
	 * group by field, the first field will be the group by field, and the second
	 * will be the aggregate value column.
	 * 
	 * The name of an aggregate column should be informative. For example:
	 * {@code aggName(aop) (child_td.getFieldName(afield))} where {@code aop} and
	 * {@code afield} are given in the constructor, and {@code child_td} is the
	 * {@code TupleDesc} of the child iterator.
	 */
	public TupleDesc getTupleDesc() {
		return td;
	}

	public void open() throws NoSuchElementException, DbException, TransactionAbortedException {
		child.open();
		while (child.hasNext())
			aggregator.merge(child.next());
		it = aggregator.iterator();
		it.open();
	}

	public void close() {
		it = null;
	}

	public void rewind() throws DbException, TransactionAbortedException {
		// some code goes here
		child.rewind();
		agggregateiter = null;
	}

	/**
	 * Returns the next {@code Tuple}. If there is a group by field, then the first
	 * field is the field by which we are grouping, and the second field is the
	 * result of computing the aggregate, If there is no group by field, then the
	 * result tuple should contain one field representing the result of the
	 * aggregate. Should return {@code null} if there are no more {@code Tuple}s.
	 */
	protected Tuple readNext() throws TransactionAbortedException, DbException {
		// some code goes here
		//Aggregator aggregaotr = null;
		System.out.println("the code is here"+gfield);
	
		Tuple t3= agggregateiter.next();
		if (t3==null)
			return null;
		
//		else  if(agggregateiter == null) {
//			if (gfield == Aggregator.NO_GROUPING)
//			{	 Type[] t2 = new Type[] {Type.INT_TYPE};
//			td= new TupleDesc(t2);
//				gfieldtype = null;}
//			else
//				gfieldtype = child.getTupleDesc().getType(gfield);
//
//			if (child.getTupleDesc().getType(afield).equals(Type.INT_TYPE))
//				aggregator = new IntAggregator(gfield, gfieldtype, afield, aop);
//			else
//				aggregator = new StringAggregator(gfield, gfieldtype, afield, aop);
//			return agggregateiter.next();
//			while (child.hasNext())
//			
//	aggregator.merge(child.next());
//			agggregateiter = aggregator.iterator();
	//agggregateiter.open();
	

		//	Tuple t3= agggregateiter.next();
		if (agggregateiter.hasNext())
			if (t3==null)
				return null;
			else 	return agggregateiter.next();
		else
			return null;
	}

}

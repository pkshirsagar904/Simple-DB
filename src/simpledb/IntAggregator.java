package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import simpledb.Aggregator.AggregateFunction;
import simpledb.Aggregator.AggregateFunctionFactory;
import simpledb.Aggregator.CountAggregateFunction;
import simpledb.Aggregator.Op;

/**
 * An {@code IntAggregator} computes some aggregate value over a set of
 * {@code IntField}s.
 */
public class IntAggregator implements Aggregator {

	/**
	 * A {@code IntAggregatorImpl} instance.
	 */
	IntAggregatorImpl impl;
	private int gbfield;
	private int afield;
	private Type gbfieldtype;
	private Op what;
	private TupleDesc td;
	AggregateFunction aggFtn;
	//private Object aggFtnFactory;
	AggregateFunctionFactory aggFtnFactory;
	Map<Field, AggregateFunction> field2aggFtn = new HashMap<Field, AggregateFunction>();
	//StringAggregator str = new StringAggregator(afield, gbfieldtype, afield, what);

	/**
	 * Constructs an {@code IntAggregator}.
	 * 
	 * @param gbfield
	 *            the 0-based index of the group-by field in the tuple, or
	 *            {@code NO_GROUPING} if there is no grouping
	 * @param gbfieldtype
	 *            the type of the group by field (e.g., {@code Type.INT_TYPE}), or
	 *            {@code null} if there is no grouping
	 * @param afield
	 *            the 0-based index of the aggregate field in the tuple
	 * @param what
	 *            the aggregation operator
	 */
	public IntAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
		// some
		this.gbfield = gbfield;
		this.afield = afield;
		this.gbfieldtype = gbfieldtype;

		Type[] types = new Type[] {gbfieldtype, Type.INT_TYPE};
		 td = new TupleDesc(types);

	}

	/**
	 * Merges a new {@code Tuple} into the aggregate, grouping as indicated in the
	 * constructor.
	 * 
	 * @param tup
	 *            the {@code Tuple} containing an aggregate field and a group-by
	 *            field
	 */
	public void merge(Tuple tup) {
		//
	//	str.merge(tup);
	
		if(field2aggFtn.containsKey(tup.getField(gbfield)))
		{
			
			field2aggFtn.merge(tup.getField(afield), aggFtn, null);
		}
		else
		{
			
			field2aggFtn.merge(tup.getField(gbfield), aggFtn, null);
			field2aggFtn.put(tup.getField(gbfield), aggFtn);
		}

		throw new UnsupportedOperationException("Implement this");
	}

	/**
	 * Creates a {@code DbIterator} over group aggregate results.
	 *
	 * @return a {@code DbIterator} whose tuples are the pair ({@code groupVal},
	 *         {@code aggregateVal}) if using group, or a single
	 *         ({@code aggregateVal}) if no grouping. The {@code aggregateVal} is
	 *         determined by the type of aggregate specified in the constructor.
	 */
	public DbIterator iterator() {
		// some code goes here
		ArrayList<Tuple> tuples = new ArrayList<Tuple>();
		Tuple tupp = new Tuple(td);
		tupp.fields[afield] = aggFtn.aggregateValue();
		tuples.add(tupp);
		Iterator<Map.Entry<Field, AggregateFunction>> newiterator = field2aggFtn.entrySet().iterator();
		while (newiterator.hasNext()) {
			Map.Entry<Field, AggregateFunction> temp = newiterator.next();
			
			tupp.setField(afield,temp.getValue().aggregateValue()); 
			
			
			
			 
			
			tuples.add(tupp);
		}

		// some code goes here
		// throw new UnsupportedOperationException("Implement this");
		return new TupleIterator(td, tuples);

		// throw new UnsupportedOperationException("Implement this");
	}

	/**
	 * An {@code IntAggregatorImpl} computes some aggregate value over a set of
	 * {@code Field}s.
	 */
	abstract class IntAggregatorImpl {

		// some code goes here

		private AggregateFunctionFactory aggFtnFactory;
		 int afield;

		IntAggregatorImpl(int afield, Op what) {

			switch (what) {
			case MAX:
				this.aggFtnFactory = new AggregateFunctionFactory() {

					@Override
					public AggregateFunction createAggregateFunction() {
						return new MaxAggregateFunction();
									
					}
				};
				break;
			default:
				throw new IllegalArgumentException(what + " is not supported");
			}
			this.afield = afield;
		}
		public abstract void merge(Tuple tup);

		// some code goes here

		/**
		 * Clears this {@code IntAggregator}.
		 */
		public void clear() {
			// some code goes here
			impl.clear();
			//throw new UnsupportedOperationException("Implement this");
		}
	}

	@Override
	public void clear() {
		// TODO Auto-generated method stub
		
	}	}


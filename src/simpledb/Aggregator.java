package simpledb;

import simpledb.Aggregator.AggregateFunction;
import simpledb.Aggregator.IntSumAggregateFunction;
import simpledb.Aggregator.Op;

/**
 * The common interface for any class that can compute an aggregate over a list
 * of Tuples.
 */
public interface Aggregator {

	static final int NO_GROUPING = -1;

	public enum Op {
		MIN, MAX, SUM, AVG, COUNT;

		/**
		 * Interface to access operations by a string containing an integer index for
		 * command-line convenience.
		 *
		 * @param s
		 *            a string containing a valid integer Op index
		 */
		public static Op getOp(String s) {
			return getOp(Integer.parseInt(s));
		}

		/**
		 * Interface to access operations by integer value for command-line convenience.
		 *
		 * @param i
		 *            a valid integer Op index
		 */
		public static Op getOp(int i) {
			return values()[i];
		}
	}

	/**
	 * An {@code AggregateFunctionFactory} can create {@code AggregateFunction}s.
	 */
	interface AggregateFunctionFactory {

		/**
		 * Creates an {@code AggregateFunction}.
		 * 
		 * @return a newly created {@code AggregateFunction}
		 */
		AggregateFunction createAggregateFunction();
	}

	/**
	 * An {@code AggregateFunction} maintains an aggregate value computed over
	 * multiple values.
	 */
	interface AggregateFunction {

		/**
		 * Merges the specified value into the aggregate value maintained by this
		 * {@code AggregateFunction}.
		 * 
		 * @param value
		 *            a value
		 */
		void merge(Field value);

		/**
		 * Returns the aggregate value maintained by this {@code AggregateFunction}.
		 * 
		 * @return the aggregate value maintained by this {@code AggregateFunction}
		 */
		Field aggregateValue();
	}

	/**
	 * A {@code CountAggregateFunction} maintains the number of values given so far.
	 */
	class CountAggregateFunction implements AggregateFunction {

		int count = 1;

		/**
		 * Merges the specified value into the aggregate value maintained by this
		 * {@code CountAggregateFunction}.
		 * 
		 * @param value
		 *            a value
		 */
		public void merge(Field value) {
			count++;
		}

		/**
		 * Returns the aggregate value maintained by this
		 * {@code CountAggregateFunction}.
		 * 
		 * @return the aggregate value maintained by this {@code CountAggregateFunction}
		 */
		public Field aggregateValue() {
			return new IntField(count);
		}
	}

	/**
	 * A {@code MinAggregateFunction} maintains the minimum of the values given so
	 * far.
	 */
	class MinAggregateFunction implements AggregateFunction {

		Field min;

		/**
		 * Merges the specified value into the aggregate value maintained by this
		 * {@code MinAggregateFunction}.
		 * 
		 * @param value
		 *            a value
		 */
		public void merge(Field value) {
			// some code goes here
			// throw new UnsupportedOperationException("Implement this");
			System.out.println("the code is here");
			if (min == null)
				min = value;

			else if (min != null && min.compare(simpledb.Predicate.Op.LESS_THAN, value))

				min = value;

		}

		/**
		 * Returns the aggregate value maintained by this {@code MinAggregateFunction}.
		 * 
		 * @return the aggregate value maintained by this {@code MinAggregateFunction}
		 */
		public Field aggregateValue() {
			return min;
		}
	}

	/**
	 * A {@code MaxAggregateFunction} maintains the maximum of the values given so
	 * far.
	 */
	class MaxAggregateFunction implements AggregateFunction {

		Field max;

		/**
		 * Merges the specified value into the aggregate value maintained by this
		 * {@code MaxAggregateFunction}.
		 * 
		 * @param value
		 *            a value
		 */
		public void merge(Field value) {
			// some code goes here
			// throw new UnsupportedOperationException("Implement this");
			if (max == null)
				max = value;
			else if (max == null || max.compare(simpledb.Predicate.Op.LESS_THAN, value))
				max = value;

			else {
			}

			System.out.println(max);
		}

		/**
		 * Returns the aggregate value maintained by this {@code MaxAggregateFunction}.
		 * 
		 * @return the aggregate value maintained by this {@code MaxAggregateFunction}
		 */
		public Field aggregateValue() {
			return max;
		}
	}

	/**
	 * A {@code IntAverageAggregateFunction} calculates the average of the integers
	 * given so far.
	 */
	class IntAverageAggregateFunction extends IntSumAggregateFunction {

		// some code goes here

		/**
		 * Merges the specified value into the aggregate value maintained by this
		 * {@code IntAverageAggregateFunction}.
		 * 
		 * @param value
		 *            a value
		 */
		int count = 0, avg = 0;

		public void merge(Field value) {
			// some code goes here
			// throw new UnsupportedOperationException("Implement this");
			int a = 0;
			if (value != null) {
				a = Integer.parseInt(value.toString());
				a += a;
				count++;
			}
			avg = a / count;
			System.out.println(avg);
		}

		/**
		 * Returns the aggregate value maintained by this
		 * {@code IntAverageAggregateFunction}.
		 * 
		 * @return the aggregate value maintained by this
		 *         {@code IntAverageAggregateFunction}
		 */
		public Field aggregateValue() {
			// some code goes here
			// throw new UnsupportedOperationException("Implement this");
			return new IntField(avg);
		}
	}

	/**
	 * A {@code IntSumAggregateFunction} maintains the sum of the integers given so
	 * far.
	 */
	class IntSumAggregateFunction implements AggregateFunction {

		// some code goes here
		int sum = 0;

		/**
		 * Merges the specified value into the aggregate value maintained by this
		 * {@code IntSumAggregateFunction}.
		 * 
		 * @param value
		 *            a value
		 */
		public void merge(Field value) {
			// some code goes here
			// throw new UnsupportedOperationException("Implement this");

			if (value != null) {
				sum = Integer.parseInt(value.toString());
				sum += sum;
				System.out.println(sum);

			}
		}

		/**
		 * Returns the aggregate value maintained by this
		 * {@code IntSumAggregateFunction}.
		 * 
		 * @return the aggregate value maintained by this
		 *         {@code IntSumAggregateFunction}
		 */
		public Field aggregateValue() {
			// some code goes here
			// throw new UnsupportedOperationException("Implement this");
			return new IntField(sum);
		}
	}

	/**
	 * Merge a new tuple into the aggregate for a distinct group value; creates a
	 * new group aggregate result if the group value has not yet been encountered.
	 *
	 * @param tup
	 *            the Tuple containing an aggregate field and a group-by field
	 */
	public void merge(Tuple tup);

	/**
	 * Create a DbIterator over group aggregate results.
	 * 
	 * @see simpledb.TupleIterator for a possible helper
	 */
	public DbIterator iterator();

	/**
	 * Clears this {@code Aggregator}.
	 */
	public void clear();

}

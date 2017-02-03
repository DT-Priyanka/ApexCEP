/**
 * Put your copyright and license info here.
 */
package com.example.droosapps;

import java.util.Random;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

/**
 * This is a simple operator that emits random number.
 */
public class RandomDataGenerator extends BaseOperator implements InputOperator {
	private int numTuples = 100;
	private transient int count = 0;
	private Random r = new Random();

	public final transient DefaultOutputPort<Product> out = new DefaultOutputPort<Product>();

	@Override
	public void beginWindow(long windowId) {
		count = 0;
	}

	@Override
	public void emitTuples() {
		if (count++ < numTuples) {
			if (count % 2 == 0) {
				out.emit(new Product(r.nextLong(), "gold", 0));
			} else {
				out.emit(new Product(r.nextLong(), "diamond", 0));
			}
		}
	}

	public int getNumTuples() {
		return numTuples;
	}

	/**
	 * Sets the number of tuples to be emitted every window.
	 * @param numTuples
	 *            number of tuples
	 */
	public void setNumTuples(int numTuples) {
		this.numTuples = numTuples;
	}
}

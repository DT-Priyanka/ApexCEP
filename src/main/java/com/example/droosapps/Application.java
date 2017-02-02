/**
 * Put your copyright and license info here.
 */
package com.example.droosapps;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.io.ConsoleOutputOperator;

@ApplicationAnnotation(name = "MyFirstApplication")
public class Application implements StreamingApplication {

	@Override
	public void populateDAG(DAG dag, Configuration conf) {
		RandomDataGenerator randomGenerator = dag.addOperator(
				"randomGenerator", RandomDataGenerator.class);
		randomGenerator.setNumTuples(5000);

		DroolsOperator drools = dag.addOperator("drools", DroolsOperator.class);
		ConsoleOutputOperator cons = dag.addOperator("console",
				new ConsoleOutputOperator());

		dag.addStream("randomData", randomGenerator.out, drools.input)
				.setLocality(Locality.CONTAINER_LOCAL);
		dag.addStream("printData", drools.output, cons.input).setLocality(
				Locality.CONTAINER_LOCAL);
	}
}

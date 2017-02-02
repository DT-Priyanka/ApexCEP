package com.example.droosapps;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;

import org.kie.api.KieServices;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.common.util.BaseOperator;

public class DroolsOperator extends BaseOperator implements
		Operator.ActivationListener<Context.OperatorContext> {

	private transient KieContainer kieContainer;
	private transient KieSession kieSession;
	// TODO run rules in different thread
	private Queue<Product> queue = new LinkedBlockingDeque<Product>(1000);
	public final transient DefaultInputPort<Product> input = new DefaultInputPort<Product>() {

		@Override
		public void process(Product prod) {
			kieSession.insert(prod);
			queue.add(prod);
		}
	};
	public final transient DefaultOutputPort<Product> output = new DefaultOutputPort<Product>();

	@Override
	public void setup(OperatorContext context) {

	}

	@Override
	public void activate(OperatorContext context) {
		KieServices kieServices = KieServices.Factory.get();
		kieContainer = kieServices.getKieClasspathContainer();
	}

	@Override
	public void teardown() {
		kieSession.destroy();
	}

	@Override
	public void beginWindow(long num) {
		kieSession = kieContainer.newKieSession("ksession-rule");
	}

	@Override
	public void endWindow() {
		try {
			kieSession.fireAllRules();
			for (int i = 0; i < queue.size(); i++) {
				output.emit(queue.poll());
			}
			queue.clear();
		} finally {
			kieSession.dispose();
		}
	}

	@Override
	public void deactivate() {

	}

}

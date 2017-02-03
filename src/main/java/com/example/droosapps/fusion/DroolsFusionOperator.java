package com.example.droosapps.fusion;

import java.util.concurrent.TimeUnit;

import org.drools.core.time.SessionPseudoClock;
import org.kie.api.KieServices;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.rule.EntryPoint;
import org.kie.api.runtime.rule.QueryResults;
import org.kie.api.runtime.rule.QueryResultsRow;
import org.kie.api.time.SessionClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.common.util.BaseOperator;
import com.example.droosapps.model.BaggageStatus;
import com.example.droosapps.model.Event;

public class DroolsFusionOperator extends BaseOperator implements Operator.ActivationListener<Context.OperatorContext>
{

  private transient static final String CEP_STREAM = "AirportStream";
  private static final Logger LOG = LoggerFactory.getLogger(DroolsFusionOperator.class);
  private transient KieSession kieSession;
  public final transient DefaultInputPort<Event> input = new DefaultInputPort<Event>()
  {

    @Override
    public void process(Event event)
    {

      LOG.info("Inserting event with id: '" + event.getId() + "' into stream: " + CEP_STREAM);
      SessionClock clock = kieSession.getSessionClock();
      if (!(clock instanceof SessionPseudoClock)) {
        String errorMessage = "This fact inserter can only be used with KieSessions that use a SessionPseudoClock";
        LOG.error(errorMessage);
        throw new IllegalStateException(errorMessage);
      }
      SessionPseudoClock pseudoClock = (SessionPseudoClock)clock;
      EntryPoint ep = kieSession.getEntryPoint(CEP_STREAM);

      ep.insert(event);

      // And then advance the clock
      // We only need to advance the time when dealing with Events. Our facts don't have timestamps.
      long advanceTime = event.getTimestamp().getTime() - pseudoClock.getCurrentTime();
      if (advanceTime > 0) {
        LOG.info("Advancing the PseudoClock with " + advanceTime + " milliseconds.");
        pseudoClock.advanceTime(advanceTime, TimeUnit.MILLISECONDS);
      } else {
        LOG.info("Not advancing time. Fact timestamp is '" + event.getTimestamp().getTime()
            + "', PseudoClock timestamp is '" + pseudoClock.getCurrentTime() + "'.");
      }

    }
  };

  public final transient DefaultOutputPort<BaggageStatus> output = new DefaultOutputPort<BaggageStatus>();

  @Override
  public void setup(OperatorContext context)
  {

  }

  @Override
  public void activate(OperatorContext context)
  {
    LOG.info("Initialize KIE.");
    KieServices kieServices = KieServices.Factory.get();
    // Load KieContainer from resources on classpath (i.e. kmodule.xml and rules).
    KieContainer kieContainer = kieServices.getKieClasspathContainer();
    // Initializing KieSession.
    LOG.info("Creating KieSession.");
    kieSession = kieContainer.newKieSession();
  }

  @Override
  public void teardown()
  {
    kieSession.dispose();
    kieSession.destroy();
  }

  @Override
  public void beginWindow(long windowId)
  {

  }

  @Override
  public void endWindow()
  {
    kieSession.fireAllRules();
    QueryResults results = kieSession.getQueryResults("getBaggageStatus");
    for (QueryResultsRow row : results) {
      BaggageStatus product = (BaggageStatus)row.get("$result");
      output.emit(product);
    }
  }

  @Override
  public void deactivate()
  {

  }

}

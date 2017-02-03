package com.example.droosapps.fusion;

import java.io.InputStream;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator.ActivationListener;
import com.example.droosapps.model.Event;

public class FusionEventsGenerator implements InputOperator, ActivationListener<Context.OperatorContext>
{
  private static transient final Logger LOG = LoggerFactory.getLogger(FusionEventsGenerator.class);
  private List<Event> events;
  private int eventsPointers;
  private int eventsPerWindow = 4;
  public final transient DefaultOutputPort<Event> eventsOut = new DefaultOutputPort<Event>();

  @Override
  public void beginWindow(long windowId)
  {
    eventsPerWindow = 4;
  }

  @Override
  public void endWindow()
  {

  }

  @Override
  public void setup(OperatorContext context)
  {

  }

  @Override
  public void teardown()
  {

  }

  @Override
  public void emitTuples()
  {
    if (eventsPerWindow-- > 0 && events.size() > eventsPointers) {
      eventsOut.emit(events.get(eventsPointers++));
    }
  }

  @Override
  public void activate(OperatorContext arg0)
  {
    LOG.info("Activating ... initializing input stream");
    InputStream eventsInputStream = this.getClass().getClassLoader().getResourceAsStream("events.csv");
    events = FactsLoader.loadEvents(eventsInputStream);
    LOG.info("events list size: " + events.size());
    eventsPointers = 0;

  }

  @Override
  public void deactivate()
  {
    // TODO Auto-generated method stub

  }

}

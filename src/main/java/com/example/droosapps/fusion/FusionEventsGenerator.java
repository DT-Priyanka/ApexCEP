package com.example.droosapps.fusion;

import java.io.InputStream;
import java.util.List;

import com.example.droosapps.model.Event;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;

public class FusionEventsGenerator implements InputOperator
{
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
    InputStream eventsInputStream = this.getClass().getClassLoader().getResourceAsStream("events.csv");
    events = FactsLoader.loadEvents(eventsInputStream);

    eventsPointers = 0;
  }

  @Override
  public void teardown()
  {
    if (eventsPerWindow-- > 0) {
      eventsOut.emit(events.get(eventsPointers++));
    }
  }

  @Override
  public void emitTuples()
  {

  }

}

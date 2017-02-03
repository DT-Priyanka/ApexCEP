package com.example.droosapps;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.example.droosapps.fusion.DroolsFusionOperator;
import com.example.droosapps.fusion.FusionEventsGenerator;

@ApplicationAnnotation(name = "DroolsFusionApp")
public class FusionApplication implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    FusionEventsGenerator eventsGenerator = dag.addOperator("EventsGenerator", FusionEventsGenerator.class);
    DroolsFusionOperator droolsOperator = dag.addOperator("droolsProcessor", DroolsFusionOperator.class);
    ConsoleOutputOperator console = dag.addOperator("console", ConsoleOutputOperator.class);

    dag.addStream("InputEvents", eventsGenerator.eventsOut, droolsOperator.input).setLocality(Locality.CONTAINER_LOCAL);
    dag.addStream("BaggageStatus", droolsOperator.output, console.input).setLocality(Locality.CONTAINER_LOCAL);
  }

}

package com.example.droosapps.model;

import java.util.Date;

/**
 * Base interface for all our <code>events</code>
 * 
 */
public interface Event extends Fact
{

  public abstract Date getTimestamp();

}

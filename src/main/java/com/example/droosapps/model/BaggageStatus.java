package com.example.droosapps.model;

public class BaggageStatus
{
  private String baggageId;
  private boolean isLost;

  private BaggageStatus()
  {

  }

  public BaggageStatus(String baggageId)
  {
    this.baggageId = baggageId;
  }

  public BaggageStatus(String baggageId, boolean isLost)
  {
    super();
    this.baggageId = baggageId;
    this.isLost = isLost;
  }

  public String getBaggageId()
  {
    return baggageId;
  }

  public void setBaggageId(String baggageId)
  {
    this.baggageId = baggageId;
  }

  public boolean isLost()
  {
    return isLost;
  }

  public void setLost(boolean isLost)
  {
    this.isLost = isLost;
  }

  @Override
  public String toString()
  {
    return "BaggageStatus [baggageId=" + baggageId + ", isLost=" + isLost + "]";
  }

}

package org.apache.hadoop.mapred.nativetask.serde;

public enum SerializationType {
  Writable(0),
  Native(1);
  
  private int type;
  
  SerializationType(int type) {
    this.type = type;
  }
  
  public int getType() {
    return type;
  }
};

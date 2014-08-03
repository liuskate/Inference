
package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Represents a directive from the {@link org.apache.hadoop.mapred.JobTracker} 
 * to the {@link org.apache.hadoop.mapred.TaskTracker} to set the tasktracker memory threshold.
 * 
 */
class SetTTMemoryThreshold extends TaskTrackerAction {
  private int threshold;

  public SetTTMemoryThreshold() {
    super(ActionType.SET_MEMORY_THRESHOLD);
  }

  public SetTTMemoryThreshold(int threshold) {
    super(ActionType.SET_MEMORY_THRESHOLD);
    this.threshold = threshold;
  }

  public int getThreshold() {
    return threshold;
  }

  public void write(DataOutput out) throws IOException {
    out.writeInt(threshold);
  }

  public void readFields(DataInput in) throws IOException {
    threshold = in.readInt();
  }
}

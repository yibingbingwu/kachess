package project.kachess.sql_lineage.util;

import java.util.concurrent.atomic.AtomicLong;

/**
 * The auto-increment ID generator: to make insert deep relationship easier. Applied across all
 * objects
 */
public final class AutoIncrement {

  public static AtomicLong idCounter = null;

  public static final void setCounter(Long newVal) {
    if (newVal != null) {
      // Allow the value to be manually set.
      // Designed for testing when every run will have the same IDs
      idCounter = new AtomicLong(newVal);
    } else {
      idCounter = new AtomicLong(System.currentTimeMillis() / 1000);
    }
  }

  public static final Long nextId() {
    return idCounter.incrementAndGet();
  }
}

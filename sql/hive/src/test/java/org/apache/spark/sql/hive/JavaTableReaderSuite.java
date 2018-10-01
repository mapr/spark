package org.apache.spark.sql.hive;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeException;
import org.apache.hadoop.hive.serde2.avro.InstanceCache;
import org.junit.Assert;
import org.junit.Test;

public class JavaTableReaderSuite {

  private static InstanceCache<String, Wrapper<String>> instanceCache = new InstanceCache<String, Wrapper<String>>() {
    @Override
    protected Wrapper<String> makeInstance(String hv, Set<String> seenSchemas) {
      return new Wrapper<String>(hv);
    }
  };

  private static volatile Wrapper<String> cachedInstance;
  private static volatile String key = "key1";
  private static AtomicBoolean failed = new AtomicBoolean(false);
  private static AtomicBoolean succeed = new AtomicBoolean(false);

  private static class Wrapper<T> {

    final T wrapped;

    private Wrapper(T wrapped) {
      this.wrapped = wrapped;
    }
  }


  /**
   * [BUG-32013] Tests if {@link InstanceCache#retrieve(Object, Set)} method is thread safe.
   *
   * @see <a href="http://10.250.1.25/show_bug.cgi?id=32013">Bug 32013 - NullPointerException in
   * AvroObjectInspectorGenerator</a>
   */
  @Test
  public void instanceCacheMustBeThreadSafe() throws Exception {

    // This thread is used to retrieve cached instances
    new Thread(() -> {
      long valueGetterThreadStarted = System.currentTimeMillis();
      while (!failed.get() && !succeed.get()) {

        Wrapper<String> retrievedInstance = null;
        try {
          retrievedInstance = instanceCache.retrieve(key, null);
        } catch (AvroSerdeException e) {
          e.printStackTrace();
        }

        if (cachedInstance != null) {
          if (cachedInstance != retrievedInstance) {
            // Got different instances. InstanceCache is not thread safe. Test is failed.
            failed.set(true);
          } else {
            // Change the key, so new instance will be cached by another thread
            key = String.valueOf(System.currentTimeMillis());
            cachedInstance = null;
          }
        }

        if (System.currentTimeMillis() - valueGetterThreadStarted >= 3_000L) {
          succeed.set(true);
        }

      }
    }).start();

    // Current thread is used to cache new instances
    while (!failed.get() && !succeed.get()) {
      // Cache a new instance, so it will be retrieved by another thread
      if (cachedInstance == null) {
        cachedInstance = instanceCache.retrieve(key, null);
      }
    }

    if (failed.get()) {
      Assert.fail("InstanceCache is not thread safe");
    }

  }

}

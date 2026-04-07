package org.apache.spark.ui.filters.hadoop_auth_copy;

import jakarta.servlet.ServletContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public abstract class RolloverSignerSecretProvider extends SignerSecretProvider {
  private static final Logger LOG = LoggerFactory.getLogger(RolloverSignerSecretProvider.class);

  private volatile byte[][] secrets;
  private ScheduledExecutorService scheduler;
  private boolean schedulerRunning;
  private boolean isDestroyed;

  public RolloverSignerSecretProvider() {
    schedulerRunning = false;
    isDestroyed = false;
  }

  @Override
  public void init(Properties config, ServletContext servletContext, long tokenValidity) throws Exception {
    initSecrets(generateNewSecret(), null);
    startScheduler(tokenValidity, tokenValidity);
  }

  protected void initSecrets(byte[] currentSecret, byte[] previousSecret) {
    secrets = new byte[][]{currentSecret, previousSecret};
  }

  protected synchronized void startScheduler(long initialDelay, long period) {
    if (!schedulerRunning) {
      schedulerRunning = true;
      scheduler = Executors.newSingleThreadScheduledExecutor();
      scheduler.scheduleAtFixedRate(this::rollSecret, initialDelay, period, TimeUnit.MILLISECONDS);
    }
  }

  @Override
  public synchronized void destroy() {
    if (!isDestroyed) {
      isDestroyed = true;
      if (scheduler != null) {
        scheduler.shutdown();
      }
      schedulerRunning = false;
      super.destroy();
    }
  }

  protected synchronized void rollSecret() {
    if (!isDestroyed) {
      LOG.debug("rolling secret");
      byte[] newSecret = generateNewSecret();
      secrets = new byte[][]{newSecret, secrets[0]};
    }
  }

  protected abstract byte[] generateNewSecret();

  @Override
  public byte[] getCurrentSecret() {
    return secrets[0];
  }

  @Override
  public byte[][] getAllSecrets() {
    return secrets;
  }
}

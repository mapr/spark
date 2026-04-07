package org.apache.spark.ui.filters.hadoop_auth_copy;

import java.security.SecureRandom;
import java.util.Random;

public class RandomSignerSecretProvider extends RolloverSignerSecretProvider {

  private final Random rand;

  public RandomSignerSecretProvider() {
    super();
    rand = new SecureRandom();
  }

  public RandomSignerSecretProvider(long seed) {
    super();
    rand = new Random(seed);
  }

  @Override
  protected byte[] generateNewSecret() {
    byte[] secret = new byte[32];
    rand.nextBytes(secret);
    return secret;
  }
}

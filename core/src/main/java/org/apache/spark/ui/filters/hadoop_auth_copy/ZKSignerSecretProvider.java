package org.apache.spark.ui.filters.hadoop_auth_copy;

import jakarta.servlet.ServletContext;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Properties;
import java.util.Random;

public class ZKSignerSecretProvider extends RolloverSignerSecretProvider {
  private static final String CONFIG_PREFIX = "signer.secret.provider.zookeeper.";
  public static final String ZOOKEEPER_CONNECTION_STRING = CONFIG_PREFIX + "connection.string";
  public static final String ZOOKEEPER_PATH = CONFIG_PREFIX + "path";
  public static final String ZOOKEEPER_AUTH_TYPE = CONFIG_PREFIX + "auth.type";
  public static final String ZOOKEEPER_KERBEROS_KEYTAB = CONFIG_PREFIX + "kerberos.keytab";
  public static final String ZOOKEEPER_KERBEROS_PRINCIPAL = CONFIG_PREFIX + "kerberos.principal";
  public static final String ZOOKEEPER_SSL_ENABLED = CONFIG_PREFIX + "ssl.enabled";
  public static final String ZOOKEEPER_SSL_KEYSTORE_LOCATION = CONFIG_PREFIX + "ssl.keystore.location";
  public static final String ZOOKEEPER_SSL_KEYSTORE_PASSWORD = CONFIG_PREFIX + "ssl.keystore.password";
  public static final String ZOOKEEPER_SSL_TRUSTSTORE_LOCATION = CONFIG_PREFIX + "ssl.truststore.location";
  public static final String ZOOKEEPER_SSL_TRUSTSTORE_PASSWORD = CONFIG_PREFIX + "ssl.truststore.password";
  public static final String DISCONNECT_FROM_ZOOKEEPER_ON_SHUTDOWN = CONFIG_PREFIX + "disconnect.on.shutdown";
  public static final String ZOOKEEPER_SIGNER_SECRET_PROVIDER_CURATOR_CLIENT_ATTRIBUTE =
      CONFIG_PREFIX + "curator.client";

  private static final String JAAS_LOGIN_ENTRY_NAME = "ZKSignerSecretProviderClient";
  private static Logger LOG = LoggerFactory.getLogger(ZKSignerSecretProvider.class);
  private static int INT_BYTES = Integer.SIZE / Byte.SIZE;
  private static int LONG_BYTES = Long.SIZE / Byte.SIZE;
  private static int DATA_VERSION = 0;

  private String path;
  private volatile byte[] nextSecret;
  private final Random rand;
  private int zkVersion;
  private long nextRolloverDate;
  private long tokenValidity;
  private CuratorFramework client;
  private boolean shouldDisconnect;

  public ZKSignerSecretProvider() {
    super();
    rand = new SecureRandom();
  }

  public ZKSignerSecretProvider(long seed) {
    super();
    rand = new Random(seed);
  }

  @Override
  public void init(Properties config, ServletContext servletContext, long tokenValidity) throws Exception {
    Object curatorClientObj =
        servletContext.getAttribute(ZOOKEEPER_SIGNER_SECRET_PROVIDER_CURATOR_CLIENT_ATTRIBUTE);
    if (curatorClientObj instanceof CuratorFramework) {
      client = (CuratorFramework) curatorClientObj;
    } else {
      client = createCuratorClient(config);
      servletContext.setAttribute(ZOOKEEPER_SIGNER_SECRET_PROVIDER_CURATOR_CLIENT_ATTRIBUTE, client);
    }
    this.tokenValidity = tokenValidity;
    shouldDisconnect = Boolean.parseBoolean(config.getProperty(DISCONNECT_FROM_ZOOKEEPER_ON_SHUTDOWN, "true"));
    path = config.getProperty(ZOOKEEPER_PATH);
    if (path == null) {
      throw new IllegalArgumentException(ZOOKEEPER_PATH + " must be specified");
    }
    try {
      nextRolloverDate = System.currentTimeMillis() + tokenValidity;
      client
          .create()
          .creatingParentsIfNeeded()
          .forPath(path, generateZKData(generateRandomSecret(), generateRandomSecret(), null));
      zkVersion = 0;
      LOG.info("Creating secret znode");
    } catch (KeeperException.NodeExistsException nee) {
      LOG.info("The secret znode already exists, retrieving data");
    }
    pullFromZK(true);
    long initialDelay = nextRolloverDate - System.currentTimeMillis();
    if (initialDelay < 1L) {
      int i = 1;
      while (initialDelay < 1L) {
        initialDelay = nextRolloverDate + tokenValidity * i - System.currentTimeMillis();
        i++;
      }
    }
    super.startScheduler(initialDelay, tokenValidity);
  }

  @Override
  public void destroy() {
    if (shouldDisconnect && client != null) {
      client.close();
    }
    super.destroy();
  }

  @Override
  protected synchronized void rollSecret() {
    super.rollSecret();
    nextRolloverDate += tokenValidity;
    byte[][] secrets = super.getAllSecrets();
    pushToZK(generateRandomSecret(), secrets[0], secrets[1]);
    pullFromZK(false);
  }

  @Override
  protected byte[] generateNewSecret() {
    return nextSecret;
  }

  private synchronized void pushToZK(byte[] newSecret, byte[] currentSecret, byte[] previousSecret) {
    byte[] bytes = generateZKData(newSecret, currentSecret, previousSecret);
    try {
      client.setData().withVersion(zkVersion).forPath(path, bytes);
    } catch (KeeperException.BadVersionException bve) {
      LOG.debug("Unable to push to znode; another server already did it");
    } catch (Exception ex) {
      LOG.error("Unexpected exception pushing data to ZooKeeper", ex);
    }
  }

  private synchronized byte[] generateZKData(byte[] newSecret, byte[] currentSecret, byte[] previousSecret) {
    int newSecretLength = newSecret.length;
    int currentSecretLength = currentSecret.length;
    int previousSecretLength = previousSecret == null ? 0 : previousSecret.length;
    ByteBuffer bb =
        ByteBuffer.allocate(
            INT_BYTES
                + INT_BYTES
                + newSecretLength
                + INT_BYTES
                + currentSecretLength
                + INT_BYTES
                + previousSecretLength
                + LONG_BYTES);
    bb.putInt(DATA_VERSION);
    bb.putInt(newSecretLength);
    bb.put(newSecret);
    bb.putInt(currentSecretLength);
    bb.put(currentSecret);
    bb.putInt(previousSecretLength);
    if (previousSecretLength > 0) {
      bb.put(previousSecret);
    }
    bb.putLong(nextRolloverDate);
    return bb.array();
  }

  private synchronized void pullFromZK(boolean isInit) {
    try {
      Stat stat = new Stat();
      byte[] bytes = client.getData().storingStatIn(stat).forPath(path);
      ByteBuffer bb = ByteBuffer.wrap(bytes);
      int dataVersion = bb.getInt();
      if (dataVersion > DATA_VERSION) {
        throw new IllegalStateException("Cannot load data from ZooKeeper; data written with newer version");
      }
      int nextSecretLength = bb.getInt();
      byte[] readNextSecret = new byte[nextSecretLength];
      bb.get(readNextSecret);
      this.nextSecret = readNextSecret;
      zkVersion = stat.getVersion();
      if (isInit) {
        int currentSecretLength = bb.getInt();
        byte[] currentSecret = new byte[currentSecretLength];
        bb.get(currentSecret);
        int previousSecretLength = bb.getInt();
        byte[] previousSecret = null;
        if (previousSecretLength > 0) {
          previousSecret = new byte[previousSecretLength];
          bb.get(previousSecret);
        }
        super.initSecrets(currentSecret, previousSecret);
        nextRolloverDate = bb.getLong();
      }
    } catch (Exception ex) {
      LOG.error("Unexpected exception while pulling data from ZooKeeper", ex);
    }
  }

  protected byte[] generateRandomSecret() {
    byte[] secret = new byte[32];
    rand.nextBytes(secret);
    return secret;
  }

  protected CuratorFramework createCuratorClient(Properties config) {
    String connectionString = config.getProperty(ZOOKEEPER_CONNECTION_STRING, "localhost:2181");
    String authType = config.getProperty(ZOOKEEPER_AUTH_TYPE, "none");
    String keytab = config.getProperty(ZOOKEEPER_KERBEROS_KEYTAB, "").trim();
    String principal = config.getProperty(ZOOKEEPER_KERBEROS_PRINCIPAL, "").trim();
    boolean sslEnabled = Boolean.parseBoolean(config.getProperty(ZOOKEEPER_SSL_ENABLED, "false"));
    String keystoreLocation = config.getProperty(ZOOKEEPER_SSL_KEYSTORE_LOCATION, "");
    String keystorePassword = config.getProperty(ZOOKEEPER_SSL_KEYSTORE_PASSWORD, "");
    String truststoreLocation = config.getProperty(ZOOKEEPER_SSL_TRUSTSTORE_LOCATION, "");
    String truststorePassword = config.getProperty(ZOOKEEPER_SSL_TRUSTSTORE_PASSWORD, "");

    CuratorFramework zkClient =
        ZookeeperClient.configure()
            .withConnectionString(connectionString)
            .withAuthType(authType)
            .withKeytab(keytab)
            .withPrincipal(principal)
            .withJaasLoginEntryName(JAAS_LOGIN_ENTRY_NAME)
            .enableSSL(sslEnabled)
            .withKeystore(keystoreLocation)
            .withKeystorePassword(keystorePassword)
            .withTruststore(truststoreLocation)
            .withTruststorePassword(truststorePassword)
            .create();
    zkClient.start();
    return zkClient;
  }
}

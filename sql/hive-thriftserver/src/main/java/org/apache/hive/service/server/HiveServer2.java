/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.service.server;

import org.apache.commons.cli.*;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.hive.common.JvmPauseMonitor;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.util.ZooKeeperHiveHelper;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.common.util.HiveStringUtils;
import org.apache.hive.service.CompositeService;
import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.cli.thrift.ThriftBinaryCLIService;
import org.apache.hive.service.cli.thrift.ThriftCLIService;
import org.apache.hive.service.cli.thrift.ThriftHttpCLIService;
import org.apache.spark.util.ShutdownHookManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.graalvm.compiler.core.common.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.runtime.AbstractFunction0;
import scala.runtime.BoxedUnit;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hive.conf.HiveConf.setLoadHiveServer2Config;

/**
 * HiveServer2.
 *
 */
public class HiveServer2 extends CompositeService {
  private static final Logger LOG = LoggerFactory.getLogger(HiveServer2.class);

  private CLIService cliService;
  private ThriftCLIService thriftCLIService;
  private CuratorFramework zooKeeperClient;
  private PersistentEphemeralNode znode;
  private String znodePath;


  public HiveServer2() {
    super(HiveServer2.class.getSimpleName());
    setLoadHiveServer2Config(true);
  }

  @Override
  public synchronized void init(HiveConf hiveConf) {
    cliService = new CLIService(this);
    addService(cliService);
    if (isHTTPTransportMode(hiveConf)) {
      thriftCLIService = new ThriftHttpCLIService(cliService);
    } else {
      thriftCLIService = new ThriftBinaryCLIService(cliService);
    }
    addService(thriftCLIService);
    super.init(hiveConf);

    // Add a shutdown hook for catching SIGTERM & SIGINT
    // this must be higher than the Hadoop Filesystem priority of 10,
    // which the default priority is.
    // The signature of the callback must match that of a scala () -> Unit
    // function
    ShutdownHookManager.addShutdownHook(
        new AbstractFunction0<BoxedUnit>() {
          public BoxedUnit apply() {
            try {
              LOG.info("Hive Server Shutdown hook invoked");
              stop();
            } catch (Throwable e) {
              LOG.warn("Ignoring Exception while stopping Hive Server from shutdown hook",
                  e);
            }
            return BoxedUnit.UNIT;
          }
        });
  }

  public static boolean isHTTPTransportMode(HiveConf hiveConf) {
    String transportMode = System.getenv("HIVE_SERVER2_TRANSPORT_MODE");
    if (transportMode == null) {
      transportMode = hiveConf.getVar(ConfVars.HIVE_SERVER2_TRANSPORT_MODE);
    }
    return transportMode != null && (transportMode.equalsIgnoreCase("http"));
  }

  @Override
  public synchronized void start() {
    super.start();
    HiveConf hiveConf = new HiveConf();
    String zooKeeperEnsemble = ZooKeeperHiveHelper.getQuorumServers(hiveConf);
    LOG.info("Zookeeper ensemble: "+zooKeeperEnsemble);
    LOG.info("Hive config: "+hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_SUPPORT_DYNAMIC_SERVICE_DISCOVERY));
    if (hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_SUPPORT_DYNAMIC_SERVICE_DISCOVERY)) {
      try
      {
        this.addServerInstanceToZooKeeper(hiveConf);
      } catch (Exception e)
      {
        LOG.error(e.getMessage(),e);
      }
    }
  }

  private String getServerInstanceURI() throws Exception {
    if ((thriftCLIService == null) || (thriftCLIService.getServerIPAddress() == null)) {
      throw new Exception("Unable to get the server address; it hasn't been initialized yet.");
    }
    return thriftCLIService.getServerIPAddress().getHostName() + ":"
            + thriftCLIService.getPortNumber();
  }

  private void addConfsToPublish(HiveConf hiveConf, Map<String, String> confsToPublish)
          throws UnknownHostException {
    // Hostname
    confsToPublish.put(ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST.varname,
            InetAddress.getLocalHost().getCanonicalHostName());
    // Transport mode
    confsToPublish.put(ConfVars.HIVE_SERVER2_TRANSPORT_MODE.varname,
            hiveConf.getVar(ConfVars.HIVE_SERVER2_TRANSPORT_MODE));
    // Transport specific confs
    if (isHTTPTransportMode(hiveConf)) {
      confsToPublish.put(ConfVars.HIVE_SERVER2_THRIFT_HTTP_PORT.varname,
              hiveConf.getVar(ConfVars.HIVE_SERVER2_THRIFT_HTTP_PORT));
      confsToPublish.put(ConfVars.HIVE_SERVER2_THRIFT_HTTP_PATH.varname,
              hiveConf.getVar(ConfVars.HIVE_SERVER2_THRIFT_HTTP_PATH));
    } else {
      confsToPublish.put(ConfVars.HIVE_SERVER2_THRIFT_PORT.varname,
              hiveConf.getVar(ConfVars.HIVE_SERVER2_THRIFT_PORT));
      confsToPublish.put(ConfVars.HIVE_SERVER2_THRIFT_SASL_QOP.varname,
              hiveConf.getVar(ConfVars.HIVE_SERVER2_THRIFT_SASL_QOP));
    }
    // Auth specific confs
    confsToPublish.put(ConfVars.HIVE_SERVER2_AUTHENTICATION.varname,
            hiveConf.getVar(ConfVars.HIVE_SERVER2_AUTHENTICATION));
    confsToPublish.put(ConfVars.HIVE_SERVER2_USE_SSL.varname,
            hiveConf.getVar(ConfVars.HIVE_SERVER2_USE_SSL));
  }

  @InterfaceAudience.Public
  public static class ZooDefs {

    @InterfaceAudience.Public
    public interface Perms {
      int READ = 1 << 0;

      int WRITE = 1 << 1;

      int CREATE = 1 << 2;

      int DELETE = 1 << 3;

      int ADMIN = 1 << 4;

      int ALL = READ | WRITE | CREATE | DELETE | ADMIN;
    }

    @InterfaceAudience.Public
    public interface Ids {
      /**
       * This Id represents anyone.
       */
      Id ANYONE_ID_UNSAFE = new Id("world", "anyone");

      /**
       * This Id is only usable to set ACLs. It will get substituted with the
       * Id's the client authenticated with.
       */
      Id AUTH_IDS = new Id("auth", "");

      /**
       * This is a completely open ACL .
       */
      @SuppressFBWarnings(value = "MS_MUTABLE_COLLECTION", justification = "Cannot break API")
      ArrayList<ACL> OPEN_ACL_UNSAFE = new ArrayList<>(
              Collections.singletonList(new ACL(org.apache.zookeeper.ZooDefs.Perms.ALL, ANYONE_ID_UNSAFE)));

      /**
       * This ACL gives the world the ability to read.
       */
      @SuppressFBWarnings(value = "MS_MUTABLE_COLLECTION", justification = "Cannot break API")
      ArrayList<ACL> READ_ACL_UNSAFE = new ArrayList<>(
              Collections
                      .singletonList(new ACL(org.apache.zookeeper.ZooDefs.Perms.READ, ANYONE_ID_UNSAFE)));
    }

  }


  private final ACLProvider zooKeeperAclProvider = new ACLProvider() {
    List<ACL> nodeAcls = new ArrayList<>();

    @Override
    public List<org.apache.zookeeper.data.ACL> getDefaultAcl() {
      if (UserGroupInformation.isSecurityEnabled()) {
        // Read all to the world
        nodeAcls.addAll(ZooDefs.Ids.READ_ACL_UNSAFE);
        // Create/Delete/Write/Admin to the authenticated user
        nodeAcls.add(new ACL(ZooDefs.Perms.ALL, ZooDefs.Ids.AUTH_IDS));
      } else {
        // ACLs for znodes on a non-kerberized cluster
        // Create/Read/Delete/Write/Admin to the world
        nodeAcls.addAll(ZooDefs.Ids.OPEN_ACL_UNSAFE);
      }
      return nodeAcls;
    }

    @Override
    public List<ACL> getAclForPath(String path) {
      return getDefaultAcl();
    }
  };

  private class DeRegisterWatcher implements Watcher {
    @Override
    public void process(WatchedEvent event) {
      if (event.getType().equals(Watcher.Event.EventType.NodeDeleted)) {
        if (znode != null) {
          try {
            znode.close();
            LOG.warn("This HiveServer2 instance is now de-registered from ZooKeeper. "
                    + "The server will be shut down after the last client sesssion completes.");
          } catch (IOException e) {
            LOG.error("Failed to close the persistent ephemeral znode", e);
          } finally {
            HiveServer2.this.setRegisteredWithZooKeeper();
            // If there are no more active client sessions, stop the server
            if (cliService.getSessionManager().getOpenSessionCount() == 0) {
              LOG.warn("This instance of HiveServer2 has been removed from the list of server "
                      + "instances available for dynamic service discovery. "
                      + "The last client session has ended - will shutdown now.");
              HiveServer2.this.stop();
            }
          }
        }
      }
    }
  }


  private void addServerInstanceToZooKeeper(HiveConf hiveConf) throws Exception {
    String zooKeeperEnsemble = ZooKeeperHiveHelper.getQuorumServers(hiveConf);
    LOG.info("Register on zookeeper: "+zooKeeperEnsemble);
    String rootNamespace = hiveConf.getVar(ConfVars.HIVE_SERVER2_ZOOKEEPER_NAMESPACE);
    String instanceURI = getServerInstanceURI();
//    setUpZooKeeperAuth(hiveConf);
    Map<String, String> confsToPublish = new HashMap<>();
    addConfsToPublish(hiveConf, confsToPublish);
    int sessionTimeout =
            (int) hiveConf.getTimeVar(ConfVars.HIVE_ZOOKEEPER_SESSION_TIMEOUT,
                    TimeUnit.MILLISECONDS);
    int baseSleepTime =
            (int) hiveConf.getTimeVar(ConfVars.HIVE_ZOOKEEPER_CONNECTION_BASESLEEPTIME,
                    TimeUnit.MILLISECONDS);
    int maxRetries = hiveConf.getIntVar(ConfVars.HIVE_ZOOKEEPER_CONNECTION_MAX_RETRIES);
    // Create a CuratorFramework instance to be used as the ZooKeeper client
    // Use the zooKeeperAclProvider to create appropriate ACLs
    zooKeeperClient =
            CuratorFrameworkFactory.builder().connectString(zooKeeperEnsemble)
                    .sessionTimeoutMs(sessionTimeout).aclProvider(zooKeeperAclProvider)
                    .retryPolicy(new ExponentialBackoffRetry(baseSleepTime, maxRetries)).build();
    LOG.info("hive server: start thrift server");
    zooKeeperClient.start();
    // Create the parent znodes recursively; ignore if the parent already exists.
    try {
      zooKeeperClient.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
              .forPath(ZooKeeperHiveHelper.ZOOKEEPER_PATH_SEPARATOR + rootNamespace);
      LOG.info("Created the root name space: " + rootNamespace + " on ZooKeeper for HiveServer2");
    } catch (KeeperException e) {
      if (e.code() != KeeperException.Code.NODEEXISTS) {
        LOG.error("Unable to create HiveServer2 namespace: " + rootNamespace + " on ZooKeeper", e);
        throw e;
      }
    }

    // Create a znode under the rootNamespace parent for this instance of the server
    // Znode name: serverUri=host:port;version=versionInfo;sequence=sequenceNumber
    try {
      String pathPrefix = ZooKeeperHiveHelper.ZOOKEEPER_PATH_SEPARATOR + rootNamespace
              + ZooKeeperHiveHelper.ZOOKEEPER_PATH_SEPARATOR + "serverUri=" + instanceURI + ";"
              + "version=" + ";" + "sequence=";

      // Publish configs for this instance as the data on the node
      String znodeData = confsToPublish.get(ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST.varname) + ":" +
              confsToPublish.get(ConfVars.HIVE_SERVER2_THRIFT_PORT.varname);
      byte[] znodeDataUTF8 = znodeData.getBytes(StandardCharsets.UTF_8);
      znode = new PersistentEphemeralNode(zooKeeperClient,
              PersistentEphemeralNode.Mode.EPHEMERAL_SEQUENTIAL, pathPrefix, znodeDataUTF8);
      znode.start();
      // We'll wait for 120s for node creation
      long znodeCreationTimeout = 120;
      if (!znode.waitForInitialCreate(znodeCreationTimeout, TimeUnit.SECONDS)) {
        throw new Exception("Max znode creation wait time: " + znodeCreationTimeout + "s exhausted");
      }
      setRegisteredWithZooKeeper();
      znodePath = znode.getActualPath();
      // Set a watch on the znode
      if (zooKeeperClient.checkExists().usingWatcher(new DeRegisterWatcher()).forPath(znodePath) == null) {
        // No node exists, throw exception
        throw new Exception("Unable to create znode for this HiveServer2 instance on ZooKeeper.");
      }
      LOG.info("Created a znode on ZooKeeper for HiveServer2 uri: " + instanceURI);
    } catch (Exception e) {
      LOG.error("Unable to create a znode for this server instance", e);
      if (znode != null) {
        znode.close();
      }
      throw (e);
    }
  }
  private void setRegisteredWithZooKeeper() {
  }

  @Override
  public synchronized void stop() {
    LOG.info("Shutting down HiveServer2");
    super.stop();
  }

  private static void startHiveServer2() {
    long attempts = 0, maxAttempts = 1;
    while (true) {
      LOG.info("Starting HiveServer2");
      HiveConf hiveConf = new HiveConf();
      maxAttempts = hiveConf.getLongVar(ConfVars.HIVE_SERVER2_MAX_START_ATTEMPTS);
      HiveServer2 server = null;
      try {
        server = new HiveServer2();
        server.init(hiveConf);
        server.start();
        try {
          JvmPauseMonitor pauseMonitor = new JvmPauseMonitor(hiveConf);
          pauseMonitor.start();
        } catch (Throwable t) {
          LOG.warn("Could not initiate the JvmPauseMonitor thread.", t);
        }
        break;
      } catch (Throwable throwable) {
        if (server != null) {
          try {
            server.stop();
          } catch (Throwable t) {
            LOG.info("Exception caught when calling stop of HiveServer2 before retrying start", t);
          } finally {
          }
        }
        if (++attempts >= maxAttempts) {
          throw new Error("Max start attempts " + maxAttempts + " exhausted", throwable);
        } else {
          LOG.warn("Error starting HiveServer2 on attempt " + attempts
              + ", will retry in 60 seconds", throwable);
          try {
            Thread.sleep(60L * 1000L);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
      }
    }
  }

  public static void main(String[] args) {
    setLoadHiveServer2Config(true);
    ServerOptionsProcessor oproc = new ServerOptionsProcessor("hiveserver2");
    ServerOptionsProcessorResponse oprocResponse = oproc.parse(args);

    HiveStringUtils.startupShutdownMessage(HiveServer2.class, args, LOG);

    // Call the executor which will execute the appropriate command based on the parsed options
    oprocResponse.getServerOptionsExecutor().execute();
  }

  /**
   * ServerOptionsProcessor.
   * Process arguments given to HiveServer2 (-hiveconf property=value)
   * Set properties in System properties
   * Create an appropriate response object,
   * which has executor to execute the appropriate command based on the parsed options.
   */
  public static class ServerOptionsProcessor {
    private final Options options = new Options();
    private org.apache.commons.cli.CommandLine commandLine;
    private final String serverName;
    private final StringBuilder debugMessage = new StringBuilder();

    @SuppressWarnings("static-access")
    public ServerOptionsProcessor(String serverName) {
      this.serverName = serverName;
      // -hiveconf x=y
      options.addOption(OptionBuilder
          .withValueSeparator()
          .hasArgs(2)
          .withArgName("property=value")
          .withLongOpt("hiveconf")
          .withDescription("Use value for given property")
          .create());
      options.addOption(new Option("H", "help", false, "Print help information"));
    }

    public ServerOptionsProcessorResponse parse(String[] argv) {
      try {
        commandLine = new GnuParser().parse(options, argv);
        // Process --hiveconf
        // Get hiveconf param values and set the System property values
        Properties confProps = commandLine.getOptionProperties("hiveconf");
        for (String propKey : confProps.stringPropertyNames()) {
          // save logging message for log4j output latter after log4j initialize properly
          debugMessage.append("Setting " + propKey + "=" + confProps.getProperty(propKey) + ";\n");
          System.setProperty(propKey, confProps.getProperty(propKey));
        }

        // Process --help
        if (commandLine.hasOption('H')) {
          return new ServerOptionsProcessorResponse(new HelpOptionExecutor(serverName, options));
        }
      } catch (ParseException e) {
        // Error out & exit - we were not able to parse the args successfully
        System.err.println("Error starting HiveServer2 with given arguments: ");
        System.err.println(e.getMessage());
        System.exit(-1);
      }
      // Default executor, when no option is specified
      return new ServerOptionsProcessorResponse(new StartOptionExecutor());
    }

  }

  /**
   * The response sent back from {@link ServerOptionsProcessor#parse(String[])}
   */
  static class ServerOptionsProcessorResponse {
    private final ServerOptionsExecutor serverOptionsExecutor;

    ServerOptionsProcessorResponse(ServerOptionsExecutor serverOptionsExecutor) {
      this.serverOptionsExecutor = serverOptionsExecutor;
    }

    ServerOptionsExecutor getServerOptionsExecutor() {
      return serverOptionsExecutor;
    }
  }

  /**
   * The executor interface for running the appropriate HiveServer2 command based on parsed options
   */
  interface ServerOptionsExecutor {
    void execute();
  }

  /**
   * HelpOptionExecutor: executes the --help option by printing out the usage
   */
  static class HelpOptionExecutor implements ServerOptionsExecutor {
    private final Options options;
    private final String serverName;

    HelpOptionExecutor(String serverName, Options options) {
      this.options = options;
      this.serverName = serverName;
    }

    @Override
    public void execute() {
      new HelpFormatter().printHelp(serverName, options);
      System.exit(0);
    }
  }

  /**
   * StartOptionExecutor: starts HiveServer2.
   * This is the default executor, when no option is specified.
   */
  static class StartOptionExecutor implements ServerOptionsExecutor {
    @Override
    public void execute() {
      try {
        startHiveServer2();
      } catch (Throwable t) {
        LOG.error("Error starting HiveServer2", t);
        System.exit(-1);
      }
    }
  }
}

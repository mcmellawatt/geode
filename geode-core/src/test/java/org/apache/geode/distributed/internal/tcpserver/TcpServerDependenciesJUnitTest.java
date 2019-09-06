package org.apache.geode.distributed.internal.tcpserver;

import static com.tngtech.archunit.base.DescribedPredicate.not;
import static com.tngtech.archunit.core.domain.JavaClass.Predicates.assignableTo;
import static com.tngtech.archunit.core.domain.JavaClass.Predicates.resideInAPackage;
import static com.tngtech.archunit.core.domain.JavaClass.Predicates.type;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;

import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.junit.ArchUnitRunner;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.runner.RunWith;

import org.apache.geode.CancelException;
import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.SystemFailure;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.cache.UnsupportedVersionException;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.PoolStatHelper;
import org.apache.geode.internal.DSFIDFactory;
import org.apache.geode.internal.GemFireVersion;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.VersionedDataInputStream;
import org.apache.geode.internal.VersionedDataOutputStream;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.client.protocol.ClientProtocolProcessor;
import org.apache.geode.internal.cache.client.protocol.ClientProtocolService;
import org.apache.geode.internal.cache.client.protocol.ClientProtocolServiceLoader;
import org.apache.geode.internal.cache.tier.CommunicationMode;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.LoggingExecutors;
import org.apache.geode.internal.logging.LoggingThread;
import org.apache.geode.internal.net.SSLConfigurationFactory;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.internal.security.SecurableCommunicationChannel;

@RunWith(ArchUnitRunner.class)
@AnalyzeClasses(packages = "org.apache.geode.distributed.internal.tcpserver..")
public class TcpServerDependenciesJUnitTest {
  @ArchTest
  public static final ArchRule membershipDoesntDependOnCoreProvisional = classes()
      .that()
      .resideInAPackage("org.apache.geode.distributed.internal.tcpserver..")

      .should()
      .onlyDependOnClassesThat(
          resideInAPackage("org.apache.geode.distributed.internal.tcpserver..")

              .or(not(resideInAPackage("org.apache.geode..")))
              .or(resideInAPackage("org.apache.geode.test.."))

              // TODO: Break dependency on geode logger
              .or(assignableTo(LoggingThread.class))
              .or(type(LogService.class))
              .or(type(LoggingExecutors.class))

              // TODO
              .or(type(SystemFailure.class))

              // TODO: Serialization needs to become its own module
              .or(type(DSFIDFactory.class))
              .or(type(DataSerializer.class))
              .or(type(DataSerializable.class))
              .or(type(VersionedDataOutputStream.class))
              .or(type(VersionedDataInputStream.class))
              .or(type(Version.class))
              .or(type(GemFireVersion.class))
              .or(type(UnsupportedVersionException.class))

              // TODO: Address config related dependencies
              .or(type(DistributionConfig.class))
              .or(type(DistributionConfigImpl.class))
              .or(type(InternalConfigurationPersistenceService.class))

              // TODO: Address cache/DS related dependencies
              .or(type(DistributedSystem.class))
              .or(type(InternalDistributedSystem.class))
              .or(type(DistributionStats.class))
              .or(type(InternalCache.class))
              .or(type(GemFireCache.class))

              // TODO: Address locator related dependencies
              .or(type(InternalLocator.class))

              // TODO: Address comms related dependencies in core
              .or(type(ClientProtocolServiceLoader.class))
              .or(type(ClientProtocolService.class))
              .or(type(ClientProtocolProcessor.class))
              .or(type(SSLConfigurationFactory.class))
              .or(type(CommunicationMode.class))
              .or(type(SecurableCommunicationChannel.class))

              // TODO: Miscellaneous
              .or(type(PoolStatHelper.class))
              .or(type(CancelException.class))

              // These may be reasonable dependencies
              .or(type(SocketCreator.class))
              .or(type(SocketCreatorFactory.class)));
}

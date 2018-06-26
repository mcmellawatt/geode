/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.cache.ha;

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import util.TestException;

import org.apache.geode.CancelCriterion;
import org.apache.geode.CancelException;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.Scope;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.CachedDeserializable;
import org.apache.geode.internal.cache.Conflatable;
import org.apache.geode.internal.cache.EnumListenerEvent;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.HARegion;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegionArguments;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.VMCachedDeserializable;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.CacheClientProxy;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tier.sockets.ClientUpdateMessage;
import org.apache.geode.internal.cache.tier.sockets.ClientUpdateMessageImpl;
import org.apache.geode.internal.cache.tier.sockets.HAEventWrapper;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.concurrent.ConcurrentHashSet;
import org.apache.geode.internal.util.BlobHelper;
import org.apache.geode.internal.util.concurrent.StoppableReentrantReadWriteLock;
import org.apache.geode.test.junit.categories.IntegrationTest;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.script.*", "javax.management.*", "org.springframework.shell.event.*",
    "org.springframework.shell.core.*", "*.IntegrationTest"})
@PrepareForTest({CacheClientNotifier.class})
@Category(IntegrationTest.class)
public class HARegionQueueIntegrationTest {

  private Cache cache;

  private Region dataRegion;

  private CacheClientNotifier ccn;

  private InternalDistributedMember member;

  private static final int NUM_QUEUES = 100;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    cache = createCache();
    dataRegion = createDataRegion();
    ccn = createCacheClientNotifier();
    member = createMember();
  }

  @After
  public void tearDown() throws Exception {
    cache.close();
  }

  private Cache createCache() {
    return new CacheFactory().set(MCAST_PORT, "0").create();
  }

  private Region createDataRegion() {
    return cache.createRegionFactory(RegionShortcut.REPLICATE).create("data");
  }

  private CacheClientNotifier createCacheClientNotifier() {
    // Create a mock CacheClientNotifier
    CacheClientNotifier ccn = mock(CacheClientNotifier.class);
    PowerMockito.mockStatic(CacheClientNotifier.class, Mockito.CALLS_REAL_METHODS);
    PowerMockito.when(CacheClientNotifier.getInstance()).thenReturn(ccn);
    return ccn;
  }

  private InternalDistributedMember createMember() {
    // Create an InternalDistributedMember
    InternalDistributedMember member = mock(InternalDistributedMember.class);
    when(member.getVersionObject()).thenReturn(Version.CURRENT);
    return member;
  }

  @Test
  public void verifyEndGiiQueueingPutsHAEventWrapperNotClientUpdateMessage() throws Exception {
    // Create a HAContainerRegion
    HAContainerWrapper haContainerWrapper = createHAContainerRegion();

    // create message and HAEventWrapper
    ClientUpdateMessage message = new ClientUpdateMessageImpl(EnumListenerEvent.AFTER_UPDATE,
        (LocalRegion) dataRegion, "key", "value".getBytes(), (byte) 0x01, null,
        new ClientProxyMembershipID(), new EventID(cache.getDistributedSystem()));
    HAEventWrapper wrapper = new HAEventWrapper(message);
    wrapper.setHAContainer(haContainerWrapper);

    // Create and update HARegionQueues forcing one queue to startGiiQueueing
    int numQueues = 10;
    HARegionQueue targetQueue = createAndUpdateHARegionQueuesWithGiiQueueing(haContainerWrapper,
        wrapper, message, numQueues);

    // Verify HAContainerWrapper (1) and refCount (numQueues(10))
    assertEquals(1, haContainerWrapper.size());

    HAEventWrapper wrapperInContainer = (HAEventWrapper) haContainerWrapper.getKey(wrapper);
    assertEquals(numQueues, wrapperInContainer.getReferenceCount());

    // Verify that the HAEventWrapper in the giiQueue now has msg = null
    // this gets set to null when wrapper is added to HAContainer (for non-gii queues)
    Queue giiQueue = targetQueue.getGiiQueue();
    assertEquals(1, giiQueue.size());

    HAEventWrapper giiQueueEntry = (HAEventWrapper) giiQueue.peek();
    assertNotNull(giiQueueEntry);
    assertNull(giiQueueEntry.getClientUpdateMessage());

    // endGiiQueueing and verify queue empty and putEventInHARegion invoked with HAEventWrapper
    // not ClientUpdateMessageImpl
    HARegionQueue spyTargetQueue = spy(targetQueue);
    spyTargetQueue.endGiiQueueing();
    assertEquals(0, giiQueue.size());

    ArgumentCaptor<Conflatable> eventCaptor = ArgumentCaptor.forClass(Conflatable.class);
    verify(spyTargetQueue).putEventInHARegion(eventCaptor.capture(), anyLong());
    Conflatable capturedEvent = eventCaptor.getValue();
    assertTrue(capturedEvent instanceof HAEventWrapper);
    assertNotNull(((HAEventWrapper) capturedEvent).getClientUpdateMessage());
  }

  @Test
  public void verifySequentialUpdateHAEventWrapperWithMap() throws Exception {
    // Create a HAContainerMap to be used by the CacheClientNotifier
    HAContainerWrapper haContainerWrapper = new HAContainerMap(new ConcurrentHashMap());
    when(ccn.getHaContainer()).thenReturn(haContainerWrapper);

    // Create a CachedDeserializable
    CachedDeserializable cd = createCachedDeserializable(haContainerWrapper);

    // Create and update HARegionQueues
    createAndUpdateHARegionQueuesSequentially(haContainerWrapper, cd, NUM_QUEUES);

    // Verify HAContainerWrapper
    verifyHAContainerWrapper(haContainerWrapper, cd, NUM_QUEUES);
  }

  @Test
  public void verifySimultaneousUpdateHAEventWrapperWithMap() throws Exception {
    // Create a HAContainerMap to be used by the CacheClientNotifier
    HAContainerWrapper haContainerWrapper = new HAContainerMap(new ConcurrentHashMap());
    when(ccn.getHaContainer()).thenReturn(haContainerWrapper);

    // Create a CachedDeserializable
    CachedDeserializable cd = createCachedDeserializable(haContainerWrapper);

    // Create and update HARegionQueues
    createAndUpdateHARegionQueuesSimultaneously(haContainerWrapper, cd, NUM_QUEUES);

    // Verify HAContainerWrapper
    verifyHAContainerWrapper(haContainerWrapper, cd, NUM_QUEUES);
  }

  @Test
  public void verifySequentialUpdateHAEventWrapperWithRegion() throws Exception {
    // Create a HAContainerRegion to be used by the CacheClientNotifier
    HAContainerWrapper haContainerWrapper = createHAContainerRegion();
    when(ccn.getHaContainer()).thenReturn(haContainerWrapper);

    // Create a CachedDeserializable
    CachedDeserializable cd = createCachedDeserializable(haContainerWrapper);

    // Create and update HARegionQueues
    createAndUpdateHARegionQueuesSequentially(haContainerWrapper, cd, NUM_QUEUES);

    // Verify HAContainerWrapper
    verifyHAContainerWrapper(haContainerWrapper, cd, NUM_QUEUES);
  }

  @Test
  public void verifySimultaneousUpdateHAEventWrapperWithRegion() throws Exception {
    // Create a HAContainerRegion to be used by the CacheClientNotifier
    HAContainerWrapper haContainerWrapper = createHAContainerRegion();
    when(ccn.getHaContainer()).thenReturn(haContainerWrapper);

    // Create a CachedDeserializable
    CachedDeserializable cd = createCachedDeserializable(haContainerWrapper);

    // Create and update HARegionQueues
    createAndUpdateHARegionQueuesSimultaneously(haContainerWrapper, cd, NUM_QUEUES);

    // Verify HAContainerWrapper
    verifyHAContainerWrapper(haContainerWrapper, cd, NUM_QUEUES);
  }

  @Test
  public void verifySimultaneousPutHAEventWrapperWithRegion() throws Exception {
    HAContainerWrapper haContainerWrapper = createHAContainerRegion();

    final int numQueues = 30;
    final int numOperations = 10000;

    Set<HAEventWrapper> haEventWrappersToValidate =
        createAndPutHARegionQueuesSimulataneously(haContainerWrapper, numQueues, numOperations);

    assertEquals(numOperations, haContainerWrapper.size());

    for (HAEventWrapper haEventWrapperToValidate : haEventWrappersToValidate) {
      HAEventWrapper wrapperInContainer =
          (HAEventWrapper) haContainerWrapper.getKey(haEventWrapperToValidate);
      assertEquals(numQueues, wrapperInContainer.getReferenceCount());
    }
  }

  @Test
  public void verifySequentialPutHAEventWrapperWithRegion() throws Exception {
    HAContainerWrapper haContainerWrapper = createHAContainerRegion();

    ClientUpdateMessage message = new ClientUpdateMessageImpl(EnumListenerEvent.AFTER_UPDATE,
        (LocalRegion) dataRegion, "key", "value".getBytes(), (byte) 0x01, null,
        new ClientProxyMembershipID(), new EventID(new byte[] {1}, 1, 2));
    HAEventWrapper haEventWrapper = new HAEventWrapper(message);
    haEventWrapper.setHAContainer(haContainerWrapper);

    final int numQueues = 10;

    createAndPutHARegionQueuesSequentially(haContainerWrapper, haEventWrapper, numQueues);

    assertEquals(1, haContainerWrapper.size());

    HAEventWrapper wrapperInContainer =
        (HAEventWrapper) haContainerWrapper.getKey(haEventWrapper);
    assertEquals(numQueues, wrapperInContainer.getReferenceCount());
  }

  @Test
  public void verifySimultaneousPutHAEventWrapperWithMap() throws Exception {
    HAContainerWrapper haContainerWrapper = new HAContainerMap(new ConcurrentHashMap());
    when(ccn.getHaContainer()).thenReturn(haContainerWrapper);

    final int numQueues = 30;
    final int numOperations = 10000;

    Set<HAEventWrapper> haEventWrappersToValidate =
        createAndPutHARegionQueuesSimulataneously(haContainerWrapper, numQueues, numOperations);

    assertEquals(numOperations, haContainerWrapper.size());

    for (HAEventWrapper haEventWrapperToValidate : haEventWrappersToValidate) {
      HAEventWrapper wrapperInContainer =
          (HAEventWrapper) haContainerWrapper.getKey(haEventWrapperToValidate);
      assertEquals(numQueues, wrapperInContainer.getReferenceCount());
    }
  }

  @Test
  public void verifySequentialPutHAEventWrapperWithMap() throws Exception {
    HAContainerWrapper haContainerWrapper = new HAContainerMap(new ConcurrentHashMap());
    when(ccn.getHaContainer()).thenReturn(haContainerWrapper);

    ClientUpdateMessage message = new ClientUpdateMessageImpl(EnumListenerEvent.AFTER_UPDATE,
        (LocalRegion) dataRegion, "key", "value".getBytes(), (byte) 0x01, null,
        new ClientProxyMembershipID(), new EventID(new byte[] {1}, 1, 2));
    HAEventWrapper haEventWrapper = new HAEventWrapper(message);
    haEventWrapper.setHAContainer(haContainerWrapper);

    final int numQueues = 10;
    createAndPutHARegionQueuesSequentially(haContainerWrapper, haEventWrapper, numQueues);

    assertEquals(1, haContainerWrapper.size());

    HAEventWrapper wrapperInContainer =
        (HAEventWrapper) haContainerWrapper.getKey(haEventWrapper);
    assertEquals(numQueues, wrapperInContainer.getReferenceCount());
  }

  @Test
  public void putGIIdEventsInQueue() throws Exception {
    // Create a HAContainerMap to be used by the CacheClientNotifier
    HAContainerWrapper haContainerWrapper = new HAContainerMap(new ConcurrentHashMap());
    when(ccn.getHaContainer()).thenReturn(haContainerWrapper);

    HARegion haRegion = Mockito.mock(HARegion.class);
    when(haRegion.getGemFireCache()).thenReturn((InternalCache) cache);

    // create message and HAEventWrapper
    ClientUpdateMessage message = new ClientUpdateMessageImpl(EnumListenerEvent.AFTER_UPDATE,
        (LocalRegion) dataRegion, "key", "value".getBytes(), (byte) 0x01, null,
        new ClientProxyMembershipID(), new EventID(cache.getDistributedSystem()));
    HAEventWrapper wrapper = new HAEventWrapper(message);
    wrapper.setHAContainer(haContainerWrapper);

    ConcurrentHashMap<Integer, HAEventWrapper> mockEntries = new ConcurrentHashMap<>();

    mockEntries.put(0, wrapper);

    when(haRegion.entrySet(false)).thenReturn(mockEntries.entrySet());

    for (int i = 0; i < 10; ++i) {
      HARegionQueue regionQueue = createHARegionQueue(haContainerWrapper, i, haRegion, true);
    }
  }

  @Test
  public void queueRemovalAndDispatchingConcurrently() throws Exception {
    // Create a HAContainerMap to be used by the CacheClientNotifier
    HAContainerWrapper haContainerWrapper = new HAContainerMap(new ConcurrentHashMap());
    when(ccn.getHaContainer()).thenReturn(haContainerWrapper);

    List<HARegionQueue> regionQueues = new ArrayList<>();

    for (int i = 0; i < 2; ++i) {
      HARegion haRegion = Mockito.mock(HARegion.class);
      when(haRegion.getGemFireCache()).thenReturn((InternalCache) cache);

      ConcurrentHashMap<Object, Object> mockRegion = new ConcurrentHashMap<>();

      when(haRegion.put(Mockito.any(Object.class), Mockito.any(Object.class))).then(answer -> {
        Object existingValue = mockRegion.put(answer.getArgument(0), answer.getArgument(1));
        return existingValue;
      });

      when(haRegion.get(Mockito.any(Object.class))).then(answer -> {
        return mockRegion.get(answer.getArgument(0));
      });

      doAnswer(answer -> {
        mockRegion.remove(answer.getArgument(0));
        return null;
      }).when(haRegion).localDestroy(Mockito.any(Object.class));

      regionQueues.add(createHARegionQueue(haContainerWrapper, i, haRegion, false));
    }

    ExecutorService service = Executors.newFixedThreadPool(2);

    List<Callable<Object>> callables = new ArrayList<>();

    for (int i = 0; i < 100000; ++i) {
      callables.clear();

      EventID eventID = new EventID(new byte[] {1}, 1, i);

      ClientUpdateMessage message = new ClientUpdateMessageImpl(EnumListenerEvent.AFTER_UPDATE,
          (LocalRegion) dataRegion, "key", "value".getBytes(), (byte) 0x01, null,
          new ClientProxyMembershipID(), eventID);

      HAEventWrapper wrapper = new HAEventWrapper(message);
      wrapper.setHAContainer(haContainerWrapper);

      for (HARegionQueue queue : regionQueues) {
        queue.put(wrapper);
      }

      for (HARegionQueue queue : regionQueues) {
        callables.add(Executors.callable(() -> {
          try {
            queue.peek();
            queue.remove();
          } catch (Exception ex) {
            throw new RuntimeException(ex);
          }
        }));

        callables.add(Executors.callable(() -> {
          try {
            queue.removeDispatchedEvents(eventID);
          } catch (Exception ex) {
            throw new RuntimeException(ex);
          }
        }));
      }

      // invokeAll() will wait until our two callables have completed
      List<Future<Object>> futures = service.invokeAll(callables, 10, TimeUnit.SECONDS);

      for (Future<Object> future : futures) {
        try {
          future.get();
        } catch (Exception ex) {
          throw new TestException(
              "Exception thrown while executing regionQueue methods concurrently on iteration: "
                  + i,
              ex);
        }
      }
    }
  }

  private HAContainerRegion createHAContainerRegion() throws Exception {
    Region haContainerRegionRegion = createHAContainerRegionRegion();

    HAContainerRegion haContainerRegion = new HAContainerRegion(haContainerRegionRegion);

    return haContainerRegion;
  }

  private Region createHAContainerRegionRegion() throws Exception {
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setDiskStoreName(null);
    factory.setDiskSynchronous(true);
    factory.setDataPolicy(DataPolicy.NORMAL);
    factory.setStatisticsEnabled(true);
    factory.setEvictionAttributes(
        EvictionAttributes.createLIFOEntryAttributes(1000, EvictionAction.OVERFLOW_TO_DISK));
    Region region = ((GemFireCacheImpl) cache).createVMRegion(
        CacheServerImpl.generateNameForClientMsgsRegion(0), factory.create(),
        new InternalRegionArguments().setDestroyLockFlag(true).setRecreateFlag(false)
            .setSnapshotInputStream(null).setImageTarget(null).setIsUsedForMetaRegion(true));
    return region;
  }

  private HARegionQueue createHARegionQueue(Map haContainer, int index, HARegion haRegion,
      boolean puttingGIIDataInQueue) throws Exception {
    StoppableReentrantReadWriteLock giiLock = Mockito.mock(StoppableReentrantReadWriteLock.class);
    doReturn(Mockito.mock(StoppableReentrantReadWriteLock.StoppableWriteLock.class)).when(giiLock)
        .writeLock();
    doReturn(Mockito.mock(StoppableReentrantReadWriteLock.StoppableReadLock.class)).when(giiLock)
        .readLock();

    StoppableReentrantReadWriteLock rwLock =
        new StoppableReentrantReadWriteLock(cache.getCancelCriterion());

    return new HARegionQueue("haRegion+" + index, haRegion, (InternalCache) cache, haContainer,
        null, (byte) 1, true, mock(HARegionQueueStats.class), giiLock, rwLock,
        mock(CancelCriterion.class), puttingGIIDataInQueue);
  }

  private HARegionQueue createHARegionQueue(Map haContainer, int index) throws Exception {
    HARegion haRegion = Mockito.mock(HARegion.class);
    when(haRegion.getGemFireCache()).thenReturn((InternalCache) cache);

    return createHARegionQueue(haContainer, index, haRegion, false);
  }

  private CachedDeserializable createCachedDeserializable(HAContainerWrapper haContainerWrapper)
      throws Exception {
    // Create ClientUpdateMessage and HAEventWrapper
    ClientUpdateMessage message = new ClientUpdateMessageImpl(EnumListenerEvent.AFTER_UPDATE,
        (LocalRegion) dataRegion, "key", "value".getBytes(), (byte) 0x01, null,
        new ClientProxyMembershipID(), new EventID(cache.getDistributedSystem()));
    HAEventWrapper wrapper = new HAEventWrapper(message);
    wrapper.setHAContainer(haContainerWrapper);

    // Create a CachedDeserializable
    // Note: The haContainerRegion must contain the wrapper and message to serialize it
    haContainerWrapper.putIfAbsent(wrapper, message);
    byte[] wrapperBytes = BlobHelper.serializeToBlob(wrapper);
    CachedDeserializable cd = new VMCachedDeserializable(wrapperBytes);
    haContainerWrapper.remove(wrapper);
    assertThat(haContainerWrapper.size()).isEqualTo(0);
    return cd;
  }

  private void createAndUpdateHARegionQueuesSequentially(HAContainerWrapper haContainerWrapper,
      CachedDeserializable cd, int numQueues) throws Exception {
    // Create some HARegionQueues
    for (int i = 0; i < numQueues; i++) {
      HARegionQueue haRegionQueue = createHARegionQueue(haContainerWrapper, i);
      haRegionQueue.updateHAEventWrapper(member, cd, "haRegion");
    }
  }

  private HARegionQueue createAndUpdateHARegionQueuesWithGiiQueueing(
      HAContainerWrapper haContainerWrapper, HAEventWrapper wrapper, ClientUpdateMessage message,
      int numQueues) throws Exception {

    HARegionQueue targetQueue = null;
    int startGiiQueueingIndex = numQueues / 2;

    // create HARegionQueues and startGiiQueuing on a region about half way through
    for (int i = 0; i < numQueues; i++) {
      HARegionQueue haRegionQueue = createHARegionQueue(haContainerWrapper, i);

      // start GII Queueing (targetRegionQueue)
      if (i == startGiiQueueingIndex) {
        targetQueue = haRegionQueue;
        targetQueue.startGiiQueueing();
      }

      haRegionQueue.put(wrapper);
    }
    return targetQueue;
  }

  private Set<HAEventWrapper> createAndPutHARegionQueuesSimulataneously(
      HAContainerWrapper haContainerWrapper, int numQueues, int numOperations) throws Exception {
    ConcurrentLinkedQueue<HARegionQueue> queues = new ConcurrentLinkedQueue<>();
    final ConcurrentHashSet<HAEventWrapper> testValidationWrapperSet = new ConcurrentHashSet<>();
    final AtomicInteger count = new AtomicInteger();

    // create HARegionQueuesv
    for (int i = 0; i < numQueues; i++) {
      queues.add(createHARegionQueue(haContainerWrapper, i));
    }

    for (int i = 0; i < numOperations; i++) {
      count.set(i);

      queues.parallelStream().forEach(haRegionQueue -> {
        try {
          // In production, each queue has its own HAEventWrapper object even though they hold the
          // same ClientUpdateMessage,
          // so we create an object for each queue in here
          ClientUpdateMessage message = new ClientUpdateMessageImpl(EnumListenerEvent.AFTER_CREATE,
              (LocalRegion) dataRegion, "key", "value".getBytes(), (byte) 0x01, null,
              new ClientProxyMembershipID(), new EventID(new byte[] {1}, 1, count.get()));

          HAEventWrapper haEventWrapper = new HAEventWrapper(message);

          testValidationWrapperSet.add(haEventWrapper);

          haRegionQueue.put(haEventWrapper);
        } catch (InterruptedException iex) {
          throw new RuntimeException(iex);
        }
      });
    }

    return testValidationWrapperSet;
  }

  private void createAndPutHARegionQueuesSequentially(HAContainerWrapper haContainerWrapper,
      HAEventWrapper haEventWrapper, int numQueues) throws Exception {
    ArrayList<HARegionQueue> queues = new ArrayList<>();

    // create HARegionQueues
    for (int i = 0; i < numQueues; i++) {
      queues.add(createHARegionQueue(haContainerWrapper, i));
    }

    for (HARegionQueue queue : queues) {
      queue.put(haEventWrapper);
    }
  }

  private void createAndUpdateHARegionQueuesSimultaneously(HAContainerWrapper haContainerWrapper,
      CachedDeserializable cd, int numQueues) throws Exception {
    // Create some HARegionQueues
    HARegionQueue[] haRegionQueues = new HARegionQueue[numQueues];
    for (int i = 0; i < numQueues; i++) {
      haRegionQueues[i] = createHARegionQueue(haContainerWrapper, i);
    }

    // Create threads to simultaneously update the HAEventWrapper
    int j = 0;
    Thread[] threads = new Thread[numQueues];
    for (HARegionQueue haRegionQueue : haRegionQueues) {
      threads[j] = new Thread(() -> {
        haRegionQueue.updateHAEventWrapper(member, cd, "haRegion");
      });
      j++;
    }

    // Start the threads
    for (int i = 0; i < numQueues; i++) {
      threads[i].start();
    }

    // Wait for the threads to complete
    for (int i = 0; i < numQueues; i++) {
      threads[i].join();
    }
  }

  private void verifyHAContainerWrapper(HAContainerWrapper haContainerWrapper,
      CachedDeserializable cd, int numQueues) {
    // Verify HAContainerRegion size
    assertThat(haContainerWrapper.size()).isEqualTo(1);

    // Verify the refCount is correct
    HAEventWrapper wrapperInContainer =
        (HAEventWrapper) haContainerWrapper.getKey(cd.getDeserializedForReading());
    assertThat(wrapperInContainer.getReferenceCount()).isEqualTo(numQueues);
  }

  private class MockClientMessage extends ClientUpdateMessageImpl {
    Message mockMessage;

    public MockClientMessage(EnumListenerEvent operation, LocalRegion region, Object keyOfInterest,
        Object value, byte valueIsObject, Object callbackArgument, ClientProxyMembershipID memberId,
        EventID eventIdentifier) {
      super(operation, region, keyOfInterest, value, valueIsObject, callbackArgument, memberId,
          eventIdentifier);
    }

    @Override
    protected Message getMessage(CacheClientProxy proxy, byte[] latestValue) throws IOException {
      mockMessage = Mockito.mock(Message.class);
      doNothing().when(mockMessage).send();
      return mockMessage;
    }

    public void verifySent() throws Exception {
      verify(mockMessage, Mockito.times(1)).send();
    }
  }
}

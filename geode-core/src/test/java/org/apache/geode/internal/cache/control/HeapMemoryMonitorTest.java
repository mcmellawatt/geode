/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
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
package org.apache.geode.internal.cache.control;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.LowMemoryException;
import org.apache.geode.cache.control.ResourceManager;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.fake.Fakes;

public class HeapMemoryMonitorTest {

  private HeapMemoryMonitor heapMonitor;
  private Function function;
  private Set memberSet;
  private DistributedMember member;
  private InternalDistributedMember myself;
  private ResourceAdvisor resourceAdvisor;
  private Integer memoryEventTolerance;
  private static final String LOW_MEMORY_REGEX =
      "Function: null cannot be executed because the members.*are running low on memory";

  @Before
  public void setup() {
    InternalCache internalCache = Fakes.cache();
    DistributedSystem distributedSystem = mock(DistributedSystem.class);
    function = mock(Function.class);
    member = mock(InternalDistributedMember.class);
    myself = mock(InternalDistributedMember.class);
    resourceAdvisor = mock(ResourceAdvisor.class);

    when(internalCache.getDistributedSystem()).thenReturn(distributedSystem);
    when(internalCache.getDistributionAdvisor()).thenReturn(resourceAdvisor);
    when(internalCache.getMyId()).thenReturn(myself);

    heapMonitor = new HeapMemoryMonitor(mock(InternalResourceManager.class), internalCache, mock(ResourceManagerStats.class));
    memberSet = new HashSet<>();
    memberSet.add(member);
    heapMonitor.setMostRecentEvent(new MemoryEvent(InternalResourceManager.ResourceType.HEAP_MEMORY,
        MemoryThresholds.MemoryState.DISABLED, MemoryThresholds.MemoryState.DISABLED, null, 0L,
        true, null)); // myself is not critical
  }

  // ========== tests for getHeapCriticalMembersFrom ==========
  @Test
  public void getHeapCriticalMembersFrom_WithEmptyCriticalMembersReturnsEmptySet() {
    getHeapCriticalMembersFrom_returnsEmptySet(Collections.emptySet(), memberSet);
  }

  @Test
  public void getHeapCriticalMembersFrom_WithEmptyArgReturnsEmptySet() {
    getHeapCriticalMembersFrom_returnsEmptySet(memberSet, Collections.emptySet());
  }

  @Test
  public void getHeapCriticalMembersFromWithEmptySetsReturnsEmptySet() {
    getHeapCriticalMembersFrom_returnsEmptySet(Collections.emptySet(), Collections.emptySet());
  }

  @Test
  public void getHeapCriticalMembersFrom_WithDisjointSetsReturnsEmptySet() {
    Set argSet = new HashSet();
    argSet.add(mock(InternalDistributedMember.class));

    getHeapCriticalMembersFrom_returnsEmptySet(memberSet, argSet);
  }

  @Test
  public void getHeapCriticalMembersFrom_WithEqualSetsReturnsMember() {
    getHeapCriticalMembersFrom_returnsNonEmptySet(memberSet, Collections.unmodifiableSet(memberSet),
        new HashSet(memberSet));
  }

  @Test
  public void getHeapCriticalMembersFrom_ReturnsMultipleMembers() {
    DistributedMember member1 = mock(InternalDistributedMember.class);
    DistributedMember member2 = mock(InternalDistributedMember.class);
    DistributedMember member3 = mock(InternalDistributedMember.class);
    DistributedMember member4 = mock(InternalDistributedMember.class);
    Set advisorSet = new HashSet();
    advisorSet.add(member1);
    advisorSet.add(member2);
    advisorSet.add(member4);
    Set argSet = new HashSet(memberSet);
    argSet.add(member1);
    argSet.add(member3);
    argSet.add(member4);
    Set expectedResult = new HashSet();
    expectedResult.add(member1);
    expectedResult.add(member4);

    getHeapCriticalMembersFrom_returnsNonEmptySet(advisorSet, argSet, expectedResult);
  }

  @Test
  public void getHeapCriticalMembersFrom_DoesNotReturnMyselfWhenNotCritical() {
    Set expectedResult = new HashSet(memberSet);
    Set advisorSet = new HashSet(memberSet);
    memberSet.add(myself);

    getHeapCriticalMembersFrom_returnsNonEmptySet(advisorSet,
        Collections.unmodifiableSet(memberSet),
        expectedResult);
  }

  @Test
  public void getHeapCriticalMembersFrom_IncludesMyselfWhenCritical() throws Exception {
    Set advisorSet = new HashSet(memberSet);
    heapMonitor.setMostRecentEvent(new MemoryEvent(InternalResourceManager.ResourceType.HEAP_MEMORY,
        MemoryThresholds.MemoryState.DISABLED, MemoryThresholds.MemoryState.CRITICAL, null, 0L,
        true, null));
    memberSet.add(myself);

    getHeapCriticalMembersFrom_returnsNonEmptySet(advisorSet,
        Collections.unmodifiableSet(memberSet),
        new HashSet(memberSet));
  }

  // ========== tests for createLowMemoryIfNeeded (with Set argument) ==========
  @Test
  public void createLowMemoryIfNeededWithSetArg_ReturnsNullWhenNotOptimizedForWrite()
      throws Exception {
    createLowMemoryIfNeededWithSetArg_returnsNull(false, false, memberSet);
  }

  @Test
  public void createLowMemoryIfNeededWithSetArg_ReturnsNullWhenLowMemoryExceptionDisabled()
      throws Exception {
    createLowMemoryIfNeededWithSetArg_returnsNull(true, true, memberSet);
  }

  @Test
  public void createLowMemoryIfNeededWithSetArg_ReturnsNullWhenNoCriticalMembers()
      throws Exception {
    createLowMemoryIfNeededWithSetArg_returnsNull(true, false, Collections.emptySet());
  }

  @Test
  public void createLowMemoryIfNeededWithSetArg_ReturnsException() throws Exception {
    setMocking(true, false, memberSet);

    LowMemoryException exception = heapMonitor.createLowMemoryIfNeeded(function, memberSet);

    assertLowMemoryException(exception);
  }

  // ========== tests for createLowMemoryIfNeeded (with DistributedMember argument) ==========
  @Test
  public void createLowMemoryIfNeededWithMemberArg_ReturnsNullWhenNotOptimizedForWrite()
      throws Exception {
    createLowMemoryIfNeededWithMemberArg_returnsNull(false, false, member);
  }

  @Test
  public void createLowMemoryIfNeededWithMemberArg_ReturnsNullWhenLowMemoryExceptionDisabled()
      throws Exception {
    createLowMemoryIfNeededWithMemberArg_returnsNull(true, true, member);
  }

  @Test
  public void createLowMemoryIfNeededWithMemberArg_ReturnsNullWhenNoCriticalMembers()
      throws Exception {
    createLowMemoryIfNeededWithMemberArg_returnsNull(true, false, member);
  }

  @Test
  public void createLowMemoryIfNeededWithMemberArg_ReturnsException() throws Exception {
    setMocking(true, false, memberSet);

    LowMemoryException exception = heapMonitor.createLowMemoryIfNeeded(function, member);

    assertLowMemoryException(exception);
  }

  // ========== tests for checkForLowMemory (with Set argument) ==========
  @Test
  public void checkForLowMemoryWithSetArg_DoesNotThrowWhenNotOptimizedForWrite() throws Exception {
    checkForLowMemoryWithSetArg_doesNotThrow(false, false, memberSet);
  }

  @Test
  public void checkForLowMemoryWithSetArg_DoesNotThrowWhenLowMemoryExceptionDisabled()
      throws Exception {
    checkForLowMemoryWithSetArg_doesNotThrow(true, true, memberSet);
  }

  @Test
  public void checkForLowMemoryWithSetArg_DoesNotThrowWhenNoCriticalMembers() throws Exception {
    checkForLowMemoryWithSetArg_doesNotThrow(true, false, Collections.emptySet());
  }

  @Test
  public void checkForLowMemoryWithSetArg_ThrowsLowMemoryException() throws Exception {
    setMocking(true, false, memberSet);

    assertThatThrownBy(() -> heapMonitor.checkForLowMemory(function, memberSet))
        .isExactlyInstanceOf(LowMemoryException.class);
  }

  // ========== tests for checkForLowMemory (with DistributedMember argument) ==========
  @Test
  public void checkForLowMemoryIfNeededWithMemberArg_ReturnsNullWhenNotOptimizedForWrite()
      throws Exception {
    checkForLowMemoryWithMemberArg_doesNotThrow(false, false, member);
  }

  @Test
  public void checkForLowMemoryIfNeededWithMemberArg_ReturnsNullWhenLowMemoryExceptionDisabled()
      throws Exception {
    checkForLowMemoryWithMemberArg_doesNotThrow(true, true, member);
  }

  @Test
  public void checkForLowMemoryIfNeededWithMemberArg_ReturnsNullWhenNoCriticalMembers()
      throws Exception {
    checkForLowMemoryWithMemberArg_doesNotThrow(true, false, member);
  }

  @Test
  public void checkForLowMemoryWithMemberArg_ReturnsException() throws Exception {
    setMocking(true, false, memberSet);

    assertThatThrownBy(() -> heapMonitor.checkForLowMemory(function, member))
        .isExactlyInstanceOf(LowMemoryException.class).hasMessageMatching(LOW_MEMORY_REGEX);
  }

  @Test
  public void updateStateAndSendEvent_ThrashingShouldNotChangeState() {
    // Initialize the most recent state to NORMAL
    heapMonitor = spy(heapMonitor);

    // Override the tolerance to allow 3 consecutive events before allowing a state transition.
    when(heapMonitor.getMemoryStateChangeTolerance()).thenReturn(3);

    // This will prevent the polling monitor from firing and causing state transitions.  We
    // want complete control over the state transitions in this test.
    heapMonitor.started = true;

    HeapMemoryMonitor.setTestBytesUsedForThresholdSet(50);
    heapMonitor.setTestMaxMemoryBytes(100);
    heapMonitor.setCriticalThreshold(90f);
    heapMonitor.setEvictionThreshold(80f);

    // If we thrash between CRITICAL_EVICTION and NORMAL, we don't expect a state transition
    // to happen because we have a memoryStateChangeTolerance of 3 in this test.
    heapMonitor.updateStateAndSendEvent(95);
    assertThat(heapMonitor.getState()).isEqualByComparingTo(MemoryThresholds.MemoryState.NORMAL);
    heapMonitor.updateStateAndSendEvent(60);
    assertThat(heapMonitor.getState()).isEqualByComparingTo(MemoryThresholds.MemoryState.NORMAL);
    heapMonitor.updateStateAndSendEvent(95);
    assertThat(heapMonitor.getState()).isEqualByComparingTo(MemoryThresholds.MemoryState.NORMAL);
    heapMonitor.updateStateAndSendEvent(60);
    assertThat(heapMonitor.getState()).isEqualByComparingTo(MemoryThresholds.MemoryState.NORMAL);
    heapMonitor.updateStateAndSendEvent(95);
    assertThat(heapMonitor.getState()).isEqualByComparingTo(MemoryThresholds.MemoryState.NORMAL);
    heapMonitor.updateStateAndSendEvent(60);
    assertThat(heapMonitor.getState()).isEqualByComparingTo(MemoryThresholds.MemoryState.NORMAL);
    heapMonitor.updateStateAndSendEvent(95);
    assertThat(heapMonitor.getState()).isEqualByComparingTo(MemoryThresholds.MemoryState.NORMAL);
    heapMonitor.updateStateAndSendEvent(60);
    assertThat(heapMonitor.getState()).isEqualByComparingTo(MemoryThresholds.MemoryState.NORMAL);
    heapMonitor.updateStateAndSendEvent(95);
    assertThat(heapMonitor.getState()).isEqualByComparingTo(MemoryThresholds.MemoryState.NORMAL);
    heapMonitor.updateStateAndSendEvent(60);
    assertThat(heapMonitor.getState()).isEqualByComparingTo(MemoryThresholds.MemoryState.NORMAL);
  }

  @Test
  public void updateStateAndSendEvent_AboveCriticalMoreThanEventTolerance() {
    // Initialize the most recent state to NORMAL
    heapMonitor = spy(heapMonitor);

    // Override the tolerance to allow 3 consecutive events before allowing a state transition.
    when(heapMonitor.getMemoryStateChangeTolerance()).thenReturn(3);

    // This will prevent the polling monitor from firing and causing state transitions.  We
    // want complete control over the state transitions in this test.
    heapMonitor.started = true;

    HeapMemoryMonitor.setTestBytesUsedForThresholdSet(50);
    heapMonitor.setTestMaxMemoryBytes(100);
    heapMonitor.setCriticalThreshold(90f);
    heapMonitor.setEvictionThreshold(80f);

    heapMonitor.updateStateAndSendEvent(95);
    heapMonitor.updateStateAndSendEvent(95);
    heapMonitor.updateStateAndSendEvent(95);
    heapMonitor.updateStateAndSendEvent(95);

    assertThat(heapMonitor.getState()).isEqualByComparingTo(MemoryThresholds.MemoryState.EVICTION_CRITICAL);
  }

  @Test
  public void updateStateAndSendEvent_AboveCriticalTwoEventsThenAboveEviction() {
    // Initialize the most recent state to NORMAL
    heapMonitor = spy(heapMonitor);

    // Override the tolerance to allow 3 consecutive events before allowing a state transition.
    when(heapMonitor.getMemoryStateChangeTolerance()).thenReturn(3);

    // This will prevent the polling monitor from firing and causing state transitions.  We
    // want complete control over the state transitions in this test.
    heapMonitor.started = true;

    HeapMemoryMonitor.setTestBytesUsedForThresholdSet(50);
    heapMonitor.setTestMaxMemoryBytes(100);
    heapMonitor.setCriticalThreshold(90f);
    heapMonitor.setEvictionThreshold(80f);

    heapMonitor.updateStateAndSendEvent(95);
    heapMonitor.updateStateAndSendEvent(95);
    heapMonitor.updateStateAndSendEvent(95);
    heapMonitor.updateStateAndSendEvent(85);

    assertThat(heapMonitor.getState()).isEqualByComparingTo(MemoryThresholds.MemoryState.EVICTION);
  }

  @Test
  public void updateStateAndSendEvent_ThreeEvictionsThenCriticalTransitionEvictionCritical() {
    // Initialize the most recent state to NORMAL
    heapMonitor = spy(heapMonitor);

    // Override the tolerance to allow 3 consecutive events before allowing a state transition.
    when(heapMonitor.getMemoryStateChangeTolerance()).thenReturn(3);

    // This will prevent the polling monitor from firing and causing state transitions.  We
    // want complete control over the state transitions in this test.
    heapMonitor.started = true;

    HeapMemoryMonitor.setTestBytesUsedForThresholdSet(50);
    heapMonitor.setTestMaxMemoryBytes(100);
    heapMonitor.setCriticalThreshold(90f);
    heapMonitor.setEvictionThreshold(80f);

    heapMonitor.updateStateAndSendEvent(85);
    heapMonitor.updateStateAndSendEvent(85);
    heapMonitor.updateStateAndSendEvent(85);
    heapMonitor.updateStateAndSendEvent(95);

    assertThat(heapMonitor.getState()).isEqualByComparingTo(MemoryThresholds.MemoryState.EVICTION_CRITICAL);
  }

  @Test
  public void updateStateAndSendEvent_EvictionDisabledTransitionToCritical() {
    // Initialize the most recent state to NORMAL
    heapMonitor = spy(heapMonitor);

    // Override the tolerance to allow 3 consecutive events before allowing a state transition.
    when(heapMonitor.getMemoryStateChangeTolerance()).thenReturn(3);

    // This will prevent the polling monitor from firing and causing state transitions.  We
    // want complete control over the state transitions in this test.
    heapMonitor.started = true;

    HeapMemoryMonitor.setTestBytesUsedForThresholdSet(50);
    heapMonitor.setTestMaxMemoryBytes(100);
    heapMonitor.setCriticalThreshold(90f);

    heapMonitor.updateStateAndSendEvent(95);
    assertThat(heapMonitor.getState()).isEqualByComparingTo(MemoryThresholds.MemoryState.EVICTION_DISABLED);

    heapMonitor.updateStateAndSendEvent(95);
    assertThat(heapMonitor.getState()).isEqualByComparingTo(MemoryThresholds.MemoryState.EVICTION_DISABLED);

    heapMonitor.updateStateAndSendEvent(95);
    assertThat(heapMonitor.getState()).isEqualByComparingTo(MemoryThresholds.MemoryState.EVICTION_DISABLED);

    heapMonitor.updateStateAndSendEvent(95);
    assertThat(heapMonitor.getState()).isEqualByComparingTo(MemoryThresholds.MemoryState.EVICTION_DISABLED_CRITICAL);
  }

  @Test
  public void updateStateAndSendEvent_CriticalDisabledTransitionToEviction() {
    // Initialize the most recent state to NORMAL
    heapMonitor = spy(heapMonitor);

    // Override the tolerance to allow 3 consecutive events before allowing a state transition.
    when(heapMonitor.getMemoryStateChangeTolerance()).thenReturn(3);

    // This will prevent the polling monitor from firing and causing state transitions.  We
    // want complete control over the state transitions in this test.
    heapMonitor.started = true;

    HeapMemoryMonitor.setTestBytesUsedForThresholdSet(50);
    heapMonitor.setTestMaxMemoryBytes(100);
    heapMonitor.setEvictionThreshold(80f);

    // It should take 4 above critical events for the state transition to take effect, because
    // our memory state change tolerance is set to 3 for this test
    heapMonitor.updateStateAndSendEvent(85);
    assertThat(heapMonitor.getState()).isEqualByComparingTo(MemoryThresholds.MemoryState.CRITICAL_DISABLED);

    heapMonitor.updateStateAndSendEvent(85);
    assertThat(heapMonitor.getState()).isEqualByComparingTo(MemoryThresholds.MemoryState.CRITICAL_DISABLED);

    heapMonitor.updateStateAndSendEvent(85);
    assertThat(heapMonitor.getState()).isEqualByComparingTo(MemoryThresholds.MemoryState.CRITICAL_DISABLED);

    heapMonitor.updateStateAndSendEvent(85);
    assertThat(heapMonitor.getState()).isEqualByComparingTo(MemoryThresholds.MemoryState.EVICTION_CRITICAL_DISABLED);
  }

  @Test
  public void updateStateAndSendEvent_BouncingBetweenEvictionAndCritical() {
    // Initialize the most recent state to NORMAL
    heapMonitor = spy(heapMonitor);

    // Override the tolerance to allow 3 consecutive events before allowing a state transition.
    when(heapMonitor.getMemoryStateChangeTolerance()).thenReturn(3);

    // This will prevent the polling monitor from firing and causing state transitions.  We
    // want complete control over the state transitions in this test.
    heapMonitor.started = true;

    HeapMemoryMonitor.setTestBytesUsedForThresholdSet(50);
    heapMonitor.setTestMaxMemoryBytes(100);
    heapMonitor.setEvictionThreshold(80f);

    heapMonitor.updateStateAndSendEvent(95);
    heapMonitor.updateStateAndSendEvent(85);
    heapMonitor.updateStateAndSendEvent(95);
    heapMonitor.updateStateAndSendEvent(85);

    assertThat(heapMonitor.getState()).isEqualByComparingTo(MemoryThresholds.MemoryState.EVICTION_CRITICAL_DISABLED);
  }

  // ========== private methods ==========
  private void getHeapCriticalMembersFrom_returnsEmptySet(Set adviseCriticalMembers, Set argSet) {
    when(resourceAdvisor.adviseCriticalMembers()).thenReturn(adviseCriticalMembers);

    Set<DistributedMember> criticalMembers = heapMonitor.getHeapCriticalMembersFrom(argSet);

    assertThat(criticalMembers).isEmpty();
  }

  private void getHeapCriticalMembersFrom_returnsNonEmptySet(Set adviseCriticalMembers, Set argSet,
      Set expectedResult) {
    when(resourceAdvisor.adviseCriticalMembers()).thenReturn(adviseCriticalMembers);

    Set<DistributedMember> criticalMembers = heapMonitor.getHeapCriticalMembersFrom(argSet);

    assertThat(criticalMembers).containsAll(expectedResult);
  }

  private void createLowMemoryIfNeededWithSetArg_returnsNull(boolean optimizeForWrite,
      boolean isLowMemoryExceptionDisabled, Set memberSetArg) throws Exception {
    setMocking(optimizeForWrite, isLowMemoryExceptionDisabled, memberSetArg);

    LowMemoryException exception = heapMonitor.createLowMemoryIfNeeded(function, memberSetArg);

    assertThat(exception).isNull();
  }

  private void createLowMemoryIfNeededWithMemberArg_returnsNull(boolean optimizeForWrite,
      boolean isLowMemoryExceptionDisabled, DistributedMember memberArg) throws Exception {
    setMocking(optimizeForWrite, isLowMemoryExceptionDisabled, Collections.emptySet());

    LowMemoryException exception = heapMonitor.createLowMemoryIfNeeded(function, memberArg);

    assertThat(exception).isNull();
  }

  private void checkForLowMemoryWithSetArg_doesNotThrow(boolean optimizeForWrite,
      boolean isLowMemoryExceptionDisabled, Set memberSetArg) throws Exception {
    setMocking(optimizeForWrite, isLowMemoryExceptionDisabled, memberSetArg);

    heapMonitor.checkForLowMemory(function, memberSetArg);
  }

  private void checkForLowMemoryWithMemberArg_doesNotThrow(boolean optimizeForWrite,
      boolean isLowMemoryExceptionDisabled, DistributedMember memberArg) throws Exception {
    setMocking(optimizeForWrite, isLowMemoryExceptionDisabled, Collections.emptySet());

    heapMonitor.checkForLowMemory(function, memberArg);
  }

  private void setMocking(boolean optimizeForWrite, boolean isLowMemoryExceptionDisabled,
      Set argSet) throws Exception {
    when(function.optimizeForWrite()).thenReturn(optimizeForWrite);
    setIsLowMemoryExceptionDisabled(isLowMemoryExceptionDisabled);
    when(resourceAdvisor.adviseCriticalMembers()).thenReturn(argSet);
  }

  private void assertLowMemoryException(LowMemoryException exception) {
    assertThat(exception).isExactlyInstanceOf(LowMemoryException.class);
    assertThat(exception.getMessage()).containsPattern(LOW_MEMORY_REGEX);
  }

  private void setIsLowMemoryExceptionDisabled(boolean isLowMemoryExceptionDisabled)
      throws Exception {
    MemoryThresholds.setLowMemoryExceptionDisabled(isLowMemoryExceptionDisabled);
  }
}

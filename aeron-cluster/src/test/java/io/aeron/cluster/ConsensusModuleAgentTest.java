/*
 * Copyright 2014-2020 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.cluster;

import io.aeron.*;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.*;
import io.aeron.cluster.service.ClusterMarkFile;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.security.DefaultAuthenticatorSupplier;
import io.aeron.status.ReadableCounter;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.*;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import static io.aeron.CommonContext.ENDPOINT_PARAM_NAME;
import static io.aeron.archive.status.RecordingPos.*;
import static io.aeron.cluster.ClusterControl.ToggleState.*;
import static io.aeron.cluster.ClusterMember.encodeAsString;
import static io.aeron.cluster.ClusterMember.parseEndpoints;
import static io.aeron.cluster.ConsensusModule.Configuration.*;
import static io.aeron.cluster.ConsensusModule.State.INIT;
import static io.aeron.cluster.ConsensusModule.State.TERMINATING;
import static io.aeron.cluster.ConsensusModuleAgent.SLOW_TICK_INTERVAL_NS;
import static io.aeron.cluster.client.AeronCluster.Configuration.PROTOCOL_SEMANTIC_VERSION;
import static io.aeron.cluster.codecs.MessageHeaderEncoder.ENCODED_LENGTH;
import static io.aeron.cluster.service.Cluster.Role.LEADER;
import static io.aeron.exceptions.AeronException.Category.ERROR;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static java.lang.Boolean.TRUE;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.agrona.BitUtil.*;
import static org.agrona.concurrent.status.CountersReader.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

public class ConsensusModuleAgentTest
{
    private static final long SLOW_TICK_INTERVAL_MS = NANOSECONDS.toMillis(SLOW_TICK_INTERVAL_NS);
    private static final String RESPONSE_CHANNEL_ONE = "aeron:udp?endpoint=localhost:11111";
    private static final String RESPONSE_CHANNEL_TWO = "aeron:udp?endpoint=localhost:22222";

    private final EgressPublisher mockEgressPublisher = mock(EgressPublisher.class);
    private final LogPublisher mockLogPublisher = mock(LogPublisher.class);
    private final Aeron mockAeron = mock(Aeron.class);
    private final ConcurrentPublication mockResponsePublication = mock(ConcurrentPublication.class);
    private final Counter mockTimedOutClientCounter = mock(Counter.class);

    private final ConsensusModule.Context ctx = new ConsensusModule.Context()
        .errorHandler(Throwable::printStackTrace)
        .errorCounter(mock(AtomicCounter.class))
        .moduleStateCounter(mock(Counter.class))
        .commitPositionCounter(mock(Counter.class))
        .controlToggleCounter(mock(Counter.class))
        .clusterNodeRoleCounter(mock(Counter.class))
        .timedOutClientCounter(mockTimedOutClientCounter)
        .idleStrategySupplier(NoOpIdleStrategy::new)
        .aeron(mockAeron)
        .clusterMemberId(0)
        .authenticatorSupplier(new DefaultAuthenticatorSupplier())
        .clusterMarkFile(mock(ClusterMarkFile.class))
        .archiveContext(new AeronArchive.Context())
        .logPublisher(mockLogPublisher)
        .egressPublisher(mockEgressPublisher);

    @BeforeEach
    public void before()
    {
        when(mockAeron.conductorAgentInvoker()).thenReturn(mock(AgentInvoker.class));
        when(mockEgressPublisher.sendEvent(any(), anyLong(), anyInt(), any(), any())).thenReturn(TRUE);
        when(mockLogPublisher.appendSessionClose(any(), anyLong(), anyLong())).thenReturn(TRUE);
        when(mockLogPublisher.appendSessionOpen(any(), anyLong(), anyLong())).thenReturn(128L);
        when(mockLogPublisher.appendClusterAction(anyLong(), anyLong(), any(ClusterAction.class)))
            .thenReturn(TRUE);
        when(mockAeron.addPublication(anyString(), anyInt())).thenReturn(mockResponsePublication);
        when(mockAeron.addSubscription(anyString(), anyInt())).thenReturn(mock(Subscription.class));
        when(mockAeron.addSubscription(anyString(), anyInt(), eq(null), any(UnavailableImageHandler.class)))
            .thenReturn(mock(Subscription.class));
        when(mockResponsePublication.isConnected()).thenReturn(TRUE);
    }

    @Test
    public void shouldLimitActiveSessions()
    {
        final TestClusterClock clock = new TestClusterClock(MILLISECONDS);
        ctx.maxConcurrentSessions(1)
            .epochClock(clock)
            .clusterClock(clock);

        final ConsensusModuleAgent agent = new ConsensusModuleAgent(ctx);

        final long correlationIdOne = 1L;
        agent.state(ConsensusModule.State.ACTIVE);
        agent.role(LEADER);
        agent.appendPositionCounter(mock(ReadableCounter.class));
        agent.onSessionConnect(correlationIdOne, 2, PROTOCOL_SEMANTIC_VERSION, RESPONSE_CHANNEL_ONE, new byte[0]);

        clock.update(17, MILLISECONDS);
        agent.doWork();

        verify(mockLogPublisher).appendSessionOpen(any(ClusterSession.class), anyLong(), anyLong());

        final long correlationIdTwo = 2L;
        agent.onSessionConnect(correlationIdTwo, 3, PROTOCOL_SEMANTIC_VERSION, RESPONSE_CHANNEL_TWO, new byte[0]);
        clock.update(clock.time() + 10L, MILLISECONDS);
        agent.doWork();

        verify(mockEgressPublisher).sendEvent(
            any(ClusterSession.class), anyLong(), anyInt(), eq(EventCode.ERROR), eq(SESSION_LIMIT_MSG));
    }

    @Test
    public void shouldCloseInactiveSession()
    {
        final TestClusterClock clock = new TestClusterClock(MILLISECONDS);
        final long startMs = SLOW_TICK_INTERVAL_MS;
        clock.update(startMs, MILLISECONDS);

        ctx.epochClock(clock)
            .clusterClock(clock);

        final ConsensusModuleAgent agent = new ConsensusModuleAgent(ctx);

        final long correlationId = 1L;
        agent.state(ConsensusModule.State.ACTIVE);
        agent.role(LEADER);
        agent.appendPositionCounter(mock(ReadableCounter.class));
        agent.onSessionConnect(correlationId, 2, PROTOCOL_SEMANTIC_VERSION, RESPONSE_CHANNEL_ONE, new byte[0]);

        agent.doWork();

        verify(mockLogPublisher).appendSessionOpen(any(ClusterSession.class), anyLong(), eq(startMs));

        final long timeMs = startMs + NANOSECONDS.toMillis(ConsensusModule.Configuration.sessionTimeoutNs());
        clock.update(timeMs, MILLISECONDS);
        agent.doWork();

        final long timeoutMs = timeMs + SLOW_TICK_INTERVAL_MS;
        clock.update(timeoutMs, MILLISECONDS);
        agent.doWork();

        verify(mockTimedOutClientCounter).incrementOrdered();
        verify(mockLogPublisher).appendSessionClose(any(ClusterSession.class), anyLong(), eq(timeoutMs));
        verify(mockEgressPublisher).sendEvent(
            any(ClusterSession.class), anyLong(), anyInt(), eq(EventCode.ERROR), eq(SESSION_TIMEOUT_MSG));
    }

    @Test
    public void shouldCloseTerminatedSession()
    {
        final TestClusterClock clock = new TestClusterClock(MILLISECONDS);
        final long startMs = SLOW_TICK_INTERVAL_MS;
        clock.update(startMs, MILLISECONDS);

        ctx.epochClock(clock)
            .clusterClock(clock);

        final ConsensusModuleAgent agent = new ConsensusModuleAgent(ctx);

        final long correlationId = 1L;
        agent.state(ConsensusModule.State.ACTIVE);
        agent.role(LEADER);
        agent.appendPositionCounter(mock(ReadableCounter.class));
        agent.onSessionConnect(correlationId, 2, PROTOCOL_SEMANTIC_VERSION, RESPONSE_CHANNEL_ONE, new byte[0]);

        agent.doWork();

        final ArgumentCaptor<ClusterSession> sessionCaptor = ArgumentCaptor.forClass(ClusterSession.class);

        verify(mockLogPublisher).appendSessionOpen(sessionCaptor.capture(), anyLong(), eq(startMs));

        final long timeMs = startMs + SLOW_TICK_INTERVAL_MS;
        clock.update(timeMs, MILLISECONDS);
        agent.doWork();

        agent.onServiceCloseSession(sessionCaptor.getValue().id());

        verify(mockLogPublisher).appendSessionClose(any(ClusterSession.class), anyLong(), eq(timeMs));
        verify(mockEgressPublisher).sendEvent(
            any(ClusterSession.class), anyLong(), anyInt(), eq(EventCode.ERROR), eq(SESSION_TERMINATED_MSG));
    }

    @Test
    public void shouldSuspendThenResume()
    {
        final TestClusterClock clock = new TestClusterClock(MILLISECONDS);

        final MutableLong stateValue = new MutableLong();
        final Counter mockState = mock(Counter.class);
        when(mockState.get()).thenAnswer((invocation) -> stateValue.value);
        doAnswer(
            (invocation) ->
            {
                stateValue.value = invocation.getArgument(0);
                return null;
            })
            .when(mockState).set(anyLong());

        final MutableLong controlValue = new MutableLong(NEUTRAL.code());
        final Counter mockControlToggle = mock(Counter.class);
        when(mockControlToggle.get()).thenAnswer((invocation) -> controlValue.value);

        doAnswer(
            (invocation) ->
            {
                controlValue.value = invocation.getArgument(0);
                return null;
            })
            .when(mockControlToggle).set(anyLong());

        doAnswer(
            (invocation) ->
            {
                final long expected = invocation.getArgument(0);
                if (expected == controlValue.value)
                {
                    controlValue.value = invocation.getArgument(1);
                    return true;
                }
                return false;
            })
            .when(mockControlToggle).compareAndSet(anyLong(), anyLong());

        ctx.moduleStateCounter(mockState);
        ctx.controlToggleCounter(mockControlToggle);
        ctx.epochClock(clock).clusterClock(clock);

        final ConsensusModuleAgent agent = new ConsensusModuleAgent(ctx);
        agent.appendPositionCounter(mock(ReadableCounter.class));

        assertEquals(INIT.code(), stateValue.get());

        agent.state(ConsensusModule.State.ACTIVE);
        agent.role(LEADER);
        assertEquals(ConsensusModule.State.ACTIVE.code(), stateValue.get());

        SUSPEND.toggle(mockControlToggle);
        clock.update(SLOW_TICK_INTERVAL_MS, MILLISECONDS);
        agent.doWork();

        assertEquals(ConsensusModule.State.SUSPENDED.code(), stateValue.get());
        assertEquals(SUSPEND.code(), controlValue.get());

        RESUME.toggle(mockControlToggle);
        clock.update(SLOW_TICK_INTERVAL_MS * 2, MILLISECONDS);
        agent.doWork();

        assertEquals(ConsensusModule.State.ACTIVE.code(), stateValue.get());
        assertEquals(NEUTRAL.code(), controlValue.get());

        final InOrder inOrder = Mockito.inOrder(mockLogPublisher);
        inOrder.verify(mockLogPublisher).appendClusterAction(anyLong(), anyLong(), eq(ClusterAction.SUSPEND));
        inOrder.verify(mockLogPublisher).appendClusterAction(anyLong(), anyLong(), eq(ClusterAction.RESUME));
    }

    @Test
    void onStopMemberRaisesAnErrorIfMemberDoesNotExist()
    {
        final int memberId = -1;
        final CountedErrorHandler countedErrorHandler = mock(CountedErrorHandler.class);
        ctx.countedErrorHandler(countedErrorHandler).clusterClock(new TestClusterClock(MILLISECONDS));
        final ConsensusModuleAgent agent = new ConsensusModuleAgent(ctx);

        agent.onStopMember(42, memberId);

        verify(countedErrorHandler).onError(argThat(
            error ->
            {
                final ClusterException ex = (ClusterException)error;
                assertEquals(ERROR, ex.category());
                assertEquals("attempt to stop non-leader member with ID=" + memberId, ex.getMessage());
                return true;
            }));
    }

    @Test
    void onStopMemberRaisesAnErrorIfMemberIsNotALeaderMember()
    {
        final ClusterMember member1 =
            parseEndpoints(777, "localhost:21210,localhost:21211,localhost:21212,localhost:21213,localhost:21214");
        final ClusterMember member2 =
            parseEndpoints(555, "localhost:31310,localhost:31311,localhost:31312,localhost:31313,localhost:31314");
        final CountedErrorHandler countedErrorHandler = mock(CountedErrorHandler.class);
        ctx.countedErrorHandler(countedErrorHandler)
            .clusterClock(new TestClusterClock(MILLISECONDS))
            .clusterMembers(encodeAsString(new ClusterMember[]{ member1, member2 }))
            .clusterMemberId(member1.id());
        final ConsensusModuleAgent agent = new ConsensusModuleAgent(ctx);

        agent.onStopMember(42, member2.id());

        verify(countedErrorHandler).onError(argThat(
            error ->
            {
                final ClusterException ex = (ClusterException)error;
                assertEquals(ERROR, ex.category());
                assertEquals("attempt to stop non-leader member with ID=" + member2.id(), ex.getMessage());
                return true;
            }));
    }

    @Test
    void onStopMemberIsANoOpWhenInvokedOnAFollower()
    {
        final ClusterMember follower =
            parseEndpoints(777, "localhost:21210,localhost:21211,localhost:21212,localhost:21213,localhost:21214");
        final CountedErrorHandler countedErrorHandler = mock(CountedErrorHandler.class);
        ctx.countedErrorHandler(countedErrorHandler)
            .clusterClock(new TestClusterClock(MILLISECONDS))
            .clusterMembers(encodeAsString(new ClusterMember[]{ follower }))
            .clusterMemberId(follower.id());
        final ConsensusModuleAgent agent = new ConsensusModuleAgent(ctx);

        agent.onStopMember(42, follower.id());

        assertEquals(INIT, agent.state());
        verifyNoInteractions(countedErrorHandler);
    }

    @Test
    @SuppressWarnings("MethodLength")
    void onStopMemberShouldInitiateGracefulLeaderStepDown()
    {
        final long leadershipTermId = 128;
        final long recordingId = 1_000_256_789;
        final long commitPosition = 1024 * 2048 * 4096L;
        final long archiveStopPosition = commitPosition * 2L;
        final int logSessionId = 123;
        final String liveLogDestination = "liveLog";
        final String replayLogDestination = "replayLogDestination";
        final String serviceControlChannel = "serviceControlChannel";
        final int serviceStreamId = 19;
        final String memberStatusChannel = "aeron:udp";
        final int memberStatusStreamId = 35;
        final int leaderAbortMessageLength = ENCODED_LENGTH + LeaderAbortEncoder.BLOCK_LENGTH;
        final int terminationPositionMessageLength = ENCODED_LENGTH + ServiceTerminationPositionEncoder.BLOCK_LENGTH;

        final ClusterMember leader =
            parseEndpoints(777, "localhost:21210,localhost:21211,localhost:21212,localhost:21213,localhost:21214");
        final ClusterMember follower1 =
            parseEndpoints(555, "localhost:31310,localhost:31311,localhost:31312,localhost:31313,localhost:31314");
        final ClusterMember follower2 =
            parseEndpoints(333, "localhost:32310,localhost:32311,localhost:32312,localhost:32313,localhost:32314");

        final CountedErrorHandler countedErrorHandler = mock(CountedErrorHandler.class);

        final Counter commitPositionCounter = mock(Counter.class);
        when(commitPositionCounter.get()).thenReturn(commitPosition);

        ctx.countedErrorHandler(countedErrorHandler)
            .clusterClock(new TestClusterClock(MILLISECONDS))
            .clusterMembers(encodeAsString(new ClusterMember[]{ follower2, leader, follower1 }))
            .clusterMemberId(leader.id())
            .commitPositionCounter(commitPositionCounter)
            .serviceControlChannel(serviceControlChannel)
            .serviceStreamId(serviceStreamId)
            .memberStatusChannel(memberStatusChannel)
            .memberStatusStreamId(memberStatusStreamId);

        final CountersReader counters = mock(CountersReader.class);
        when(counters.maxCounterId()).thenReturn(1);
        when(counters.getCounterState(0)).thenReturn(RECORD_ALLOCATED);
        final AtomicBuffer metaDataBuffer = mock(AtomicBuffer.class);
        when(metaDataBuffer.getInt(TYPE_ID_OFFSET)).thenReturn(RECORDING_POSITION_TYPE_ID);
        when(metaDataBuffer.getInt(KEY_OFFSET + SESSION_ID_OFFSET)).thenReturn(logSessionId);
        when(metaDataBuffer.getLong(KEY_OFFSET + RECORDING_ID_OFFSET)).thenReturn(recordingId);
        when(counters.metaDataBuffer()).thenReturn(metaDataBuffer);
        final AtomicBuffer valuesBuffer = mock(AtomicBuffer.class);
        when(counters.valuesBuffer()).thenReturn(valuesBuffer);
        when(mockAeron.countersReader()).thenReturn(counters);

        final ConcurrentPublication serviceProxyPublication = mock(ConcurrentPublication.class);
        final UnsafeBuffer serviceProxyBuffer = mockTryClaim(serviceProxyPublication, terminationPositionMessageLength);
        when(mockAeron.addPublication(serviceControlChannel, serviceStreamId)).thenReturn(serviceProxyPublication);

        final ExclusivePublication publication1 = mock(ExclusivePublication.class);
        final UnsafeBuffer buffer1 = mockTryClaim(publication1, leaderAbortMessageLength);

        final ExclusivePublication publication2 = mock(ExclusivePublication.class);
        final UnsafeBuffer buffer2 = mockTryClaim(publication2, leaderAbortMessageLength);

        when(mockAeron.addExclusivePublication(any(), eq(memberStatusStreamId))).thenAnswer(
            (Answer<ExclusivePublication>)invocation ->
            {
                final ChannelUri uri = ChannelUri.parse(invocation.getArgument(0));
                final String endpoint = uri.get(ENDPOINT_PARAM_NAME);
                if (follower1.memberFacingEndpoint().equals(endpoint))
                {
                    return publication1;
                }
                else if (follower2.memberFacingEndpoint().equals(endpoint))
                {
                    return publication2;
                }
                throw new IllegalArgumentException("Unknown URI: " + uri);
            });

        final Subscription subscription = mock(Subscription.class);
        final Image image = mock(Image.class);
        when(image.subscription()).thenReturn(subscription);
        when(subscription.imageBySessionId(logSessionId)).thenReturn(image);

        final AeronArchive archive = mock(AeronArchive.class);
        when(archive.getStopPosition(recordingId)).thenReturn(archiveStopPosition);

        final ConsensusModuleAgent agent = new ConsensusModuleAgent(ctx);
        agent.role(LEADER);
        agent.initialiseLeadershipTermId(leadershipTermId);
        agent.replayLogDestination(replayLogDestination);
        agent.liveLogDestination(liveLogDestination);
        agent.archive(archive);
        assertTrue(agent.findImageAndLogAdapter(subscription, logSessionId));

        agent.onStopMember(42, leader.id());

        assertEquals(TERMINATING, agent.state());
        final InOrder inOrder = inOrder(
            commitPositionCounter,
            publication1,
            publication2,
            serviceProxyPublication,
            archive,
            subscription);
        inOrder.verify(commitPositionCounter).get();
        inOrder.verify(subscription).asyncRemoveDestination(replayLogDestination);
        inOrder.verify(subscription).asyncRemoveDestination(liveLogDestination);
        inOrder.verify(archive).getStopPosition(recordingId);
        inOrder.verify(archive).stopAllReplays(recordingId);
        inOrder.verify(archive).truncateRecording(recordingId, commitPosition);
        inOrder.verify(publication2).tryClaim(eq(leaderAbortMessageLength), any(BufferClaim.class));
        inOrder.verify(publication1).tryClaim(eq(leaderAbortMessageLength), any(BufferClaim.class));
        inOrder.verify(serviceProxyPublication).tryClaim(eq(terminationPositionMessageLength), any(BufferClaim.class));
        inOrder.verifyNoMoreInteractions();

        verifyLeaderAbortMessage(buffer1, leader.id(), leadershipTermId, commitPosition);
        verifyLeaderAbortMessage(buffer2, leader.id(), leadershipTermId, commitPosition);
        verifyTerminationPositionMessage(serviceProxyBuffer, commitPosition);
    }

    private UnsafeBuffer mockTryClaim(final Publication publication, final int length)
    {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[align(HEADER_LENGTH + length, CACHE_LINE_LENGTH)]);
        when(publication.tryClaim(eq(length), any(BufferClaim.class))).thenAnswer(
            (Answer<Long>)invocation ->
            {
                final BufferClaim bufferClaim = invocation.getArgument(1);
                bufferClaim.wrap(buffer, 0, buffer.capacity());
                return 1L;
            });
        return buffer;
    }

    private void verifyLeaderAbortMessage(
        final UnsafeBuffer buffer, final int leaderId, final long leadershipTermId, final long commitPosition)
    {
        final int offset = HEADER_LENGTH + MessageHeaderEncoder.ENCODED_LENGTH;
        assertEquals(leadershipTermId, buffer.getLong(offset, LITTLE_ENDIAN));
        assertEquals(commitPosition, buffer.getLong(offset + SIZE_OF_LONG, LITTLE_ENDIAN));
        assertEquals(leaderId, buffer.getInt(offset + SIZE_OF_LONG * 2, LITTLE_ENDIAN));
    }

    private void verifyTerminationPositionMessage(final UnsafeBuffer buffer, final long commitPosition)
    {
        final int offset = HEADER_LENGTH + MessageHeaderEncoder.ENCODED_LENGTH;
        assertEquals(commitPosition, buffer.getLong(offset, LITTLE_ENDIAN));
    }
}

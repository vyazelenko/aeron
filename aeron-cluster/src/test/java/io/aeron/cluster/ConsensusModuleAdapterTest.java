package io.aeron.cluster;

import io.aeron.ControlledFragmentAssembler;
import io.aeron.Subscription;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.MessageHeaderEncoder;
import io.aeron.cluster.codecs.StopMemberEncoder;
import io.aeron.logbuffer.Header;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

import static io.aeron.cluster.ConsensusModuleAdapter.FRAGMENT_LIMIT;
import static io.aeron.logbuffer.FrameDescriptor.UNFRAGMENTED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class ConsensusModuleAdapterTest
{
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);
    private final Subscription subscription = mock(Subscription.class);
    private final ConsensusModuleAgent consensusModuleAgent = mock(ConsensusModuleAgent.class);
    private final ConsensusModuleAdapter consensusModuleAdapter =
        new ConsensusModuleAdapter(subscription, consensusModuleAgent);

    @Test
    void pollThrowsClusterExceptionIfSchemaIdDoesNotMatch()
    {
        final int schemaId = MessageHeaderEncoder.SCHEMA_ID + 333;
        final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
        messageHeaderEncoder.wrap(buffer, 0).schemaId(schemaId);
        mockControlledPoll(0);

        final ClusterException ex = assertThrows(ClusterException.class, consensusModuleAdapter::poll);

        assertEquals("expected schemaId=" + MessageHeaderEncoder.SCHEMA_ID + ", actual=" + schemaId, ex.getMessage());
    }

    @Test
    void pollShouldDecodeStopMemberMessage()
    {
        final long correlationId = 42;
        final int memberId = 777;
        final int pollResult = 1;
        final StopMemberEncoder stopMemberEncoder = new StopMemberEncoder();
        stopMemberEncoder.wrapAndApplyHeader(buffer, 0, new MessageHeaderEncoder())
            .correlationId(correlationId)
            .memberId(memberId);
        mockControlledPoll(pollResult);

        final int result = consensusModuleAdapter.poll();

        assertEquals(pollResult, result);
        verify(consensusModuleAgent).onStopMember(correlationId, memberId);
        verifyNoMoreInteractions(consensusModuleAgent);
    }

    private void mockControlledPoll(final int pollResult)
    {
        when(subscription.controlledPoll(any(ControlledFragmentAssembler.class), eq(FRAGMENT_LIMIT)))
            .thenAnswer((Answer<Integer>)invocation ->
            {
                final Header header = mock(Header.class);
                when(header.flags()).thenReturn(UNFRAGMENTED);
                final ControlledFragmentAssembler fragmentAssembler = invocation.getArgument(0);
                fragmentAssembler.onFragment(buffer, 0, buffer.capacity(), header);
                return pollResult;
            });
    }
}
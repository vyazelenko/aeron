package io.aeron.cluster;

import io.aeron.Subscription;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.LeaderAbortEncoder;
import io.aeron.cluster.codecs.MessageHeaderEncoder;
import io.aeron.logbuffer.Header;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static io.aeron.logbuffer.FrameDescriptor.UNFRAGMENTED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

class MemberStatusAdapterTest
{
    private final Header header = mock(Header.class);
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);
    private final Subscription subscription = mock(Subscription.class);
    private final ConsensusModuleAgent consensusModuleAgent = mock(ConsensusModuleAgent.class);
    private final MemberStatusAdapter memberStatusAdapter = new MemberStatusAdapter(subscription, consensusModuleAgent);

    @Test
    void onFragmentThrowsClusterExceptionUponInvalidSchemaId()
    {
        final int schemaId = MessageHeaderEncoder.SCHEMA_ID + 14;
        when(header.flags()).thenReturn(UNFRAGMENTED);
        final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
        headerEncoder.wrap(buffer, 0).schemaId(schemaId);

        final ClusterException ex = Assertions.assertThrows(ClusterException.class,
            () -> memberStatusAdapter.onFragment(buffer, 0, buffer.capacity(), header));

        assertEquals("expected schemaId=" + MessageHeaderEncoder.SCHEMA_ID + ", actual=" + schemaId, ex.getMessage());
    }

    @Test
    void onFragmentDecodesLeaderAbortMessage()
    {
        final int leaderId = 211;
        final long leadershipTermId = 42L;
        final long commitPosition = 256;
        final LeaderAbortEncoder leaderAbortEncoder = new LeaderAbortEncoder();
        leaderAbortEncoder.wrapAndApplyHeader(buffer, 0, new MessageHeaderEncoder());
        leaderAbortEncoder.leaderId(leaderId).leadershipTermId(leadershipTermId).commitPosition(commitPosition);
        when(header.flags()).thenReturn(UNFRAGMENTED);

        memberStatusAdapter.onFragment(buffer, 0, buffer.capacity(), header);

        verify(consensusModuleAgent).onLeaderAbort(leaderId, leadershipTermId, commitPosition);
    }
}
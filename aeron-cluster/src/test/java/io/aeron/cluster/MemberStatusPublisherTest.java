package io.aeron.cluster;

import io.aeron.ExclusivePublication;
import io.aeron.Publication;
import io.aeron.cluster.codecs.LeaderAbortEncoder;
import io.aeron.cluster.codecs.MessageHeaderEncoder;
import io.aeron.exceptions.AeronException;
import io.aeron.logbuffer.BufferClaim;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InOrder;

import static io.aeron.cluster.MemberStatusPublisher.SEND_ATTEMPTS;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class MemberStatusPublisherTest
{
    private final BufferClaim bufferClaim = mockBufferClaim();
    private final ExclusivePublication publication = mock(ExclusivePublication.class);
    private final MemberStatusPublisher memberStatusPublisher = new MemberStatusPublisher(bufferClaim);

    @Test
    void leaderAbortReturnsTrueUponSuccess()
    {
        final int leaderId = 42;
        final long leadershipTermId = 10;
        final long commitPosition = 0;
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + LeaderAbortEncoder.BLOCK_LENGTH;
        when(publication.tryClaim(length, bufferClaim)).thenReturn(13L);

        final boolean result =
            memberStatusPublisher.leaderAbort(publication, leaderId, leadershipTermId, commitPosition);

        assertTrue(result);
        final InOrder inOrder = inOrder(publication, bufferClaim);
        inOrder.verify(publication).tryClaim(length, bufferClaim);
        inOrder.verify(bufferClaim).commit();
        inOrder.verifyNoMoreInteractions();
        final MutableDirectBuffer buffer = bufferClaim.buffer();
        final int dataOffset = bufferClaim.offset() + MessageHeaderEncoder.ENCODED_LENGTH;
        assertEquals(leadershipTermId, buffer.getLong(dataOffset, LITTLE_ENDIAN));
        assertEquals(commitPosition, buffer.getLong(dataOffset + SIZE_OF_LONG, LITTLE_ENDIAN));
        assertEquals(leaderId, buffer.getInt(dataOffset + SIZE_OF_LONG * 2, LITTLE_ENDIAN));
    }

    @Test
    void leaderAbortReturnsFalseIfAllSendAttemptsFailed()
    {
        final int leaderId = 8;
        final long leadershipTermId = 1_000_000;
        final long commitPosition = 1024;
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + LeaderAbortEncoder.BLOCK_LENGTH;

        final boolean result =
            memberStatusPublisher.leaderAbort(publication, leaderId, leadershipTermId, commitPosition);

        assertFalse(result);
        verify(publication, times(SEND_ATTEMPTS)).tryClaim(length, bufferClaim);
        verifyNoInteractions(bufferClaim);
    }

    @ParameterizedTest
    @ValueSource(longs = { Publication.CLOSED, Publication.MAX_POSITION_EXCEEDED })
    void leaderAbortThrowsAeronExceptionIfTryClaimFailsWithError(final long errorResult)
    {
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + LeaderAbortEncoder.BLOCK_LENGTH;
        when(publication.tryClaim(length, bufferClaim)).thenReturn(errorResult);

        final AeronException ex = assertThrows(AeronException.class,
            () -> memberStatusPublisher.leaderAbort(publication, 1, 14, 5));

        assertEquals("unexpected publication state: " + errorResult, ex.getMessage());
    }

    private BufferClaim mockBufferClaim()
    {
        final BufferClaim bufferClaim = mock(BufferClaim.class);
        when(bufferClaim.buffer()).thenReturn(new UnsafeBuffer(new byte[128]));
        when(bufferClaim.offset()).thenReturn(8);
        return bufferClaim;
    }
}
package io.aeron.cluster.service;

import io.aeron.Publication;
import io.aeron.cluster.codecs.MessageHeaderEncoder;
import io.aeron.cluster.codecs.StopMemberEncoder;
import io.aeron.exceptions.AeronException;
import io.aeron.logbuffer.BufferClaim;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InOrder;

import static io.aeron.Publication.*;
import static io.aeron.cluster.service.ConsensusModuleProxy.SEND_ATTEMPTS;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ConsensusModuleProxyTest
{
    private final Publication publication = mock(Publication.class);
    private final BufferClaim bufferClaim = mockBufferClaim();
    private final ConsensusModuleProxy consensusModuleProxy = new ConsensusModuleProxy(publication, bufferClaim);

    @Test
    void stopMemberReturnsTrueAfterASuccessfulSend()
    {
        final int messageLength = MessageHeaderEncoder.ENCODED_LENGTH + StopMemberEncoder.BLOCK_LENGTH;
        final long correlationId = 22;
        final int memberId = 81;
        when(publication.tryClaim(messageLength, bufferClaim)).thenReturn(1024L);

        final boolean result = consensusModuleProxy.stopMember(correlationId, memberId);

        assertTrue(result);
        final InOrder inOrder = inOrder(publication, bufferClaim);
        inOrder.verify(publication).tryClaim(messageLength, bufferClaim);
        inOrder.verify(bufferClaim).commit();
        inOrder.verifyNoMoreInteractions();
        final MutableDirectBuffer buffer = bufferClaim.buffer();
        final int dataOffset = bufferClaim.offset() + MessageHeaderEncoder.ENCODED_LENGTH;
        assertEquals(correlationId, buffer.getLong(dataOffset, LITTLE_ENDIAN));
        assertEquals(memberId, buffer.getInt(dataOffset + SIZE_OF_LONG, LITTLE_ENDIAN));
    }

    @Test
    void stopMemberReturnsFalseAfterAllSendAttemptsFailed()
    {
        final int messageLength = MessageHeaderEncoder.ENCODED_LENGTH + StopMemberEncoder.BLOCK_LENGTH;
        final long correlationId = 42;
        final int memberId = 111;

        final boolean result = consensusModuleProxy.stopMember(correlationId, memberId);

        assertFalse(result);
        verify(publication, times(SEND_ATTEMPTS)).tryClaim(messageLength, bufferClaim);
    }

    @ParameterizedTest
    @ValueSource(longs = { NOT_CONNECTED, CLOSED, MAX_POSITION_EXCEEDED })
    void stopMemberThrowsAeronExceptionIfTryClaimFailsWithAnError(final long errorCode)
    {
        final int messageLength = MessageHeaderEncoder.ENCODED_LENGTH + StopMemberEncoder.BLOCK_LENGTH;
        when(publication.tryClaim(messageLength, bufferClaim)).thenReturn(errorCode);

        assertThrows(AeronException.class, () -> consensusModuleProxy.stopMember(1, 0));
    }

    private BufferClaim mockBufferClaim()
    {
        final BufferClaim bufferClaim = mock(BufferClaim.class);
        when(bufferClaim.buffer()).thenReturn(new UnsafeBuffer(new byte[128]));
        when(bufferClaim.offset()).thenReturn(8);
        return bufferClaim;
    }
}
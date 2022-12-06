/*
 * Copyright 2014-2023 Real Logic Limited.
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
package io.aeron;

import io.aeron.command.*;
import io.aeron.exceptions.AeronException;
import io.aeron.exceptions.ClientTimeoutException;
import io.aeron.exceptions.ConductorServiceTimeoutException;
import io.aeron.exceptions.DriverTimeoutException;
import io.aeron.exceptions.RegistrationException;
import io.aeron.exceptions.TimeoutException;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.status.HeartbeatTimestamp;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import org.agrona.ErrorHandler;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.*;
import org.agrona.concurrent.broadcast.CopyBroadcastReceiver;
import org.agrona.concurrent.status.CountersManager;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.function.ToIntFunction;

import static io.aeron.ErrorCode.INVALID_CHANNEL;
import static io.aeron.logbuffer.LogBufferDescriptor.PARTITION_COUNT;
import static io.aeron.logbuffer.LogBufferDescriptor.TERM_MIN_LENGTH;
import static io.aeron.status.HeartbeatTimestamp.HEARTBEAT_TYPE_ID;
import static java.lang.Boolean.TRUE;
import static java.nio.ByteBuffer.allocateDirect;
import static org.agrona.concurrent.status.CountersReader.COUNTER_LENGTH;
import static org.agrona.concurrent.status.CountersReader.METADATA_LENGTH;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(InterruptingTestCallback.class)
class ClientConductorTest
{
    private static final int TERM_BUFFER_LENGTH = TERM_MIN_LENGTH;

    private static final int SESSION_ID_1 = 13;
    private static final int SESSION_ID_2 = 15;

    private static final String CHANNEL = "aeron:udp?endpoint=localhost:40124";
    private static final int STREAM_ID_1 = 1002;
    private static final int STREAM_ID_2 = 1004;
    private static final int SEND_BUFFER_CAPACITY = 1024;
    private static final int COUNTER_BUFFER_LENGTH = 1024;

    private static final long CORRELATION_ID = 2000;
    private static final long CORRELATION_ID_2 = 2002;
    private static final long CLOSE_CORRELATION_ID = 2001;
    private static final long UNKNOWN_CORRELATION_ID = 3000;

    private static final long KEEP_ALIVE_INTERVAL = TimeUnit.MILLISECONDS.toNanos(500);
    private static final long AWAIT_TIMEOUT = 100;
    private static final long INTER_SERVICE_TIMEOUT_MS = 1000;

    private static final int SUBSCRIPTION_POSITION_ID = 2;
    private static final int SUBSCRIPTION_POSITION_REGISTRATION_ID = 4001;

    private static final String SOURCE_INFO = "127.0.0.1:40789";

    private final PublicationBuffersReadyFlyweight publicationReady = new PublicationBuffersReadyFlyweight();
    private final SubscriptionReadyFlyweight subscriptionReady = new SubscriptionReadyFlyweight();
    private final OperationSucceededFlyweight operationSuccess = new OperationSucceededFlyweight();
    private final ErrorResponseFlyweight errorResponse = new ErrorResponseFlyweight();
    private final ClientTimeoutFlyweight clientTimeout = new ClientTimeoutFlyweight();

    private final UnsafeBuffer publicationReadyBuffer = new UnsafeBuffer(new byte[SEND_BUFFER_CAPACITY]);
    private final UnsafeBuffer subscriptionReadyBuffer = new UnsafeBuffer(new byte[SEND_BUFFER_CAPACITY]);
    private final UnsafeBuffer operationSuccessBuffer = new UnsafeBuffer(new byte[SEND_BUFFER_CAPACITY]);
    private final UnsafeBuffer errorMessageBuffer = new UnsafeBuffer(new byte[SEND_BUFFER_CAPACITY]);
    private final UnsafeBuffer clientTimeoutBuffer = new UnsafeBuffer(new byte[SEND_BUFFER_CAPACITY]);

    private final CopyBroadcastReceiver mockToClientReceiver = mock(CopyBroadcastReceiver.class);
    private final UnsafeBuffer counterValuesBuffer = new UnsafeBuffer(new byte[COUNTER_BUFFER_LENGTH]);
    private final UnsafeBuffer counterMetaDataBuffer = new UnsafeBuffer(
        new byte[COUNTER_BUFFER_LENGTH * (METADATA_LENGTH / COUNTER_LENGTH)]);

    private final ExpandableArrayBuffer tempBuffer = new ExpandableArrayBuffer();

    private long timeMs = 0;
    private final EpochClock epochClock = () -> timeMs += 10;

    private long timeNs = 0;
    private final NanoClock nanoClock = () -> timeNs += 10_000_000;

    private final ErrorHandler mockClientErrorHandler = spy(new PrintError());

    private ClientConductor conductor;
    private final DriverProxy driverProxy = mock(DriverProxy.class);
    private final AvailableImageHandler availableImageHandler = mock(AvailableImageHandler.class);
    private final UnavailableImageHandler unavailableImageHandler = mock(UnavailableImageHandler.class);
    private final UnavailableImageExtendedHandler unavailableImageExtendedHandler =
        mock(UnavailableImageExtendedHandler.class);
    private final Runnable mockCloseHandler = mock(Runnable.class);
    private final LogBuffersFactory logBuffersFactory = mock(LogBuffersFactory.class);
    private final Lock mockClientLock = mock(Lock.class);
    private final Aeron mockAeron = mock(Aeron.class);
    private boolean suppressPrintError = false;

    @BeforeEach
    void setUp()
    {
        final Aeron.Context ctx = new Aeron.Context()
            .clientLock(mockClientLock)
            .epochClock(epochClock)
            .nanoClock(nanoClock)
            .awaitingIdleStrategy(new NoOpIdleStrategy())
            .toClientBuffer(mockToClientReceiver)
            .driverProxy(driverProxy)
            .logBuffersFactory(logBuffersFactory)
            .errorHandler(mockClientErrorHandler)
            .availableImageHandler(availableImageHandler)
            .unavailableImageHandler(unavailableImageHandler)
            .unavailableImageExtendedHandler(unavailableImageExtendedHandler)
            .closeHandler(mockCloseHandler)
            .keepAliveIntervalNs(KEEP_ALIVE_INTERVAL)
            .driverTimeoutMs(AWAIT_TIMEOUT)
            .interServiceTimeoutNs(TimeUnit.MILLISECONDS.toNanos(INTER_SERVICE_TIMEOUT_MS));

        ctx.countersMetaDataBuffer(counterMetaDataBuffer);
        ctx.countersValuesBuffer(counterValuesBuffer);

        when(mockAeron.context()).thenReturn(ctx);

        when(mockClientLock.tryLock()).thenReturn(TRUE);

        when(driverProxy.addPublication(CHANNEL, STREAM_ID_1)).thenReturn(CORRELATION_ID);
        when(driverProxy.addPublication(CHANNEL, STREAM_ID_2)).thenReturn(CORRELATION_ID_2);
        when(driverProxy.removePublication(CORRELATION_ID)).thenReturn(CLOSE_CORRELATION_ID);
        when(driverProxy.removeSubscription(CORRELATION_ID)).thenReturn(CLOSE_CORRELATION_ID);

        conductor = new ClientConductor(ctx, mockAeron);

        publicationReady.wrap(publicationReadyBuffer, 0);
        subscriptionReady.wrap(subscriptionReadyBuffer, 0);
        operationSuccess.wrap(operationSuccessBuffer, 0);
        errorResponse.wrap(errorMessageBuffer, 0);
        clientTimeout.wrap(clientTimeoutBuffer, 0);

        publicationReady.correlationId(CORRELATION_ID);
        publicationReady.registrationId(CORRELATION_ID);
        publicationReady.sessionId(SESSION_ID_1);
        publicationReady.streamId(STREAM_ID_1);
        publicationReady.logFileName(SESSION_ID_1 + "-log");

        operationSuccess.correlationId(CLOSE_CORRELATION_ID);

        final UnsafeBuffer[] termBuffersSession1 = new UnsafeBuffer[PARTITION_COUNT];
        final UnsafeBuffer[] termBuffersSession2 = new UnsafeBuffer[PARTITION_COUNT];

        for (int i = 0; i < PARTITION_COUNT; i++)
        {
            termBuffersSession1[i] = new UnsafeBuffer(allocateDirect(TERM_BUFFER_LENGTH));
            termBuffersSession2[i] = new UnsafeBuffer(allocateDirect(TERM_BUFFER_LENGTH));
        }

        final UnsafeBuffer logMetaDataSession1 = new UnsafeBuffer(allocateDirect(TERM_BUFFER_LENGTH));
        final UnsafeBuffer logMetaDataSession2 = new UnsafeBuffer(allocateDirect(TERM_BUFFER_LENGTH));

        final MutableDirectBuffer header1 = DataHeaderFlyweight.createDefaultHeader(SESSION_ID_1, STREAM_ID_1, 0);
        final MutableDirectBuffer header2 = DataHeaderFlyweight.createDefaultHeader(SESSION_ID_2, STREAM_ID_2, 0);

        LogBufferDescriptor.storeDefaultFrameHeader(logMetaDataSession1, header1);
        LogBufferDescriptor.storeDefaultFrameHeader(logMetaDataSession2, header2);

        final LogBuffers logBuffersSession1 = mock(LogBuffers.class);
        final LogBuffers logBuffersSession2 = mock(LogBuffers.class);

        when(logBuffersFactory.map(SESSION_ID_1 + "-log")).thenReturn(logBuffersSession1);
        when(logBuffersFactory.map(SESSION_ID_2 + "-log")).thenReturn(logBuffersSession2);

        when(logBuffersSession1.duplicateTermBuffers()).thenReturn(termBuffersSession1);
        when(logBuffersSession2.duplicateTermBuffers()).thenReturn(termBuffersSession2);

        when(logBuffersSession1.metaDataBuffer()).thenReturn(logMetaDataSession1);
        when(logBuffersSession2.metaDataBuffer()).thenReturn(logMetaDataSession2);
        when(logBuffersSession1.termLength()).thenReturn(TERM_BUFFER_LENGTH);
        when(logBuffersSession2.termLength()).thenReturn(TERM_BUFFER_LENGTH);
    }

    // --------------------------------
    // Publication related interactions
    // --------------------------------

    @Test
    void addPublicationShouldNotifyMediaDriver()
    {
        whenReceiveBroadcastOnMessage(
            ControlProtocolEvents.ON_PUBLICATION_READY,
            publicationReadyBuffer,
            (buffer) -> publicationReady.length());

        conductor.addPublication(CHANNEL, STREAM_ID_1);

        verify(driverProxy).addPublication(CHANNEL, STREAM_ID_1);
    }

    @Test
    void addPublicationShouldMapLogFile()
    {
        whenReceiveBroadcastOnMessage(
            ControlProtocolEvents.ON_PUBLICATION_READY,
            publicationReadyBuffer,
            (buffer) -> publicationReady.length());

        conductor.addPublication(CHANNEL, STREAM_ID_1);

        verify(logBuffersFactory).map(SESSION_ID_1 + "-log");
    }

    @Test
    @InterruptAfter(5)
    void addPublicationShouldTimeoutWithoutReadyMessage()
    {
        assertThrows(DriverTimeoutException.class, () -> conductor.addPublication(CHANNEL, STREAM_ID_1));
    }

    @Test
    void closingPublicationShouldNotifyMediaDriver()
    {
        whenReceiveBroadcastOnMessage(
            ControlProtocolEvents.ON_PUBLICATION_READY, publicationReadyBuffer, (buffer) -> publicationReady.length());

        final Publication publication = conductor.addPublication(CHANNEL, STREAM_ID_1);

        whenReceiveBroadcastOnMessage(
            ControlProtocolEvents.ON_OPERATION_SUCCESS,
            operationSuccessBuffer,
            (buffer) -> OperationSucceededFlyweight.LENGTH);

        publication.close();

        verify(driverProxy).removePublication(CORRELATION_ID);
    }

    @Test
    void closingPublicationShouldPurgeCache()
    {
        whenReceiveBroadcastOnMessage(
            ControlProtocolEvents.ON_PUBLICATION_READY, publicationReadyBuffer, (buffer) -> publicationReady.length());

        final Publication firstPublication = conductor.addPublication(CHANNEL, STREAM_ID_1);

        whenReceiveBroadcastOnMessage(
            ControlProtocolEvents.ON_OPERATION_SUCCESS,
            operationSuccessBuffer,
            (buffer) -> OperationSucceededFlyweight.LENGTH);

        firstPublication.close();

        whenReceiveBroadcastOnMessage(
            ControlProtocolEvents.ON_PUBLICATION_READY, publicationReadyBuffer, (buffer) -> publicationReady.length());

        final Publication secondPublication = conductor.addPublication(CHANNEL, STREAM_ID_1);

        assertThat(firstPublication, not(sameInstance(secondPublication)));
    }

    @Test
    void shouldFailToAddPublicationOnMediaDriverError()
    {
        whenReceiveBroadcastOnMessage(
            ControlProtocolEvents.ON_ERROR,
            errorMessageBuffer,
            (buffer) ->
            {
                errorResponse.errorCode(INVALID_CHANNEL);
                errorResponse.errorMessage("invalid channel");
                errorResponse.offendingCommandCorrelationId(CORRELATION_ID);
                return errorResponse.length();
            });

        assertThrows(RegistrationException.class, () -> conductor.addPublication(CHANNEL, STREAM_ID_1));
    }

    @Test
    void closingPublicationDoesNotRemoveOtherPublications()
    {
        whenReceiveBroadcastOnMessage(
            ControlProtocolEvents.ON_PUBLICATION_READY, publicationReadyBuffer, (buffer) -> publicationReady.length());

        final Publication publication = conductor.addPublication(CHANNEL, STREAM_ID_1);

        whenReceiveBroadcastOnMessage(
            ControlProtocolEvents.ON_PUBLICATION_READY,
            publicationReadyBuffer,
            (buffer) ->
            {
                publicationReady.streamId(STREAM_ID_2);
                publicationReady.sessionId(SESSION_ID_2);
                publicationReady.logFileName(SESSION_ID_2 + "-log");
                publicationReady.correlationId(CORRELATION_ID_2);
                publicationReady.registrationId(CORRELATION_ID_2);
                return publicationReady.length();
            });

        conductor.addPublication(CHANNEL, STREAM_ID_2);

        whenReceiveBroadcastOnMessage(
            ControlProtocolEvents.ON_OPERATION_SUCCESS,
            operationSuccessBuffer,
            (buffer) -> OperationSucceededFlyweight.LENGTH);

        publication.close();

        verify(driverProxy).removePublication(CORRELATION_ID);
        verify(driverProxy, never()).removePublication(CORRELATION_ID_2);
    }

    @Test
    void shouldNotMapBuffersForUnknownCorrelationId()
    {
        whenReceiveBroadcastOnMessage(
            ControlProtocolEvents.ON_PUBLICATION_READY,
            publicationReadyBuffer,
            (buffer) ->
            {
                publicationReady.correlationId(UNKNOWN_CORRELATION_ID);
                publicationReady.registrationId(UNKNOWN_CORRELATION_ID);
                return publicationReady.length();
            });

        whenReceiveBroadcastOnMessage(
            ControlProtocolEvents.ON_PUBLICATION_READY,
            publicationReadyBuffer,
            (buffer) ->
            {
                publicationReady.correlationId(CORRELATION_ID);
                return publicationReady.length();
            });

        final Publication publication = conductor.addPublication(CHANNEL, STREAM_ID_1);
        conductor.doWork();

        verify(logBuffersFactory, times(1)).map(anyString());
        assertThat(publication.registrationId(), is(CORRELATION_ID));
    }

    @Test
    void addSubscriptionShouldNotifyMediaDriver()
    {
        addSubscription(CHANNEL, STREAM_ID_1, CORRELATION_ID, null, null);

        verify(driverProxy).addSubscription(CHANNEL, STREAM_ID_1);
    }

    @Test
    void closingSubscriptionShouldNotifyMediaDriver()
    {
        final Subscription subscription = addSubscription(CHANNEL, STREAM_ID_1, CORRELATION_ID, null, null);

        whenReceiveBroadcastOnMessage(
            ControlProtocolEvents.ON_OPERATION_SUCCESS,
            operationSuccessBuffer,
            (buffer) ->
            {
                operationSuccess.correlationId(CLOSE_CORRELATION_ID);
                return CorrelatedMessageFlyweight.LENGTH;
            });

        subscription.close();

        verify(driverProxy).removeSubscription(CORRELATION_ID);
    }

    @Test
    @InterruptAfter(5)
    void addSubscriptionShouldTimeoutWithoutOperationSuccessful()
    {
        assertThrows(DriverTimeoutException.class, () -> conductor.addSubscription(CHANNEL, STREAM_ID_1));
    }

    @Test
    void shouldFailToAddSubscriptionOnMediaDriverError()
    {
        when(driverProxy.addSubscription(anyString(), anyInt())).thenReturn(CORRELATION_ID);

        whenReceiveBroadcastOnMessage(
            ControlProtocolEvents.ON_ERROR,
            errorMessageBuffer,
            (buffer) ->
            {
                errorResponse.errorCode(INVALID_CHANNEL);
                errorResponse.errorMessage("invalid channel");
                errorResponse.offendingCommandCorrelationId(CORRELATION_ID);
                return errorResponse.length();
            });

        assertThrows(RegistrationException.class, () -> conductor.addSubscription(CHANNEL, STREAM_ID_1));
    }

    @Test
    void clientNotifiedOfNewImageShouldMapLogFile()
    {
        final Subscription subscription = addSubscription(CHANNEL, STREAM_ID_1, CORRELATION_ID, null, null);

        addImage(subscription, CORRELATION_ID_2, SESSION_ID_1);

        verify(logBuffersFactory).map(eq(SESSION_ID_1 + "-log"));
    }

    @Test
    void clientNotifiedOfNewAndInactiveImages()
    {
        final Subscription subscription = addSubscription(
            CHANNEL, STREAM_ID_1, CORRELATION_ID, availableImageHandler, unavailableImageHandler);

        addImage(subscription, CORRELATION_ID, SESSION_ID_1);

        assertFalse(subscription.hasNoImages());
        assertTrue(subscription.isConnected());
        verify(availableImageHandler).onAvailableImage(any(Image.class));

        final UnavailableImageReason reason =
            new UnavailableImageReason("this is the reason why Image went unavailable");
        conductor.onUnavailableImage(CORRELATION_ID, subscription.registrationId(), reason);

        verify(unavailableImageHandler).onUnavailableImage(any(Image.class));
        verify(unavailableImageExtendedHandler).onUnavailableImage(any(Image.class), eq(reason));
        assertTrue(subscription.hasNoImages());
        assertFalse(subscription.isConnected());
    }

    @Test
    void shouldIgnoreUnknownNewImage()
    {
        conductor.onAvailableImage(
            CORRELATION_ID_2,
            SESSION_ID_2,
            SUBSCRIPTION_POSITION_REGISTRATION_ID,
            SUBSCRIPTION_POSITION_ID,
            SESSION_ID_2 + "-log",
            SOURCE_INFO);

        verify(logBuffersFactory, never()).map(anyString());
        verify(availableImageHandler, never()).onAvailableImage(any(Image.class));
    }

    @Test
    void shouldIgnoreUnknownInactiveImage()
    {
        conductor.onUnavailableImage(
            CORRELATION_ID_2, SUBSCRIPTION_POSITION_REGISTRATION_ID, new UnavailableImageReason("test"));

        verifyNoInteractions(logBuffersFactory, unavailableImageHandler, unavailableImageExtendedHandler);
    }

    @Test
    void shouldTimeoutInterServiceIfTooLongBetweenDoWorkCalls()
    {
        suppressPrintError = true;

        conductor.doWork();

        timeNs += (TimeUnit.MILLISECONDS.toNanos(INTER_SERVICE_TIMEOUT_MS) + 1);

        conductor.doWork();

        verify(mockClientErrorHandler).onError(any(ConductorServiceTimeoutException.class));

        assertTrue(conductor.isTerminating());
    }

    @Test
    void shouldTerminateAndErrorOnClientTimeoutFromDriver()
    {
        suppressPrintError = true;

        conductor.onClientTimeout();
        verify(mockClientErrorHandler).onError(any(TimeoutException.class));

        boolean threwException = false;
        try
        {
            conductor.doWork();
        }
        catch (final AgentTerminationException ex)
        {
            threwException = true;
        }

        assertTrue(threwException);
        assertTrue(conductor.isTerminating());

        conductor.onClose();
        verify(mockCloseHandler).run();
    }

    @Test
    void shouldNotCloseAndErrorOnClientTimeoutForAnotherClientIdFromDriver()
    {
        whenReceiveBroadcastOnMessage(
            ControlProtocolEvents.ON_CLIENT_TIMEOUT,
            clientTimeoutBuffer,
            (buffer) ->
            {
                clientTimeout.clientId(conductor.driverListenerAdapter().clientId() + 1);
                return ClientTimeoutFlyweight.LENGTH;
            });

        conductor.doWork();

        verify(mockClientErrorHandler, never()).onError(any(TimeoutException.class));

        assertFalse(conductor.isClosed());
    }

    @Test
    void onClientTimeoutForceClosesResources()
    {
        suppressPrintError = true;

        final Subscription subscription1 = addSubscription(CHANNEL, STREAM_ID_1, 10, null, null);
        addImage(subscription1, 20, SESSION_ID_1);
        addImage(subscription1, 30, SESSION_ID_2);

        final AvailableImageHandler availableImageHandler = mock(AvailableImageHandler.class);
        final UnavailableImageHandler unavailableImageHandler = mock(UnavailableImageHandler.class);
        final Subscription subscription2 = addSubscription(
            CHANNEL, STREAM_ID_2, 40, availableImageHandler, unavailableImageHandler);
        addImage(subscription2, 50, SESSION_ID_1);

        conductor.onClientTimeout();

        final String expectedErrorMessage = "client timeout from driver";
        final ArgumentCaptor<Throwable> errorCapture = ArgumentCaptor.forClass(Throwable.class);
        verify(mockClientErrorHandler).onError(errorCapture.capture());
        final Throwable exception = errorCapture.getValue();
        assertInstanceOf(ClientTimeoutException.class, exception);
        assertEquals("FATAL - " + expectedErrorMessage, exception.getMessage());

        final ArgumentCaptor<Image> imageArgumentCaptor = ArgumentCaptor.forClass(Image.class);
        verify(availableImageHandler).onAvailableImage(imageArgumentCaptor.capture());
        verify(unavailableImageHandler).onUnavailableImage(imageArgumentCaptor.getValue());

        final ArgumentCaptor<UnavailableImageReason> imageReasonArgumentCaptor =
            ArgumentCaptor.forClass(UnavailableImageReason.class);
        verify(unavailableImageExtendedHandler, times(3)).onUnavailableImage(
            any(), imageReasonArgumentCaptor.capture());
        final UnavailableImageReason reason = imageReasonArgumentCaptor.getValue();
        assertEquals(expectedErrorMessage, reason.reason());
        final List<UnavailableImageReason> reasons = imageReasonArgumentCaptor.getAllValues();
        assertEquals(3, reasons.size());
        for (final UnavailableImageReason unavailableImageReason : reasons)
        {
            assertSame(reason, unavailableImageReason);
        }
    }

    @Test
    void onCloseForceClosesResources()
    {
        final Subscription subscription1 =
            addSubscription(CHANNEL, STREAM_ID_1, 1, availableImageHandler, unavailableImageHandler);
        addImage(subscription1, 2, SESSION_ID_1);
        addImage(subscription1, 3, SESSION_ID_2);

        final Subscription subscription2 = addSubscription(
            CHANNEL, STREAM_ID_2, 4, null, null);
        addImage(subscription2, 5, SESSION_ID_1);

        conductor.onClose();

        final String expectedErrorMessage = "client closed";

        final ArgumentCaptor<Image> imageArgumentCaptor = ArgumentCaptor.forClass(Image.class);
        verify(availableImageHandler, times(2)).onAvailableImage(imageArgumentCaptor.capture());
        verify(unavailableImageHandler).onUnavailableImage(imageArgumentCaptor.getAllValues().get(0));
        verify(unavailableImageHandler).onUnavailableImage(imageArgumentCaptor.getAllValues().get(1));

        final ArgumentCaptor<UnavailableImageReason> imageReasonArgumentCaptor =
            ArgumentCaptor.forClass(UnavailableImageReason.class);
        verify(unavailableImageExtendedHandler, times(3)).onUnavailableImage(
            any(), imageReasonArgumentCaptor.capture());
        final UnavailableImageReason reason = imageReasonArgumentCaptor.getValue();
        assertEquals(expectedErrorMessage, reason.reason());
        final List<UnavailableImageReason> reasons = imageReasonArgumentCaptor.getAllValues();
        assertEquals(3, reasons.size());
        for (final UnavailableImageReason unavailableImageReason : reasons)
        {
            assertSame(reason, unavailableImageReason);
        }
    }

    @Test
    void forceClosesResourcesUponConductorServiceTimeoutException()
    {
        final Subscription subscription1 =
            addSubscription(CHANNEL, STREAM_ID_1, 1, availableImageHandler, unavailableImageHandler);
        addImage(subscription1, 2, SESSION_ID_1);

        final Subscription subscription2 = addSubscription(
            CHANNEL, STREAM_ID_2, 3, null, null);
        addImage(subscription2, 4, SESSION_ID_1);

        timeNs += TimeUnit.HOURS.toNanos(1);

        final ConductorServiceTimeoutException exception = assertThrowsExactly(
            ConductorServiceTimeoutException.class, () -> conductor.removeDestination(111, "aeron:ipc"));
        assertThat(exception.getMessage(),
            Matchers.startsWith("FATAL - service interval exceeded: timeout=" +
            mockAeron.context().interServiceTimeoutNs() + "ns, interval="));

        final String expectedErrorMessage = "client closed";

        final ArgumentCaptor<Image> imageArgumentCaptor = ArgumentCaptor.forClass(Image.class);
        verify(availableImageHandler).onAvailableImage(imageArgumentCaptor.capture());
        verify(unavailableImageHandler).onUnavailableImage(imageArgumentCaptor.getValue());

        final ArgumentCaptor<UnavailableImageReason> imageReasonArgumentCaptor =
            ArgumentCaptor.forClass(UnavailableImageReason.class);
        verify(unavailableImageExtendedHandler, times(2)).onUnavailableImage(
            any(), imageReasonArgumentCaptor.capture());
        final UnavailableImageReason reason = imageReasonArgumentCaptor.getValue();
        assertEquals(exception.getMessage().substring(8), reason.reason());
    }

    @Test
    void forceClosesResourcesUponDriverTimeoutException()
    {
        final Subscription subscription1 =
            addSubscription(CHANNEL, STREAM_ID_1, 1, availableImageHandler, unavailableImageHandler);
        addImage(subscription1, 2, SESSION_ID_1);

        final Subscription subscription2 = addSubscription(
            CHANNEL, STREAM_ID_2, 3, null, null);
        addImage(subscription2, 4, SESSION_ID_1);

        timeNs += mockAeron.context().keepAliveIntervalNs();
        timeMs += mockAeron.context().driverTimeoutMs() * 2;

        final DriverTimeoutException exception = assertThrowsExactly(
            DriverTimeoutException.class, () -> conductor.removeDestination(111, "aeron:ipc"));
        assertThat(exception.getMessage(), Matchers.allOf(
            Matchers.startsWith("FATAL - driver (" + mockAeron.context().aeronDirectoryName() + ") keepalive: age="),
            Matchers.endsWith("ms > timeout=" + mockAeron.context().driverTimeoutMs() + "ms")));

        final ArgumentCaptor<Image> imageArgumentCaptor = ArgumentCaptor.forClass(Image.class);
        verify(availableImageHandler).onAvailableImage(imageArgumentCaptor.capture());
        verify(unavailableImageHandler).onUnavailableImage(imageArgumentCaptor.getValue());

        final ArgumentCaptor<UnavailableImageReason> imageReasonArgumentCaptor =
            ArgumentCaptor.forClass(UnavailableImageReason.class);
        verify(unavailableImageExtendedHandler, times(2)).onUnavailableImage(
            any(), imageReasonArgumentCaptor.capture());
        final UnavailableImageReason reason = imageReasonArgumentCaptor.getValue();
        assertEquals(exception.getMessage().substring(8), reason.reason());
    }

    @Test
    void forceClosesResourcesIfHeartbeatTimestampIsInactive()
    {
        final Subscription subscription1 =
            addSubscription(CHANNEL, STREAM_ID_1, 1, availableImageHandler, unavailableImageHandler);
        addImage(subscription1, 2, SESSION_ID_1);

        final Subscription subscription2 = addSubscription(
            CHANNEL, STREAM_ID_2, 3, null, null);
        addImage(subscription2, 4, SESSION_ID_1);

        final CountersManager countersManager = new CountersManager(mockAeron.context()
            .countersMetaDataBuffer(), mockAeron.context().countersValuesBuffer());
        final int counterId = HeartbeatTimestamp.allocateCounterId(
            tempBuffer, "client-heartbeat", HEARTBEAT_TYPE_ID, countersManager, mockAeron.clientId());
        assertTrue(HeartbeatTimestamp.isActive(countersManager, counterId, HEARTBEAT_TYPE_ID, mockAeron.clientId()));
        final String errorMessage = "unexpected close of heartbeat timestamp counter: " + counterId;

        timeNs += mockAeron.context().keepAliveIntervalNs();
        // force create HeartbeatTimestamp instance
        assertThrowsExactly(
            DriverTimeoutException.class, () -> conductor.removeDestination(111, "aeron:ipc"));

        timeNs += mockAeron.context().keepAliveIntervalNs();
        countersManager.free(counterId);
        final AeronException exception = assertThrowsExactly(
            AeronException.class, () -> conductor.removeDestination(111, "aeron:ipc"));
        assertEquals("ERROR - " + errorMessage, exception.getMessage());

        final ArgumentCaptor<Image> imageArgumentCaptor = ArgumentCaptor.forClass(Image.class);
        verify(availableImageHandler).onAvailableImage(imageArgumentCaptor.capture());
        verify(unavailableImageHandler).onUnavailableImage(imageArgumentCaptor.getValue());

        final ArgumentCaptor<UnavailableImageReason> imageReasonArgumentCaptor =
            ArgumentCaptor.forClass(UnavailableImageReason.class);
        verify(unavailableImageExtendedHandler, times(2)).onUnavailableImage(
            any(), imageReasonArgumentCaptor.capture());
        final UnavailableImageReason reason = imageReasonArgumentCaptor.getValue();
        assertEquals(errorMessage, reason.reason());
    }

    @Test
    void forceClosesResourcesUponAgentTerminationExceptionDuringClientRequest()
    {
        final Subscription subscription1 =
            addSubscription(CHANNEL, STREAM_ID_1, 1, availableImageHandler, unavailableImageHandler);
        addImage(subscription1, 2, SESSION_ID_1);

        final Subscription subscription2 = addSubscription(
            CHANNEL, STREAM_ID_2, 3, null, null);
        addImage(subscription2, 4, SESSION_ID_1);

        whenReceiveBroadcastOnMessage(
            ControlProtocolEvents.ON_SUBSCRIPTION_READY,
            subscriptionReadyBuffer,
            (buffer) ->
            {
                throw new AgentTerminationException("BOO");
            });

        final AgentTerminationException exception = assertThrowsExactly(
            AgentTerminationException.class, () -> conductor.addSubscription("aeron:ipc", 333));
        assertEquals("BOO", exception.getMessage());

        final ArgumentCaptor<Image> imageArgumentCaptor = ArgumentCaptor.forClass(Image.class);
        verify(availableImageHandler).onAvailableImage(imageArgumentCaptor.capture());
        verify(unavailableImageHandler).onUnavailableImage(imageArgumentCaptor.getValue());

        final ArgumentCaptor<UnavailableImageReason> imageReasonArgumentCaptor =
            ArgumentCaptor.forClass(UnavailableImageReason.class);
        verify(unavailableImageExtendedHandler, times(2)).onUnavailableImage(
            any(), imageReasonArgumentCaptor.capture());
        final UnavailableImageReason reason = imageReasonArgumentCaptor.getValue();
        assertEquals("agent termination exception: " + exception.getMessage(), reason.reason());
    }

    @Test
    void forceClosesResourcesIfDriverEventsAdapterIsInvalid()
    {
        final Subscription subscription1 =
            addSubscription(CHANNEL, STREAM_ID_1, 1, availableImageHandler, unavailableImageHandler);
        addImage(subscription1, 2, SESSION_ID_1);
        addImage(subscription1, 3, SESSION_ID_2);

        final Subscription subscription2 = addSubscription(
            CHANNEL, STREAM_ID_2, 4, null, null);
        addImage(subscription2, 5, SESSION_ID_2);

        whenReceiveBroadcastOnMessage(
            ControlProtocolEvents.ON_SUBSCRIPTION_READY,
            subscriptionReadyBuffer,
            (buffer) ->
            {
                throw new IllegalStateException("crap");
            });

        final IllegalStateException exception = assertThrowsExactly(
            IllegalStateException.class, () -> conductor.addSubscription("aeron:ipc", 333));
        assertEquals("crap", exception.getMessage());

        final ArgumentCaptor<Image> imageArgumentCaptor = ArgumentCaptor.forClass(Image.class);
        verify(availableImageHandler, times(2)).onAvailableImage(imageArgumentCaptor.capture());
        verify(unavailableImageHandler).onUnavailableImage(imageArgumentCaptor.getAllValues().get(0));
        verify(unavailableImageHandler).onUnavailableImage(imageArgumentCaptor.getAllValues().get(1));

        final ArgumentCaptor<UnavailableImageReason> imageReasonArgumentCaptor =
            ArgumentCaptor.forClass(UnavailableImageReason.class);
        verify(unavailableImageExtendedHandler, times(3)).onUnavailableImage(
            any(), imageReasonArgumentCaptor.capture());
        final UnavailableImageReason reason = imageReasonArgumentCaptor.getValue();
        assertEquals("driver events adapter is invalid: " + exception.getMessage(), reason.reason());
    }

    @Test
    void subscriptionCloseNotifiesUnavailableImageExtendedHandler()
    {
        final Subscription subscription =
            addSubscription(CHANNEL, STREAM_ID_1, 1, availableImageHandler, unavailableImageHandler);
        addImage(subscription, 2, SESSION_ID_1);
        addImage(subscription, 3, SESSION_ID_2);

        subscription.close();

        final ArgumentCaptor<Image> imageArgumentCaptor = ArgumentCaptor.forClass(Image.class);
        verify(availableImageHandler, times(2)).onAvailableImage(imageArgumentCaptor.capture());
        verify(unavailableImageHandler).onUnavailableImage(imageArgumentCaptor.getAllValues().get(0));
        verify(unavailableImageHandler).onUnavailableImage(imageArgumentCaptor.getAllValues().get(1));

        final ArgumentCaptor<UnavailableImageReason> imageReasonArgumentCaptor =
            ArgumentCaptor.forClass(UnavailableImageReason.class);
        verify(unavailableImageExtendedHandler, times(2)).onUnavailableImage(
            any(), imageReasonArgumentCaptor.capture());
        final UnavailableImageReason reason = imageReasonArgumentCaptor.getValue();
        assertEquals("subscription closed", reason.reason());
        for (final UnavailableImageReason unavailableImageReason : imageReasonArgumentCaptor.getAllValues())
        {
            assertSame(reason, unavailableImageReason);
        }
    }

    @Test
    void subscriptionCloseNotifiesUnavailableImageHandler()
    {
        final Aeron.Context context = mockAeron.context();
        context.unavailableImageExtendedHandler(null);

        conductor = new ClientConductor(context, mockAeron);

        final Subscription subscription =
            addSubscription(CHANNEL, STREAM_ID_1, 1, availableImageHandler, unavailableImageHandler);
        addImage(subscription, 2, SESSION_ID_1);
        addImage(subscription, 3, SESSION_ID_2);

        subscription.close();

        final ArgumentCaptor<Image> imageArgumentCaptor = ArgumentCaptor.forClass(Image.class);
        verify(availableImageHandler, times(2)).onAvailableImage(imageArgumentCaptor.capture());
        verify(unavailableImageHandler).onUnavailableImage(imageArgumentCaptor.getAllValues().get(0));
        verify(unavailableImageHandler).onUnavailableImage(imageArgumentCaptor.getAllValues().get(1));

        verifyNoInteractions(unavailableImageExtendedHandler);
    }

    @Test
    void onErrorCloseSubscription()
    {
        final Subscription subscription =
            addSubscription(CHANNEL, STREAM_ID_1, 1, availableImageHandler, unavailableImageHandler);
        addImage(subscription, 2, SESSION_ID_1);
        addImage(subscription, 3, SESSION_ID_2);

        final ErrorCode errorCode = ErrorCode.RESOURCE_TEMPORARILY_UNAVAILABLE;
        final String errorMessage = "subscription is not available";
        conductor.onError(subscription.registrationId(), errorCode.value(), errorCode, errorMessage);

        assertTrue(subscription.isClosed());

        final ArgumentCaptor<Image> imageArgumentCaptor = ArgumentCaptor.forClass(Image.class);
        verify(availableImageHandler, times(2)).onAvailableImage(imageArgumentCaptor.capture());
        verify(unavailableImageHandler).onUnavailableImage(imageArgumentCaptor.getAllValues().get(0));
        verify(unavailableImageHandler).onUnavailableImage(imageArgumentCaptor.getAllValues().get(1));

        final ArgumentCaptor<UnavailableImageReason> imageReasonArgumentCaptor =
            ArgumentCaptor.forClass(UnavailableImageReason.class);
        verify(unavailableImageExtendedHandler, times(2)).onUnavailableImage(
            any(), imageReasonArgumentCaptor.capture());
        final UnavailableImageReason reason = imageReasonArgumentCaptor.getValue();
        assertEquals(
            "error received from the driver: code=" + errorCode + " message=" + errorMessage,
            reason.reason());
        for (final UnavailableImageReason unavailableImageReason : imageReasonArgumentCaptor.getAllValues())
        {
            assertSame(reason, unavailableImageReason);
        }
    }

    @Test
    void onAsyncErrorCloseSubscription()
    {
        final Subscription subscription =
            addSubscription(CHANNEL, STREAM_ID_1, 1, availableImageHandler, unavailableImageHandler);
        addImage(subscription, 2, SESSION_ID_1);
        addImage(subscription, 3, SESSION_ID_2);

        final ErrorCode errorCode = ErrorCode.GENERIC_ERROR;
        final String errorMessage = "something went wrong";
        conductor.onAsyncError(subscription.registrationId(), errorCode.value(), errorCode, errorMessage);

        assertTrue(subscription.isClosed());

        final ArgumentCaptor<Image> imageArgumentCaptor = ArgumentCaptor.forClass(Image.class);
        verify(availableImageHandler, times(2)).onAvailableImage(imageArgumentCaptor.capture());
        verify(unavailableImageHandler).onUnavailableImage(imageArgumentCaptor.getAllValues().get(0));
        verify(unavailableImageHandler).onUnavailableImage(imageArgumentCaptor.getAllValues().get(1));

        final ArgumentCaptor<UnavailableImageReason> imageReasonArgumentCaptor =
            ArgumentCaptor.forClass(UnavailableImageReason.class);
        verify(unavailableImageExtendedHandler, times(2)).onUnavailableImage(
            any(), imageReasonArgumentCaptor.capture());
        final UnavailableImageReason reason = imageReasonArgumentCaptor.getValue();
        assertEquals(
            "error received from the driver: code=" + errorCode + " message=" + errorMessage,
            reason.reason());
        for (final UnavailableImageReason unavailableImageReason : imageReasonArgumentCaptor.getAllValues())
        {
            assertSame(reason, unavailableImageReason);
        }
    }

    @Test
    void onAsyncErrorClosePendingSubscriptionSubscription()
    {
        final long registrationId =
            conductor.asyncAddSubscription(CHANNEL, STREAM_ID_1, availableImageHandler, unavailableImageHandler);
        final Subscription subscription =
            ((ClientConductor.PendingSubscription)conductor.resourceByRegIdMap().get(registrationId)).subscription;
        conductor.addImage(subscription, 2, SESSION_ID_1, SUBSCRIPTION_POSITION_ID, SESSION_ID_1 + "-log",
            SOURCE_INFO);

        final ErrorCode errorCode = ErrorCode.UNKNOWN_SUBSCRIPTION;
        final String errorMessage = "foo";
        conductor.onAsyncError(subscription.registrationId(), errorCode.value(), errorCode, errorMessage);

        assertTrue(subscription.isClosed());

        final ArgumentCaptor<Image> imageArgumentCaptor = ArgumentCaptor.forClass(Image.class);
        verify(availableImageHandler).onAvailableImage(imageArgumentCaptor.capture());
        verify(unavailableImageHandler).onUnavailableImage(imageArgumentCaptor.getValue());

        final ArgumentCaptor<UnavailableImageReason> imageReasonArgumentCaptor =
            ArgumentCaptor.forClass(UnavailableImageReason.class);
        verify(unavailableImageExtendedHandler).onUnavailableImage(any(), imageReasonArgumentCaptor.capture());
        final UnavailableImageReason reason = imageReasonArgumentCaptor.getValue();
        assertEquals(
            "error received from the driver: code=" + errorCode + " message=" + errorMessage,
            reason.reason());
    }

    private Subscription addSubscription(
        final String channel,
        final int streamId,
        final long correlationId,
        final AvailableImageHandler availableImageHandler,
        final UnavailableImageHandler unavailableImageHandler)
    {
        when(driverProxy.addSubscription(channel, streamId)).thenReturn(correlationId);

        whenReceiveBroadcastOnMessage(
            ControlProtocolEvents.ON_SUBSCRIPTION_READY,
            subscriptionReadyBuffer,
            (buffer) ->
            {
                subscriptionReady.correlationId(correlationId);
                return SubscriptionReadyFlyweight.LENGTH;
            });

        return conductor.addSubscription(channel, streamId, availableImageHandler, unavailableImageHandler);
    }

    private void addImage(final Subscription subscription, final long correlationId, final int sessionId)
    {
        conductor.onAvailableImage(
            correlationId,
            sessionId,
            subscription.registrationId(),
            SUBSCRIPTION_POSITION_ID,
            sessionId + "-log",
            SOURCE_INFO);
    }

    private void whenReceiveBroadcastOnMessage(
        final int msgTypeId, final MutableDirectBuffer buffer, final ToIntFunction<MutableDirectBuffer> filler)
    {
        doAnswer(
            (invocation) ->
            {
                final int length = filler.applyAsInt(buffer);
                conductor.driverListenerAdapter().onMessage(msgTypeId, buffer, 0, length);

                return 1;
            })
            .when(mockToClientReceiver).receive(any(MessageHandler.class));
    }

    class PrintError implements ErrorHandler
    {
        public void onError(final Throwable throwable)
        {
            if (!suppressPrintError)
            {
                throwable.printStackTrace();
            }
        }
    }
}

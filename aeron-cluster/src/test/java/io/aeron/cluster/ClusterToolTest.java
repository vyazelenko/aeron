/*
 *  Copyright 2014-2020 Real Logic Limited.
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

import io.aeron.cluster.service.ClusterMarkFile;
import io.aeron.test.SlowTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.file.Path;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.cluster.ClusterTool.*;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.*;

class ClusterToolTest
{
    private final CapturingPrintStream capturingPrintStream = new CapturingPrintStream();

    @SlowTest
    @Test
    @Timeout(30)
    void shouldHandleSnapshotOnLeaderOnly() throws InterruptedException
    {
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            final TestNode leader = cluster.awaitLeader();
            final long initialSnapshotCount = cluster.countRecordingLogSnapshots(leader);

            assertTrue(snapshot(
                leader.consensusModule().context().clusterDir(),
                capturingPrintStream.resetAndGetPrintStream()));

            assertThat(
                capturingPrintStream.flushAndGetContent(),
                containsString("SNAPSHOT applied successfully"));

            assertEquals(initialSnapshotCount + 1, cluster.countRecordingLogSnapshots(leader));

            for (final TestNode follower : cluster.followers())
            {
                assertFalse(snapshot(
                    follower.consensusModule().context().clusterDir(),
                    capturingPrintStream.resetAndGetPrintStream()));

                assertThat(
                    capturingPrintStream.flushAndGetContent(),
                    containsString("Current node is not the leader"));
            }
        }
    }

    @SlowTest
    @Test
    @Timeout(30)
    void shouldNotSnapshotWhenSuspendedOnly() throws InterruptedException
    {
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            final TestNode leader = cluster.awaitLeader();
            final long initialSnapshotCount = cluster.countRecordingLogSnapshots(leader);

            assertTrue(suspend(
                leader.consensusModule().context().clusterDir(),
                capturingPrintStream.resetAndGetPrintStream()));

            assertThat(
                capturingPrintStream.flushAndGetContent(),
                containsString("SUSPEND applied successfully"));

            assertFalse(snapshot(
                leader.consensusModule().context().clusterDir(),
                capturingPrintStream.resetAndGetPrintStream()));

            final String expectedMessage =
                "Unable to SNAPSHOT as the state of the consensus module is SUSPENDED, but needs to be ACTIVE";
            assertThat(capturingPrintStream.flushAndGetContent(), containsString(expectedMessage));

            assertEquals(initialSnapshotCount, cluster.countRecordingLogSnapshots(leader));
        }
    }

    @SlowTest
    @Test
    @Timeout(30)
    void shouldSuspendAndResume() throws InterruptedException
    {
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            final TestNode leader = cluster.awaitLeader();

            assertTrue(suspend(
                leader.consensusModule().context().clusterDir(),
                capturingPrintStream.resetAndGetPrintStream()));

            assertThat(
                capturingPrintStream.flushAndGetContent(),
                containsString("SUSPEND applied successfully"));

            assertTrue(resume(
                leader.consensusModule().context().clusterDir(),
                capturingPrintStream.resetAndGetPrintStream()));

            assertThat(
                capturingPrintStream.flushAndGetContent(),
                containsString("RESUME applied successfully"));
        }
    }

    @Test
    void failIfMarkFileUnavailable(final @TempDir Path emptyClusterDir)
    {
        assertFalse(snapshot(emptyClusterDir.toFile(), capturingPrintStream.resetAndGetPrintStream()));
        assertThat(
            capturingPrintStream.flushAndGetContent(),
            containsString(ClusterMarkFile.FILENAME + " does not exist."));
    }

    @Test
    void stopMemberFailsWithErrorIfClusterMarkFileDoesNotExists(final @TempDir Path emptyClusterDir)
    {
        System.clearProperty(AERON_CLUSTER_TOOL_TIMEOUT_PROP_NAME);

        stopMember(capturingPrintStream.resetAndGetPrintStream(), emptyClusterDir.toFile(), 111);

        assertThat(
            capturingPrintStream.flushAndGetContent(),
            containsString(ClusterMarkFile.FILENAME + " does not exist."));
    }

    @Test
    @Timeout(20)
    void stopMemberInitiatesGracefulLeaderStepDown() throws InterruptedException
    {
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            final TestNode leader = cluster.awaitLeader();

            final int leaderId = leader.index();
            final File clusterDir = leader.consensusModule().context().clusterDir();
            leader.terminationExpected(true);

            stopMember(capturingPrintStream.resetAndGetPrintStream(), clusterDir, leaderId);

            final TestNode newLeader = cluster.awaitLeader(leaderId);

            assertNotSame(leader, newLeader);
            assertNotEquals(leaderId, newLeader.index());
        }
    }

    static class CapturingPrintStream
    {
        private final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        private final PrintStream printStream = new PrintStream(byteArrayOutputStream);

        PrintStream resetAndGetPrintStream()
        {
            byteArrayOutputStream.reset();
            return printStream;
        }

        String flushAndGetContent()
        {
            printStream.flush();
            return new String(byteArrayOutputStream.toByteArray(), US_ASCII);
        }
    }
}

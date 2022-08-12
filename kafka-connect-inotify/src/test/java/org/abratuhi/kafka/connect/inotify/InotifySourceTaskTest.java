package org.abratuhi.kafka.connect.inotify;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class InotifySourceTaskTest {

    private InotifySourceTask task = new InotifySourceTask();

    @Test
    void startTmp() throws IOException, InterruptedException {
        Path testDir = Files.createTempDirectory("testdir");
        task.start(Map.of(InotifySourceConfig.SOURCE_DIR, testDir.toString()));
        Thread.sleep(1_000L);
        Files.createTempFile(testDir, "testfile1", "testfile1");
        task.stop();
        List<SourceRecord> taskConfigs = task.poll();

        assertThat(taskConfigs.size()).isEqualTo(1);
        assertThat(taskConfigs.get(0).topic()).startsWith("testfile1");

        Files.createTempFile(testDir, "testfile2", "testfile2");
        taskConfigs = task.poll();
        assertThat(taskConfigs.size()).isEqualTo(0);
    }
}

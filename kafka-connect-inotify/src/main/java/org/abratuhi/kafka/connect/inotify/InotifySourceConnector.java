package org.abratuhi.kafka.connect.inotify;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.io.IOException;
import java.nio.file.*;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class InotifySourceConnector extends SourceConnector {
    private boolean stopped = false;

    private Queue<Path> changes = new ConcurrentLinkedQueue<>();

    public void start(Map<String, String> map) {
        System.out.println("InotifySourceConnector#start");

        Path baseDir = Path.of(map.get(InotifySourceConfig.SOURCE_DIR));

        try {
            WatchService watcher = FileSystems.getDefault().newWatchService();

            baseDir.register(watcher,
                    StandardWatchEventKinds.ENTRY_CREATE,
                    StandardWatchEventKinds.ENTRY_MODIFY,
                    StandardWatchEventKinds.ENTRY_DELETE
            );


            // see https://docs.oracle.com/javase/tutorial/essential/io/notification.html
            while (!stopped) {

                // wait for key to be signaled
                WatchKey key;
                try {
                    key = watcher.take();
                } catch (InterruptedException x) {
                    return;
                }

                for (WatchEvent<?> event: key.pollEvents()) {
                    WatchEvent.Kind<?> kind = event.kind();

                    // This key is registered only
                    // for ENTRY_CREATE events,
                    // but an OVERFLOW event can
                    // occur regardless if events
                    // are lost or discarded.
                    if (kind == StandardWatchEventKinds.OVERFLOW) {
                        continue;
                    }

                    // The filename is the
                    // context of the event.
                    WatchEvent<Path> ev = (WatchEvent<Path>) event;
                    Path filename = ev.context();

                    // Verify that the new
                    //  file is a text file.
                    try {
                        // Resolve the filename against the directory.
                        // If the filename is "test" and the directory is "foo",
                        // the resolved name is "test/foo".
                        Path child = baseDir.resolve(filename);
                        if (!Files.probeContentType(child).equals("text/plain")) {
                            System.err.format("New file '%s'" +
                                    " is not a plain text file.%n", filename);
                            continue;
                        }
                    } catch (IOException x) {
                        System.err.println(x);
                        continue;
                    }

                    changes.add(filename);
                }

                // Reset the key -- this step is critical if you want to
                // receive further watch events.  If the key is no longer valid,
                // the directory is inaccessible so exit the loop.
                boolean valid = key.reset();
                if (!valid) {
                    break;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Class<? extends Task> taskClass() {
        return InotifySourceTask.class;
    }

    public List<Map<String, String>> taskConfigs(int i) {
        System.out.println("InotifySourceConnector#taskConfigs");
        return IntStream.of(i)
                .mapToObj(value -> changes.poll())
                .map(path -> Map.of(InotifySourceConfig.SOURCE_FILE, path.toString()))
                .collect(Collectors.toList());
    }

    public void stop() {
        System.out.println("InotifySourceConnector#stop");
        stopped = true;
    }

    public ConfigDef config() {
        return null;
    }

    public String version() {
        return Version.get();
    }
}

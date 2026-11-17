# Bounded native executable acquisition

Host #8 prerequisite: before starting the pinned metadata-only native transport,
Host now copies from one opened regular-file descriptor with a 512 MiB cap.
Empty and oversized files are rejected before creating a destination. Final-path
symlinks and FIFO/device inputs cannot turn preparation into an unbounded read.
A file that grows beyond its observed size or is truncated during copying is
rejected. The destination is created exclusively, starts non-executable, and is
read back for the unchanged SHA-256 pin before receiving execute permission.
Preparation failure removes the newly created copy, never an existing target.
The outer session preparation still cleans up its fresh private directory.

The byte limit bounds disk consumption and copying, not filesystem latency.
The private directory and its ancestors remain trusted; this is not a hostile
filesystem, concurrent owner mutation, power-loss, or hard wall-time guarantee.
The 512 MiB cap is a resource policy, not an additional accepted binary digest.
The existing macOS ARM64 platform restriction and exact executable digest are
unchanged. No new native executable, credential, task launch, model invocation,
or production qualification is introduced.

## Component evidence

`cargo test --offline --locked` exercises actual local filesystem handling:
correct bytes/mode, exclusive target preservation, failed pin cleanup, final
symlink, directory, unopened FIFO, empty file, and sparse over-limit file.
An endless synthetic reader proves growth handling writes no more than the
observed length; short and exact readers cover truncation and equality.
These fixtures are never executed. Existing scripted peer and SQLite tests
remain component evidence, not pinned-native acceptance.

Host #8 remains open for the full pinned-native session/turn/item conformance,
uncertain launch recovery and task/cancellation qualification requirements.

# zettarepl

**zettarepl** is a cross-platform ZFS replication solution. It provides:
* Snapshot-based *PUSH* and *PULL* replication over SSH or high-speed unencrypted connection
* Extensible snapshot creation and replication schedule, replication of manually created snapshots
* Consistent recursive snapshots with possibility to exclude certain  datasets
* All modern ZFS features support including resumable replication
* Flexible snapshot retention on both local and remote sides
* Comprehensive logging that helps you to understand what is going on and why
* Configuration via simple and clear YAML file
* Full integration with [FreeNAS](http://www.freenas.org/), the World’s #1 data storage solution

## Configuration

**zettarepl** is configured via single [YAML](http://yaml.org/) file that defines common replication parameters and
a list of tasks to perform.

### Common replication parameters

```yaml
# Specifies the maximum number of simultaneously running replication tasks
# Default is unlimited
max-parallel-replication-tasks: null

# Specifies timezone which is used for snapshot creation date and time
# Defaults to system timezone
timezone: "US/Pacific"
```

### Periodic snapshot tasks

Periodic snapshot tasks automatically create snapshot tasks at specified schedule. This feature of zettarepl can be
used standalone without performing any actual replication.

```yaml
periodic-snapshot-tasks:
  # Each task in zettarepl must have an unique id to make references for it
  src:
    # Dataset to make snapshots
    dataset: data/src

    # You must explicitly specify if you want recursive or non-recursive
    # snapshots
    recursive: true

    # You can exclude certain datasets from recursive snapshots
    # Please note that you won't be able to use such snapshots with recursive
    # functions of ZFS (e.g. zfs rollback -r) as it would fail with
    # "data/src/excluded@snapshot: snapshot does not exist"
    # They are still consistent with each other, i.e. they are not created
    # independently but in one transaction.
    exclude:
      - data/src/excluded

    # You can specify lifetime for snapshots so they would get automatically
    # deleted up after a certain amount of time.
    # Lifetime is specified in ISO8601 Duration Format.
    # "P365D" means "365 days", "PT12H" means "12 hours" and "P30DT12H" means
    # "30 days and 12 hours"
    # When this is not specified, snapshots are not deleted automatically.
    lifetime: P365D

    # This is a very important parameter that defines how your snapshots would
    # be named depending on their creation date.
    # zettarepl does not readsnapshot creation date from metadata (this can be
    # very slow for reasonably big amount of snapshots), instead it relies
    # solely on their names to parse their creation date.
    # Due to this optimization, naming schema must contain all of "%Y", "%m",
    # "%d", "%H" and "%M" format strings to allow unambiguous parsing of string
    # to date and time accurate to the minute.
    # Do not create two periodic snapshot tasks for same dataset with naming
    # schemas that can be mixed up, e.g. "snap-%Y-%m-%d-%H-%M" and
    # "snap-%Y-%d-%m-%H-%M". zettarepl won't be able to check for it on early
    # stage and will get confused.
    naming-schema: snap-%Y-%m-%d-%H-%M

    # Crontab-like schedule when this replication task would run
    # default schedule is * * * * * (every minute)
    schedule:
      minute: "*/15"    # Every 15 minutes
      hour: "8,12,17"   # At 8, 12 and 17'o clock
      day-of-month: "*" # At any day of month
      month: "*"
      day-of-week: "*"
      start: "09:30"
      end: "17:00"
```

zettarepl periodic snapshot tasks retention is smart enough to behave correctly in variety of situations. E.g., if task
**a** creates snapshots every hour and stores them for one day and task **b** creates snapshots every two hours and
stores them for two days and they share naming schema, you'll get correct behavior: at the beginning of the new day,
you'll have 24 snapshots for previous day and 12 snapshots for the day before it; retention for task **a** won't delete
snapshots that are still retained for task **b**.

### Replication tasks

Replication tasks send snapshots (created either by periodic snapshot task or manually) from one host to other. It can
either be local host to remote (*push* replication) or remote host to local (*pull* replication). zettarepl requires
SSH connection to remote host. Remote host can also be a local host, in that case SSH usage is bypassed. Replication
tasks can be both triggered automatically by periodic snapshot task or schedule or be ran manually.

#### Push replication

```yaml
replication-tasks:
  src:
    # Either push or pull
    direction: push

    # Transport option defines remote host to send/receive snapshots. You
    # can also just specify "local" to send/receive snapshots to localhost
    transport:
      type: local

    # Source dataset
    source-dataset: data/src
    # Target dataset
    target-dataset: data/dst

    # Or you can specify multiple source datasets, e.g.:
    # source-dataset:
    #   - data/src/work
    #   - data/src/holiday/summer
    # They would be replicated to data/dst/work and data/dst/holiday/summer

    # "recursive" and "exclude" work exactly like they work for periodic
    # snapshot tasks
    recursive: true
    exclude:
      - data/src/excluded

    # Send dataset properties along with snapshots. Enabled by default.
    # Disable this if you use custom mountpoints and don't want them to be
    # replicated to remote system.
    properties: true

    # When sending properties, exclude these properties
    properties-exclude:
      - mountpoint

    # When sending properties, override these properties
    properties-override:
      compression: gzip-9

    # Send a replication stream package, which will replicate the specified filesystem, and all descendent file systems.
    # When received, all properties, snapshots, descendent file systems, and clones are preserved.
    # You must have recursive set to true, exclude to empty list, properties to true. Disabled by default.
    replicate: false

    # Use the following encryption parameters to create a target dataset.
    encryption:
      # Encryption key
      key: "0a0b0c0d0e0f"
      # Key format. Can be "hex" or "passphrase"
      key-format: "hex"
      # Path to store encryption key.
      # A special value "$TrueNAS" will store the key in TrueNAS database.
      key-location: "/data/keys/dataset.key"

    # You must specify at least one of the following two fields for push
    # replication:

    # List of periodic snapshot tasks ids that are used as snapshot sources
    # for this replication task.
    # "recursive" and "exclude" fields must match between replication task
    # and all periodic snapshot tasks bound to it, i.e. you can't do
    # recursive replication of non-recursive snapshots and you must
    # exclude all child snapshots that your periodic snapshot tasks exclude
    periodic-snapshot-tasks:
      - src

    # List of naming schemas for snapshots to replicate (in addition to
    # periodic-snapshot-tasks, if specified). Use this if you want to
    # replicate manually created snapshots.
    # As any other naming schema, this must contain all of "%Y", "%m",
    # "%d", "%H" and "%M". You won't be able to replicate snapshots
    # that can't be parsed into their creation dates with zettarepl.
    also-include-naming-schema:
      - manual-snap-%Y-%m-%d_%H-%M

    # If true, replication task will run automatically either after bound
    # periodic snapshot task or on schedule
    auto: true

    # Same crontab-like schedule used to run the replication task.
    # Required when you don't have any bound periodic snapshot task but
    # want replication task to run automatically on schedule. Otherwise,
    # overrides bound periodic snapshot task schedule.
    schedule:
      minute: "0"

    # How to delete snapshots on target. "source" means "delete snapshots
    # that are no more present on source", more policies documented
    # below
    retention-policy: source
```

There are also a few additional options that you can use:

```yaml
    # Using this option will replicate all snapshots which names match
    # specified regular expression. The performance on the systems with large
    # number of snapshots will be lower, as snapshots metadata needs to be read
    # in order to determine snapshots creation order.
    name-regex: "auto-[0-9-]+|manual-[0-9]+"

    # You can restrict snapshots that your task replicates. For example,
    # you can bind replication to periodic snapshot task that takes snapshots
    # every hour but only replicate snapshots taken at even hours.
    restrict-schedule:
      hour: "*/2"

    # Set it to true to user your task schedule as restrict-schedule
    # (or in addition to it)
    only-matching-schedule: true

    # Set it to "set" to set all destination datasets to readonly=on
    # after finishing the replication.
    # Set it to "require" to require all existing destination datasets
    # to have readonly=on property.
    # Set it to "ignore" to avoid this kind of behavior (default).
    readonly: ignore

    # By default, if there are snapshots on target dataset but none of them
    # matches any of source snapshot, replication is aborted. This is done
    # to prevent misconfigured replication from destroying snapshots on
    # target dataset. Enabling allow-from-scratch overrides this, so if no
    # snapshots are related, remote dataset is recreated from scratch without
    # using incremental replication (because it is not possible in that case)
    allow-from-scratch: true

    # When enabled, this will prevent source dataset snapshots from being
    # deleted by retention if replication fails for some reason. This
    # is done by checking all of related targets for being replicated
    # properly before running a retention, so failing remote for certain
    # dataset will prevent this dataset snapshots from retention until
    # the remote is fixed. That's why this option is disabled by default.
    hold-pending-snapshots: true

    # Custom lifetime for replicated snapshots (as usual in ISO8601 format)
    retention-policy: custom
    lifetime: P180D
    # Additional lifetimes might be specified for snapshots that match specific
    # schedules
    lifetimes:      
      daily:
        schedule:
          hour: 0
        lifetime: P360D
      weekly:
        schedule:
          hour: 0
          dow: 1
        lifetime: P720D

    # Do not delete remote snapshots at all
    retention-policy: none

    # Replication stream compression (zfs stream piped to external program,
    # one of lz4, pigz, plzip, xz)
    # Only available for SSH transport
    compression: lz4

    # Replication speed limit (stream piped to `throttle` utility)
    # Only available for SSH transport
    speed-limit: 1048576

    # ZFS options (all disabled by default)
    dedup: true
    large-block: true
    embed: true
    compressed: true

    # Number of retries before giving up on recoverable error (default is 5)
    retries: 5
```

#### Pull replication

Pull replication is a little bit simpler. Obviously you can't specify `periodic-snapshot-tasks` or
`hold-pending-snapshots` because it does not do snapshot creation or retention on remote side. Following options are
required:

```yaml
    # Naming schema(s) that are used to parse snapshot dates on remote side.
    # Only snapshots matching any of the specified naming schema(s) are
    # replicated.
    naming-schema:
      - snap-%Y-%m-%d_%H-%M
```

If you want pull replication to run automatically, you must specify `schedule` because there is no other way to trigger
it. `restrict-schedule` option does not have any sense for pull replication because you can just enable
`only-matching-schedule`. `also-include-naming-schema` is also forbidden, please specify all your naming schemas in
`naming-schema` field.

#### Transports

##### `local`

Local transport is the simplest: it just replicates snapshots to another dataset on local host. No options are
available.

```yaml
    transport:
      type: local
```

##### `ssh`

```yaml
    transport:
      type: ssh

      # Basic SSH connection parameters
      hostname: 192.168.0.187
      port: 22
      username: root

      # Only private key authorization is supported
      private-key: |
        -----BEGIN RSA PRIVATE KEY-----
        ...
        -----END RSA PRIVATE KEY-----

      # You must explicitly specify remote host key and it is checked strictly
      host-key: "ecdsa-sha2-nistp256 ..."

      # default is 10
      connect-timeout: 10

      # SSH encryption cipher.
      # "fast" is "aes128-ctr,aes192-ctr,aes256-ctr"
      # "none" completely disables SSH encryption, both server and client must
      # have HPN-SSH patch
      cipher: standard
```

##### `ssh+netcat`

SSH transport is limited in performance even with encryption completely disabled due to lots of overhead in SSH protocol
itselt, in kernel-userspace context switched. For high performance replication (singles and tens gigabits per second)
in trusted network we recommend using `ssh+netcat` transport. It is only supported on FreeBSD and requires
[py-libzfs](https://github.com/freenas/py-libzfs) installed but offers the best performance possible.

```yaml
    transport:
      type: ssh+netcat

      # These are all SSH transport options (SSH connection is still required
      # but only used for control purposes)

      hostname: 192.168.0.187
      port: 22
      username: root

      private-key: |
        -----BEGIN RSA PRIVATE KEY-----
        ...
        -----END RSA PRIVATE KEY-----

      host-key: "ecdsa-sha2-nistp256 ..."

      connect-timeout: 10

      # To establish connection between hosts, at least one of the sides must
      # be able to open TCP port that other side can connect too. Here is
      # where you choose which side will open port: "local" or "remote"
      active-side: local

      # Network interface that active side will bind to. By default this is
      # "0.0.0.0"
      active-side-listen-address: "192.168.0.1"

      # Port range that active side will try to open
      active-side-min-port: 1024
      active-side-max-port: 65535

      # IP address that passive side will use to connect to the active side
      # If active side is "local", this is default to `SSH_CLIENT` environment
      # variable that is seen when local host connects to remote via SSH
      # If active side is "remote", this is default to SSH hostname
      # This can be overriden for sophisticated network configurations (e.g.
      # LAGG)
      passive-side-connect-address: "192.168.1.1"
```

## Usage

Start zettarepl with following command:

```bash
zettarepl run /path/to/definition.yaml
```

It will run indefinitely, processing tasks on schedule and writing logs to stderr

If you only want to run one cycle of scheduler, i.e. run tasks that should be ran at current minute and exit, run:

```bash
zettarepl run --once /path/to/definition.yaml
```

You can change log level with `-l` option. Default logging level is `info`, which should inform you about each action
zettarepl is doing and explain why it is doing it. If everything is working as expected, you can change it to `warning`
or even `error` someday.

On the other hand, if you are a developer, you might want to set it to `debug`. If you don't need lots of SSH client
debugging messages, you can change it to `debug,paramiko:info` (and this way you can configure custom level for any
other python logger).

Color logging is automatically disabled when stderr is not a tty.

## Development

### Running integration tests

Prerequisites:
* FreeBSD or Linux VM with recent Python 3 installation. TrueNAS preferred (there are some TrueNAS-specific tests that
  will otherwise fail) but not required.
* root SSH key that will allow passwordless `ssh root@localhost`
* `data` ZFS pool mounted in `/mnt/data`
* pytest installed (just run `easy_install pytest`)

To run all tests:
```
root@truenas[~/dev/zettarepl]# pytest integration-tests/
```

To run a specific test file:
```
root@truenas[~/dev/zettarepl]# pytest integration-tests/retention/test_zfs_hold.py 
```

# -*- coding=utf-8 -*-
import textwrap

from mock import Mock

from zettarepl.dataset.mtab import Mtab


def test__mtab__freebsd():
    shell = Mock()
    shell.exec.return_value = textwrap.dedent("""\
        freenas-boot/ROOT/default on / (zfs, local, noatime, nfsv4acls)
        devfs on /dev (devfs, local, multilabel)
        tmpfs on /etc (tmpfs, local)
        tmpfs on /mnt (tmpfs, local)
        tmpfs on /var (tmpfs, local)
        fdescfs on /dev/fd (fdescfs)
        data on /mnt/data (zfs, local, nfsv4acls)
        data/.system on /var/db/system (zfs, local, nfsv4acls)
        data/.system/cores on /var/db/system/cores (zfs, local, nfsv4acls)
        data/.system/samba4 on /var/db/system/samba4 (zfs, local, nfsv4acls)
        data/.system/webui on /var/db/system/webui (zfs, local, nfsv4acls)
        data/my dataset on /mnt/data/my dataset (zfs, local, nfsv4acls)
    """)

    mtab = Mtab(shell)

    assert mtab.get("data/my dataset") == "/mnt/data/my dataset"


def test__mtab__linux():
    # FIXME: I don't know how `mount` output for ZFS filesystems on Linux really looks like
    shell = Mock()
    shell.exec.return_value = textwrap.dedent("""\
        data/my dataset on /mnt/data/my dataset type zfs (rw,noatime,nodiratime,discard,errors=remount-ro,data=ordered)
    """)

    mtab = Mtab(shell)

    assert mtab.get("data/my dataset") == "/mnt/data/my dataset"

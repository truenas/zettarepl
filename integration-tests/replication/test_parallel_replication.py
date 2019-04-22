# -*- coding=utf-8 -*-
import subprocess
import textwrap
import time
from unittest.mock import Mock

import yaml

from zettarepl.definition.definition import Definition
from zettarepl.snapshot.list import list_snapshots
from zettarepl.replication.task.task import ReplicationTask
from zettarepl.transport.local import LocalShell
from zettarepl.utils.itertools import select_by_class
from zettarepl.utils.test import wait_replication_tasks_to_complete
from zettarepl.zettarepl import Zettarepl


def test_parallel_replication():
    subprocess.call("zfs destroy -r data/src", shell=True)
    subprocess.call("zfs receive -A data/dst", shell=True)
    subprocess.call("zfs destroy -r data/dst", shell=True)

    subprocess.check_call("zfs create data/src", shell=True)

    subprocess.check_call("zfs create data/src/a", shell=True)
    subprocess.check_call("dd if=/dev/urandom of=/mnt/data/src/a/blob bs=1M count=1", shell=True)
    subprocess.check_call("zfs snapshot data/src/a@2018-10-01_01-00", shell=True)

    subprocess.check_call("zfs create data/src/b", shell=True)
    subprocess.check_call("dd if=/dev/urandom of=/mnt/data/src/b/blob bs=1M count=1", shell=True)
    subprocess.check_call("zfs snapshot data/src/b@2018-10-01_01-00", shell=True)

    subprocess.check_call("zfs create data/dst", shell=True)
    subprocess.check_call("zfs create data/dst/a", shell=True)
    subprocess.check_call("zfs create data/dst/b", shell=True)

    definition = Definition.from_data(yaml.load(textwrap.dedent("""\
        timezone: "UTC"

        periodic-snapshot-tasks:
          src-a:
            dataset: data/src/a
            recursive: true
            lifetime: PT1H
            naming-schema: "%Y-%m-%d_%H-%M"
            schedule:
              minute: "0"
          src-b:
            dataset: data/src/b
            recursive: true
            lifetime: PT1H
            naming-schema: "%Y-%m-%d_%H-%M"
            schedule:
              minute: "0"

        replication-tasks:
          src-a:
            direction: push
            transport:
              type: ssh
              hostname: localhost
              private-key: |
                -----BEGIN RSA PRIVATE KEY-----
                MIIEowIBAAKCAQEA6+D7AWdNnM8T5f2P1j5VIwVABjugtL252iEhhGWTNaR2duCK
                kuZmG3+o55b0vo2mUpjWt+CKsLkVL/e/JgZqzlHYm+MhRs4q7zODo/IEtx/uAVKM
                zqWS0Zs9NRdU1UnhsETjrhhQhNFwx7MUYlB2s8+mAduLbeqRuVIKsXzg5Rz+m3VL
                Wl4R82A10oZl0UPyIcEHAtHMgMVyGzQcNUsxsp/oN40nnkXiXHaXtSMxjEtOPLno
                t21bJ0ZV8RFmXtJqHXgyTTM5maJM3JwqhMHD2tcHodqCcnxvuuWv31pAB+HKQ8IM
                dORYnhZqqs/Bt80gRLQuJBpNeX2/cKPDDMCRnQIDAQABAoIBAQCil6+N9R5rw9Ys
                iA85GDhpbnoGkd2iGNHeiU3oTHgf1uEN6pO61PR3ahUMpmLIYy3N66q+jxoq3Tm8
                meL6HBxNYd+U/Qh4HS89OV45iV80t97ArJ2A6GL+9ypGyXFhoI7giWwEGqCOHSzH
                iyq25k4cfjspNqOyval7fBEA7Vq8smAMDJQE7WIJWzqrTbVAmVf9ho4r5dYxYBNW
                fXWo84DU8K+p0mE0BTokqqMWhKiA5JJG7OZB/iyeW2BWFOdASXvQmh1hRwMzpU4q
                BcZ7cJHz248SNSGMe5R3w7SmLO7PRr1/QkktJNdFmT7o/RGmQh8+KHql6r/vIzMM
                ci60OAxlAoGBAPYsZJZF3HK70fK3kARSzOD1LEVBDTCLnpVVzMSp6thG8cQqfCI5
                pCfT/NcUsCAP6J+yl6dqdtonXISmGolI1s1KCBihs5D4jEdjbg9KbKh68AsHXaD3
                v5L3POJ9hQnI6zJdvCfxniHdUArfyYhqsp1bnCn+85g4ed7BzDqMX2IDAoGBAPVL
                Y45rALw7lsjxJndyFdffJtyAeuwxgJNwWGuY21xhwqPbuwsgLHsGerHNKB5QAJT8
                JOlrcrfC13s6Tt4wmIy/o2h1p9tMaitmVR6pJzEfHyJhSRTbeFybQ9yqlKHuk2tI
                jcUZV/59cyRrjhPKWoVym3Fh/P7D1t1kfdTvBrvfAoGAUH0rVkb5UTo/5xBFsmQw
                QM1o8CvY2CqOa11mWlcERjrMCcuqUrZuCeeyH9DP1WveL3kBROf2fFWqVmTJAGIk
                eXLfOs6EG75of17vOWioJl4r5i8+WccniDH2YkeQHCbpX8puHtFNVt05spSBHG1m
                gTTW1pRZqUet8TuEPxBuj2kCgYAVjCrRruqgnmdvfWeQpI/wp6SlSBAEQZD24q6R
                vRq/8cKEXGAA6TGfGQGcLtZwWzzB2ahwbMTmCZKeO5AECqbL7mWvXm6BYCQPbeza
                Raews/grL/qYf3MCR41djAqEcw22Jeh2QPSu4VxE/cG8UVFEWb335tCvnIp6ZkJ7
                ewfPZwKBgEnc8HH1aq8IJ6vRBePNu6M9ON6PB9qW+ZHHcy47bcGogvYRQk1Ng77G
                LdZpyjWzzmb0Z4kjEYcrlGdbNQf9iaT0r+SJPzwBDG15+fRqK7EJI00UhjB0T67M
                otrkElxOBGqHSOl0jfUBrpSkSHiy0kDc3/cTAWKn0gowaznSlR9N
                -----END RSA PRIVATE KEY-----
              host-key: "ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBKwIWcodi3rHl0zS3VaddXnwfgIcpp1ECR9KhaB1cyIspgcHOA98wxY8onw+zHxpMUB4pve+t416FFLSkmlJ2f4="
            source-dataset: data/src/a
            target-dataset: data/dst/a
            recursive: true
            periodic-snapshot-tasks:
              - src-a
            auto: true
            retention-policy: none
            speed-limit: 100000
          src-b:
            direction: push
            transport:
              type: ssh
              hostname: localhost
              private-key: |
                -----BEGIN RSA PRIVATE KEY-----
                MIIEowIBAAKCAQEA6+D7AWdNnM8T5f2P1j5VIwVABjugtL252iEhhGWTNaR2duCK
                kuZmG3+o55b0vo2mUpjWt+CKsLkVL/e/JgZqzlHYm+MhRs4q7zODo/IEtx/uAVKM
                zqWS0Zs9NRdU1UnhsETjrhhQhNFwx7MUYlB2s8+mAduLbeqRuVIKsXzg5Rz+m3VL
                Wl4R82A10oZl0UPyIcEHAtHMgMVyGzQcNUsxsp/oN40nnkXiXHaXtSMxjEtOPLno
                t21bJ0ZV8RFmXtJqHXgyTTM5maJM3JwqhMHD2tcHodqCcnxvuuWv31pAB+HKQ8IM
                dORYnhZqqs/Bt80gRLQuJBpNeX2/cKPDDMCRnQIDAQABAoIBAQCil6+N9R5rw9Ys
                iA85GDhpbnoGkd2iGNHeiU3oTHgf1uEN6pO61PR3ahUMpmLIYy3N66q+jxoq3Tm8
                meL6HBxNYd+U/Qh4HS89OV45iV80t97ArJ2A6GL+9ypGyXFhoI7giWwEGqCOHSzH
                iyq25k4cfjspNqOyval7fBEA7Vq8smAMDJQE7WIJWzqrTbVAmVf9ho4r5dYxYBNW
                fXWo84DU8K+p0mE0BTokqqMWhKiA5JJG7OZB/iyeW2BWFOdASXvQmh1hRwMzpU4q
                BcZ7cJHz248SNSGMe5R3w7SmLO7PRr1/QkktJNdFmT7o/RGmQh8+KHql6r/vIzMM
                ci60OAxlAoGBAPYsZJZF3HK70fK3kARSzOD1LEVBDTCLnpVVzMSp6thG8cQqfCI5
                pCfT/NcUsCAP6J+yl6dqdtonXISmGolI1s1KCBihs5D4jEdjbg9KbKh68AsHXaD3
                v5L3POJ9hQnI6zJdvCfxniHdUArfyYhqsp1bnCn+85g4ed7BzDqMX2IDAoGBAPVL
                Y45rALw7lsjxJndyFdffJtyAeuwxgJNwWGuY21xhwqPbuwsgLHsGerHNKB5QAJT8
                JOlrcrfC13s6Tt4wmIy/o2h1p9tMaitmVR6pJzEfHyJhSRTbeFybQ9yqlKHuk2tI
                jcUZV/59cyRrjhPKWoVym3Fh/P7D1t1kfdTvBrvfAoGAUH0rVkb5UTo/5xBFsmQw
                QM1o8CvY2CqOa11mWlcERjrMCcuqUrZuCeeyH9DP1WveL3kBROf2fFWqVmTJAGIk
                eXLfOs6EG75of17vOWioJl4r5i8+WccniDH2YkeQHCbpX8puHtFNVt05spSBHG1m
                gTTW1pRZqUet8TuEPxBuj2kCgYAVjCrRruqgnmdvfWeQpI/wp6SlSBAEQZD24q6R
                vRq/8cKEXGAA6TGfGQGcLtZwWzzB2ahwbMTmCZKeO5AECqbL7mWvXm6BYCQPbeza
                Raews/grL/qYf3MCR41djAqEcw22Jeh2QPSu4VxE/cG8UVFEWb335tCvnIp6ZkJ7
                ewfPZwKBgEnc8HH1aq8IJ6vRBePNu6M9ON6PB9qW+ZHHcy47bcGogvYRQk1Ng77G
                LdZpyjWzzmb0Z4kjEYcrlGdbNQf9iaT0r+SJPzwBDG15+fRqK7EJI00UhjB0T67M
                otrkElxOBGqHSOl0jfUBrpSkSHiy0kDc3/cTAWKn0gowaznSlR9N
                -----END RSA PRIVATE KEY-----
              host-key: "ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBKwIWcodi3rHl0zS3VaddXnwfgIcpp1ECR9KhaB1cyIspgcHOA98wxY8onw+zHxpMUB4pve+t416FFLSkmlJ2f4="
            source-dataset: data/src/b
            target-dataset: data/dst/b
            recursive: true
            periodic-snapshot-tasks:
              - src-b
            auto: true
            retention-policy: none
            speed-limit: 100000
    """)))

    local_shell = LocalShell()
    zettarepl = Zettarepl(Mock(), local_shell)
    zettarepl._spawn_retention = Mock()
    zettarepl.set_tasks(definition.tasks)
    zettarepl._spawn_replication_tasks(select_by_class(ReplicationTask, definition.tasks))

    start = time.monotonic()
    wait_replication_tasks_to_complete(zettarepl)
    end = time.monotonic()
    assert 10 <= end - start <= 15

    zettarepl._spawn_retention.assert_called_once()

    assert len(list_snapshots(local_shell, "data/dst/a", False)) == 1
    assert len(list_snapshots(local_shell, "data/dst/b", False)) == 1

    subprocess.call("zfs destroy -r data/dst", shell=True)
    subprocess.check_call("zfs create data/dst", shell=True)
    subprocess.check_call("zfs create data/dst/a", shell=True)
    subprocess.check_call("zfs create data/dst/b", shell=True)

    zettarepl._replication_tasks_can_run_in_parallel = Mock(return_value=False)
    zettarepl._spawn_replication_tasks(select_by_class(ReplicationTask, definition.tasks))

    start = time.monotonic()
    wait_replication_tasks_to_complete(zettarepl)
    end = time.monotonic()
    assert 20 <= end - start <= 25

    assert len(list_snapshots(local_shell, "data/dst/a", False)) == 1
    assert len(list_snapshots(local_shell, "data/dst/b", False)) == 1

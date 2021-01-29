# -*- coding=utf-8 -*-
import logging
import subprocess
import textwrap

import yaml

from zettarepl.definition.definition import Definition
from zettarepl.replication.task.task import ReplicationTask
from zettarepl.utils.itertools import select_by_class
from zettarepl.utils.test import create_zettarepl, set_localhost_transport_options, wait_replication_tasks_to_complete


def test_replication_retry(caplog):
    subprocess.call("zfs destroy -r data/src", shell=True)

    subprocess.check_call("zfs create data/src", shell=True)
    subprocess.check_call("zfs snapshot data/src@2018-10-01_01-00", shell=True)

    definition = yaml.safe_load(textwrap.dedent("""\
        timezone: "UTC"

        periodic-snapshot-tasks:
          src:
            dataset: data/src
            recursive: true
            lifetime: PT1H
            naming-schema: "%Y-%m-%d_%H-%M"
            schedule:
              minute: "0"

        replication-tasks:
          src:
            transport:
              type: ssh
              hostname: 127.0.0.1
            direction: push
            source-dataset: data/src
            target-dataset: data/dst
            recursive: true
            periodic-snapshot-tasks:
              - src
            auto: false
            retention-policy: none
            retries: 2
    """))
    set_localhost_transport_options(definition["replication-tasks"]["src"]["transport"])
    definition["replication-tasks"]["src"]["transport"]["private-key"] = textwrap.dedent("""\
        -----BEGIN RSA PRIVATE KEY-----
        MIIEowIBAAKCAQEA0/5hQu83T9Jdl1NT9malC0ovHMHLspa4t6dFTSHWRUHsA3+t
        q50bBfrsS+hm4qMndxm9Sqig5/TqlM00W49SkooyU/0j4Q4xjvJ61RXOtHXPOoMH
        opLjRlmbuxkWCb0CmwXvIunaebBFfPx/VuwNJNNv9ZNcgeQJj5ggjI7hnikK4Pn4
        jpqcivqIStNO/6q+9NLsNkMQu8vq/zuxC9ePyeaywbbAIcpKREsWgiNtuhsPxnRS
        +gVQ+XVgE6RFJzMO13MtE+E4Uphseip+fSNVmLeAQyGUrUg12JevJYnMbLOQtacB
        GNDMHSwcwAzqVYPq8oqjQhWvqBntjcd/qK3P+wIDAQABAoIBAHy8tzoNS7x6CXvb
        GhJn/0EPW31OQq9IpFPb5pkmCdAio97DJ8tM2/O+238mtjMw0S3xRUJCyrrxj34S
        6HXfdTSogEiPMKdiFKMJ5mCvPjtM/qxtIPb1+ykP3ORQNHlyb7AL49PlShpEL/8F
        C2B38Jv0lXIoTUxYg4+scaqDABpw9aaYTODcJ9uvFhAcAHALKaN0iiz050dWoH9D
        CkJ1UwoHVUz6XGZ3lOR/qxUDGd72Ara0cizCXQZIkOtu8Kfnfnlx3pqOZJgbkr49
        JY3LQId5bVhNlQLKlTSAameIiAJETeLvxHzJHCvMm0LnKDfLiejq/dEk5CMgjrVz
        ExV+ioECgYEA72zxquQJo051o2mrG0DhVBT0QzXo+8yjNYVha2stBOMGvEnL0n2H
        VFDdWhpZVzRs1uR6sJC14YTGfBNk7NTaQSorgrKvYs1E/krZEMsFquwIcLtbHxYP
        zjBSQwYA7jIEFViIkZwptb+qfA+c1YehZTYzx4R/hlkkLlTObyRFcyECgYEA4qtK
        /7UaBG4kumW+cdRnqJ+KO21PylBnGaCm6yH6DO5SKlqoHvYdyds70Oat9fPX4BRJ
        2aMTivZMkGgu6Dc1AViRgBoTIReMQ9TY3y8d0unMtBddAIx0guiP/rtPrCRTC07m
        s31b6wkLTnPnW3W2N8t4LfdTLpsgmA3t5Q6Iu5sCgYB9Lg+4kpu7Z3U4KDJPAIAP
        Lxl63n/ezuJyRDdoK1QRXwWRgl/vwLP10IW661XUs1NIk5LWKAMAUyRXkOhOrwch
        1QOExRnP5ZTyA340OoHPGLNdBYgh264N1tPbuRLZdwsNggl9YBGqtfhT/vG37r7i
        pREzesIWIxs4ohyAnY02IQKBgQDARd0Qm2a+a0/sbXHmzO5BM1PmpQsR6rIKIyR0
        QBYD8gTwuIXz/YG3QKi0w3i9MWLlSVB7tMFXFyZLOJTRlkL4KVEDARtI7tikkWCF
        sUnzJy/ldAwH8xzCDtRWmD01IHrxFLTNfIEEFl/o5JhUFL3FBmujUjDVT/GOCgLK
        UlHaEQKBgFUGEgI6/GvV/JecnEWLqd+HpRHiBywpfOkAFmJGokdAOvF0QDFHK9/P
        stO7TRqUHufxZQIeTJ7sGdsabEAypiKSFBR8w1qVg+iQZ+M+t0vCgXlnHLaw2SeJ
        1YT8kH1TsdzozkxJ7tFa1A5YI37ZiUiN7ykJ0l4Zal6Nli9z5Oa0
        -----END RSA PRIVATE KEY-----
    """)  # Some random invalid SSH key
    definition = Definition.from_data(definition)

    caplog.set_level(logging.INFO)
    zettarepl = create_zettarepl(definition)
    zettarepl._spawn_replication_tasks(select_by_class(ReplicationTask, definition.tasks))
    wait_replication_tasks_to_complete(zettarepl)

    assert any(
        "non-recoverable replication error" in record.message
        for record in caplog.get_records("call")
    )

# -*- coding=utf-8 -*-
import argparse
import ipaddress
import json
import random
import string
import socket
import sys

import libzfs


def address_family(address):
    try:
        ipaddress.IPv6Address(address)
    except ipaddress.AddressValueError:
        return socket.AF_INET
    else:
        return socket.AF_INET6


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--listen")
    parser.add_argument("--listen-min-port", type=int, default=1024)
    parser.add_argument("--listen-max-port", type=int, default=65535)

    parser.add_argument("--connect")
    parser.add_argument("--connect-port", type=int)
    parser.add_argument("--connect-token")

    subparsers = parser.add_subparsers(title="subcommands")

    send_parser = subparsers.add_parser("send")
    send_parser.set_defaults(command="send")
    send_parser.add_argument("dataset")
    send_parser.add_argument("--replicate", action="store_true")
    send_parser.add_argument("--properties", action="store_true")
    send_parser.add_argument("--dedup", action="store_true")
    send_parser.add_argument("--large-block", action="store_true")
    send_parser.add_argument("--embed", action="store_true")
    send_parser.add_argument("--compressed", action="store_true")
    send_parser.add_argument("--raw", action="store_true")
    send_parser.add_argument("--snapshot")
    send_parser.add_argument("--incremental-base")
    send_parser.add_argument("--include-intermediate", action="store_true")
    send_parser.add_argument("--receive-resume-token")

    receive_parser = subparsers.add_parser("receive")
    receive_parser.set_defaults(command="receive")
    receive_parser.add_argument("dataset")
    receive_parser.add_argument("--mount", action="store_true")
    receive_parser.add_argument("--props", type=json.loads)

    args = parser.parse_args()

    if args.listen:
        e = None
        for port in range(args.listen_min_port, args.listen_max_port + 1):
            try:
                s = socket.create_server((args.listen, port), family=address_family(args.listen), dualstack_ipv6=True)
                break
            except socket.error as e:
                if e.errno == socket.errno.EADDRINUSE:
                    pass
                else:
                    raise
        else:
            sys.stderr.write(f"Failed to listen specified port range: {e!r}\n")
            sys.exit(1)
        s.listen()
        token = "".join([random.choice(string.ascii_letters + string.digits) for _ in range(128)])
        sys.stdout.write(f"{json.dumps({'port': port, 'token': token})}\n")
        client, addr = s.accept()
        remote_token = client.recv(128)
        if remote_token.decode("ascii", "ignore") != token:
            sys.stderr.write(f"Received invalid token: {remote_token!r}\n")
            sys.exit(1)
        fh = client.fileno()

    elif args.connect:
        s = socket.socket(address_family(args.connect))
        s.connect((args.connect, args.connect_port))
        s.send(args.connect_token.encode("ascii"))
        fh = s.fileno()

    else:
        sys.stderr.write("Must either specify --listen or --connect\n")
        sys.exit(1)

    zfs = libzfs.ZFS()

    if args.command == "receive":
        try:
            zfs.receive(args.dataset, fh, force=True, nomount=not args.mount, resumable=True, props=args.props)
        except libzfs.ZFSException as e:
            sys.stderr.write(f"{e.args[0]}\n")
            sys.exit(1)

    elif args.command == "send":
        try:
            dataset = zfs.get_object(args.dataset)

            flags = set()
            if args.replicate:
                flags.add(libzfs.SendFlag.REPLICATE)
            if args.include_intermediate:
                flags.add(libzfs.SendFlag.DOALL)
            if args.properties:
                flags.add(libzfs.SendFlag.PROPS)
            if args.dedup:
                flags.add(libzfs.SendFlag.DEDUP)
            if args.large_block:
                flags.add(libzfs.SendFlag.LARGEBLOCK)
            if args.embed:
                flags.add(libzfs.SendFlag.EMBED_DATA)
            if args.compressed:
                flags.add(libzfs.SendFlag.COMPRESS)
            if args.raw:
                flags.add(libzfs.SendFlag.RAW)

            if args.receive_resume_token is None:
                assert args.snapshot is not None

                dataset.send(fh, fromname=args.incremental_base, toname=args.snapshot, flags=flags)
            else:
                assert args.snapshot is None
                assert args.incremental_base is None

                zfs.send_resume(fh, args.receive_resume_token, flags)

        except libzfs.ZFSException as e:
            sys.stderr.write(f"{e.args[0]}\n")
            sys.exit(1)

    else:
        sys.stderr.write("t\n")
        sys.exit(1)

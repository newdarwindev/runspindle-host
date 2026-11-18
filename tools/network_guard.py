#!/usr/bin/env python3
"""Supervise an owned process tree; deny and count attempted network syscalls.

No syscall is continued. This protects trusted test code against accidental
network use, not malicious code installing a replacement seccomp filter.
"""

from __future__ import annotations

import array
import ctypes
import errno
import os
import select
import signal
import socket
import sys
import threading
import time
from pathlib import Path
from types import FrameType

NETWORK_CALLS = (
    "socket",
    "connect",
    "bind",
    "listen",
    "accept",
    "accept4",
    "io_uring_setup",
)
VIOLATION = 125


class Data(ctypes.Structure):
    _fields_ = [
        ("nr", ctypes.c_int),
        ("arch", ctypes.c_uint32),
        ("ip", ctypes.c_uint64),
        ("args", ctypes.c_uint64 * 6),
    ]


class Request(ctypes.Structure):
    _fields_ = [
        ("id", ctypes.c_uint64),
        ("pid", ctypes.c_uint32),
        ("flags", ctypes.c_uint32),
        ("data", Data),
    ]


class Response(ctypes.Structure):
    _fields_ = [
        ("id", ctypes.c_uint64),
        ("val", ctypes.c_int64),
        ("error", ctypes.c_int32),
        ("flags", ctypes.c_uint32),
    ]


def library() -> ctypes.CDLL:
    lib = ctypes.CDLL("libseccomp.so.2", use_errno=True)
    # Initializes feature discovery in the supervisor as well as the child.
    if lib.seccomp_api_get() < 6:
        raise RuntimeError("blocked: seccomp notification API unavailable")
    lib.seccomp_init.argtypes = [ctypes.c_uint32]
    lib.seccomp_init.restype = ctypes.c_void_p
    lib.seccomp_rule_add.argtypes = [
        ctypes.c_void_p,
        ctypes.c_uint32,
        ctypes.c_int,
        ctypes.c_uint,
    ]
    lib.seccomp_syscall_resolve_name.argtypes = [ctypes.c_char_p]
    lib.seccomp_load.argtypes = [ctypes.c_void_p]
    lib.seccomp_release.argtypes = [ctypes.c_void_p]
    lib.seccomp_notify_fd.argtypes = [ctypes.c_void_p]
    lib.seccomp_notify_receive.argtypes = [ctypes.c_int, ctypes.POINTER(Request)]
    lib.seccomp_notify_respond.argtypes = [ctypes.c_int, ctypes.POINTER(Response)]
    return lib


def install(lib: ctypes.CDLL) -> int:
    context = lib.seccomp_init(0x7FFF0000)
    if not context:
        raise RuntimeError("blocked: seccomp_init")
    try:
        for name in NETWORK_CALLS:
            number = lib.seccomp_syscall_resolve_name(name.encode("ascii"))
            if number < 0 or lib.seccomp_rule_add(context, 0x7FC00000, number, 0):
                raise RuntimeError(f"blocked: syscall rule {name}")
        if lib.seccomp_load(context):
            raise RuntimeError("blocked: seccomp_load")
        descriptor = lib.seccomp_notify_fd(context)
        if descriptor < 0:
            raise RuntimeError("blocked: seccomp_notify_fd")
        # libseccomp owns its original fd; transfer a duplicate before release.
        return os.dup(descriptor)
    finally:
        lib.seccomp_release(context)


def kill_owned_tree() -> None:
    """Reap only this subreaper's descendants, including detached sessions."""
    deadline = time.monotonic() + 3
    while True:
        children_by_parent: dict[int, list[int]] = {}
        for entry in Path("/proc").iterdir():
            if not entry.name.isdigit():
                continue
            try:
                stat = (entry / "stat").read_text()
                parent_pid = int(stat.rsplit(")", 1)[1].split()[1])
            except (FileNotFoundError, ProcessLookupError):
                continue
            children_by_parent.setdefault(parent_pid, []).append(int(entry.name))
        pending = [os.getpid()]
        handles = []
        seen = set()
        while pending:
            parent_pid = pending.pop()
            for child_pid in children_by_parent.get(parent_pid, []):
                if child_pid in seen:
                    continue
                seen.add(child_pid)
                try:
                    descriptor = os.pidfd_open(child_pid)
                except ProcessLookupError:
                    continue
                try:
                    stat = Path(f"/proc/{child_pid}/stat").read_text()
                    actual_parent = int(stat.rsplit(")", 1)[1].split()[1])
                    if actual_parent != parent_pid:
                        os.close(descriptor)
                        continue
                except FileNotFoundError:
                    os.close(descriptor)
                    continue
                pending.append(child_pid)
                handles.append(descriptor)
        for descriptor in reversed(handles):
            try:
                signal.pidfd_send_signal(descriptor, signal.SIGKILL)
            except ProcessLookupError:
                pass
            finally:
                os.close(descriptor)
        try:
            while os.waitpid(-1, os.WNOHANG)[0]:
                pass
        except ChildProcessError:
            return
        if time.monotonic() >= deadline:
            raise RuntimeError("blocked: owned descendant cleanup timed out")
        time.sleep(0.01)


def supervise(command: list[str]) -> int:
    if sys.platform != "linux" or not command:
        raise RuntimeError("blocked: Linux and a command required")
    lib = library()
    libc = ctypes.CDLL(None, use_errno=True)
    # Reparent orphaned grandchildren to this disposable supervisor only.
    # Waiting for ECHILD prevents a zero-exit parent hiding a late descendant.
    if libc.prctl(36, 1, 0, 0, 0):  # PR_SET_CHILD_SUBREAPER
        raise RuntimeError("blocked: child subreaper unavailable")
    parent, child = socket.socketpair()
    pid = os.fork()
    if pid == 0:
        try:
            parent.close()
            descriptor = install(lib)
            child.sendmsg(
                [b"F"],
                [
                    (
                        socket.SOL_SOCKET,
                        socket.SCM_RIGHTS,
                        array.array("i", [descriptor]),
                    )
                ],
            )
            os.close(descriptor)
            if child.recv(1) != b"R":
                os._exit(126)
            child.close()
            os.execvp(command[0], command)
        except BaseException as error:
            print(f"network guard setup: {error}", file=sys.stderr, flush=True)
            os._exit(126)
    child.close()

    def cancelled(signum: int, _frame: FrameType | None) -> None:
        raise SystemExit(128 + signum)

    signal.signal(signal.SIGTERM, cancelled)
    signal.signal(signal.SIGINT, cancelled)
    parent.settimeout(5)
    listener = -1
    stopped = threading.Event()
    attempts = [0]
    errors: list[str] = []
    monitor = None
    try:
        _, ancillary, flags, _ = parent.recvmsg(
            1, socket.CMSG_SPACE(array.array("i").itemsize)
        )
        if flags & socket.MSG_CTRUNC or len(ancillary) != 1:
            raise RuntimeError("blocked: notification descriptor handshake")
        level, kind, data = ancillary[0]
        if (level, kind) != (socket.SOL_SOCKET, socket.SCM_RIGHTS):
            raise RuntimeError("blocked: unexpected descriptor")
        descriptors = array.array("i")
        descriptors.frombytes(data)
        if len(descriptors) != 1:
            raise RuntimeError("blocked: descriptor count")
        listener = descriptors[0]

        def receive() -> None:
            try:
                while not stopped.is_set():
                    readable, _, _ = select.select([listener], [], [], 0.05)
                    if not readable:
                        continue
                    request = Request()
                    result = lib.seccomp_notify_receive(listener, ctypes.byref(request))
                    if result == -errno.ECANCELED:
                        return  # No filter users remain.
                    if result == -errno.ENOENT:
                        # Canceled before delivery is still suspicious, never a pass.
                        errors.append("notification canceled before receipt")
                        return
                    if result:
                        errors.append(f"notification receive {result}")
                        return
                    attempts[0] += 1
                    response = Response(request.id, 0, -errno.EPERM, 0)
                    result = lib.seccomp_notify_respond(
                        listener, ctypes.byref(response)
                    )
                    if result not in (0, -errno.ENOENT):
                        errors.append(f"notification response {result}")
                        return
            except BaseException as error:
                errors.append(type(error).__name__)

        monitor = threading.Thread(target=receive, daemon=True)
        monitor.start()
        parent.sendall(b"R")
        parent.close()
        status = None
        while True:
            try:
                observed, value = os.waitpid(-1, 0)
                if observed == pid:
                    status = value
            except ChildProcessError:
                break
        stopped.set()
        monitor.join(timeout=1)
        if monitor.is_alive():
            errors.append("notification monitor did not stop")
        if attempts[0] or errors:
            print(
                f"network guard: denied={attempts[0]} errors={errors}",
                file=sys.stderr,
                flush=True,
            )
            return VIOLATION
        if status is None:
            return 126
        code = os.waitstatus_to_exitcode(status)
        return code if code >= 0 else 128 - code
    finally:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        kill_owned_tree()
        stopped.set()
        parent.close()
        if listener >= 0:
            os.close(listener)


if __name__ == "__main__":
    raise SystemExit(supervise(sys.argv[1:]))

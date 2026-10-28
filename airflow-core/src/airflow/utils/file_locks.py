# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
Advisory file locking with the semantics of ``fcntl.flock``, on POSIX and Windows.

On POSIX this is ``fcntl.flock`` itself. On Windows it is ``LockFileEx``/``UnlockFileEx``
on the first byte of the file, which gives the same shared/exclusive/non-blocking
semantics between processes that lock through this module. As with ``flock``, the lock
belongs to the open file handle and is released when the handle is closed or the process
exits.

One difference: Windows byte-range locks are mandatory rather than advisory, so while an
exclusive lock is held, other handles cannot read or write the locked first byte. That
only matters for files that are also accessed without taking this lock, which lock files
are not.
"""

from __future__ import annotations

import sys
from typing import IO

if sys.platform == "win32":
    import ctypes
    import errno
    import msvcrt
    from ctypes import wintypes

    LOCK_SH = 0x01
    LOCK_EX = 0x02
    LOCK_NB = 0x04
    LOCK_UN = 0x08

    _LOCKFILE_FAIL_IMMEDIATELY = 0x01
    _LOCKFILE_EXCLUSIVE_LOCK = 0x02
    _ERROR_LOCK_VIOLATION = 33

    class _OVERLAPPED(ctypes.Structure):
        _fields_ = (
            ("Internal", wintypes.LPVOID),
            ("InternalHigh", wintypes.LPVOID),
            ("Offset", wintypes.DWORD),
            ("OffsetHigh", wintypes.DWORD),
            ("hEvent", wintypes.HANDLE),
        )

    _kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    _kernel32.LockFileEx.argtypes = (
        wintypes.HANDLE,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.DWORD,
        ctypes.POINTER(_OVERLAPPED),
    )
    _kernel32.LockFileEx.restype = wintypes.BOOL
    _kernel32.UnlockFileEx.argtypes = (
        wintypes.HANDLE,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.DWORD,
        ctypes.POINTER(_OVERLAPPED),
    )
    _kernel32.UnlockFileEx.restype = wintypes.BOOL

    def flock(file: IO | int, operation: int) -> None:
        """Lock or unlock the file like ``fcntl.flock``, raising ``BlockingIOError`` on contention."""
        fd = file if isinstance(file, int) else file.fileno()
        handle = msvcrt.get_osfhandle(fd)
        overlapped = _OVERLAPPED()  # zeroed: lock the byte at offset 0
        if operation & LOCK_UN:
            if not _kernel32.UnlockFileEx(handle, 0, 1, 0, ctypes.byref(overlapped)):
                raise ctypes.WinError(ctypes.get_last_error())
            return
        flags = 0
        if operation & LOCK_EX:
            flags |= _LOCKFILE_EXCLUSIVE_LOCK
        if operation & LOCK_NB:
            flags |= _LOCKFILE_FAIL_IMMEDIATELY
        if not _kernel32.LockFileEx(handle, flags, 0, 1, 0, ctypes.byref(overlapped)):
            error = ctypes.get_last_error()
            if error == _ERROR_LOCK_VIOLATION:
                raise BlockingIOError(errno.EAGAIN, "The file is locked by another process")
            raise ctypes.WinError(error)

else:
    from fcntl import LOCK_EX, LOCK_NB, LOCK_SH, LOCK_UN, flock  # noqa: F401

__all__ = ["LOCK_EX", "LOCK_NB", "LOCK_SH", "LOCK_UN", "flock"]

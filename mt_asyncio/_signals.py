"""Process-level signal plumbing for the runtime.

The runtime catches signals through a self-pipe: a dummy Python handler writes
the signal number to the wakeup fd, and the poll thread turns each byte into a
runtime ``Event``. These helpers install/remove that plumbing; the mapping from
signal number to Event lives in the runtime (``_sig_add``/``_sig_rem``).
"""

import errno
import signal
import threading


def _noop(*args, **kwargs):
    return


def _is_main_thread():
    return threading.main_thread().ident == threading.current_thread().ident


def _check_sig(sig):
    if not isinstance(sig, int):
        raise TypeError(f'sig must be an int, not {sig!r}')

    if sig not in signal.valid_signals():
        raise ValueError(f'invalid signal number {sig}')


def _set_sig_wfd(fd):
    if fd >= 0:
        return signal.set_wakeup_fd(fd, warn_on_full_buffer=False)
    return signal.set_wakeup_fd(fd)


def _sig_add(sig):
    if not _is_main_thread():
        raise ValueError('Signals can only be handled from the main thread')

    _check_sig(sig)
    try:
        # register a dummy signal handler so Python will write the signal no in the wakeup fd
        signal.signal(sig, _noop)
        # set SA_RESTART to limit EINTR occurrences
        signal.siginterrupt(sig, False)
    except OSError as exc:
        if exc.errno == errno.EINVAL:
            raise RuntimeError(f'signum {sig} cannot be caught')
        raise


def _sig_rem(sig):
    if not _is_main_thread():
        raise ValueError('Signals can only be handled from the main thread')

    if sig == signal.SIGINT:
        handler = signal.default_int_handler
    else:
        handler = signal.SIG_DFL

    try:
        signal.signal(sig, handler)
    except OSError as exc:
        if exc.errno == errno.EINVAL:
            raise RuntimeError(f'signum {sig} cannot be caught')
        raise

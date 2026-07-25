use std::sync::{Mutex, atomic};

use pyo3::prelude::*;

use crate::events::{Event, Waiter};

//: readiness word layout: | tick: 8 bits | shutdown: 1 bit | readiness: 5 bits |
const READABLE: usize = 0b00_0001;
const WRITABLE: usize = 0b00_0010;
const READ_CLOSED: usize = 0b00_0100;
const WRITE_CLOSED: usize = 0b00_1000;
const ERROR: usize = 0b01_0000;
const SHUTDOWN: usize = 0b10_0000;

const TICK_SHIFT: u32 = 6;
const TICK_MASK: usize = 0xff << TICK_SHIFT;

const READ_ALL: usize = READABLE | READ_CLOSED | ERROR;
const WRITE_ALL: usize = WRITABLE | WRITE_CLOSED | ERROR;

#[inline(always)]
fn tick_of(word: usize) -> u8 {
    ((word & TICK_MASK) >> TICK_SHIFT) as u8
}

#[derive(Default)]
struct Waiters {
    reader: Option<Py<Event>>,
    writer: Option<Py<Event>>,
    //: native readiness callbacks (used by the asyncio-compat loop): invoked
    //  inline on the poll thread when the direction becomes ready, avoiding the
    //  extra reactor->worker->consumer hop of the Event/Waiter path
    reader_cb: Option<Py<PyAny>>,
    writer_cb: Option<Py<PyAny>>,
}

//: whatever was parked on a direction that just became ready: Event waiters
//  (scheduled onto workers) and/or native callbacks (invoked with the GIL held)
pub(crate) struct WakeSet {
    pub(crate) reader: Option<Py<Event>>,
    pub(crate) writer: Option<Py<Event>>,
    pub(crate) reader_cb: Option<Py<PyAny>>,
    pub(crate) writer_cb: Option<Py<PyAny>>,
}

//: per-fd persistent I/O state. The fd is registered with the poller once for
//  its whole lifetime (edge-triggered, both interests); readiness is cached
//  here and consumed in userspace.
//  Cache-padded to avoid false sharing between entries.
#[repr(align(128))]
pub(crate) struct ScheduledIO {
    pub(crate) fd: i32,
    readiness: atomic::AtomicUsize,
    tick_r: atomic::AtomicU8,
    tick_w: atomic::AtomicU8,
    waiters: Mutex<Waiters>,
}

impl ScheduledIO {
    pub(crate) fn new(fd: i32) -> Self {
        Self {
            fd,
            readiness: atomic::AtomicUsize::new(0),
            tick_r: atomic::AtomicU8::new(0),
            tick_w: atomic::AtomicU8::new(0),
            waiters: Mutex::new(Waiters::default()),
        }
    }

    #[inline(always)]
    fn current_tick(&self) -> u8 {
        tick_of(self.readiness.load(atomic::Ordering::Acquire))
    }

    #[inline]
    fn arm(&self, py: Python, mask: usize) -> PyResult<Option<Py<Event>>> {
        if self.readiness.load(atomic::Ordering::Acquire) & (mask | SHUTDOWN) != 0 {
            return Ok(None);
        }
        let event = Py::new(py, Event::new())?;
        {
            let mut slots = self.waiters.lock().unwrap();
            //: re-check under the lock: the poller sets readiness before
            //  taking this same lock in `wake`, so either we observe the bits
            //  here or `wake` observes our slot
            if self.readiness.load(atomic::Ordering::Acquire) & (mask | SHUTDOWN) != 0 {
                return Ok(None);
            }
            let slot = if mask & READABLE != 0 {
                &mut slots.reader
            } else {
                &mut slots.writer
            };
            *slot = Some(event.clone_ref(py));
        }
        Ok(Some(event))
    }

    //: like `arm`, but stores a native Python callback instead of allocating an
    //  Event/Waiter. Returns true if the direction is already ready (callback
    //  NOT stored — the caller should perform the syscall), false if the
    //  callback was registered and will fire on the next readiness edge.
    #[inline]
    fn arm_cb(&self, mask: usize, cb: Py<PyAny>) -> bool {
        if self.readiness.load(atomic::Ordering::Acquire) & (mask | SHUTDOWN) != 0 {
            return true;
        }
        let mut slots = self.waiters.lock().unwrap();
        //: re-check under the lock, mirroring `arm`: either we observe the bits
        //  here or `wake` observes our slot
        if self.readiness.load(atomic::Ordering::Acquire) & (mask | SHUTDOWN) != 0 {
            return true;
        }
        let slot = if mask & READABLE != 0 {
            &mut slots.reader_cb
        } else {
            &mut slots.writer_cb
        };
        *slot = Some(cb);
        false
    }

    // NOTE: closed states are cleared as well, since a genuinely closed direction never
    //       returns EWOULDBLOCK (recv gives EOF/reset, send gives EPIPE).
    //       A closed bit observed here is stale — epoll reports EPOLLHUP for fresh
    //       unbound/unconnected sockets, which the eager registration captures —
    //       and any future real close delivers a fresh edge regardless.
    #[inline]
    fn clear(&self, ready: usize, tick: u8) {
        _ = self
            .readiness
            .fetch_update(atomic::Ordering::AcqRel, atomic::Ordering::Acquire, |curr| {
                if tick_of(curr) != tick {
                    return None;
                }
                Some(curr & !ready)
            });
    }

    //: atomically drain the direction's readiness bits, reporting whether any were set
    fn consume(&self, mask: usize) -> bool {
        self.readiness
            .fetch_update(atomic::Ordering::AcqRel, atomic::Ordering::Acquire, |curr| {
                if curr & mask == 0 {
                    return None;
                }
                Some(curr & !mask)
            })
            .is_ok()
    }

    // runtime API
    //: must be called before `wake` for the `arm` re-check to be sound
    pub(crate) fn set_readiness(&self, ready: usize) {
        _ = self
            .readiness
            .fetch_update(atomic::Ordering::AcqRel, atomic::Ordering::Acquire, |curr| {
                let tick = (usize::from(tick_of(curr).wrapping_add(1))) << TICK_SHIFT;
                Some(tick | (curr & !TICK_MASK) | ready)
            });
    }

    pub(crate) fn wake(&self, ready: usize) -> WakeSet {
        let mut slots = self.waiters.lock().unwrap();
        let (reader, reader_cb) = if ready & READ_ALL != 0 {
            (slots.reader.take(), slots.reader_cb.take())
        } else {
            (None, None)
        };
        let (writer, writer_cb) = if ready & WRITE_ALL != 0 {
            (slots.writer.take(), slots.writer_cb.take())
        } else {
            (None, None)
        };
        WakeSet {
            reader,
            writer,
            reader_cb,
            writer_cb,
        }
    }

    pub(crate) fn shutdown(&self) -> WakeSet {
        self.readiness.fetch_or(SHUTDOWN, atomic::Ordering::AcqRel);
        self.wake(READ_ALL | WRITE_ALL)
    }

    // downstream API
    pub(crate) fn arm_r(&self, py: Python, timeout: Option<usize>) -> PyResult<Option<Py<Waiter>>> {
        match self.arm(py, READ_ALL)? {
            Some(event) => Ok(Some(Waiter::from_event(py, event, timeout))),
            None => {
                self.tick_r.store(self.current_tick(), atomic::Ordering::Release);
                Ok(None)
            }
        }
    }

    pub(crate) fn arm_w(&self, py: Python, timeout: Option<usize>) -> PyResult<Option<Py<Waiter>>> {
        match self.arm(py, WRITE_ALL)? {
            Some(event) => Ok(Some(Waiter::from_event(py, event, timeout))),
            None => {
                self.tick_w.store(self.current_tick(), atomic::Ordering::Release);
                Ok(None)
            }
        }
    }

    //: callback-based arm for the asyncio-compat loop (see `arm_cb`). Mirrors
    //  `arm_r`/`arm_w`: returns true if already ready (do the syscall now),
    //  false if the callback was registered.
    pub(crate) fn arm_r_cb(&self, cb: Py<PyAny>) -> bool {
        if self.arm_cb(READ_ALL, cb) {
            self.tick_r.store(self.current_tick(), atomic::Ordering::Release);
            return true;
        }
        false
    }

    pub(crate) fn arm_w_cb(&self, cb: Py<PyAny>) -> bool {
        if self.arm_cb(WRITE_ALL, cb) {
            self.tick_w.store(self.current_tick(), atomic::Ordering::Release);
            return true;
        }
        false
    }

    pub(crate) fn clear_r(&self) {
        self.clear(READ_ALL, self.tick_r.load(atomic::Ordering::Acquire));
    }

    pub(crate) fn clear_w(&self) {
        self.clear(WRITE_ALL, self.tick_w.load(atomic::Ordering::Acquire));
    }

    pub(crate) fn consume_r(&self) -> bool {
        self.consume(READ_ALL)
    }

    pub(crate) fn consume_w(&self) -> bool {
        self.consume(WRITE_ALL)
    }
}

pub(crate) fn readiness_from_event(event: &mio::event::Event) -> usize {
    let mut ready = 0;
    if event.is_readable() {
        ready |= READABLE;
    }
    if event.is_writable() {
        ready |= WRITABLE;
    }
    if event.is_read_closed() {
        ready |= READ_CLOSED;
    }
    if event.is_write_closed() {
        ready |= WRITE_CLOSED;
    }
    if event.is_error() {
        ready |= ERROR;
    }
    ready
}

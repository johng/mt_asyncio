use pyo3::{IntoPyObjectExt, prelude::*, types::PyList};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex, atomic},
};

use crate::{
    errors::abort,
    handles::{self, BoxedHandle, Handle},
    runtime::Runtime,
    time::Timer,
};

#[pyclass(frozen, subclass, module = "mt_asyncio._mt_asyncio")]
pub(crate) struct Event {
    flag: atomic::AtomicBool,
    watchers: Mutex<VecDeque<Waker>>,
}

impl Event {
    #[inline]
    fn notify(&self, py: Python) {
        let mut guard = self.watchers.lock().unwrap();
        while let Some(waker) = guard.pop_front() {
            waker.wake(py);
        }
    }

    fn unnotify(&self) {
        let guard = self.watchers.lock().unwrap();
        for waker in guard.iter() {
            waker.hold();
        }
    }

    #[inline]
    fn add_waker(&self, py: Python, waker: Waker) {
        let mut guard = self.watchers.lock().unwrap();
        if self.flag.load(atomic::Ordering::Acquire) {
            waker.wake(py);
            return;
        }
        guard.push_back(waker);
    }
}

#[pymethods]
impl Event {
    #[new]
    pub(crate) fn new() -> Self {
        Self {
            flag: false.into(),
            watchers: Mutex::new(VecDeque::new()),
        }
    }

    pub(crate) fn set(&self, py: Python) {
        if self
            .flag
            .compare_exchange(false, true, atomic::Ordering::Release, atomic::Ordering::Relaxed)
            .is_ok()
        {
            self.notify(py);
        }
    }

    pub(crate) fn clear(&self) {
        if self
            .flag
            .compare_exchange(true, false, atomic::Ordering::Release, atomic::Ordering::Relaxed)
            .is_ok()
        {
            self.unnotify();
        }
    }

    pub(crate) fn is_set(&self) -> bool {
        self.flag.load(atomic::Ordering::Acquire)
    }

    // TODO: timeout resolution should be micros!
    fn waiter(pyself: Py<Self>, py: Python, timeout: Option<usize>) -> Py<Waiter> {
        Waiter::from_event(py, pyself, timeout)
    }
}

impl Handle for Py<Event> {
    #[inline]
    fn run(self: Box<Self>, py: Python, _runtime: &Py<Runtime>) {
        self.get().set(py);
    }
}

#[pyclass(frozen, module = "mt_asyncio._mt_asyncio")]
pub(crate) struct Waiter {
    registered: atomic::AtomicBool,
    aborted: Arc<atomic::AtomicBool>,
    events: Vec<Py<Event>>,
    timeout: Option<usize>,
    checkpoint: arc_swap::ArcSwapOption<CoroSuspension>,
}

impl Waiter {
    pub(crate) fn from_event(py: Python, event: Py<Event>, timeout: Option<usize>) -> Py<Self> {
        let slf = Self {
            registered: false.into(),
            aborted: Arc::new(false.into()),
            events: vec![event],
            timeout,
            checkpoint: None.into(),
        };
        Py::new(py, slf).unwrap()
    }

    pub fn new_for_suspension() -> Self {
        Self {
            registered: false.into(),
            aborted: Arc::new(false.into()),
            events: vec![],
            timeout: None,
            checkpoint: None.into(),
        }
    }

    fn build_sentinel(&self, py: Python) -> Option<Sentinel> {
        match self.events.len() {
            0..=1 => None,
            v => Some(Sentinel::new(py, v)),
        }
    }

    fn register(&self, py: Python, runtime: Py<Runtime>, suspension: Arc<CoroSuspension>) {
        for (idx, event) in self.events.iter().enumerate() {
            let waker = Waker {
                runtime: runtime.clone_ref(py),
                target: suspension.clone(),
                idx,
            };
            event.get().add_waker(py, waker);
        }
        if let Some(timeout) = self.timeout {
            let when = runtime.get()._get_clock() + (timeout as u128);
            let timer = Timer {
                when,
                target: suspension.clone(),
            };
            runtime.get().add_timer(timer);
        }
    }

    pub(crate) fn register_coro(
        pyself: Py<Self>,
        py: Python,
        runtime: Py<Runtime>,
        target: SuspensionTarget,
        checkpoint: Option<Arc<Py<Self>>>,
    ) {
        let rself = pyself.get();
        if rself
            .registered
            .compare_exchange(false, true, atomic::Ordering::Release, atomic::Ordering::Relaxed)
            .is_ok()
        {
            let sentinel = rself.build_sentinel(py);
            if rself.events.is_empty() {
                //: a checkpoint waiter: resumes immediately, but leaves a handle
                //  `abort` can use to throw in at the next suspension point
                let suspension = Arc::new(CoroSuspension::new(
                    target,
                    sentinel,
                    rself.aborted.clone(),
                    Some(Arc::new(pyself.clone_ref(py))),
                ));
                rself.checkpoint.swap(Some(suspension.clone()));
                if rself.aborted.load(atomic::Ordering::Acquire) {
                    suspension.error(py, runtime.get(), abort());
                    return;
                }
                suspension.resume(py, runtime.get(), py.None(), 0);
                return;
            }
            let suspension = match checkpoint {
                Some(checkpoint) => {
                    let rcheckpoint = checkpoint.get();
                    let suspension = Arc::new(CoroSuspension::new(
                        target,
                        sentinel,
                        rcheckpoint.aborted.clone(),
                        Some(checkpoint.clone()),
                    ));
                    rcheckpoint.checkpoint.swap(Some(suspension.clone()));
                    suspension
                }
                _ => CoroSuspension::new(target, sentinel, Arc::new(false.into()), None).into(),
            };
            rself.register(py, runtime, suspension);
        } else {
            panic!("Waiter already registered")
        }
    }

    pub(crate) fn abort_coro(&self, py: Python) {
        if self
            .aborted
            .compare_exchange(false, true, atomic::Ordering::Release, atomic::Ordering::Relaxed)
            .is_ok()
            && let Some(checkpoint) = self.checkpoint.load().as_ref()
        {
            checkpoint.error(py, crate::get_runtime(py).unwrap().get(), abort());
        }
    }
}

#[pymethods]
impl Waiter {
    #[new]
    #[pyo3(signature = (*events))]
    pub fn new(events: Vec<Py<Event>>) -> Self {
        Self {
            registered: false.into(),
            aborted: Arc::new(false.into()),
            events,
            timeout: None,
            checkpoint: None.into(),
        }
    }

    #[staticmethod]
    fn checkpoint() -> Self {
        Self::new_for_suspension()
    }

    fn abort(&self, py: Python) {
        self.abort_coro(py);
    }

    fn __await__(pyself: Py<Self>) -> Py<Self> {
        pyself
    }

    fn __next__(pyself: Py<Self>) -> Option<Py<Self>> {
        match pyself.get().registered.load(atomic::Ordering::Acquire) {
            false => Some(pyself),
            true => None,
        }
    }

    fn send(&self, value: Py<PyAny>) -> PyResult<Py<PyAny>> {
        Err(pyo3::exceptions::PyStopIteration::new_err(value))
    }

    pub(crate) fn throw(&self, value: Bound<PyAny>) -> PyResult<()> {
        let err = PyErr::from_value(value);
        Err(err)
    }
}

#[derive(Debug)]
#[pyclass(frozen, module = "mt_asyncio._mt_asyncio", name = "Result")]
pub(crate) struct ResultHolder {
    size: usize,
    data: Mutex<Vec<Py<PyAny>>>,
}

#[pymethods]
impl ResultHolder {
    #[new]
    #[pyo3(signature = (size = 1))]
    pub fn new(py: Python, size: usize) -> Self {
        let mut data = Vec::with_capacity(size);
        for _ in 0..size {
            data.push(py.None());
        }
        Self {
            size,
            data: Mutex::new(data),
        }
    }

    #[pyo3(signature = (value, index = None))]
    pub fn store(&self, value: Py<PyAny>, index: Option<usize>) {
        let index = index.unwrap_or(0);
        let mut guard = self.data.lock().unwrap();
        guard[..][index] = value;
    }

    fn fetch(&self, py: Python) -> Py<PyAny> {
        let guard = self.data.lock().unwrap();
        match self.size {
            1 => guard.first().unwrap().clone_ref(py),
            _ => PyList::new(py, &guard[..]).unwrap().into_py_any(py).unwrap(),
        }
    }
}

pub struct Waker {
    runtime: Py<Runtime>,
    target: Arc<CoroSuspension>,
    idx: usize,
}

impl Waker {
    pub fn wake(&self, py: Python) {
        self.target.resume(py, self.runtime.get(), py.None(), self.idx);
    }

    fn hold(&self) {
        self.target.suspend();
    }
}

#[derive(Debug)]
pub(crate) enum SuspensionTarget {
    Coro(Py<PyAny>),
    CoroCtx((Py<PyAny>, Py<PyAny>)),
}

//: a parked coroutine plus what it takes to put it back on a worker: the
//  resume target, the multi-event sentinel (if it waits on more than one
//  event), and the checkpoint that can abort it
#[derive(Debug)]
pub(crate) struct CoroSuspension {
    pub target: SuspensionTarget,
    consumed: atomic::AtomicBool,
    sentinel: Option<Sentinel>,
    aborted: Arc<atomic::AtomicBool>,
    checkpoint: Option<Arc<Py<Waiter>>>,
}

impl CoroSuspension {
    pub(crate) fn new(
        target: SuspensionTarget,
        sentinel: Option<Sentinel>,
        aborted: Arc<atomic::AtomicBool>,
        checkpoint: Option<Arc<Py<Waiter>>>,
    ) -> Self {
        Self {
            target,
            consumed: false.into(),
            sentinel,
            aborted,
            checkpoint,
        }
    }

    fn to_handle(&self, py: Python, value: Py<PyAny>) -> BoxedHandle {
        match &self.target {
            SuspensionTarget::Coro(target) => {
                let handle = handles::PyCoroHandle {
                    coro: target.clone_ref(py),
                    value,
                    checkpoint: self.checkpoint.clone(),
                };
                Box::new(handle)
            }
            SuspensionTarget::CoroCtx((target, ctx)) => {
                let handle = handles::PyCoroCtxHandle {
                    coro: target.clone_ref(py),
                    ctx: ctx.clone_ref(py),
                    value,
                    checkpoint: self.checkpoint.clone(),
                };
                Box::new(handle)
            }
        }
    }

    fn to_throw_handle(&self, py: Python, err: PyErr) -> BoxedHandle {
        let value = err.into_value(py).as_any().clone_ref(py);
        match &self.target {
            SuspensionTarget::Coro(target) => {
                let handle = handles::PyCoroThrower {
                    coro: target.clone_ref(py),
                    value,
                    checkpoint: self.checkpoint.clone(),
                };
                Box::new(handle)
            }
            SuspensionTarget::CoroCtx((target, ctx)) => {
                let handle = handles::PyCoroCtxThrower {
                    coro: target.clone_ref(py),
                    ctx: ctx.clone_ref(py),
                    value,
                    checkpoint: self.checkpoint.clone(),
                };
                Box::new(handle)
            }
        }
    }

    fn suspend(&self) {
        if let Some(sentinel) = &self.sentinel {
            sentinel.increment();
        }
    }

    pub fn resume(&self, py: Python, runtime: &Runtime, value: Py<PyAny>, order: usize) {
        if self.aborted.load(atomic::Ordering::Acquire) {
            return;
        }
        if let Some(sentinel) = &self.sentinel {
            if let Some(composed_value) = sentinel.decrement(py, (order, value)) {
                runtime.add_handle(self.to_handle(py, composed_value));
            }
            return;
        }
        if self
            .consumed
            .compare_exchange(false, true, atomic::Ordering::Release, atomic::Ordering::Relaxed)
            .is_ok()
        {
            runtime.add_handle(self.to_handle(py, value));
        }
    }

    pub fn error(&self, py: Python, runtime: &Runtime, value: PyErr) {
        if let Some(sentinel) = &self.sentinel {
            if sentinel.consume() {
                runtime.add_handle(self.to_throw_handle(py, value));
            }
            return;
        }
        if self
            .consumed
            .compare_exchange(false, true, atomic::Ordering::Release, atomic::Ordering::Relaxed)
            .is_ok()
        {
            runtime.add_handle(self.to_throw_handle(py, value));
        }
    }
}

//: countdown for a waiter parked on several events: the coroutine resumes once
//  every event has fired, with the results composed in registration order
#[derive(Debug)]
pub(crate) struct Sentinel {
    counter: atomic::AtomicUsize,
    res: ResultHolder,
}

impl Sentinel {
    fn new(py: Python, len: usize) -> Self {
        Self {
            counter: len.into(),
            res: ResultHolder::new(py, len),
        }
    }

    fn increment(&self) {
        self.counter.fetch_add(1, atomic::Ordering::Release);
    }

    fn decrement(&self, py: Python, result: (usize, Py<PyAny>)) -> Option<Py<PyAny>> {
        let prev = self.counter.fetch_sub(1, atomic::Ordering::Release);
        if prev == 0 {
            self.counter.fetch_add(1, atomic::Ordering::Release);
            return None;
        }
        if prev >= 1 {
            self.res.store(result.1, Some(result.0));
        }
        if prev == 1 {
            return Some(self.res.fetch(py));
        }
        None
    }

    fn consume(&self) -> bool {
        match self.counter.load(atomic::Ordering::Acquire) {
            0 => false,
            _ => {
                self.counter.store(0, atomic::Ordering::Release);
                true
            }
        }
    }
}

pub(crate) fn init_pymodule(module: &Bound<PyModule>) -> PyResult<()> {
    module.add_class::<Event>()?;
    module.add_class::<Waiter>()?;
    module.add_class::<ResultHolder>()?;

    Ok(())
}

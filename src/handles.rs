use pyo3::prelude::*;
use std::sync::Arc;

use crate::{
    events::{SuspensionTarget, Waiter},
    runtime::Runtime,
};

pub trait Handle {
    fn run(self: Box<Self>, py: Python, runtime: &Py<Runtime>);
}

pub(crate) type BoxedHandle = Box<dyn Handle + Send>;

//: the coroutine yielded something the runtime cannot drive. The usual cause is
//  a foreign awaitable: a stock `asyncio.Future` yields *itself*, which only
//  CPython's `Task.__step` knows how to park on. Delivered into the coroutine at
//  the suspension point, as CPython does with its own "bad yield" diagnostic.
fn bad_yield_error(value: &Bound<PyAny>) -> PyErr {
    pyo3::exceptions::PyTypeError::new_err(format!(
        "cannot await {value:?} on the mt_asyncio runtime: coroutines may only yield mt_asyncio \
         `Waiter` objects (a stock `asyncio.Future` yields itself and is not drivable here)"
    ))
}

//: dispatch whatever a coroutine just yielded: reschedule it on a bare
//  suspension, park it on a waiter, or hand back the error to throw in for
//  anything we cannot drive. Shared by the send and throw paths, so a coroutine
//  that catches an injected error and suspends again keeps running.
fn dispatch_yielded(
    py: Python,
    runtime: &Py<Runtime>,
    coro: &Py<PyAny>,
    ctx: Option<&Py<PyAny>>,
    checkpoint: Option<&Arc<Py<Waiter>>>,
    yielded: &Bound<PyAny>,
) -> Option<PyErr> {
    if yielded.is_none() {
        let handle: BoxedHandle = match ctx {
            Some(ctx) => Box::new(PyCoroCtxHandle {
                coro: coro.clone_ref(py),
                ctx: ctx.clone_ref(py),
                value: py.None(),
                checkpoint: checkpoint.cloned(),
            }),
            None => Box::new(PyCoroHandle {
                coro: coro.clone_ref(py),
                value: py.None(),
                checkpoint: checkpoint.cloned(),
            }),
        };
        runtime.get().defer_handle(handle);
        return None;
    }
    if let Ok(waiter) = yielded.extract::<Py<Waiter>>() {
        let target = match ctx {
            Some(ctx) => SuspensionTarget::CoroCtx((coro.clone_ref(py), ctx.clone_ref(py))),
            None => SuspensionTarget::Coro(coro.clone_ref(py)),
        };
        Waiter::register_coro(waiter, py, runtime.clone_ref(py), target, checkpoint.cloned());
        return None;
    }
    Some(bad_yield_error(yielded))
}

//: queue an error to be thrown into a coroutine at its current suspension point
fn throw_into(
    py: Python,
    runtime: &Py<Runtime>,
    coro: &Py<PyAny>,
    ctx: Option<&Py<PyAny>>,
    checkpoint: Option<&Arc<Py<Waiter>>>,
    err: PyErr,
) {
    let value = err.into_value(py).as_any().clone_ref(py);
    let handle: BoxedHandle = match ctx {
        Some(ctx) => Box::new(PyCoroCtxThrower {
            coro: coro.clone_ref(py),
            ctx: ctx.clone_ref(py),
            value,
            checkpoint: checkpoint.cloned(),
        }),
        None => Box::new(PyCoroThrower {
            coro: coro.clone_ref(py),
            value,
            checkpoint: checkpoint.cloned(),
        }),
    };
    runtime.get().add_handle(handle);
}

//: a handle that invokes a Python callable with no arguments — used both by the
//  loop's `call_soon` and to fire pending native readiness callbacks off the
//  poll thread (e.g. on shutdown, where no GIL is held at the scheduling site)
pub(crate) struct CallbackHandle(pub Py<PyAny>);

impl Handle for CallbackHandle {
    fn run(self: Box<Self>, py: Python, _runtime: &Py<Runtime>) {
        if let Err(err) = self.0.call0(py) {
            err.write_unraisable(py, None);
        }
    }
}

//: one step of a native coroutine. `PyIter_Send` pushes `value` in and gives
//  back either a suspension request or the coroutine's return.
pub(crate) struct PyCoroHandle {
    pub coro: Py<PyAny>,
    pub value: Py<PyAny>,
    pub checkpoint: Option<Arc<Py<Waiter>>>,
}

impl PyCoroHandle {
    pub fn new(py: Python, coro: Py<PyAny>) -> Self {
        Self {
            coro,
            value: py.None(),
            checkpoint: None,
        }
    }

    #[inline]
    fn call(self: Box<Self>, py: Python, runtime: &Py<Runtime>) {
        unsafe {
            let mut ret = std::ptr::null_mut::<pyo3::ffi::PyObject>();
            let result = pyo3::ffi::PyIter_Send(self.coro.as_ptr(), self.value.as_ptr(), &raw mut ret);

            match result {
                pyo3::ffi::PySendResult::PYGEN_NEXT => {
                    // if it's just a bare suspension, reschedule
                    if ret == py.None().as_ptr() {
                        pyo3::ffi::Py_DECREF(ret);
                        runtime.get().defer_handle(self);
                        return;
                    }

                    // normally a waiter: `await` chains are driven by the
                    // interpreter, so nothing else reaches us from our own code
                    let yielded = Bound::from_owned_ptr(py, ret);
                    if let Some(err) =
                        dispatch_yielded(py, runtime, &self.coro, None, self.checkpoint.as_ref(), &yielded)
                    {
                        throw_into(py, runtime, &self.coro, None, self.checkpoint.as_ref(), err);
                    }
                }
                pyo3::ffi::PySendResult::PYGEN_ERROR => {
                    let err = pyo3::PyErr::fetch(py);
                    println!("UNHANDLED COROUTINE ERROR {:?}", self.coro.bind(py));
                    err.display(py);
                }
                //: `PyIter_Send` hands back a new reference to the return value
                //  that nothing downstream consumes — adopt it so it is released
                pyo3::ffi::PySendResult::PYGEN_RETURN => drop(Bound::from_owned_ptr(py, ret)),
            }
        }
    }
}

impl Handle for PyCoroHandle {
    fn run(self: Box<Self>, py: Python, runtime: &Py<Runtime>) {
        self.call(py, runtime);
    }
}

//: as `PyCoroHandle`, with the step wrapped in the coroutine's `contextvars`
//  context (the runtime's `context=True` mode)
pub(crate) struct PyCoroCtxHandle {
    pub coro: Py<PyAny>,
    pub ctx: Py<PyAny>,
    pub value: Py<PyAny>,
    pub checkpoint: Option<Arc<Py<Waiter>>>,
}

impl PyCoroCtxHandle {
    pub fn new(py: Python, coro: Py<PyAny>, ctx: Py<PyAny>) -> Self {
        Self {
            coro,
            ctx,
            value: py.None(),
            checkpoint: None,
        }
    }

    #[inline]
    fn call(self: Box<Self>, py: Python, runtime: &Py<Runtime>) {
        unsafe {
            let mut ret = std::ptr::null_mut::<pyo3::ffi::PyObject>();
            let ctx = self.ctx.as_ptr();

            pyo3::ffi::PyContext_Enter(ctx);
            let result = pyo3::ffi::PyIter_Send(self.coro.as_ptr(), self.value.as_ptr(), &raw mut ret);
            pyo3::ffi::PyContext_Exit(ctx);

            match result {
                pyo3::ffi::PySendResult::PYGEN_NEXT => {
                    if ret == py.None().as_ptr() {
                        pyo3::ffi::Py_DECREF(ret);
                        runtime.get().defer_handle(self);
                        return;
                    }

                    let yielded = Bound::from_owned_ptr(py, ret);
                    let ctx = Some(&self.ctx);
                    if let Some(err) =
                        dispatch_yielded(py, runtime, &self.coro, ctx, self.checkpoint.as_ref(), &yielded)
                    {
                        throw_into(py, runtime, &self.coro, ctx, self.checkpoint.as_ref(), err);
                    }
                }
                pyo3::ffi::PySendResult::PYGEN_ERROR => {
                    let err = pyo3::PyErr::fetch(py);
                    println!("UNHANDLED COROUTINE ERROR {:?}", self.coro.bind(py));
                    err.display(py);
                }
                //: as above: release the return value's reference
                pyo3::ffi::PySendResult::PYGEN_RETURN => drop(Bound::from_owned_ptr(py, ret)),
            }
        }
    }
}

impl Handle for PyCoroCtxHandle {
    fn run(self: Box<Self>, py: Python, runtime: &Py<Runtime>) {
        self.call(py, runtime);
    }
}

//: reports the outcome of a `throw` into a coroutine: a coroutine that caught
//  the exception and suspended again must keep being driven, or it is stranded
#[inline]
fn after_throw(
    py: Python,
    runtime: &Py<Runtime>,
    coro: &Py<PyAny>,
    ctx: Option<&Py<PyAny>>,
    checkpoint: Option<&Arc<Py<Waiter>>>,
    res: PyResult<Bound<PyAny>>,
) {
    match res {
        Ok(yielded) => {
            if let Some(err) = dispatch_yielded(py, runtime, coro, ctx, checkpoint, &yielded) {
                throw_into(py, runtime, coro, ctx, checkpoint, err);
            }
        }
        Err(err) if !err.is_instance_of::<pyo3::exceptions::PyStopIteration>(py) => {
            println!("UNHANDLED COROUTINE THROW {:?}", coro.bind(py));
            err.print(py);
        }
        Err(_) => {}
    }
}

//: resumes a coroutine by throwing into it (cancellation / error delivery)
pub(crate) struct PyCoroThrower {
    pub coro: Py<PyAny>,
    pub value: Py<PyAny>,
    pub checkpoint: Option<Arc<Py<Waiter>>>,
}

impl Handle for PyCoroThrower {
    fn run(self: Box<Self>, py: Python, runtime: &Py<Runtime>) {
        let throw_method = pyo3::intern!(py, "throw");

        unsafe {
            let ret =
                pyo3::ffi::PyObject_CallMethodOneArg(self.coro.as_ptr(), throw_method.as_ptr(), self.value.as_ptr());
            let res = Bound::from_owned_ptr_or_err(py, ret);
            after_throw(py, runtime, &self.coro, None, self.checkpoint.as_ref(), res);
        }
    }
}

pub(crate) struct PyCoroCtxThrower {
    pub coro: Py<PyAny>,
    pub ctx: Py<PyAny>,
    pub value: Py<PyAny>,
    pub checkpoint: Option<Arc<Py<Waiter>>>,
}

impl Handle for PyCoroCtxThrower {
    fn run(self: Box<Self>, py: Python, runtime: &Py<Runtime>) {
        let throw_method = pyo3::intern!(py, "throw");
        let ctx = self.ctx.as_ptr();

        unsafe {
            //: copy context to avoid threadstate issues
            let cctx = pyo3::ffi::PyContext_Copy(ctx);

            pyo3::ffi::PyContext_Enter(cctx);
            let ret =
                pyo3::ffi::PyObject_CallMethodOneArg(self.coro.as_ptr(), throw_method.as_ptr(), self.value.as_ptr());
            pyo3::ffi::PyContext_Exit(cctx);
            pyo3::ffi::Py_DECREF(cctx);

            let res = Bound::from_owned_ptr_or_err(py, ret);
            after_throw(py, runtime, &self.coro, Some(&self.ctx), self.checkpoint.as_ref(), res);
        }
    }
}

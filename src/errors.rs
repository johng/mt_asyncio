use pyo3::{
    create_exception,
    exceptions::{PyBaseException, PyRuntimeError},
    prelude::*,
};

create_exception!(_mt_asyncio, CancelledError, PyBaseException, "CancelledError");
create_exception!(
    _mt_asyncio,
    RuntimeAlreadyInitializedError,
    PyRuntimeError,
    "RuntimeAlreadyInitializedError"
);
create_exception!(
    _mt_asyncio,
    RuntimeNotInitializedError,
    PyRuntimeError,
    "RuntimeNotInitializedError"
);

pub(crate) fn abort() -> PyErr {
    CancelledError::new_err("Execution aborted")
}

pub(crate) fn init_pymodule(module: &Bound<PyModule>) -> PyResult<()> {
    module.add("CancelledError", module.py().get_type::<CancelledError>())?;
    module.add(
        "RuntimeAlreadyInitializedError",
        module.py().get_type::<RuntimeAlreadyInitializedError>(),
    )?;
    module.add(
        "RuntimeNotInitializedError",
        module.py().get_type::<RuntimeNotInitializedError>(),
    )?;
    Ok(())
}

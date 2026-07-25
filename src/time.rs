use pyo3::prelude::*;
use std::{cmp::Ordering, sync::Arc};

use crate::events::CoroSuspension;
use crate::handles::Handle;

//: a deadline in the runtime's clock (micros since epoch) and the coroutine to
//  resume when it passes. Ordering is inverted so the `BinaryHeap` behaves as a
//  min-heap on `when`.
pub struct Timer {
    pub(crate) when: u128,
    pub(crate) target: Arc<CoroSuspension>,
}

impl PartialEq for Timer {
    fn eq(&self, _other: &Self) -> bool {
        false
    }
}

impl Eq for Timer {}

impl PartialOrd for Timer {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Timer {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.when < other.when {
            return Ordering::Greater;
        }
        if self.when > other.when {
            return Ordering::Less;
        }
        Ordering::Equal
    }
}

impl Handle for Timer {
    fn run(self: Box<Self>, py: Python, runtime: &Py<crate::runtime::Runtime>) {
        self.target.resume(py, runtime.get(), py.None(), 0);
    }
}

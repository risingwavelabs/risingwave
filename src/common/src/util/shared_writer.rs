use std::cell::RefCell;
use std::io::Write;
use std::rc::Rc;

/// WARNING: Do NOT violate the Rc<RefCell<>> borrow rule when use this.
#[derive(Clone)]
pub struct SharedWriter<W: Write> {
    inner: Rc<RefCell<W>>,
}

impl<W: Write + Default> SharedWriter<W> {
    pub fn new(writer: W) -> Self {
        Self {
            inner: Rc::new(RefCell::new(writer)),
        }
    }

    pub fn into_inner(self) -> W {
        self.inner.take()
    }
}

impl<W: Write> Write for SharedWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.inner.borrow_mut().write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.borrow_mut().flush()
    }
}

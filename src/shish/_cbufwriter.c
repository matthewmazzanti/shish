/*
 * C extension for buffered async byte writer.
 *
 * ByteWriteStream.write(data) returns a WriteAwaitable.
 * ByteWriteStream.flush() returns a FlushAwaitable.
 *
 * Fast path (data fits in buffer): tp_iternext does memcpy and
 * raises StopIteration(len) — no Python frame, no event loop.
 *
 * Flush/write-through: drain via write() syscall, yield Future
 * on EAGAIN.
 *
 * Python blueprint: src/shish/_bufwriter_py.py
 */

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <unistd.h>


/* ═══════════════════════════════════════════════════════════════════
 * Forward declarations and struct definitions
 * ═══════════════════════════════════════════════════════════════════ */

static PyTypeObject WriteAwaitableType;
static PyTypeObject FlushAwaitableType;
static PyTypeObject ByteWriteStreamType;

typedef struct {
    PyObject_HEAD
    PyObject *fd_obj;       /* Fd object (for close/closed) */
    int fd;                 /* raw fd int */
    PyObject *loop;         /* cached event loop */
    char *buffer;           /* internal write buffer */
    Py_ssize_t buf_len;     /* bytes currently in buffer */
    Py_ssize_t buf_cap;     /* buffer capacity */
    Py_ssize_t flush_pos;   /* flush drain progress */
    Py_ssize_t wt_pos;      /* write-through progress */
} ByteWriteStreamObject;

typedef struct {
    PyObject_HEAD
    ByteWriteStreamObject *writer;
    Py_buffer view;         /* pinned reference to caller's data */
    Py_ssize_t length;      /* cached view.len */
    int phase;              /* 0=initial, 1=flushing, 2=write-through */
    PyObject *future;       /* non-NULL while suspended on add_writer */
} WriteAwaitableObject;

typedef struct {
    PyObject_HEAD
    ByteWriteStreamObject *writer;
    PyObject *future;       /* non-NULL while suspended on add_writer */
} FlushAwaitableObject;


/* ═══════════════════════════════════════════════════════════════════
 * Static helpers
 * ═══════════════════════════════════════════════════════════════════ */

/* Raise StopIteration(n) and return NULL. */
static PyObject *
stop_iteration_ssize(Py_ssize_t value)
{
    PyObject *result = PyLong_FromSsize_t(value);
    if (result == NULL)
        return NULL;
    PyErr_SetObject(PyExc_StopIteration, result);
    Py_DECREF(result);
    return NULL;
}

/* Raise StopIteration(None) and return NULL. */
static PyObject *
stop_iteration_none(void)
{
    PyErr_SetNone(PyExc_StopIteration);
    return NULL;
}

/* Try to drain the internal buffer synchronously.
 * Returns 1 if fully drained, 0 on EAGAIN, -1 on error. */
static int
drain_buffer(ByteWriteStreamObject *w)
{
    while (w->flush_pos < w->buf_len) {
        Py_ssize_t n = write(w->fd, w->buffer + w->flush_pos,
                             w->buf_len - w->flush_pos);
        if (n >= 0) {
            w->flush_pos += n;
            continue;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK)
            return 0;
        PyErr_SetFromErrno(PyExc_OSError);
        return -1;
    }
    w->buf_len = 0;
    w->flush_pos = 0;
    return 1;
}

/* Create a Future, register add_writer, set _asyncio_future_blocking.
 * Stores future in *future_slot. Returns new ref for yield, or NULL. */
static PyObject *
wait_writable(ByteWriteStreamObject *w, PyObject **future_slot)
{
    PyObject *future = PyObject_CallMethod(w->loop, "create_future", NULL);
    if (future == NULL)
        return NULL;

    PyObject *set_result = PyObject_GetAttrString(future, "set_result");
    if (set_result == NULL)
        goto error;

    PyObject *r = PyObject_CallMethod(w->loop, "add_writer", "iOO",
                                      w->fd, set_result, Py_None);
    Py_DECREF(set_result);
    if (r == NULL)
        goto error;
    Py_DECREF(r);

    /* asyncio Task checks this flag to distinguish yield vs yield-from */
    if (PyObject_SetAttrString(future, "_asyncio_future_blocking",
                               Py_True) < 0) {
        PyObject *r2 = PyObject_CallMethod(w->loop, "remove_writer",
                                           "i", w->fd);
        Py_XDECREF(r2);
        goto error;
    }

    *future_slot = future;          /* steals ref for storage */
    return Py_NewRef(future);       /* new ref for yield value */

error:
    Py_DECREF(future);
    return NULL;
}

/* Remove event loop writer registration if active. */
static void
cancel_writer(ByteWriteStreamObject *w, PyObject **future_slot)
{
    if (*future_slot == NULL)
        return;
    PyObject *r = PyObject_CallMethod(w->loop, "remove_writer", "i", w->fd);
    Py_XDECREF(r);
    Py_CLEAR(*future_slot);
}


/* ═══════════════════════════════════════════════════════════════════
 * WriteAwaitable — returned by ByteWriteStream.write()
 * ═══════════════════════════════════════════════════════════════════ */

static WriteAwaitableObject *
WriteAwaitable_create(ByteWriteStreamObject *writer, Py_buffer *view)
{
    WriteAwaitableObject *self = PyObject_New(WriteAwaitableObject,
                                             &WriteAwaitableType);
    if (self == NULL) {
        PyBuffer_Release(view);
        return NULL;
    }
    self->writer = (ByteWriteStreamObject *)Py_NewRef((PyObject *)writer);
    self->view = *view;             /* transfer buffer ownership */
    self->length = view->len;
    self->phase = 0;
    self->future = NULL;
    return self;
}

static void
WriteAwaitable_dealloc(WriteAwaitableObject *self)
{
    cancel_writer(self->writer, &self->future);
    PyBuffer_Release(&self->view);
    Py_DECREF(self->writer);
    PyObject_Del(self);
}

/* ── Internal steps ── */

/* Copy data into buffer at pos, raise StopIteration(length). */
static PyObject *
WriteAwaitable_buffer_data(WriteAwaitableObject *self, Py_ssize_t pos)
{
    ByteWriteStreamObject *w = self->writer;
    memcpy(w->buffer + pos, self->view.buf, self->length);
    w->buf_len = pos + self->length;
    Py_ssize_t length = self->length;
    PyBuffer_Release(&self->view);
    return stop_iteration_ssize(length);
}

/* Forward declaration — flush_step may call writethrough_step. */
static PyObject *
WriteAwaitable_writethrough_step(WriteAwaitableObject *self);

/* Drain the writer's internal buffer, then buffer or write-through. */
static PyObject *
WriteAwaitable_flush_step(WriteAwaitableObject *self)
{
    ByteWriteStreamObject *w = self->writer;
    int result = drain_buffer(w);
    if (result < 0)
        return NULL;
    if (result == 0)
        return wait_writable(w, &self->future);

    /* Buffer drained — buffer if it fits, otherwise write-through */
    if (self->length < w->buf_cap)
        return WriteAwaitable_buffer_data(self, 0);

    self->phase = 2;
    return WriteAwaitable_writethrough_step(self);
}

/* Write caller's data directly to fd. */
static PyObject *
WriteAwaitable_writethrough_step(WriteAwaitableObject *self)
{
    ByteWriteStreamObject *w = self->writer;

    while (w->wt_pos < self->length) {
        Py_ssize_t n = write(w->fd,
                             (char *)self->view.buf + w->wt_pos,
                             self->length - w->wt_pos);
        if (n >= 0) {
            w->wt_pos += n;
            continue;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK)
            return wait_writable(w, &self->future);
        PyErr_SetFromErrno(PyExc_OSError);
        return NULL;
    }

    w->wt_pos = 0;
    Py_ssize_t length = self->length;
    PyBuffer_Release(&self->view);
    return stop_iteration_ssize(length);
}

/* ── tp_iternext — main entry point ── */

static PyObject *
WriteAwaitable_iternext(WriteAwaitableObject *self)
{
    ByteWriteStreamObject *w = self->writer;

    /* If resuming after yield, clean up writer callback */
    cancel_writer(w, &self->future);

    /* Phase 0: initial entry — try fast path */
    if (self->phase == 0) {
        Py_ssize_t length = self->length;

        /* Fast path: fits in buffer */
        if (length < w->buf_cap && w->buf_len + length <= w->buf_cap)
            return WriteAwaitable_buffer_data(self, w->buf_len);

        /* Need to flush first if buffer has data */
        if (w->buf_len > 0) {
            self->phase = 1;
            return WriteAwaitable_flush_step(self);
        }

        /* Buffer empty but data >= buf_cap — write-through */
        self->phase = 2;
        return WriteAwaitable_writethrough_step(self);
    }

    /* Phase 1: flushing buffer */
    if (self->phase == 1)
        return WriteAwaitable_flush_step(self);

    /* Phase 2: write-through */
    return WriteAwaitable_writethrough_step(self);
}

/* ── Awaitable protocol ── */

static PyObject *
WriteAwaitable_await(PyObject *self, PyObject *Py_UNUSED(ignored))
{
    return Py_NewRef(self);
}

static PyObject *
WriteAwaitable_send(WriteAwaitableObject *self,
                    PyObject *Py_UNUSED(value))
{
    return WriteAwaitable_iternext(self);
}

static PyObject *
WriteAwaitable_throw(WriteAwaitableObject *self, PyObject *args)
{
    cancel_writer(self->writer, &self->future);
    PyBuffer_Release(&self->view);

    PyObject *type, *value = NULL, *tb = NULL;
    if (!PyArg_ParseTuple(args, "O|OO", &type, &value, &tb))
        return NULL;
    PyErr_Restore(Py_NewRef(type), Py_XNewRef(value), Py_XNewRef(tb));
    return NULL;
}

static PyObject *
WriteAwaitable_close(WriteAwaitableObject *self,
                     PyObject *Py_UNUSED(ignored))
{
    cancel_writer(self->writer, &self->future);
    PyBuffer_Release(&self->view);
    Py_RETURN_NONE;
}

/* ── Type definition ── */

static PyMethodDef WriteAwaitable_methods[] = {
    {"send",      (PyCFunction)WriteAwaitable_send,
                  METH_O,       "send(value) — resume the awaitable"},
    {"throw",     (PyCFunction)WriteAwaitable_throw,
                  METH_VARARGS, "throw(exc) — inject exception"},
    {"close",     (PyCFunction)WriteAwaitable_close,
                  METH_NOARGS,  "close() — cleanup"},
    {"__await__", WriteAwaitable_await,
                  METH_NOARGS,  "Return iterator for await."},
    {NULL}
};

static PyAsyncMethods WriteAwaitable_async = {
    .am_await = (unaryfunc)WriteAwaitable_await,
};

static PyTypeObject WriteAwaitableType = {
    .ob_base = PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "shish._cbufwriter.WriteAwaitable",
    .tp_basicsize = sizeof(WriteAwaitableObject),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_dealloc = (destructor)WriteAwaitable_dealloc,
    .tp_as_async = &WriteAwaitable_async,
    .tp_iter = PyObject_SelfIter,
    .tp_iternext = (iternextfunc)WriteAwaitable_iternext,
    .tp_methods = WriteAwaitable_methods,
    .tp_doc = "Awaitable for a single buffered write.",
};


/* ═══════════════════════════════════════════════════════════════════
 * FlushAwaitable — returned by ByteWriteStream.flush()
 * ═══════════════════════════════════════════════════════════════════ */

static FlushAwaitableObject *
FlushAwaitable_create(ByteWriteStreamObject *writer)
{
    FlushAwaitableObject *self = PyObject_New(FlushAwaitableObject,
                                             &FlushAwaitableType);
    if (self == NULL)
        return NULL;
    self->writer = (ByteWriteStreamObject *)Py_NewRef((PyObject *)writer);
    self->future = NULL;
    return self;
}

static void
FlushAwaitable_dealloc(FlushAwaitableObject *self)
{
    cancel_writer(self->writer, &self->future);
    Py_DECREF(self->writer);
    PyObject_Del(self);
}

static PyObject *
FlushAwaitable_iternext(FlushAwaitableObject *self)
{
    cancel_writer(self->writer, &self->future);

    int result = drain_buffer(self->writer);
    if (result < 0)
        return NULL;
    if (result == 0)
        return wait_writable(self->writer, &self->future);
    return stop_iteration_none();
}

static PyObject *
FlushAwaitable_await(PyObject *self, PyObject *Py_UNUSED(ignored))
{
    return Py_NewRef(self);
}

static PyObject *
FlushAwaitable_send(FlushAwaitableObject *self,
                    PyObject *Py_UNUSED(value))
{
    return FlushAwaitable_iternext(self);
}

static PyObject *
FlushAwaitable_throw(FlushAwaitableObject *self, PyObject *args)
{
    cancel_writer(self->writer, &self->future);

    PyObject *type, *value = NULL, *tb = NULL;
    if (!PyArg_ParseTuple(args, "O|OO", &type, &value, &tb))
        return NULL;
    PyErr_Restore(Py_NewRef(type), Py_XNewRef(value), Py_XNewRef(tb));
    return NULL;
}

static PyObject *
FlushAwaitable_close(FlushAwaitableObject *self,
                     PyObject *Py_UNUSED(ignored))
{
    cancel_writer(self->writer, &self->future);
    Py_RETURN_NONE;
}

static PyMethodDef FlushAwaitable_methods[] = {
    {"send",      (PyCFunction)FlushAwaitable_send,
                  METH_O,       "send(value) — resume the awaitable"},
    {"throw",     (PyCFunction)FlushAwaitable_throw,
                  METH_VARARGS, "throw(exc) — inject exception"},
    {"close",     (PyCFunction)FlushAwaitable_close,
                  METH_NOARGS,  "close() — cleanup"},
    {"__await__", FlushAwaitable_await,
                  METH_NOARGS,  "Return iterator for await."},
    {NULL}
};

static PyAsyncMethods FlushAwaitable_async = {
    .am_await = (unaryfunc)FlushAwaitable_await,
};

static PyTypeObject FlushAwaitableType = {
    .ob_base = PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "shish._cbufwriter.FlushAwaitable",
    .tp_basicsize = sizeof(FlushAwaitableObject),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_dealloc = (destructor)FlushAwaitable_dealloc,
    .tp_as_async = &FlushAwaitable_async,
    .tp_iter = PyObject_SelfIter,
    .tp_iternext = (iternextfunc)FlushAwaitable_iternext,
    .tp_methods = FlushAwaitable_methods,
    .tp_doc = "Awaitable that drains the internal buffer.",
};


/* ═══════════════════════════════════════════════════════════════════
 * ByteWriteStream — the buffered writer
 * ═══════════════════════════════════════════════════════════════════ */

static int
ByteWriteStream_init(ByteWriteStreamObject *self, PyObject *args,
                     PyObject *kwargs)
{
    static char *kwlist[] = {"owned_fd", "buffer_size", NULL};
    PyObject *fd_obj;
    Py_ssize_t buffer_size = 8192;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|n", kwlist,
                                     &fd_obj, &buffer_size))
        return -1;

    /* Extract int fd from Fd object */
    PyObject *fd_attr = PyObject_GetAttrString(fd_obj, "fd");
    if (fd_attr == NULL)
        return -1;
    self->fd = (int)PyLong_AsLong(fd_attr);
    Py_DECREF(fd_attr);
    if (self->fd == -1 && PyErr_Occurred())
        return -1;

    /* Cache the running event loop */
    PyObject *asyncio = PyImport_ImportModule("asyncio");
    if (asyncio == NULL)
        return -1;
    self->loop = PyObject_CallMethod(asyncio, "get_running_loop", NULL);
    Py_DECREF(asyncio);
    if (self->loop == NULL)
        return -1;

    /* Allocate buffer */
    self->buffer = PyMem_Malloc(buffer_size);
    if (self->buffer == NULL) {
        PyErr_NoMemory();
        return -1;
    }
    self->buf_len = 0;
    self->buf_cap = buffer_size;
    self->flush_pos = 0;
    self->wt_pos = 0;

    /* Keep reference to Fd object for close/closed */
    self->fd_obj = Py_NewRef(fd_obj);

    /* Set non-blocking */
    int flags = fcntl(self->fd, F_GETFL);
    if (flags == -1) {
        PyErr_SetFromErrno(PyExc_OSError);
        return -1;
    }
    if (fcntl(self->fd, F_SETFL, flags | O_NONBLOCK) == -1) {
        PyErr_SetFromErrno(PyExc_OSError);
        return -1;
    }

    return 0;
}

static void
ByteWriteStream_dealloc(ByteWriteStreamObject *self)
{
    if (self->buffer != NULL)
        PyMem_Free(self->buffer);
    Py_XDECREF(self->fd_obj);
    Py_XDECREF(self->loop);
    Py_TYPE(self)->tp_free((PyObject *)self);
}

/* ── write(data) → WriteAwaitable ── */

static PyObject *
ByteWriteStream_write(ByteWriteStreamObject *self, PyObject *data)
{
    Py_buffer view;
    if (PyObject_GetBuffer(data, &view, PyBUF_SIMPLE) < 0)
        return NULL;
    return (PyObject *)WriteAwaitable_create(self, &view);
}

/* ── flush() → FlushAwaitable ── */

static PyObject *
ByteWriteStream_flush(ByteWriteStreamObject *self,
                      PyObject *Py_UNUSED(ignored))
{
    return (PyObject *)FlushAwaitable_create(self);
}

/* ── close_fd() — close without flushing ── */

static PyObject *
ByteWriteStream_close_fd(ByteWriteStreamObject *self,
                         PyObject *Py_UNUSED(ignored))
{
    return PyObject_CallMethod(self->fd_obj, "close", NULL);
}

/* ── remove_writer() — clean up event loop registration ── */

static PyObject *
ByteWriteStream_remove_writer(ByteWriteStreamObject *self,
                              PyObject *Py_UNUSED(ignored))
{
    PyObject *r = PyObject_CallMethod(self->loop, "remove_writer",
                                      "i", self->fd);
    if (r == NULL)
        return NULL;
    Py_DECREF(r);
    Py_RETURN_NONE;
}

/* ── closed property ── */

static PyObject *
ByteWriteStream_get_closed(ByteWriteStreamObject *self,
                           void *Py_UNUSED(closure))
{
    return PyObject_GetAttrString(self->fd_obj, "closed");
}

/* ── from_fd(owned_fd, buffer_size=8192) classmethod ── */

static PyObject *
ByteWriteStream_from_fd(PyTypeObject *cls, PyObject *args, PyObject *kwargs)
{
    return PyObject_Call((PyObject *)cls, args, kwargs);
}

/* ── Type definition ── */

static PyMethodDef ByteWriteStream_methods[] = {
    {"write",         (PyCFunction)ByteWriteStream_write,
                      METH_O,      "write(data) — returns WriteAwaitable"},
    {"flush",         (PyCFunction)ByteWriteStream_flush,
                      METH_NOARGS, "flush() — returns FlushAwaitable"},
    {"close_fd",      (PyCFunction)ByteWriteStream_close_fd,
                      METH_NOARGS, "close_fd() — close without flushing"},
    {"remove_writer", (PyCFunction)ByteWriteStream_remove_writer,
                      METH_NOARGS, "remove_writer() — clean up event loop"},
    {"from_fd",       (PyCFunction)ByteWriteStream_from_fd,
                      METH_VARARGS | METH_KEYWORDS | METH_CLASS,
                      "from_fd(owned_fd, buffer_size=8192) — create from fd"},
    {NULL}
};

static PyGetSetDef ByteWriteStream_getset[] = {
    {"closed", (getter)ByteWriteStream_get_closed, NULL,
     "Whether the fd is closed.", NULL},
    {NULL}
};

static PyTypeObject ByteWriteStreamType = {
    .ob_base = PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "shish._cbufwriter.ByteWriteStream",
    .tp_basicsize = sizeof(ByteWriteStreamObject),
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
    .tp_new = PyType_GenericNew,
    .tp_init = (initproc)ByteWriteStream_init,
    .tp_dealloc = (destructor)ByteWriteStream_dealloc,
    .tp_methods = ByteWriteStream_methods,
    .tp_getset = ByteWriteStream_getset,
    .tp_doc = "Buffered async byte writer — C implementation.\n\n"
              "write() returns a C WriteAwaitable. On the fast path\n"
              "(data fits in buffer), resolves via memcpy + StopIteration\n"
              "with no Python frames or event loop interaction.\n\n"
              "Subclass to add async close() and context manager.",
};


/* ═══════════════════════════════════════════════════════════════════
 * Module definition
 * ═══════════════════════════════════════════════════════════════════ */

static int
module_exec(PyObject *mod)
{
    if (PyType_Ready(&WriteAwaitableType) < 0)
        return -1;
    if (PyType_Ready(&FlushAwaitableType) < 0)
        return -1;
    if (PyType_Ready(&ByteWriteStreamType) < 0)
        return -1;

    if (PyModule_AddType(mod, &ByteWriteStreamType) < 0)
        return -1;

    /* WriteAwaitable and FlushAwaitable are not exported —
     * created internally by write() and flush() */
    return 0;
}

static PyModuleDef_Slot module_slots[] = {
    {Py_mod_exec, module_exec},
    {0, NULL}
};

static PyModuleDef moduledef = {
    .m_base = PyModuleDef_HEAD_INIT,
    .m_name = "shish._cbufwriter",
    .m_doc = "C extension for buffered async byte writer.",
    .m_size = 0,
    .m_slots = module_slots,
};

PyMODINIT_FUNC
PyInit__cbufwriter(void)
{
    return PyModuleDef_Init(&moduledef);
}

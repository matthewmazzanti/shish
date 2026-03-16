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
    char *buf;
    Py_ssize_t len;
    Py_ssize_t cap;
} Buf;

typedef struct {
    PyObject_HEAD
    PyObject *fd_obj;       /* Fd object (for close/closed) */
    int fd;                 /* raw fd int */
    PyObject *loop;         /* cached event loop */
    Buf wbuf;               /* write buffer */
    Py_ssize_t flush_pos;   /* flush drain progress */
} ByteWriteStreamObject;

enum WritePhase {
    ENTRY = 0,              /* initial entry — acquire buffer, decide path */
    FLUSH = 1,              /* resume after yielding during flush */
    WRITE = 2,              /* resume after yielding during write-through */
    DONE = -1,              /* terminal — completed successfully */
    ERROR = -2,             /* terminal — re-raise on any further call */
};

typedef struct {
    PyObject_HEAD
    ByteWriteStreamObject *writer;
    PyObject *data;         /* raw data ref (buffer acquired in SETUP) */
    Py_buffer view;         /* view.obj != NULL when acquired */
    enum WritePhase phase;
    Py_ssize_t wt_pos;      /* write-through position */
} WriteAwaitableObject;

typedef struct {
    PyObject_HEAD
    ByteWriteStreamObject *writer;
} FlushAwaitableObject;


/* ═══════════════════════════════════════════════════════════════════
 * Helpers
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

/* Create a Future that resolves when fd is writable.
 * add_writer(fd, future.set_result, None) — callback resolves the future.
 * Caller must call remove_writer on resume or cleanup.
 * Returns new reference, or NULL on error. */
static PyObject *
wait_writable(ByteWriteStreamObject *w)
{
    PyObject *future = PyObject_CallMethod(w->loop, "create_future", NULL);
    if (future == NULL)
        return NULL;

    PyObject *set_result = PyObject_GetAttrString(future, "set_result");
    if (set_result == NULL)
        goto error;

    PyObject *r = PyObject_CallMethod(
        w->loop, "add_writer", "iOO", w->fd, set_result, Py_None
    );
    Py_DECREF(set_result);
    if (r == NULL)
        goto error;
    Py_DECREF(r);

    /* asyncio Task checks this flag to distinguish yield vs yield-from */
    if (PyObject_SetAttrString(future, "_asyncio_future_blocking", Py_True) < 0)
        goto error_writer;

    return future;

error_writer: {
    PyObject *r2 = PyObject_CallMethod(w->loop, "remove_writer", "i", w->fd);
    Py_XDECREF(r2);
}
error:
    Py_DECREF(future);
    return NULL;
}

/* Remove event loop writer registration.
 * Always safe to call — no-op if no writer registered.
 * Clears any error from the internal method call. */
static void
remove_writer(ByteWriteStreamObject *w)
{
    if (w->loop == NULL)
        return;
    PyObject *r = PyObject_CallMethod(w->loop, "remove_writer", "i", w->fd);
    if (r != NULL)
        Py_DECREF(r);
    else
        PyErr_Clear();
}



/* ═══════════════════════════════════════════════════════════════════
 * WriteAwaitable — returned by ByteWriteStream.write()
 * ═══════════════════════════════════════════════════════════════════ */

static void
WriteAwaitable_dealloc(WriteAwaitableObject *self)
{
    /* Save exception state — dealloc can be called with active exception */
    PyObject *err_type, *err_value, *err_tb;
    PyErr_Fetch(&err_type, &err_value, &err_tb);
    remove_writer(self->writer);
    PyErr_Restore(err_type, err_value, err_tb);

    if (self->view.obj)
        PyBuffer_Release(&self->view);
    Py_XDECREF(self->data);
    Py_DECREF(self->writer);
    PyObject_Del(self);
}

/* ── tp_iternext — goto-based coroutine ──
 *
 * Translates an async function into a re-entrant C iterator.
 * The switch at the top dispatches to labeled resume points; the body reads
 * top-to-bottom like the original async code.
 *
 * To "yield" (suspend and return a Future to the event loop):
 *   1. Save phase for re-entry   (self->phase = FLUSH)
 *   2. Return the Future          (return wait_writable(w))
 *   3. Place a label              (flush:)
 *   4. Clean up from the yield    (remove_writer)
 *
 * On the next iternext call, the switch jumps to the label,
 * resuming execution right after the yield point.
 *
 * Local variables do not survive across yields — each iternext call is a fresh
 * stack frame. Any state needed after a yield must be stored on the struct
 * (self->wt_pos, w->flush_pos, etc.). Locals are fine within a single entry (e.g.
 * `written`).
 *
 * The compiler (gcc -O2) optimizes the switch+goto into a single indirect jump
 * — the jump table points directly at each label, eliminating the intermediate
 * gotos entirely.
 *
 * Paths:
 *   Small data, buffer has room  → ENTRY → memcpy → StopIteration
 *   Small data, must flush first → ENTRY → flush loop ⇄ FLUSH → memcpy → StopIteration
 *   Large data, buffer empty     → ENTRY → write loop ⇄ WRITE → StopIteration
 *   Large data, must flush first → ENTRY → flush loop ⇄ FLUSH → write loop ⇄ WRITE → StopIteration
 */

static PyObject *
WriteAwaitable_iternext(WriteAwaitableObject *self)
{
    ByteWriteStreamObject *w = self->writer;

    switch (self->phase) {
        case DONE:
            PyErr_SetString(PyExc_RuntimeError, "WriteAwaitable already completed");
            return NULL;
        case ERROR:
            PyErr_SetString(PyExc_RuntimeError, "WriteAwaitable already failed");
            return NULL;
        case ENTRY: goto ENTRY;
        case FLUSH: goto FLUSH;
        case WRITE: goto WRITE;
        default: Py_UNREACHABLE();
    }

    /* ── Acquire buffer from caller's data ── */
ENTRY:
    if (PyObject_GetBuffer(self->data, &self->view, PyBUF_SIMPLE) < 0) {
        return NULL;
    }

    /* ── Flush internal buffer if it can't absorb the new data ── */
    if (w->wbuf.len > 0 && w->wbuf.cap < w->wbuf.len + self->view.len) {
        while (w->flush_pos < w->wbuf.len) {
            Py_ssize_t written = write(
                w->fd,
                w->wbuf.buf + w->flush_pos,
                (size_t)(w->wbuf.len - w->flush_pos)
            );
            if (written >= 0) {
                w->flush_pos += written;
                continue;
            }
            if (errno != EAGAIN && errno != EWOULDBLOCK) {
                goto error;
            }

            /* ── yield: await fd writable ──
             * Safe to drop: flush_pos records progress on the writer,
             * so the next write() resumes the partial flush. */
            self->phase = FLUSH;
            return wait_writable(w);
FLUSH:
            remove_writer(w);
        }
        w->wbuf.len = 0;
        w->flush_pos = 0;
    }

    /* ── Fast path: copy small data into buffer ── */
    if (self->view.len < w->wbuf.cap) {
        memcpy(
            w->wbuf.buf + w->wbuf.len,
            self->view.buf,
            (size_t)self->view.len
        );
        w->wbuf.len += self->view.len;
        /* ── return: bytes written to buffer ── */
        goto done;
    }

    /* ── Write-through: large data bypasses buffer ── */
    self->wt_pos = 0;
    while (self->wt_pos < self->view.len) {
        Py_ssize_t written = write(
            w->fd,
            (const char *)self->view.buf + self->wt_pos,
            (size_t)(self->view.len - self->wt_pos)
        );
        if (written >= 0) {
            self->wt_pos += written;
            continue;
        }
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            goto error;
        }
        /* ── yield: await fd writable ──
         * Safe to drop: wt_pos dies with the awaitable. Partial data
         * is already on the fd; writer buffer state is clean. */
        self->phase = WRITE;
        return wait_writable(w);
WRITE:
        remove_writer(w);
    }

    // fallthrough: return bytes written
done:
    self->phase = DONE;
    stop_iteration_ssize(self->view.len);
    PyBuffer_Release(&self->view);
    return NULL;

error:
    self->phase = ERROR;
    PyBuffer_Release(&self->view);
    PyErr_SetFromErrno(PyExc_OSError);
    return NULL;
}

/* ── Awaitable protocol ── */

static PyObject *
WriteAwaitable_await(PyObject *self, PyObject *Py_UNUSED(ignored))
{
    return Py_NewRef(self);
}

static PyObject *
WriteAwaitable_send(
    WriteAwaitableObject *self, PyObject *Py_UNUSED(value)
)
{
    return WriteAwaitable_iternext(self);
}

static PyObject *
WriteAwaitable_throw(WriteAwaitableObject *self, PyObject *args)
{
    remove_writer(self->writer);
    if (self->view.obj) {
        PyBuffer_Release(&self->view);
    }

    PyObject *type, *value = NULL, *tb = NULL;
    if (!PyArg_ParseTuple(args, "O|OO", &type, &value, &tb))
        return NULL;
    PyErr_Restore(Py_NewRef(type), Py_XNewRef(value), Py_XNewRef(tb));
    return NULL;
}

static PyObject *
WriteAwaitable_close(
    WriteAwaitableObject *self, PyObject *Py_UNUSED(ignored)
)
{
    remove_writer(self->writer);
    if (self->view.obj) {
        PyBuffer_Release(&self->view);
    }
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
    FlushAwaitableObject *self = PyObject_New(
        FlushAwaitableObject, &FlushAwaitableType
    );
    if (self == NULL)
        return NULL;
    self->writer = (ByteWriteStreamObject *)Py_NewRef((PyObject *)writer);
    return self;
}

static void
FlushAwaitable_dealloc(FlushAwaitableObject *self)
{
    PyObject *err_type, *err_value, *err_tb;
    PyErr_Fetch(&err_type, &err_value, &err_tb);
    remove_writer(self->writer);
    PyErr_Restore(err_type, err_value, err_tb);

    Py_DECREF(self->writer);
    PyObject_Del(self);
}

static PyObject *
FlushAwaitable_iternext(FlushAwaitableObject *self)
{
    ByteWriteStreamObject *w = self->writer;
    remove_writer(w);

    while (w->flush_pos < w->wbuf.len) {
        Py_ssize_t written = write(
            w->fd,
            w->wbuf.buf + w->flush_pos,
            (size_t)(w->wbuf.len - w->flush_pos)
        );
        if (written >= 0) {
            w->flush_pos += written;
            continue;
        }
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            PyErr_SetFromErrno(PyExc_OSError);
            return NULL;
        }
        return wait_writable(w);
    }
    w->wbuf.len = 0;
    w->flush_pos = 0;
    return stop_iteration_none();
}

static PyObject *
FlushAwaitable_await(PyObject *self, PyObject *Py_UNUSED(ignored))
{
    return Py_NewRef(self);
}

static PyObject *
FlushAwaitable_send(
    FlushAwaitableObject *self, PyObject *Py_UNUSED(value)
)
{
    return FlushAwaitable_iternext(self);
}

static PyObject *
FlushAwaitable_throw(FlushAwaitableObject *self, PyObject *args)
{
    remove_writer(self->writer);

    PyObject *type, *value = NULL, *tb = NULL;
    if (!PyArg_ParseTuple(args, "O|OO", &type, &value, &tb))
        return NULL;
    PyErr_Restore(Py_NewRef(type), Py_XNewRef(value), Py_XNewRef(tb));
    return NULL;
}

static PyObject *
FlushAwaitable_close(
    FlushAwaitableObject *self, PyObject *Py_UNUSED(ignored)
)
{
    remove_writer(self->writer);
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
ByteWriteStream_init(
    ByteWriteStreamObject *self, PyObject *args, PyObject *kwargs
)
{
    static char *kwlist[] = {"owned_fd", "buffer_size", NULL};
    PyObject *fd_obj;
    Py_ssize_t buffer_size = 8192;

    if (!PyArg_ParseTupleAndKeywords(
            args, kwargs, "O|n", kwlist, &fd_obj, &buffer_size
        ))
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
    self->wbuf.buf = PyMem_Malloc(buffer_size);
    if (self->wbuf.buf == NULL) {
        PyErr_NoMemory();
        return -1;
    }
    self->wbuf.len = 0;
    self->wbuf.cap = buffer_size;
    self->flush_pos = 0;

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
    if (self->wbuf.buf != NULL)
        PyMem_Free(self->wbuf.buf);
    Py_XDECREF(self->fd_obj);
    Py_XDECREF(self->loop);
    Py_TYPE(self)->tp_free((PyObject *)self);
}

/* ── write(data) → WriteAwaitable ── */

static PyObject *
ByteWriteStream_write(ByteWriteStreamObject *self, PyObject *data)
{
    WriteAwaitableObject *aw = PyObject_New(
        WriteAwaitableObject, &WriteAwaitableType
    );
    if (aw == NULL)
        return NULL;
    aw->writer = (ByteWriteStreamObject *)Py_NewRef((PyObject *)self);
    aw->data = Py_NewRef(data);
    aw->view.obj = NULL;
    aw->phase = ENTRY;
    aw->wt_pos = 0;
    return (PyObject *)aw;
}

/* ── flush() → FlushAwaitable ── */

static PyObject *
ByteWriteStream_flush(
    ByteWriteStreamObject *self, PyObject *Py_UNUSED(ignored)
)
{
    return (PyObject *)FlushAwaitable_create(self);
}

/* ── close_fd() — close without flushing ── */

static PyObject *
ByteWriteStream_close_fd(
    ByteWriteStreamObject *self, PyObject *Py_UNUSED(ignored)
)
{
    return PyObject_CallMethod(self->fd_obj, "close", NULL);
}

/* ── _remove_writer() — exposed for Python subclass ── */

static PyObject *
ByteWriteStream_remove_writer(
    ByteWriteStreamObject *self, PyObject *Py_UNUSED(ignored)
)
{
    PyObject *r = PyObject_CallMethod(self->loop, "remove_writer",
        "i", self->fd);
    if (r == NULL)
        return NULL;
    Py_DECREF(r);
    Py_RETURN_NONE;
}

/* ── Properties ── */

static PyObject *
ByteWriteStream_get_closed(
    ByteWriteStreamObject *self, void *Py_UNUSED(closure)
)
{
    return PyObject_GetAttrString(self->fd_obj, "closed");
}

static PyObject *
ByteWriteStream_get_buffer_size(
    ByteWriteStreamObject *self, void *Py_UNUSED(closure)
)
{
    return PyLong_FromSsize_t(self->wbuf.cap);
}

static PyObject *
ByteWriteStream_get_buffered(
    ByteWriteStreamObject *self, void *Py_UNUSED(closure)
)
{
    return PyLong_FromSsize_t(self->wbuf.len);
}

/* ── from_fd(owned_fd, buffer_size=8192) classmethod ── */

static PyObject *
ByteWriteStream_from_fd(
    PyTypeObject *cls, PyObject *args, PyObject *kwargs
)
{
    return PyObject_Call((PyObject *)cls, args, kwargs);
}

/* ── Type definition ── */

static PyMethodDef ByteWriteStream_methods[] = {
    {"write",          (PyCFunction)ByteWriteStream_write,
                       METH_O,      "write(data) — returns WriteAwaitable"},
    {"flush",          (PyCFunction)ByteWriteStream_flush,
                       METH_NOARGS, "flush() — returns FlushAwaitable"},
    {"close_fd",       (PyCFunction)ByteWriteStream_close_fd,
                       METH_NOARGS, "close_fd() — close without flushing"},
    {"_remove_writer", (PyCFunction)ByteWriteStream_remove_writer,
                       METH_NOARGS, "_remove_writer() — clean up event loop"},
    {"from_fd",        (PyCFunction)ByteWriteStream_from_fd,
                       METH_VARARGS | METH_KEYWORDS | METH_CLASS,
                       "from_fd(owned_fd, buffer_size=8192) — create from fd"},
    {NULL}
};

static PyGetSetDef ByteWriteStream_getset[] = {
    {"closed", (getter)ByteWriteStream_get_closed, NULL,
     "Whether the fd is closed.", NULL},
    {"buffer_size", (getter)ByteWriteStream_get_buffer_size, NULL,
     "Buffer capacity in bytes.", NULL},
    {"buffered", (getter)ByteWriteStream_get_buffered, NULL,
     "Bytes currently in the write buffer.", NULL},
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

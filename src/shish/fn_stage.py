"""Pipeline stage contexts and text/byte wrapping.

ByteStageCtx and TextStageCtx are the stdin/stdout pairs that Fn
pipeline stages receive. decode() and make_byte_wrapper() handle the
text-to-byte bridging so users can write text-mode functions that run
in the byte-level pipeline.
"""

from __future__ import annotations

import dataclasses as dc
import typing as ty
from collections import abc
from functools import wraps

from shish._defaults import DEFAULT_ENCODING
from shish.streams import (
    ByteReadStream,
    ByteWriteStream,
    TextReadStream,
    TextWriteStream,
)

# =============================================================================
# Stage contexts — stdin/stdout pairs for Fn pipeline stages
# =============================================================================


@dc.dataclass
class ByteStageCtx:
    """Byte-level stdin/stdout/stderr for an Fn pipeline stage."""

    stdin: ByteReadStream
    stdout: ByteWriteStream
    stderr: ByteWriteStream


@dc.dataclass
class TextStageCtx:
    """Text-level stdin/stdout/stderr for an Fn pipeline stage."""

    stdin: TextReadStream
    stdout: TextWriteStream
    stderr: TextWriteStream


# =============================================================================
# Stage function type aliases
# =============================================================================

ByteFn = abc.Callable[[ByteStageCtx], abc.Awaitable[int]]
TextFn = abc.Callable[[TextStageCtx], abc.Awaitable[int]]


# =============================================================================
# @decode decorator — wrap text Fn into byte Fn
# =============================================================================


@ty.overload
def decode(func: TextFn, /) -> ByteFn: ...
@ty.overload
def decode(func: str, /) -> abc.Callable[[TextFn], ByteFn]: ...
@ty.overload
def decode() -> abc.Callable[[TextFn], ByteFn]: ...


def decode(
    func: TextFn | str | None = None,
) -> ByteFn | abc.Callable[[TextFn], ByteFn]:
    """Wrap a TextStageCtx function into a ByteStageCtx function.

    Three forms:
        @decode          — bare decorator, uses utf-8
        @decode()        — explicit call, uses utf-8
        @decode("latin-1") — explicit encoding
    """
    if callable(func):
        # @decode without parens: func is the decorated function
        return make_byte_wrapper(func, DEFAULT_ENCODING)

    # @decode() or @decode("latin-1"): func is encoding or None
    encoding = func if isinstance(func, str) else DEFAULT_ENCODING

    def decorator(inner: TextFn) -> ByteFn:
        return make_byte_wrapper(inner, encoding)

    return decorator


def make_byte_wrapper(func: TextFn, encoding: str) -> ByteFn:
    """Build a ByteStageCtx wrapper that decodes/encodes around func."""

    @wraps(func)
    async def wrapper(ctx: ByteStageCtx) -> int:
        text_ctx = TextStageCtx(
            stdin=TextReadStream(ctx.stdin, encoding=encoding),
            stdout=TextWriteStream(ctx.stdout, encoding=encoding),
            stderr=TextWriteStream(ctx.stderr, encoding=encoding),
        )
        return await func(text_ctx)

    return wrapper

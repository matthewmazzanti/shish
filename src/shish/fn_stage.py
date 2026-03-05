"""Pipeline stage contexts and text/byte wrapping.

ByteStageCtx and TextStageCtx are the stdin/stdout pairs that Fn
pipeline stages receive. decode() and make_byte_wrapper() handle the
text-to-byte bridging so users can write text-mode functions that run
in the byte-level pipeline.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from functools import wraps
from typing import overload

from shish.streams import (
    ByteReadStream,
    ByteWriteStream,
    TextReadStream,
    TextWriteStream,
)

# =============================================================================
# Stage contexts — stdin/stdout pairs for Fn pipeline stages
# =============================================================================


@dataclass
class ByteStageCtx:
    """Byte-level stdin/stdout pair for an Fn pipeline stage."""

    stdin: ByteReadStream
    stdout: ByteWriteStream


@dataclass
class TextStageCtx:
    """Text-level stdin/stdout pair for an Fn pipeline stage."""

    stdin: TextReadStream
    stdout: TextWriteStream


# =============================================================================
# Stage function type aliases
# =============================================================================

ByteFn = Callable[[ByteStageCtx], Awaitable[int]]
TextFn = Callable[[TextStageCtx], Awaitable[int]]


# =============================================================================
# @decode decorator — wrap text Fn into byte Fn
# =============================================================================


@overload
def decode(func: TextFn, /) -> ByteFn: ...
@overload
def decode(func: str, /) -> Callable[[TextFn], ByteFn]: ...
@overload
def decode() -> Callable[[TextFn], ByteFn]: ...


def decode(
    func: TextFn | str | None = None,
) -> ByteFn | Callable[[TextFn], ByteFn]:
    """Wrap a TextStageCtx function into a ByteStageCtx function.

    Three forms:
        @decode          — bare decorator, uses utf-8
        @decode()        — explicit call, uses utf-8
        @decode("latin-1") — explicit encoding
    """
    if callable(func):
        # @decode without parens: func is the decorated function
        return make_byte_wrapper(func, "utf-8")

    # @decode() or @decode("latin-1"): func is encoding or None
    encoding = func if isinstance(func, str) else "utf-8"

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
        )
        return await func(text_ctx)

    return wrapper

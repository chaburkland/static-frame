import types
import typing
from collections import deque

import typing_extensions as tp
import numpy as np

from static_frame.core.container import ContainerBase
from static_frame.core.frame import Frame
from static_frame.core.index import Index
from static_frame.core.index_hierarchy import IndexHierarchy
from static_frame.core.series import Series

# _UnionGenericAlias comes from tp.Union, UnionType from | expressions
# tp.Optional returns a _UnionGenericAlias
if hasattr(types, 'UnionType') and hasattr(types, 'GenericAlias'):
    UNION_TYPES = (typing._UnionGenericAlias, types.UnionType) # type: ignore
    GENERIC_TYPES = (typing._GenericAlias, types.GenericAlias) # type: ignore
else:
    UNION_TYPES = typing._UnionGenericAlias # type: ignore
    GENERIC_TYPES = typing._GenericAlias # type: ignore

TPair = tp.Tuple[tp.Any, tp.Any]
TLogRecord = tp.Tuple[tp.Tuple[tp.Any, ...], tp.Any, tp.Any]

class ValidationError(TypeError):
    def __init__(self, log: tp.Sequence[TLogRecord]) -> None:
        tab = '   '
        for p, v, h in log:
            path = ' / '.join(str(n) for n in p)
            print(f'\n{path}: expected {str(h)} found: {str(type(v))}')

#-------------------------------------------------------------------------------
# handlers for getting components out of generics
# NOTE: we create an instance of dtype.type() so as to not modify h_generic, as it might be Union or other generic that cannot be wrapped in a tp.Type

def get_series_pairs(value: tp.Any, hint: tp.Any) -> tp.Iterable[TPair]:
    h_index, h_generic = tp.get_args(hint) # there must be two
    yield value.index, h_index
    yield value.dtype.type(), h_generic
    # yield value.dtype.type, tp.Type[h_generic]

def get_index_pairs(value: tp.Any, hint: tp.Any) -> tp.Iterable[TPair]:
    [h_generic] = tp.get_args(hint)
    yield value.dtype.type(), h_generic

def get_ndarray_pairs(value: tp.Any, hint: tp.Any) -> tp.Iterable[TPair]:
    h_shape, h_dtype = tp.get_args(hint)
    yield value.dtype, h_dtype

def get_dtype_pairs(value: tp.Any, hint: tp.Any) -> tp.Iterable[TPair]:
    [h_generic] = tp.get_args(hint)
    yield value.type(), h_generic

#-------------------------------------------------------------------------------

def validate_pair(
        value: tp.Any,
        hint: tp.Any,
        fail_fast: bool = False,
        parent: tp.Tuple[tp.Any, ...] = (),
        ) -> tp.Iterable[TLogRecord]:

    q = deque(((value, hint),))
    log: tp.List[TLogRecord] = []

    while q:
        if fail_fast and log:
            return log

        v, h = q.popleft()
        p_next = parent + (h,)

        if h is tp.Any:
            continue

        if isinstance(h, UNION_TYPES):
            # NOTE: must check union types first as tp.Union matches as generic type
            u_log: tp.List[TLogRecord] = []
            for c_hint in tp.get_args(h): # get components
                # handing one pair at a time with a secondary call will allow nested types in the union to be evaluated on their own
                c_log = validate_pair(v, c_hint, fail_fast, p_next)
                if not c_log: # no error found, can exit
                    break
                else: # find all errors
                    u_log.extend(c_log)
            else: # no breaks, so no matches within union
                log.extend(u_log)

        elif isinstance(h, GENERIC_TYPES): # type: ignore[unreachable]
            # have a generic container
            origin = tp.get_origin(h)
            if origin is type: # a tp.Type[x] generic
                [t] = tp.get_args(h)
                try: # the v should be a subclass of t
                    check = issubclass(t, v)
                except TypeError:
                    check = False

                if check:
                    continue
                else:
                    log.append((p_next, v, h))
            else:
                if not isinstance(v, origin):
                    log.append((p_next, v, origin))
                    continue

                if isinstance(v, Index):
                    q.extend(get_index_pairs(v, h))
                elif isinstance(v, Series):
                    q.extend(get_series_pairs(v, h))
                elif isinstance(v, np.ndarray):
                    q.extend(get_ndarray_pairs(v, h))
                elif isinstance(v, np.dtype):
                    q.extend(get_dtype_pairs(v, h))
                else:
                    raise NotImplementedError(f'no handling for generic {origin}')

        elif isinstance(h, type):
            # special cases
            if v.__class__ is bool:
                if h is bool:
                    continue
                else:
                    log.append((p_next, v, h))
            # general case
            elif isinstance(v, h):
                continue
            else:
                log.append((p_next, v, h))
        else:
            raise NotImplementedError(f'no handling for {v}, {h}')

    return log


def validate_pair_raises(value: tp.Any, hint: tp.Any) -> None:
    log = validate_pair(value, hint)
    if log:
        raise ValidationError(log)


TVFunc = tp.TypeVar('TVFunc', bound=tp.Callable[..., tp.Any])

def validate(func: TVFunc) -> TVFunc:
    return func
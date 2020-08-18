import typing as tp
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor

import numpy as np


from static_frame.core.container import ContainerOperand
from static_frame.core.bus import Bus
from static_frame.core.display import Display
from static_frame.core.display import DisplayActive
from static_frame.core.display import DisplayConfig
from static_frame.core.display import DisplayHeader
from static_frame.core.doc_str import doc_inject
from static_frame.core.frame import Frame
from static_frame.core.index_auto import IndexAutoFactoryType
from static_frame.core.node_selector import InterfaceGetItem
from static_frame.core.series import Series
from static_frame.core.util import AnyCallable
from static_frame.core.util import Bloc2DKeyType
from static_frame.core.util import GetItemKeyType
from static_frame.core.util import GetItemKeyTypeCompound
from static_frame.core.util import IndexInitializer
from static_frame.core.util import NameType
from static_frame.core.util import UFunc
from static_frame.core.util import DTYPE_OBJECT


FrameOrSeries = tp.Union[Frame, Series]
IteratorFrameItems = tp.Iterator[tp.Tuple[tp.Hashable, FrameOrSeries]]
GeneratorFrameItems = tp.Callable[..., IteratorFrameItems]

def call_attr(bundle: tp.Tuple[FrameOrSeries, str, tp.Any, tp.Any]) -> FrameOrSeries:
    # process pool requires a single argument
    frame, attr, args, kwargs = bundle
    func = getattr(frame, attr)
    post = func(*args, **kwargs)
    # post might be an element
    if not isinstance(post, (Frame, Series)):
        # promote to a Series to permit concatenation
        return Series.from_element(post, index=(frame.name,))
    return post


class Batch(ContainerOperand):
    '''
    A lazily evaluated container of Frames that broadcasts operations on component Frames.
    '''

    __slots__ = (
            '_items',
            '_name',
            '_max_workers',
            '_chunksize',
            '_use_threads',
            )

    @classmethod
    def from_frames(cls,
            frames: tp.Iterable[Frame],
            *,
            name: NameType = None,
            max_workers: tp.Optional[int] = None,
            chunksize: int = 1,
            use_threads: bool = False,
            ) -> 'Batch':
        '''Return a :obj:`Batch` from an iterable of :obj:`Frame`; labels will be drawn from :obj:`Frame.name`.
        '''
        return cls(((f.name, f) for f in frames),
                name=name,
                max_workers=max_workers,
                chunksize=chunksize,
                use_threads=use_threads,
                )

    def __init__(self,
            items: IteratorFrameItems,
            *,
            name: NameType = None,
            max_workers: tp.Optional[int] = None,
            chunksize: int = 1,
            use_threads: bool = False,
            ):
        self._items = items # might be a generator!
        self._name = name
        self._max_workers = max_workers
        self._chunksize = chunksize
        self._use_threads = use_threads

    #---------------------------------------------------------------------------

    def _realize(self) -> None:
        # realize generator
        if not hasattr(self._items, '__len__'):
            self._items = tuple(self._items) #type: ignore

    def _derive(self,
            gen: GeneratorFrameItems,
            name: NameType = None,
            ) -> 'Batch':
        '''Utility for creating derived Batch
        '''
        return self.__class__(gen(),
                name=name if name is not None else self._name,
                max_workers=self._max_workers,
                chunksize=self._chunksize,
                use_threads=self._use_threads,
                )

    #---------------------------------------------------------------------------
    # name interface

    @property #type: ignore
    @doc_inject()
    def name(self) -> NameType:
        '''{}'''
        return self._name

    def rename(self, name: NameType) -> 'Batch':
        '''
        Return a new Batch with an updated name attribute.
        '''
        def gen() -> IteratorFrameItems:
            yield from self._items
        return self._derive(gen, name=name)

    #---------------------------------------------------------------------------
    @property
    def shapes(self) -> Series:
        '''A :obj:`Series` describing the shape of each iterated :obj:`Frame`.

        Returns:
            :obj:`tp.Tuple[int]`
        '''
        self._realize()

        items = ((label, f.shape) for label, f in self._items)
        return Series.from_items(items, name='shape', dtype=DTYPE_OBJECT)


    def display(self,
            config: tp.Optional[DisplayConfig] = None
            ) -> Display:
        config = config or DisplayActive.get()

        self._realize()

        items = ((label, f.__class__) for label, f in self._items)
        series = Series.from_items(items, name=self._name)

        display_cls = Display.from_values((),
                header=DisplayHeader(self.__class__, self._name),
                config=config)
        return series._display(config, display_cls)


    #---------------------------------------------------------------------------
    def _apply_attr(self,
            *args: tp.Any,
            attr: str,
            **kwargs: tp.Any,
            ) -> 'Batch':
        '''
        Apply a method on a Frame given as an attr string.
        '''
        if self._max_workers is None:
            def gen() -> IteratorFrameItems:
                for label, frame in self._items:
                    yield label, call_attr((frame, attr, args, kwargs))
            return self._derive(gen)

        pool_executor = ThreadPoolExecutor if self._use_threads else ProcessPoolExecutor
        print('using', pool_executor)
        labels = []

        def arg_gen() -> tp.Iterator[tp.Tuple[FrameOrSeries, str, tp.Any, tp.Any]]:
            for label, frame in self._items:
                labels.append(label)
                yield frame, attr, args, kwargs

        def gen_pool() -> IteratorFrameItems:
            with pool_executor(max_workers=self._max_workers) as executor:
                yield from zip(labels,
                        executor.map(call_attr, arg_gen(), chunksize=self._chunksize)
                        )

        return self._derive(gen_pool)


    def apply(self, func: AnyCallable) -> 'Batch':
        if self._max_workers is None:
            def gen() -> IteratorFrameItems:
                for label, frame in self._items:
                    yield label, func(frame)
            return self._derive(gen)

        pool_executor = ThreadPoolExecutor if self._use_threads else ProcessPoolExecutor
        print('using', pool_executor)

        labels = []

        def arg_gen() -> tp.Iterator[FrameOrSeries]:
            for label, frame in self._items:
                labels.append(label)
                yield frame

        def gen_pool() -> IteratorFrameItems:
            with pool_executor(max_workers=self._max_workers) as executor:
                yield from zip(labels,
                        executor.map(func, arg_gen(), chunksize=self._chunksize)
                        )

        return self._derive(gen_pool)


    #---------------------------------------------------------------------------
    # extraction

    def _extract_iloc(self, key: GetItemKeyTypeCompound) -> 'Batch':
        return self._apply_attr(
                attr='_extract_iloc',
                key=key
                )
    def _extract_loc(self, key: GetItemKeyTypeCompound) -> 'Batch':
        return self._apply_attr(
                attr='_extract_loc',
                key=key
                )

    def _extract_bloc(self, key: Bloc2DKeyType) -> 'Batch':
        return self._apply_attr(
                attr='_extract_bloc',
                key=key
                )

    @doc_inject(selector='selector')
    def __getitem__(self, key: GetItemKeyType) -> 'Batch':
        ''
        return self._apply_attr(
                attr='__getitem__',
                key=key
                )
    #---------------------------------------------------------------------------
    # interfaces

    @property
    def loc(self) -> InterfaceGetItem['Batch']:
        return InterfaceGetItem(self._extract_loc)

    @property
    def iloc(self) -> InterfaceGetItem['Batch']:
        return InterfaceGetItem(self._extract_iloc)

    @property
    def bloc(self) -> InterfaceGetItem['Batch']:
        return InterfaceGetItem(self._extract_bloc)

    #---------------------------------------------------------------------------
    # axis and shape ufunc methods

    def _ufunc_unary_operator(self,
            operator: UFunc
            ) -> 'Batch':
        return self._apply_attr(
                attr='_ufunc_unary_operator',
                operator=operator
                )

    def _ufunc_binary_operator(self, *,
            operator: UFunc,
            other: tp.Any,
            ) -> 'Batch':
        return self._apply_attr(
                attr='_ufunc_binary_operator',
                operator=operator,
                other=other,
                )

    def _ufunc_axis_skipna(self, *,
            axis: int,
            skipna: bool,
            ufunc: UFunc,
            ufunc_skipna: UFunc,
            composable: bool,
            dtypes: tp.Tuple[np.dtype, ...],
            size_one_unity: bool
            ) -> 'Batch':
        return self._apply_attr(
                attr='_ufunc_axis_skipna',
                axis=axis,
                skipna=skipna,
                ufunc=ufunc,
                ufunc_skipna=ufunc_skipna,
                composable=composable,
                dtypes=dtypes,
                size_one_unity=size_one_unity,
                )

    def _ufunc_shape_skipna(self, *,
            axis: int,
            skipna: bool,
            ufunc: UFunc,
            ufunc_skipna: UFunc,
            composable: bool,
            dtypes: tp.Tuple[np.dtype, ...],
            size_one_unity: bool
            ) -> 'Batch':

        return self._apply_attr(
                attr='_ufunc_shape_skipna',
                axis=axis,
                skipna=skipna,
                ufunc=ufunc,
                ufunc_skipna=ufunc_skipna,
                composable=composable,
                dtypes=dtypes,
                size_one_unity=size_one_unity,
                )

    #---------------------------------------------------------------------------
    def keys(self) -> tp.Iterator[tp.Hashable]:
        for k, _ in self._items:
            yield k

    def __iter__(self) -> tp.Iterator[tp.Hashable]:
        '''
        Iterator of column labels, same as :py:meth:`Frame.keys`.
        '''
        yield from self.keys()

    def values(self) -> tp.Iterator[FrameOrSeries]:
        for _, v in self._items:
            yield v

    def items(self) -> IteratorFrameItems:
        '''
        Iterator of labels, :obj:`Frame`.
        '''
        return self._items.__iter__()

    #---------------------------------------------------------------------------
    # exporter

    def to_frame(self, *,
            axis: int = 0,
            union: bool = True,
            index: tp.Optional[tp.Union[IndexInitializer, IndexAutoFactoryType]] = None,
            columns: tp.Optional[tp.Union[IndexInitializer, IndexAutoFactoryType]] = None,
            name: NameType = None,
            fill_value: object = np.nan,
            consolidate_blocks: bool = False
        ) -> Frame:

        labels = []
        def gen() -> tp.Iterator[FrameOrSeries]:
            for label, frame in self._items:
                labels.append(label)
                yield frame

        name = name if name is not None else self._name

        if axis == 0 and index is None:
            index = labels
        if axis == 1 and columns is None:
            columns = labels

        return Frame.from_concat(gen(), #type: ignore
                axis=axis,
                union=union,
                index=index,
                columns=columns,
                name=name,
                fill_value=fill_value,
                consolidate_blocks=consolidate_blocks,
                )


    def to_bus(self) -> 'Bus':
        '''Realize the :obj:`Batch` as an :obj:`Bus`. Note that, as a :obj:`Bus` must have all labels (even if :obj:`Frame` are loaded lazily)
        '''
        return Bus(Series.from_items(self.items(), name=self._name, dtype=DTYPE_OBJECT))



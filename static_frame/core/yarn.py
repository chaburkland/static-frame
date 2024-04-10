from __future__ import annotations

from collections.abc import Set
from itertools import chain

import numpy as np
import typing_extensions as tp

from static_frame.core.axis_map import buses_to_iloc_hierarchy
from static_frame.core.axis_map import buses_to_loc_hierarchy
from static_frame.core.bus import Bus
from static_frame.core.container import ContainerBase
from static_frame.core.container_util import index_from_optional_constructor
from static_frame.core.container_util import index_many_concat
from static_frame.core.container_util import iter_component_signature_bytes
from static_frame.core.container_util import rehierarch_from_index_hierarchy
from static_frame.core.container_util import sort_index_for_order
from static_frame.core.display import Display
from static_frame.core.display import DisplayActive
from static_frame.core.display import DisplayHeader
from static_frame.core.display_config import DisplayConfig
from static_frame.core.doc_str import doc_inject
from static_frame.core.exception import ErrorInitYarn
from static_frame.core.exception import RelabelInvalid
from static_frame.core.frame import Frame
from static_frame.core.index import Index
from static_frame.core.index_auto import IndexAutoFactory
from static_frame.core.index_auto import TIndexAutoFactory
from static_frame.core.index_auto import TRelabelInput
from static_frame.core.index_base import IndexBase
from static_frame.core.index_hierarchy import IndexHierarchy
from static_frame.core.node_iter import IterNodeApplyType
from static_frame.core.node_iter import IterNodeNoArg
from static_frame.core.node_iter import IterNodeType
from static_frame.core.node_selector import InterfaceSelectTrio
from static_frame.core.node_selector import InterGetItemILocReduces
from static_frame.core.node_selector import InterGetItemLocReduces
from static_frame.core.series import Series
from static_frame.core.store_client_mixin import StoreClientMixin
from static_frame.core.style_config import StyleConfig
from static_frame.core.util import DEFAULT_SORT_KIND
from static_frame.core.util import DTYPE_OBJECT
from static_frame.core.util import NAME_DEFAULT
from static_frame.core.util import TDtypeObject
from static_frame.core.util import TILocSelector
from static_frame.core.util import TIndexCtorSpecifier
from static_frame.core.util import TIndexCtorSpecifiers
from static_frame.core.util import TIndexInitializer
from static_frame.core.util import TLabel
from static_frame.core.util import TLocSelector
from static_frame.core.util import TName
from static_frame.core.util import TNDArrayAny
from static_frame.core.util import TNDArrayIntDefault
from static_frame.core.util import TNDArrayObject
from static_frame.core.util import is_callable_or_mapping
from static_frame.core.util import iterable_to_array_1d
from static_frame.core.util import PositionsAllocator
from static_frame.core.util import INT_TYPES

TSeriesObject = Series[tp.Any, np.object_]
TFrameAny = Frame[tp.Any, tp.Any, tp.Unpack[tp.Tuple[tp.Any, ...]]]
TBusAny = Bus[tp.Any]

#-------------------------------------------------------------------------------
TVIndex = tp.TypeVar('TVIndex', bound=IndexBase, default=tp.Any)

class Yarn(ContainerBase, StoreClientMixin, tp.Generic[TVIndex]):
    '''
    A :obj:`Series`-like container made of an ordered collection of :obj:`Bus`. :obj:`Yarn` can be indexed independently of the contained :obj:`Bus`, permitting independent labels per contained :obj:`Frame`.
    '''

    __slots__ = (
            '_values',
            '_hierarchy',
            '_index',
            '_indexer',
            '_name',
            '_deepcopy_from_bus',
            )

    _values: TNDArrayObject
    _hierarchy: IndexHierarchy
    _index: IndexBase
    _indexer: TNDArrayIntDefault
    _name: TName

    _NDIM: int = 1

    @classmethod
    def from_buses(cls,
            buses: tp.Iterable[TBusAny],
            *,
            name: TName = None,
            retain_labels: bool,
            deepcopy_from_bus: bool = False,
            ) -> tp.Self:
        '''Return a :obj:`Yarn` from an iterable of :obj:`Bus`; labels will be drawn from :obj:`Bus.name`.
        '''
        values, _ = iterable_to_array_1d(buses, dtype=DTYPE_OBJECT)

        hierarchy = buses_to_iloc_hierarchy(
                values,
                deepcopy_from_bus=deepcopy_from_bus,
                init_exception_cls=ErrorInitYarn,
                )

        if retain_labels:
            index = buses_to_loc_hierarchy(
                    values,
                    deepcopy_from_bus=deepcopy_from_bus,
                    init_exception_cls=ErrorInitYarn,
                    )
        else:
            index = hierarchy.level_drop(1) #type: ignore

        return cls(values,
                hierarchy=hierarchy,
                index=index,
                name=name,
                deepcopy_from_bus=deepcopy_from_bus,
                )

    @classmethod
    def from_concat(cls,
            containers: tp.Iterable[TYarnAny],
            *,
            index: tp.Optional[tp.Union[TIndexInitializer, TIndexAutoFactory]] = None,
            name: TName = NAME_DEFAULT,
            deepcopy_from_bus: bool = False,
            ) -> tp.Self:
        '''
        Concatenate multiple :obj:`Yarn` into a new :obj:`Yarn`. Loaded status of :obj:`Frame` within each :obj:`Bus` will not be altered.

        Args:
            containers:
            index: Optionally provide new labels for the result of the concatenation.
            name:
            deepcopy_from_bus:
        '''
        bus_components: tp.List[TBusAny] = []
        index_components: tp.Optional[tp.List[IndexBase]] = None if index is not None else []
        for element in containers:
            if isinstance(element, Yarn):
                bus_components.extend(element._values)
                if index_components is not None:
                    index_components.append(element.index)
            else:
                raise NotImplementedError(f'cannot instantiate from {type(element)}')

        array = np.empty(len(bus_components), dtype=DTYPE_OBJECT)
        for i, bus in enumerate(bus_components):
            array[i] = bus
        array.flags.writeable = False

        if index_components is not None:
            index = index_many_concat(index_components, Index)

        # series: TSeriesObject = Series(array)
        return cls(array,
                deepcopy_from_bus=deepcopy_from_bus,
                index=index,
                name=name,
                )

    #---------------------------------------------------------------------------
    def __init__(self,
            series: tp.Union[TSeriesObject, tp.Iterable[TBusAny]], # rename: values
            *,
            index: TIndexInitializer | TIndexAutoFactory | None = None,
            index_constructor: tp.Optional[TIndexCtorSpecifier] = None,
            deepcopy_from_bus: bool = False,
            indexer: tp.Optional[TNDArrayIntDefault] = None,
            hierarchy: tp.Optional[IndexHierarchy] = None,
            name: TName = None,
            own_index: bool = False,
            ) -> None:
        '''
        Args:
            series: An iterable (or :obj:`Series`) of :obj:`Bus`. The length of this container may not be the same as ``index``, if provided.
            index: Optionally provide an index for the :obj:`Frame` contained in all :obj:`Bus`.
            index_constructor:
            deepcopy_from_bus:
            hierarchy: Optionally provide a depth-two `IndexHierarchy` constructed from `Bus` integer positions on the outer level, and contained `Frame` labels on the inner level.
            name:
            own_index:
        '''

        if isinstance(series, Series):
            if series.dtype != DTYPE_OBJECT:
                raise ErrorInitYarn(
                        f'Series passed to initializer must have dtype object, not {series.dtype}')
            self._values = series.values
        else:
            try:
                self._values, _ = iterable_to_array_1d(series, dtype=DTYPE_OBJECT)
            except RuntimeError as e:
                raise ErrorInitYarn(e) from None

        self._name = name
        self._deepcopy_from_bus = deepcopy_from_bus

        if hierarchy is None:
            self._hierarchy = buses_to_iloc_hierarchy(
                    self._values,
                    deepcopy_from_bus=self._deepcopy_from_bus,
                    init_exception_cls=ErrorInitYarn,
                    )
        else: # NOTE: we assume this hierarchy is well-formed
            self._hierarchy = hierarchy

        if own_index:
            self._index = index #type: ignore
        elif index is None or index is IndexAutoFactory:
            self._index = IndexAutoFactory.from_optional_constructor(
                    len(self._hierarchy),
                    default_constructor=Index,
                    explicit_constructor=index_constructor
                    )
        else: # an iterable of labels or an Index
            self._index = index_from_optional_constructor(index,
                    default_constructor=Index,
                    explicit_constructor=index_constructor
                    )

        if len(self._index) > len(self._hierarchy): # pyright: ignore
            raise ErrorInitYarn(f'Length of supplied index ({len(self._index)}) not of sufficient size ({len(self._hierarchy)}).') # pyright: ignore

        if indexer is None:
            self._indexer = PositionsAllocator.get(len(self._index))
        else:
            self._indexer = indexer
            if len(self._indexer) != len(self._index):
                raise ErrorInitYarn(f'Length of supplied indexer ({len(self._indexer)}) not of sufficient size ({len(self._index)}).') # pyright: ignore


    #---------------------------------------------------------------------------
    # deferred loading of axis info

    def unpersist(self) -> None:
        '''For the :obj:`Bus` contained in this object, replace all loaded :obj:`Frame` with :obj:`FrameDeferred`.
        '''
        for b in self._values:
            b.unpersist()

    #---------------------------------------------------------------------------
    def __reversed__(self) -> tp.Iterator[TLabel]:
        '''
        Returns a reverse iterator on the :obj:`Yarn` index.

        Returns:
            :obj:`Index`
        '''
        return reversed(self._index)

    #---------------------------------------------------------------------------
    # name interface

    @property
    @doc_inject()
    def name(self) -> TName:
        '''{}'''
        return self._name

    def rename(self, name: TName) -> tp.Self:
        '''
        Return a new :obj:`Yarn` with an updated name attribute.

        Args:
            name
        '''
        # NOTE: do not need to call _update_index_labels; can continue to defer
        return self.__class__(self._values,
                index=self._index,
                hierarchy=self._hierarchy,
                name=name,
                deepcopy_from_bus=self._deepcopy_from_bus,
                )

    #---------------------------------------------------------------------------
    # interfaces

    @property
    def loc(self) -> InterGetItemLocReduces[TYarnAny, np.object_]:
        return InterGetItemLocReduces(self._extract_loc) # type: ignore

    @property
    def iloc(self) -> InterGetItemILocReduces[TYarnAny, np.object_]:
        return InterGetItemILocReduces(self._extract_iloc) # type: ignore

    @property
    def drop(self) -> InterfaceSelectTrio[TYarnAny]:
        '''
        Interface for dropping elements from :obj:`Yarn`.
        '''
        return InterfaceSelectTrio( #type: ignore
                func_iloc=self._drop_iloc,
                func_loc=self._drop_loc,
                func_getitem=self._drop_loc
                )

    #---------------------------------------------------------------------------
    @property
    def iter_element(self) -> IterNodeNoArg[TYarnAny]:
        '''
        Iterator of elements.
        '''
        return IterNodeNoArg(
                container=self,
                function_items=self._axis_element_items,
                function_values=self._axis_element,
                yield_type=IterNodeType.VALUES,
                apply_type=IterNodeApplyType.SERIES_VALUES,
                )

    @property
    def iter_element_items(self) -> IterNodeNoArg[TYarnAny]:
        '''
        Iterator of label, element pairs.
        '''
        return IterNodeNoArg(
                container=self,
                function_items=self._axis_element_items,
                function_values=self._axis_element,
                yield_type=IterNodeType.ITEMS,
                apply_type=IterNodeApplyType.SERIES_VALUES,
                )


    #---------------------------------------------------------------------------
    # common attributes from the numpy array

    @property
    def dtype(self) -> TDtypeObject:
        '''
        Return the dtype of the realized NumPy array.

        Returns:
            :obj:`numpy.dtype`
        '''
        return DTYPE_OBJECT # always dtype object

    @property
    def shape(self) -> tp.Tuple[int]:
        '''
        Return a tuple describing the shape of the realized NumPy array.

        Returns:
            :obj:`Tuple[int]`
        '''
        return (self._hierarchy.shape[0],)

    @property
    def ndim(self) -> int:
        '''
        Return the number of dimensions, which for a :obj:`Yarn` is always 1.

        Returns:
            :obj:`int`
        '''
        return self._NDIM

    @property
    def size(self) -> int:
        '''
        Return the size of the underlying NumPy array.

        Returns:
            :obj:`int`
        '''
        return self._hierarchy.shape[0]

    #---------------------------------------------------------------------------

    @property
    def index(self) -> IndexBase:
        '''
        The index instance assigned to this container.

        Returns:
            :obj:`Index`
        '''
        return self._index

    #---------------------------------------------------------------------------
    # dictionary-like interface

    def keys(self) -> IndexBase:
        '''
        Iterator of index labels.

        Returns:
            :obj:`Iterator[Hashable]`
        '''
        return self._index

    def __iter__(self) -> tp.Iterator[TLabel]:
        '''
        Iterator of index labels, same as :obj:`static_frame.Series.keys`.

        Returns:
            :obj:`Iterator[Hashasble]`
        '''
        return self._index.__iter__()

    def __contains__(self, value: TLabel) -> bool:
        '''
        Inclusion of value in index labels.

        Returns:
            :obj:`bool`
        '''
        return self._index.__contains__(value)

    def get(self, key: TLabel,
            default: tp.Any = None,
            ) -> tp.Any:
        '''
        Return the value found at the index key, else the default if the key is not found.

        Returns:
            :obj:`Any`
        '''
        if key not in self._index:
            return default
        return self.__getitem__(key)

    def items(self) -> tp.Iterator[tp.Tuple[TLabel, TFrameAny]]:
        '''Iterator of pairs of :obj:`Yarn` label and contained :obj:`Frame`.
        '''
        labels = iter(self._index)
        for b_pos, frame_label in self._hierarchy._extract_iloc(self._indexer):
            yield next(labels), self._values[b_pos]._extract_loc(frame_label)


        # labels = iter(self._index)
        # for bus in self._values:
        #     # NOTE: cannot use Bus.items() as it may not have the same index representation as the Yarn; Bus._axis_element is optimized for handling max_persist > 1 loading
        #     for f in bus._axis_element():
        #         yield next(labels), f

    _items_store = items

    @property
    def values(self) -> TNDArrayAny:
        '''A 1D object array of all :obj:`Frame` contained in all contained :obj:`Bus`.
        '''
        array = np.empty(shape=len(self._index), dtype=DTYPE_OBJECT)

        for i, (b_pos, frame_label) in enumerate(
                self._hierarchy._extract_iloc(self._indexer)):
            array[i] = self._values[b_pos]._extract_loc(frame_label)

        array.flags.writeable = False
        return array

        # array = np.empty(shape=len(self._index), dtype=DTYPE_OBJECT)
        # np.concatenate([b.values for b in self._values], out=array)
        # array.flags.writeable = False
        # return array


    #---------------------------------------------------------------------------
    @doc_inject()
    def equals(self,
            other: tp.Any,
            *,
            compare_name: bool = False,
            compare_dtype: bool = False,
            compare_class: bool = False,
            skipna: bool = True,
            ) -> bool:
        '''
        {doc}

        Note: this will attempt to load and compare all Frame managed by the Bus.

        Args:
            {compare_name}
            {compare_dtype}
            {compare_class}
            {skipna}
        '''

        if id(other) == id(self):
            return True

        if compare_class and self.__class__ != other.__class__:
            return False
        elif not isinstance(other, Yarn):
            return False

        if compare_name and self._name != other._name:
            return False

        # length of series in Yarn might be different but may still have the same frames, so look at realized length
        if len(self) != len(other):
            return False

        if not self._index.equals(
                other.index, # call property to force index creation
                compare_name=compare_name,
                compare_dtype=compare_dtype,
                compare_class=compare_class,
                skipna=skipna,
                ):
            return False

        # can zip because length of Series already match
        # using .values will force loading all Frame into memory; better to use items() to permit collection
        for (_, frame_self), (_, frame_other) in zip(self.items(), other.items()):
            if not frame_self.equals(frame_other,
                    compare_name=compare_name,
                    compare_dtype=compare_dtype,
                    compare_class=compare_class,
                    skipna=skipna,
                    ):
                return False

        return True

    #---------------------------------------------------------------------------
    # transformations resulting in changed dimensionality

    @doc_inject(selector='head', class_name='Yarn')
    def head(self, count: int = 5) -> TYarnAny:
        '''{doc}

        Args:
            {count}

        Returns:
            :obj:`Yarn`
        '''
        return self.iloc[:count]

    @doc_inject(selector='tail', class_name='Yarn')
    def tail(self, count: int = 5) -> TYarnAny:
        '''{doc}s

        Args:
            {count}

        Returns:
            :obj:`Yarn`
        '''
        return self.iloc[-count:]


    #---------------------------------------------------------------------------
    # transformations resulting in the same dimensionality

    # @doc_inject(selector='sort')
    # def sort_index(self,
    #         *,
    #         ascending: TBoolOrBools = True,
    #         kind: TSortKinds = DEFAULT_SORT_KIND,
    #         key: tp.Optional[tp.Callable[[IndexBase], tp.Union[TNDArrayAny, IndexBase]]] = None,
    #         ) -> tp.Self:
    #     '''
    #     Return a new Bus ordered by the sorted Index.

    #     Args:
    #         *
    #         {ascendings}
    #         {kind}
    #         {key}

    #     Returns:
    #         :obj:`Bus`
    #     '''
    #     order = sort_index_for_order(self._index, kind=kind, ascending=ascending, key=key)

#        return self._derive_from_series(series, own_data=True)

    #---------------------------------------------------------------------------
    # extraction


# f1 0 (0, f1)
# f2 1 (0, f2)
# f3 2 (1, f3)
# f4 3 (1, f4)

# f3 2
# f1 0
# f4 3

    def _extract_iloc(self, key: TILocSelector) -> TYarnAny | TFrameAny:
        '''
        Returns:
            Yarn or, if an element is selected, a Frame
        '''
        indexer = self._indexer[key]

        if isinstance(indexer, INT_TYPES):
            # got a single element, return a Frame
            b_pos, frame_label = self._hierarchy._extract_iloc(indexer)
            return self._values[b_pos]._extract_loc(frame_label) # type: ignore


        # target_hierarchy = self._hierarchy._extract_iloc(indexer)
        # NOTE: we might need to prune the hierarchy if our selection on longer needs certain buses
        # get the outer-most index of the hierarchical index; we cannot use index_at_depth
        # target_bus_labels = target_hierarchy.unique(
        #         depth_level=0,
        #         order_by_occurrence=True)



        return self.__class__(self._values,
                index=self._index.iloc[key],
                deepcopy_from_bus=self._deepcopy_from_bus,
                hierarchy=self._hierarchy,
                indexer=indexer,
                name=self._name,
                own_index=True,
                )



    # def _extract_iloc(self, key: TILocSelector) -> TYarnAny | TFrameAny:
    #     '''
    #     Returns:
    #         Yarn or, if an element is selected, a Frame
    #     '''
    #     target_hierarchy = self._hierarchy._extract_iloc(key)
    #     if isinstance(target_hierarchy, tuple):
    #         # got a single element, return a Frame
    #         b_pos, frame_label = target_hierarchy
    #         return self._values[b_pos]._extract_loc(frame_label) # type: ignore

    #     # get the outer-most index of the hierarchical index; we cannot use index_at_depth
    #     target_bus_labels = target_hierarchy.unique(
    #             depth_level=0,
    #             order_by_occurrence=True)
    #     ctor = next(iter(target_hierarchy._index_constructors))
    #     target_bus_index = ctor(target_bus_labels)

    #     # create a Boolean array equal to the entire realized length
    #     valid = np.full(len(self._index), False)
    #     valid[key] = True

    #     buses = np.empty(len(target_bus_index), dtype=DTYPE_OBJECT)

    #     pos = 0
    #     for b_pos, width in self._hierarchy.label_widths_at_depth(0):
    #         if b_pos not in target_bus_index:
    #             pos += width
    #             continue
    #         # create Boolean selection within this Bus
    #         extract_per_bus = valid[pos: pos + width]
    #         pos += width
    #         # given original bus position, look-up the bus position in returned array
    #         idx = target_bus_index.loc_to_iloc(b_pos)
    #         buses[idx] = self._values[b_pos]._extract_iloc(extract_per_bus) # type: ignore

    #     buses.flags.writeable = False

    #     return self.__class__(buses,
    #             index=self._index.iloc[key],
    #             deepcopy_from_bus=self._deepcopy_from_bus,
    #             name=self._name,
    #             own_index=True,
    #             )


    def _extract_loc(self, key: TLocSelector) -> TYarnAny | TFrameAny:
        # use the index active for this Yarn
        key_iloc = self._index._loc_to_iloc(key)
        return self._extract_iloc(key_iloc)


    @doc_inject(selector='selector')
    def __getitem__(self, key: TLocSelector) -> TYarnAny | TFrameAny:
        '''Selector of values by label.

        Args:
            key: {key_loc}
        '''
        return self._extract_loc(key)

    #---------------------------------------------------------------------------
    # utilities for alternate extraction: drop

    def _drop_iloc(self, key: TILocSelector) -> tp.Self:
        invalid = np.full(len(self._index), True)
        invalid[key] = False
        return self._extract_iloc(invalid) # type: ignore

    def _drop_loc(self, key: TLocSelector) -> tp.Self:
        return self._drop_iloc(self._index._loc_to_iloc(key))

    #---------------------------------------------------------------------------
    # axis functions

    def _axis_element_items(self,
            ) -> tp.Iterator[tp.Tuple[TLabel, tp.Any]]:
        '''Generator of index, value pairs, equivalent to Series.items(). Repeated to have a common signature as other axis functions.
        '''
        yield from self.items()

    def _axis_element(self,
            ) -> tp.Iterator[tp.Any]:

        for bus in self._values:
            yield from bus._axis_element()

    #---------------------------------------------------------------------------
    def __len__(self) -> int:
        '''Length of values.
        '''
        return self._index.__len__()

    @doc_inject()
    def display(self,
            config: tp.Optional[DisplayConfig] = None,
            *,
            style_config: tp.Optional[StyleConfig] = None,
            ) -> Display:
        '''{doc}

        Args:
            {config}
        '''
        # NOTE: the key change over serires is providing the Bus as the displayed class
        config = config or DisplayActive.get()
        display_cls = Display.from_values((),
                header=DisplayHeader(self.__class__, self._name),
                config=config)

        # NOTE: do not load FrameDeferred, so concatenate contained Series's values directly
        # array = np.empty(shape=len(self._index), dtype=DTYPE_OBJECT)
        # np.concatenate(
        #     [b._values_mutable for b in self._values],
        #     out=array)
        # array.flags.writeable = False

        # create temporary series just for display
        series: TSeriesObject = Series(self.values, index=self._index, own_index=True)
        return series._display(config,
                display_cls=display_cls,
                style_config=style_config,
                )

    #---------------------------------------------------------------------------
    # extended discriptors; in general, these do not force loading Frame

    @property
    def mloc(self) -> TSeriesObject:
        '''Returns a :obj:`Series` showing a tuple of memory locations within each loaded Frame.
        '''
        # TODO: USE INDEXER
        return Series.from_concat((b.mloc for b in self._values),
                index=self._index)

    @property
    def dtypes(self) -> TFrameAny:
        '''Returns a Frame of dtypes for all loaded Frames.
        '''
        # TODO: USE INDEXER
        return Frame.from_concat(
                frames=(f.dtypes for f in self._values),
                fill_value=None,
                ).relabel(index=self._index)

    @property
    def shapes(self) -> TSeriesObject:
        '''A :obj:`Series` describing the shape of each loaded :obj:`Frame`. Unloaded :obj:`Frame` will have a shape of None.

        Returns:
            :obj:`tp.Series`
        '''
        # TODO: USE INDEXER

        return Series.from_concat((b.shapes for b in self._values),
                index=self._index)

    @property
    def nbytes(self) -> int:
        '''Total bytes of data currently loaded in :obj:`Frame` contained in this :obj:`Yarn`.
        '''
        # TODO: USE INDEXER
        return sum(b.nbytes for b in self._values)

    @property
    def status(self) -> TFrameAny:
        '''
        Return a :obj:`Frame` indicating loaded status, size, bytes, and shape of all loaded :obj:`Frame` in :obj:`Bus` contined in this :obj:`Yarn`.
        '''
        # TODO: USE INDEXER

        f: TFrameAny = Frame.from_concat(
                (b.status for b in self._values),
                index=IndexAutoFactory)
        return f.relabel(index=self._index)


    #---------------------------------------------------------------------------
    # exporter

    def to_series(self) -> TSeriesObject: # can get generic Bus index
        '''Return a :obj:`Series` with the :obj:`Frame` contained in all contained :obj:`Bus`.
        '''
        # NOTE: this should load all deferred Frame
        return Series(self.values, index=self._index, own_index=True)

    def _to_signature_bytes(self,
            include_name: bool = True,
            include_class: bool = True,
            encoding: str = 'utf-8',
            ) -> bytes:

        v = (f._to_signature_bytes(
                include_name=include_name,
                include_class=include_class,
                encoding=encoding,
                ) for f in self._axis_element())

        return b''.join(chain(
                iter_component_signature_bytes(self,
                        include_name=include_name,
                        include_class=include_class,
                        encoding=encoding),
                (self._index._to_signature_bytes(
                        include_name=include_name,
                        include_class=include_class,
                        encoding=encoding),
                self._hierarchy._to_signature_bytes(
                        include_name=include_name,
                        include_class=include_class,
                        encoding=encoding),),
                v))




    #---------------------------------------------------------------------------
    # index manipulation

    @doc_inject(selector='relabel', class_name='Yarn')
    def relabel(self,
            index: tp.Optional[TRelabelInput]
            ) -> tp.Self:
        '''
        {doc}

        Args:
            index: {relabel_input_index}
        '''
        #NOTE: we name the parameter index for alignment with the corresponding Frame method
        own_index = False
        if index is IndexAutoFactory:
            index_init = None
        elif index is None:
            index_init = self._index
        elif is_callable_or_mapping(index):
            index_init = self._index.relabel(index)
            own_index = True
        elif isinstance(index, Set):
            raise RelabelInvalid()
        else:
            index_init = index #type: ignore

        return self.__class__(self._values, # no change to Buses
                index=index_init, # pyright: ignore
                deepcopy_from_bus=self._deepcopy_from_bus,
                hierarchy=self._hierarchy, # no change
                own_index=own_index,
                )

    @doc_inject(selector='relabel_flat', class_name='Yarn')
    def relabel_flat(self) -> tp.Self:
        '''
        {doc}
        '''
        if not isinstance(self._index, IndexHierarchy):
            raise RuntimeError('cannot flatten an Index that is not an IndexHierarchy')

        return self.__class__(self._values, # no change to Buses
                index=self._index.flat(),
                deepcopy_from_bus=self._deepcopy_from_bus,
                hierarchy=self._hierarchy, # no change
                own_index=True,
                )

    @doc_inject(selector='relabel_level_add', class_name='Yarn')
    def relabel_level_add(self,
            level: TLabel
            ) -> tp.Self:
        '''
        {doc}

        Args:
            level: {level}
        '''
        return self.__class__(self._values, # no change to Buses
                index=self._index.level_add(level),
                deepcopy_from_bus=self._deepcopy_from_bus,
                hierarchy=self._hierarchy, # no change
                own_index=True,
                )

    @doc_inject(selector='relabel_level_drop', class_name='Yarn')
    def relabel_level_drop(self,
            count: int = 1
            ) -> tp.Self:
        '''
        {doc}

        Args:
            count: {count}
        '''
        if not isinstance(self._index, IndexHierarchy):
            raise RuntimeError('cannot drop level of an Index that is not an IndexHierarchy')

        return self.__class__(self._values, # no change to Buses
                index=self._index.level_drop(count),
                deepcopy_from_bus=self._deepcopy_from_bus,
                hierarchy=self._hierarchy, # no change
                own_index=True,
                )

    def rehierarch(self,
            depth_map: tp.Sequence[int],
            *,
            index_constructors: TIndexCtorSpecifiers = None,
            ) -> tp.Self:
        '''
        Return a new :obj:`Series` with new a hierarchy based on the supplied ``depth_map``.
        '''
        if self.index.depth == 1:
            raise RuntimeError('cannot rehierarch when there is no hierarchy')

        index, iloc_map = rehierarch_from_index_hierarchy(
                labels=self._index, #type: ignore
                depth_map=depth_map,
                index_constructors=index_constructors,
                name=self._index.name,
                )

        return self._extract_iloc(iloc_map).relabel(index) # type: ignore


TYarnAny = Yarn[tp.Any]



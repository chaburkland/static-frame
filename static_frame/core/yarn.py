import typing as tp
# from itertools import zip_longest
# from functools import partial
# from copy import deepcopy

import numpy as np

from static_frame.core.frame import Frame
from static_frame.core.bus import Bus
from static_frame.core.container import ContainerBase
# from static_frame.core.container_util import axis_window_items
from static_frame.core.display import Display
from static_frame.core.display import DisplayActive
from static_frame.core.display import DisplayHeader
from static_frame.core.display_config import DisplayConfig
from static_frame.core.doc_str import doc_inject
# from static_frame.core.exception import AxisInvalid
from static_frame.core.exception import ErrorInitYarn
# from static_frame.core.exception import NotImplementedAxis
# from static_frame.core.frame import Frame
# from static_frame.core.hloc import HLoc
from static_frame.core.index_base import IndexBase
from static_frame.core.index_hierarchy import IndexHierarchy
# from static_frame.core.node_iter import IterNodeAxis
# from static_frame.core.node_iter import IterNodeConstructorAxis
# from static_frame.core.node_iter import IterNodeType
# from static_frame.core.node_iter import IterNodeWindow
# from static_frame.core.node_iter import IterNodeApplyType
# from static_frame.core.node_selector import InterfaceGetItem
from static_frame.core.util import IndexInitializer

from static_frame.core.series import Series
# from static_frame.core.store import Store
# from static_frame.core.store import StoreConfigMapInitializer
from static_frame.core.store_client_mixin import StoreClientMixin
# from static_frame.core.store_hdf5 import StoreHDF5
# from static_frame.core.store_sqlite import StoreSQLite
# from static_frame.core.store_xlsx import StoreXLSX
# from static_frame.core.store_zip import StoreZipCSV
# from static_frame.core.store_zip import StoreZipParquet
# from static_frame.core.store_zip import StoreZipPickle
# from static_frame.core.store_zip import StoreZipTSV
# from static_frame.core.util import AnyCallable
# from static_frame.core.util import array_deepcopy
# from static_frame.core.util import duplicate_filter
# from static_frame.core.util import get_tuple_constructor
from static_frame.core.util import GetItemKeyType
# from static_frame.core.util import GetItemKeyTypeCompound
# from static_frame.core.util import INT_TYPES
from static_frame.core.util import NameType
# from static_frame.core.util import NULL_SLICE
# from static_frame.core.util import PathSpecifier
from static_frame.core.util import DTYPE_OBJECT
from static_frame.core.util import NAME_DEFAULT

# from static_frame.core.util import concat_resolved
from static_frame.core.style_config import StyleConfig
from static_frame.core.axis_map import buses_to_hierarchy
from static_frame.core.index_auto import IndexAutoFactoryType
from static_frame.core.index_auto import IndexAutoFactory
from static_frame.core.node_selector import InterfaceGetItem


class Yarn(ContainerBase, StoreClientMixin):
    '''
    A :obj:`Series`-like container of ordered collections of :obj:`Bus`. If the labels of the index are unique accross all contained :obj:`Bus`, ``retain_labels`` can be set to ``False`` and underlying labels are simply concatenated; otherwise, ``retain_labels`` must be set to ``True`` and an additional depth-level is added to the index labels.
    '''

    __slots__ = (
            '_series',
            '_hierarchy',
            '_retain_labels',
            '_assign_index',
            '_index',
            '_deepcopy_from_bus',
            )

    _series: Series
    _hierarchy: tp.Optional[Series]
    _index: IndexBase
    _assign_index: bool

    _NDIM: int = 1

    @classmethod
    def from_buses(cls,
            buses: tp.Iterable[Bus],
            *,
            name: NameType = None,
            retain_labels: bool,
            deepcopy_from_bus: bool = False,
            ) -> 'Yarn':
        '''Return a :obj:`Yarn` from an iterable of :obj:`Bus`; labels will be drawn from :obj:`Bus.name`.
        '''
        series = Series.from_items(
                    ((b.name, b) for b in buses),
                    dtype=DTYPE_OBJECT,
                    name=name,
                    )
        return cls(series,
                retain_labels=retain_labels,
                deepcopy_from_bus=deepcopy_from_bus,
                )

    @classmethod
    def from_concat(cls,
            containers: tp.Iterable['Bus'],
            *,
            index: tp.Optional[tp.Union[IndexInitializer, IndexAutoFactoryType]] = None,
            name: NameType = NAME_DEFAULT,
            retain_labels: bool,
            deepcopy_from_bus: bool = False,
            ) -> 'Bus':
        '''
        Concatenate multiple :obj:`Bus` into a new :obj:`Yarn`.

        Args:
            index: Optionally provide new labels for each Bus. This is not resultant index on the :obj:`Yarn`.
        '''
        # will extract .values, .index from Bus, which will correct load from Store as needed
        series = Series.from_concat(containers, index=index, name=name)
        return cls(series,
                deepcopy_from_bus=deepcopy_from_bus,
                retain_labels=retain_labels,
                )


    #---------------------------------------------------------------------------
    def __init__(self,
            series: Series,
            *,
            retain_labels: bool,
            hierarchy: tp.Optional[IndexHierarchy] = None,
            deepcopy_from_bus: bool = False,
            ) -> None:
        '''
        Args:
            series: A :obj:`Series` of :obj:`Bus`.
        '''
        if series.dtype != DTYPE_OBJECT:
            raise ErrorInitYarn(
                    f'Series passed to initializer must have dtype object, not {series.dtype}')

        self._series = series # Bus by Bus label

        self._retain_labels = retain_labels
        self._hierarchy = hierarchy # pass in delegation moves
        #self._index assigned in _update_index_labels()
        self._deepcopy_from_bus = deepcopy_from_bus

        self._assign_index = True # Boolean to control deferred index creation

    #---------------------------------------------------------------------------
    # deferred loading of axis info

    def _update_index_labels(self) -> None:
        # _hierarchy might be None while we still need to set self._index
        if self._hierarchy is None:
            self._hierarchy = buses_to_hierarchy(
                    self._series.values,
                    deepcopy_from_bus=self._deepcopy_from_bus,
                    init_exception_cls=ErrorInitYarn,
                    )

        if not self._retain_labels:
            self._index = self._hierarchy.level_drop(1) #type: ignore
        else: # get hierarchical
            self._index = self._hierarchy

        self._assign_index = False

    #---------------------------------------------------------------------------
    def __reversed__(self) -> tp.Iterator[tp.Hashable]:
        '''
        Returns a reverse iterator on the :obj:`Yarn` index.

        Returns:
            :obj:`Index`
        '''
        return reversed(self._series._index) #type: ignore

    #---------------------------------------------------------------------------
    # name interface

    @property #type: ignore
    @doc_inject()
    def name(self) -> NameType:
        '''{}'''
        return self._series._name

    def rename(self, name: NameType) -> 'Yarn':
        '''
        Return a new :obj:`Yarn` with an updated name attribute.

        Args:
            name
        '''
        # NOTE: do not need to call _update_index_labels; can continue to defer
        series = self._series.rename(name)
        return self.__class__(series,
                retain_labels=self._retain_labels,
                hierarchy=self._hierarchy,
                deepcopy_from_bus=self._deepcopy_from_bus,
                )

    #---------------------------------------------------------------------------
    # interfaces

    @property
    def loc(self) -> InterfaceGetItem['Yarn']:
        return InterfaceGetItem(self._extract_loc)

    @property
    def iloc(self) -> InterfaceGetItem['Yarn']:
        return InterfaceGetItem(self._extract_iloc)

    # @property
    # def drop(self) -> InterfaceSelectTrio['Yarn']:
    #     '''
    #     Interface for dropping elements from :obj:`Yarn`.
    #     '''
    #     return InterfaceSelectTrio( #type: ignore
    #             func_iloc=self._drop_iloc,
    #             func_loc=self._drop_loc,
    #             func_getitem=self._drop_loc
    #             )

    #---------------------------------------------------------------------------
    # @property
    # def iter_element(self) -> IterNodeNoArg['Bus']:
    #     '''
    #     Iterator of elements.
    #     '''
    #     return IterNodeNoArg(
    #             container=self,
    #             function_items=self._axis_element_items,
    #             function_values=self._axis_element,
    #             yield_type=IterNodeType.VALUES,
    #             apply_type=IterNodeApplyType.SERIES_VALUES,
    #             )

    # @property
    # def iter_element_items(self) -> IterNodeNoArg['Bus']:
    #     '''
    #     Iterator of label, element pairs.
    #     '''
    #     return IterNodeNoArg(
    #             container=self,
    #             function_items=self._axis_element_items,
    #             function_values=self._axis_element,
    #             yield_type=IterNodeType.ITEMS,
    #             apply_type=IterNodeApplyType.SERIES_VALUES,
    #             )


    #---------------------------------------------------------------------------
    # common attributes from the numpy array

    @property
    def dtype(self) -> np.dtype:
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
        if self._assign_index:
            self._update_index_labels()
        return (self._hierarchy.shape[0],) #type: ignore

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
        if self._assign_index:
            self._update_index_labels()
        return self._hierarchy.shape[0] #type: ignore

    #---------------------------------------------------------------------------

    @property
    def index(self) -> IndexBase:
        '''
        The index instance assigned to this container.

        Returns:
            :obj:`Index`
        '''
        if self._assign_index:
            self._update_index_labels()
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

    def __iter__(self) -> tp.Iterator[tp.Hashable]:
        '''
        Iterator of index labels, same as :obj:`static_frame.Series.keys`.

        Returns:
            :obj:`Iterator[Hashasble]`
        '''
        return self._index.__iter__()

    def __contains__(self, value: tp.Hashable) -> bool:
        '''
        Inclusion of value in index labels.

        Returns:
            :obj:`bool`
        '''
        return self._index.__contains__(value)

    def get(self, key: tp.Hashable,
            default: tp.Any = None,
            ) -> tp.Any:
        '''
        Return the value found at the index key, else the default if the key is not found.

        Returns:
            :obj:`Any`
        '''
        if key not in self._series._index:
            return default
        return self.__getitem__(key)

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
        raise NotImplementedError()

    #---------------------------------------------------------------------------
    # transformations resulting in changed dimensionality

    @doc_inject(selector='head', class_name='Yarn')
    def head(self, count: int = 5) -> 'Yarn':
        '''{doc}

        Args:
            {count}

        Returns:
            :obj:`Yarn`
        '''
        return self.iloc[:count]

    @doc_inject(selector='tail', class_name='Yarn')
    def tail(self, count: int = 5) -> 'Yarn':
        '''{doc}s

        Args:
            {count}

        Returns:
            :obj:`Yarn`
        '''
        return self.iloc[-count:]


    #---------------------------------------------------------------------------
    # extraction

    def _extract_iloc(self, key: GetItemKeyType) -> 'Yarn':
        '''
        Returns:
            Yarn or, if an element is selected, a Frame
        '''
        if self._assign_index:
            self._update_index_labels()

        target_hierarchy = self._hierarchy._extract_iloc(key)
        if isinstance(target_hierarchy, tuple):
            # got a single element, return a Frame
            return self._series[target_hierarchy[0]][target_hierarchy[1]]

        # get the outer-most index of the hierarchical index
        target_bus_index = target_hierarchy._levels.index

        # do avoid having to do a group by or other selection on the targetted bus, we create a Boolean array equal to the entire realized lengt.
        valid = np.full(len(self._index), False)
        valid[key] = True

        buses = np.empty(len(target_bus_index), dtype=DTYPE_OBJECT)
        # must run accross all labels to get incremental slices of Boolean array, but maybe there is a way to avoid
        pos = 0
        for bus_label, width in self._hierarchy.label_widths_at_depth(0):
            # this should always be a bus
            if bus_label not in target_bus_index:
                pos += width
                continue
            extract_per_bus = valid[pos: pos+width]
            pos += width

            idx = target_bus_index.loc_to_iloc(bus_label)
            buses[idx] = self._series[bus_label]._extract_iloc(extract_per_bus)

        buses.flags.writeable = False
        target_series = Series(buses,
                index=target_bus_index,
                own_index=True,
                name=self._series._name,
                )

        return self.__class__(target_series,
                retain_labels=self._retain_labels,
                hierarchy=target_hierarchy,
                deepcopy_from_bus=self._deepcopy_from_bus,
                )

    def _extract_loc(self, key: GetItemKeyType) -> 'Yarn':

        if self._assign_index:
            self._update_index_labels()

        # use the index active for this Yarn
        key_iloc = self._index._loc_to_iloc(key)
        return self._extract_iloc(key_iloc)


    @doc_inject(selector='selector')
    def __getitem__(self, key: GetItemKeyType) -> 'Yarn':
        '''Selector of values by label.

        Args:
            key: {key_loc}
        '''
        return self._extract_loc(key)

    #---------------------------------------------------------------------------
    # utilities for alternate extraction: drop

    # def _drop_iloc(self, key: GetItemKeyType) -> 'Bus':
    #     series = self._series._drop_iloc(key)
    #     return self._derive(series)

    # def _drop_loc(self, key: GetItemKeyType) -> 'Bus':
    #     return self._drop_iloc(self._series._index._loc_to_iloc(key))

    #---------------------------------------------------------------------------
    # axis functions

    # def _axis_element_items(self,
    #         ) -> tp.Iterator[tp.Tuple[tp.Hashable, tp.Any]]:
    #     '''Generator of index, value pairs, equivalent to Series.items(). Repeated to have a common signature as other axis functions.
    #     '''
    #     yield from zip(self._series._index, self._series.values)

    # def _axis_element(self,
    #         ) -> tp.Iterator[tp.Any]:
    #     yield from self._series.values

    #---------------------------------------------------------------------------
    # dictionary-like interface; these will force loadings contained Frame

    def items(self) -> tp.Iterator[tp.Tuple[tp.Hashable, Frame]]:
        '''Iterator of pairs of :obj:`Yarn` label and contained :obj:`Frame`.
        '''
        labels = iter(self._index)
        for bus in self._series.values:
            # NOTE: cannot use Bus.items() as it may not have the right index; cannot use Bus.values, as that will load all Frames at once
            for i in range(len(bus)):
                yield next(labels), bus._extract_iloc(i)

    _items_store = items

    @property
    def values(self) -> np.ndarray:
        '''A 1D object array of all :obj:`Frame` contained in all contained :obj:`Bus`.
        '''
        array = np.empty(shape=len(self._index), dtype=DTYPE_OBJECT)
        np.concatenate([b.values for b in self._series.values], out=array)
        array.flags.writeable = False
        return array

    #---------------------------------------------------------------------------
    def __len__(self) -> int:
        '''Length of values.
        '''
        if self._assign_index:
            self._update_index_labels()
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
        if self._assign_index:
            self._update_index_labels()

        # NOTE: the key change over serires is providing the Bus as the displayed class
        config = config or DisplayActive.get()
        display_cls = Display.from_values((),
                header=DisplayHeader(self.__class__, self._series._name),
                config=config)

        array = np.empty(shape=len(self._index), dtype=DTYPE_OBJECT)
        # NOTE: do not load FrameDeferred, so concate contained Series's values directly
        np.concatenate([b._series.values for b in self._series.values], out=array)
        array.flags.writeable = False
        series = Series(array, index=self._index, own_index=True)

        return series._display(config,

                display_cls=display_cls,
                style_config=style_config,
                )

    #---------------------------------------------------------------------------
    # extended discriptors; in general, these do not force loading Frame

    @property
    def mloc(self) -> Series:
        '''Returns a :obj:`Series` showing a tuple of memory locations within each loaded Frame.
        '''
        if self._assign_index:
            self._update_index_labels()

        return Series.from_concat((b.mloc for b in self._series.values),
                index=self._index)

    @property
    def dtypes(self) -> Frame:
        '''Returns a Frame of dtypes for all loaded Frames.
        '''
        if self._assign_index:
            self._update_index_labels()

        f = Frame.from_concat(
                frames=(f.dtypes for f in self._series.values),
                fill_value=None,
                ).relabel(index=self._index)
        return tp.cast(Frame, f)

    @property
    def shapes(self) -> Series:
        '''A :obj:`Series` describing the shape of each loaded :obj:`Frame`. Unloaded :obj:`Frame` will have a shape of None.

        Returns:
            :obj:`tp.Series`
        '''
        if self._assign_index:
            self._update_index_labels()

        return Series.from_concat((b.shapes for b in self._series.values),
                index=self._index)

    @property
    def nbytes(self) -> int:
        '''Total bytes of data currently loaded in :obj:`Bus` contained in this :obj:`Yarn`.
        '''
        return sum(b.nbytes for b in self._series.values)

    @property
    def status(self) -> Frame:
        '''
        Return a :obj:`Frame` indicating loaded status, size, bytes, and shape of all loaded :obj:`Frame` in :obj:`Bus` contined in this :obj:`Yarn`.
        '''
        if self._assign_index:
            self._update_index_labels()

        f = Frame.from_concat(
                (b.status for b in self._series.values),
                index=IndexAutoFactory)
        return f.relabel(index=self._index)


    #---------------------------------------------------------------------------
    # exporter

    def to_series(self) -> Series:
        '''Return a :obj:`Series` with the :obj:`Frame` contained in all contained :obj:`Bus`.
        '''
        if self._assign_index:
            self._update_index_labels()

        # NOTE: this should load all deferred Frame
        return Series(self.values, index=self._index, own_index=True)

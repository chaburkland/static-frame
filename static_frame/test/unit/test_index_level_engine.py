import unittest
import typing as tp
from copy import deepcopy
from types import SimpleNamespace

import numpy as np

from static_frame import IndexHierarchy
from static_frame.core.exception import ErrorInitIndexNonUnique
from static_frame.core.index import Index
from static_frame.core.index_level_engine import IndexLevelEngine
from static_frame.core.index_hierarchy import build_indexers_from_product
from static_frame.core.util import PositionsAllocator
from static_frame.test.test_case import TestCase


class TestUnit(TestCase):

    #---------------------------------------------------------------------------

    def test_init_a(self) -> None:
        indices = [
                Index(np.arange(5)),
                Index(tuple("ABCDE")),
                ]
        indexers = [
                np.array([3, 3, 0, 1, 4, 0, 3, 2, 2, 0]),
                np.array([4, 2, 1, 0, 3, 0, 3, 2, 0, 4]),
                ]

        engine = IndexLevelEngine(indices=indices, indexers=indexers)

        self.assertListEqual(list(engine.encoded_indexer_map), [35, 19, 8, 1, 28, 0, 27, 18, 2, 32])
        self.assertFalse(engine.encoding_can_overflow)
        self.assertListEqual(engine.bit_offset_encoders.tolist(), [0, 3])

    def test_init_b(self) -> None:
        indices = [Index(()) for _ in range(4)]
        indexers = [np.array(()) for _ in range(4)]

        engine = IndexLevelEngine(indices=indices, indexers=indexers)

        self.assertListEqual(list(engine.encoded_indexer_map), [])
        self.assertFalse(engine.encoding_can_overflow)
        self.assertListEqual(engine.bit_offset_encoders.tolist(), [0, 0, 0, 0])

    def test_init_c(self) -> None:
        indices = [Index((0, 1)), Index((0, 1))]
        indexers = [
                np.array([0, 0, 1, 1, 1]),
                np.array([0, 1, 0, 1, 1]),
                ]

        with self.assertRaises(ErrorInitIndexNonUnique):
            IndexLevelEngine(indices=indices, indexers=indexers)

    #---------------------------------------------------------------------------

    def test_build_offsets_and_overflow_a(self) -> None:
        def check(sizes: tp.List[int], offsets: tp.List[int], overflow: bool) -> None:
            actual_offset, actual_overflow = IndexLevelEngine.build_offsets_and_overflow(sizes)
            self.assertListEqual(actual_offset.tolist(), offsets)
            self.assertEqual(actual_overflow, overflow)

        check([17, 99], [0, 5], False)
        check([1, 1], [0, 1], False)
        check([1, 2, 4, 8, 16, 32], [0, 1, 3, 6, 10, 15], False)
        check([2**30, 2, 3, 4], [0, 31, 33, 35], False)
        check([2**40, 2**18, 15], [0, 41, 60], False)
        check([2**40, 2**18, 16], [0, 41, 60], True)

    #---------------------------------------------------------------------------

    def test_build_encoded_indexers_map_a(self) -> None:
        sizes = [188, 5, 77]
        indexers = build_indexers_from_product(sizes)

        offset, overflow = IndexLevelEngine.build_offsets_and_overflow(sizes)

        self.assertListEqual(offset.tolist(), [0, 8, 11])
        self.assertFalse(overflow)

        engine = SimpleNamespace(
                encoding_can_overflow=overflow,
                bit_offset_encoders=offset,
                )
        result = IndexLevelEngine.build_encoded_indexers_map(self=engine, indexers=indexers) # type: ignore
        self.assertEqual(len(result), len(indexers[0]))

        self.assertEqual(min(result), 0)
        self.assertEqual(max(result), 156859)

        # Manually check every element to ensure it encodes to the same value
        for i, row in enumerate(np.array(indexers).T):
            encoded = np.bitwise_or.reduce(row.astype(np.uint64) << offset)
            self.assertEqual(i, result[encoded])

    def test_build_encoded_indexers_map_b(self) -> None:
        size = 2**20
        sizes = [size for _ in range(4)]

        arr = PositionsAllocator.get(size)
        indexers = [arr for _ in range(4)]

        offset, overflow = IndexLevelEngine.build_offsets_and_overflow(sizes)

        self.assertListEqual(offset.tolist(), [0, 21, 42, 63])
        self.assertTrue(overflow)

        engine = SimpleNamespace(
                encoding_can_overflow=overflow,
                bit_offset_encoders=offset,
                )
        result = IndexLevelEngine.build_encoded_indexers_map(self=engine, indexers=indexers) # type: ignore
        self.assertEqual(len(result), len(indexers[0]))

        self.assertEqual(min(result), 0)
        self.assertEqual(max(result), 9671401945228815945957375)

        # Manually encode the last row to ensure it matches!
        indexer = np.array([size - 1 for _ in range(4)], dtype=object)
        encoded = np.bitwise_or.reduce(indexer << offset)
        self.assertEqual(max(result), encoded)

    #---------------------------------------------------------------------------

    def test_build_key_indexers_from_key_a(self) -> None:
        ih = IndexHierarchy.from_product(range(3), range(4, 7), tuple("ABC"))

        engineA = ih._engine
        engineB = deepcopy(ih._engine)
        engineB.encoding_can_overflow = True

        def check(
                key: tuple, # type: ignore
                expected: tp.List[tp.List[int]],
                ) -> None:
            resultA = engineA.build_key_indexers(key, indices=ih._indices)
            self.assertEqual(resultA.dtype, np.uint64)
            self.assertListEqual(resultA.tolist(), expected)

            resultB = engineB.build_key_indexers(key, indices=ih._indices)
            self.assertEqual(resultB.dtype, object)
            self.assertListEqual(resultB.tolist(), expected)

        check((0, 5, 'A'), [0, 1, 0]) # type: ignore
        check((0, 5, ['A']), [[0, 1, 0]])
        check(([0, 1],  5, ['B']), [[0, 1, 1],
                                    [1, 1, 1]])
        check(([0, 1], 5, 'A'), [[0, 1, 0],
                                 [1, 1, 0]])
        check(([0, 1], [4, 5, 6], 'C'), [[0, 0, 2],
                                         [0, 1, 2],
                                         [0, 2, 2],
                                         [1, 0, 2],
                                         [1, 1, 2],
                                         [1, 2, 2]])

    #---------------------------------------------------------------------------

    def test_is_single_element_a(self) -> None:
        self.assertTrue(IndexLevelEngine.is_single_element(None))
        self.assertTrue(IndexLevelEngine.is_single_element(True))
        self.assertTrue(IndexLevelEngine.is_single_element(123))
        self.assertTrue(IndexLevelEngine.is_single_element(1.0023))
        self.assertTrue(IndexLevelEngine.is_single_element(np.nan))

        self.assertFalse(IndexLevelEngine.is_single_element([False]))
        self.assertFalse(IndexLevelEngine.is_single_element([2.3, 8878.33]))
        self.assertFalse(IndexLevelEngine.is_single_element(()))
        self.assertFalse(IndexLevelEngine.is_single_element(np.arange(5)))

    #---------------------------------------------------------------------------

    def test_loc_to_iloc_a(self) -> None:
        indices = [
                Index(np.arange(5)),
                Index(tuple("ABCDE")),
                ]
        indexers = [
                np.array([3, 3, 0, 1, 4, 0, 3, 2, 2, 0]),
                np.array([4, 2, 1, 0, 3, 0, 3, 2, 0, 4]),
                ]

        engine = IndexLevelEngine(indices=indices, indexers=indexers)

        self.assertEqual(engine.loc_to_iloc((2, 'A'), indices), 8)
        self.assertEqual(engine.loc_to_iloc((2, ['A']), indices), [8])
        self.assertEqual(engine.loc_to_iloc(([2], 'A'), indices), [8])
        self.assertEqual(engine.loc_to_iloc(([2], ['A']), indices), [8])

        self.assertEqual(engine.loc_to_iloc(([0, 3], 'E'), indices), [9, 0])
        self.assertEqual(engine.loc_to_iloc(([0, 3], ['E']), indices), [9, 0])
        self.assertEqual(engine.loc_to_iloc(([3, 0], 'E'), indices), [0, 9])
        self.assertEqual(engine.loc_to_iloc(([3, 0], ['E']), indices), [0, 9])

        self.assertEqual(engine.loc_to_iloc(np.array([0, 'E'], dtype=object), indices), 9)

    def test_loc_to_iloc_b(self) -> None:
        indices = [
                Index(np.arange(5)),
                Index(tuple("ABCDE")),
                ]
        indexers = [
                np.array([3, 3, 0, 1, 4, 0, 3, 2, 2, 0]),
                np.array([4, 2, 1, 0, 3, 0, 3, 2, 0, 4]),
                ]

        engine = IndexLevelEngine(indices=indices, indexers=indexers)

        with self.assertRaises(KeyError):
            engine.loc_to_iloc((5, 'A'), indices)

        with self.assertRaises(KeyError):
            engine.loc_to_iloc((2, ['E']), indices)

        with self.assertRaises(KeyError):
            engine.loc_to_iloc(([0, 1, 2], ['A', 'B', 'C']), indices)

    #---------------------------------------------------------------------------

    def test_nbytes_a(self) -> None:
        indices = [
                Index(np.arange(5)),
                Index(tuple("ABCDE")),
                ]
        indexers = [
                np.array([3, 3, 0, 1, 4, 0, 3, 2, 2, 0]),
                np.array([4, 2, 1, 0, 3, 0, 3, 2, 0, 4]),
                ]

        engine = IndexLevelEngine(indices=indices, indexers=indexers)

        self.assertEqual(engine.nbytes, 720 + 8 + 8 + 25) # automap + 2 uint64 bit offsets + PyBool

    #---------------------------------------------------------------------------

    def test_deepcopy_a(self) -> None:
        indices = [
                Index(np.arange(5)),
                Index(tuple("ABCDE")),
                ]
        indexers = [
                np.array([3, 3, 0, 1, 4, 0, 3, 2, 2, 0]),
                np.array([4, 2, 1, 0, 3, 0, 3, 2, 0, 4]),
                ]

        engine = IndexLevelEngine(indices=indices, indexers=indexers)

        engine_copy = deepcopy(engine)

        self.assertEqual(engine.encoding_can_overflow, engine_copy.encoding_can_overflow)
        self.assertListEqual(engine.bit_offset_encoders.tolist(), engine_copy.bit_offset_encoders.tolist())
        self.assertEqual(engine.encoded_indexer_map, engine_copy.encoded_indexer_map)

        self.assertNotEqual(id(engine.bit_offset_encoders), id(engine_copy.bit_offset_encoders))
        self.assertNotEqual(id(engine.encoded_indexer_map), id(engine_copy.encoded_indexer_map))


if __name__ == '__main__':
    unittest.main()

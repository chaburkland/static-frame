import typing as tp
import numpy as np
import static_frame as sf



from static_frame.performance.perf_test import PerfTest

class SampleData:

    _store: tp.Dict[str, tp.Any] = {}


    @classmethod
    def create(cls) -> None:


        size_map = { 5:  2_000,
                     6:  3_500,
                     7:  1_000,
                     8: 2_000,
                     9: 5_000,
                    10: 7_625,
                    15:  5_000,
                    20: 5_000,
                    25:  3_250,
                    50:  2_625}

        groups = np.arange(sum(size_map.values()))

        group_sizes = []
        for k, v in size_map.items():
            group_sizes.extend([k] * v)

        def gen_group_values() -> tp.Iterator[np.ndarray]:
            for group in groups:
                yield np.full(group_sizes[group], group)

        def gen_group_unique_values() -> tp.Iterator[np.ndarray]:
            for group in groups:
                yield np.arange(group_sizes[group])

        group_values = sf.Series(np.concatenate(list(gen_group_values())), name='group')
        group_unique_values = sf.Series(np.concatenate(list(gen_group_unique_values())), name='group_unique')
        value_values = sf.Series(np.random.random(len(group_values)), name='data')
        frame = sf.Frame.from_concat((group_values, group_unique_values, value_values), axis=1)

        cls._store['pivot_a'] = frame

    @classmethod
    def get(cls, key: str) -> tp.Any:
        return cls._store[key]



class Pivot(PerfTest):

    NUMBER = 1

    @classmethod
    def sf(cls) -> None:
        f1 = SampleData.get('pivot_a')
        f2 = f1.pivot(index_fields='group_unique', columns_fields='group', data_fields='data')

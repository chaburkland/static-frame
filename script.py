import typing as tp
import frame_fixtures as ff
import pickle
import numpy as np
import static_frame as sf


with open("/home/burkland/.cached_objects/quilt_results_obj.pkl", "rb") as f:
    results_from_default = pickle.load(f)


def gen_results() -> tp.Iterator[sf.Quilt]:
    f1 = ff.parse("s(4,7)|v(int,float)|c(I,str)").rename("f1")
    f2 = ff.parse("s(4,7)|v(str)|c(I,str)").rename("f2")
    f3 = ff.parse("s(4,7)|v(bool)|c(I,str)").rename("f3")

    for axis in (0, 1):
        q = sf.Quilt.from_frames((f1, f2, f3), include_index=False, axis=axis)
        q._update_axis_labels()

        # yield results_from_default[(18*axis) + 0]
        yield results_from_default[(18*axis) + 1]
        yield results_from_default[(18*axis) + 2]
        yield results_from_default[(18*axis) + 3]
        yield results_from_default[(18*axis) + 4]
        yield results_from_default[(18*axis) + 5]
        yield results_from_default[(18*axis) + 6]
        yield results_from_default[(18*axis) + 7]
        yield results_from_default[(18*axis) + 8]
        yield results_from_default[(18*axis) + 9]
        yield results_from_default[(18*axis) + 10]

        yield q._extract(None)  #             1
        #yield q._extract(0)  #                2
        #yield q._extract([0])  #              3
        #yield q._extract(None, 0)  #          4
        #yield q._extract(None, [0])  #        5
        #yield q._extract(0, 0)  #             6
        #yield q._extract(0, [0])  #           7
        #yield q._extract([0], 0)  #           8
        #yield q._extract([0], [0])  #         9
        #yield q._extract_array(None)  #      10
        #yield q._extract_array(0)  #         11
        yield q._extract_array([0])  #       12
        yield q._extract_array(None, 0)  #   13
        yield q._extract_array(None, [0])  # 14
        yield q._extract_array(0, 0)  #      15
        yield q._extract_array(0, [0])  #    16
        yield q._extract_array([0], 0)  #    17
        yield q._extract_array([0], [0])  #  18



actual_results = tuple(gen_results())


for i, (actual, expected) in enumerate(zip(actual_results, results_from_default)):
    if isinstance(actual, (sf.Quilt, sf.Frame, sf.Series)):
        if not actual.equals(expected):
            breakpoint()
            a = 1

    elif isinstance(actual, np.ndarray):
        if (actual != expected).any():
            breakpoint()
            b = 2

    elif not actual == expected:
        breakpoint()
        c = 3

print("All equal")

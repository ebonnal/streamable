from datetime import timedelta
import timeit
from streamable import stream
from tests.utils.func import identity
import pytest

N = 100_000
ints = stream(range(N))
REL_RATIO = 0.2


def time_consumption(s: stream, times: int = 10):
    duration = timeit.timeit(s.__call__, number=times) / times
    print(f"duration: {duration} seconds")
    print(f"throughput: {N / duration:.2f} elems/s")
    return duration


baseline = time_consumption(ints)


def test_throughput_baseline() -> None:
    assert time_consumption(ints) == pytest.approx(baseline, rel=REL_RATIO)


RATIOS = {
    "buffer": 179.89852308350356,
    "cast": 1.1283094620823886,
    "catch": 7.178932157590864,
    "do": 6.785570133187456,
    "filter": 3.7374839906316493,
    "flatten": 6.40954520850023,
    "flatten_concurrency": 248.526369817968,
    "group": 8.471055776029859,
    "group_by": 26.137714660364686,
    "group_within": 1192.9364943783305,
    "map": 4.472742198923241,
    "map_concurrency": 1182.5817440323467,
    "map_concurrency_as_completed": 1220.633046683077,
    "observe": 14.640171485387322,
    "observe_every_int": 15.95579576606422,
    "observe_every_timedelta": 19.518184625329752,
    "skip": 6.811385461119417,
    "take": 10.627913599621785,
    "throttle": 20.037072087797387,
}

############
# buffer #
############


def test_throughput_buffer() -> None:
    assert time_consumption(ints.buffer(N)) / baseline == pytest.approx(
        RATIOS["buffer"], rel=REL_RATIO
    )


########
# cast #
########


def test_throughput_cast() -> None:
    assert time_consumption(ints.cast(str)) / baseline == pytest.approx(
        RATIOS["cast"], rel=REL_RATIO
    )


#########
# catch #
#########


def test_throughput_catch() -> None:
    assert time_consumption(ints.catch(ValueError)) / baseline == pytest.approx(
        RATIOS["catch"], rel=REL_RATIO
    )


#######
# do #
#######


def test_throughput_do() -> None:
    assert time_consumption(ints.do(identity)) / baseline == pytest.approx(
        RATIOS["do"], rel=REL_RATIO
    )


##########
# filter #
##########


def test_throughput_filter() -> None:
    assert time_consumption(ints.filter(identity)) / baseline == pytest.approx(
        RATIOS["filter"], rel=REL_RATIO
    )


###########
# flatten #
###########


def test_throughput_flatten() -> None:
    assert time_consumption(
        stream((i,) for i in range(N)).flatten()
    ) / baseline == pytest.approx(RATIOS["flatten"], rel=REL_RATIO)


def test_throughput_flatten_concurrency() -> None:
    assert time_consumption(
        stream((i,) for i in range(N)).flatten(concurrency=10)
    ) / baseline == pytest.approx(RATIOS["flatten_concurrency"], rel=REL_RATIO)


#########
# group #
#########


def test_throughput_group() -> None:
    assert time_consumption(ints.group(5)) / baseline == pytest.approx(
        RATIOS["group"], rel=REL_RATIO
    )


def test_throughput_group_by() -> None:
    assert time_consumption(ints.group(5, by=bool)) / baseline == pytest.approx(
        RATIOS["group_by"], rel=REL_RATIO
    )


def test_throughput_group_within() -> None:
    assert time_consumption(
        ints.group(within=timedelta(seconds=1))
    ) / baseline == pytest.approx(RATIOS["group_within"], rel=REL_RATIO)


#######
# map #
#######


def test_throughput_map() -> None:
    assert time_consumption(ints.map(str)) / baseline == pytest.approx(
        RATIOS["map"], rel=REL_RATIO
    )


def test_throughput_map_concurrency() -> None:
    assert time_consumption(
        ints.map(identity, concurrency=2)
    ) / baseline == pytest.approx(RATIOS["map_concurrency"], rel=REL_RATIO)


def test_throughput_map_concurrency_as_completed() -> None:
    assert time_consumption(
        ints.map(identity, concurrency=2, as_completed=True)
    ) / baseline == pytest.approx(RATIOS["map_concurrency_as_completed"], rel=REL_RATIO)


###########
# observe #
###########


def test_throughput_observe() -> None:
    assert time_consumption(
        ints.observe("ints", do=identity)
    ) / baseline == pytest.approx(RATIOS["observe"], rel=REL_RATIO)


def test_throughput_observe_every_int() -> None:
    assert time_consumption(
        ints.observe("ints", do=identity, every=N)
    ) / baseline == pytest.approx(RATIOS["observe_every_int"], rel=REL_RATIO)


def test_throughput_observe_every_timedelta() -> None:
    assert time_consumption(
        ints.observe("ints", do=identity, every=timedelta(seconds=1))
    ) / baseline == pytest.approx(RATIOS["observe_every_timedelta"], rel=REL_RATIO)


########
# skip #
########


def test_throughput_skip() -> None:
    assert time_consumption(ints.skip(N)) / baseline == pytest.approx(
        RATIOS["skip"], rel=REL_RATIO
    )


########
# take #
########


def test_throughput_take() -> None:
    assert time_consumption(ints.take(N)) / baseline == pytest.approx(
        RATIOS["take"], rel=REL_RATIO
    )


############
# throttle #
############


def test_throughput_throttle() -> None:
    assert time_consumption(
        ints.throttle(N, per=timedelta(seconds=1))
    ) / baseline == pytest.approx(RATIOS["throttle"], rel=REL_RATIO)

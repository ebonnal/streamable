import pytest
from validate_version import is_valid_bump
from packaging.version import Version


@pytest.mark.parametrize(
    "old, new",
    [
        ("1.2.3", "2.0.0"),  # major bump
        ("1.2.3", "1.3.0"),  # minor bump
        ("1.2.3", "1.2.4"),  # patch bump
        ("1.2.3", "1.2.4rc0"),  # new rc0 for patch
        ("1.2.3", "1.3.0rc0"),  # new rc0 for minor
        ("1.2.3", "2.0.0rc0"),  # new rc0 for major
        ("1.2.3rc0", "1.2.3rc1"),  # rc0 to rc1
        ("1.2.3rc1", "1.2.3rc2"),  # rc1 to rc2
        ("1.2.3", "1.2.4a0"),  # new alpha0 for patch
        ("1.2.3a0", "1.2.3a1"),  # alpha0 to alpha1
        ("1.2.3", "1.2.4b0"),  # new beta0 for patch
        ("1.2.3b0", "1.2.3b1"),  # beta0 to beta1
        ("1.2.3a2", "1.2.3b0"),  # alpha to beta0
        ("1.2.3b1", "1.2.3rc0"),  # beta to rc0
        ("1.2.3a2", "1.2.3rc0"),  # alpha to rc0
        ("2.1.0rc8", "2.1.0"),  # final release from rc
        ("v1.2.3", "v1.2.4"),  # leading v
        ("1.2.3", "1.2.3"),  # same version
    ],
)
def test_valid_bump(old: str, new: str) -> None:
    assert is_valid_bump(Version(old), Version(new))


@pytest.mark.parametrize(
    "old, new",
    [
        ("1.2.3", "1.2.5"),  # skipped patch
        ("1.2.3", "1.3.1"),  # minor bump without patch reset
        ("1.2.3", "1.1.1"),  # minor going backward on same major
        ("1.2.3rc1", "1.2.3rc0"),  # rc going backwards
        ("1.2.3", "1.2.4rc1"),  # new version starting at rc1
        ("1.2.3b0", "1.2.3rc1"),  # beta to rc1
        ("1.2.3rc1", "1.2.3rc3"),  # skipped rc
        ("1.2.3rc1", "1.2.3a0"),  # rc back to alpha
        ("1.2.3", "1.2.4.post1"),  # post-release
        ("1.2.3", "1.2.4.dev0"),  # dev release
        ("1.2.3", "1.2.4+local"),  # local version
        ("1.2.3", "1!1.2.4"),  # epoch
        ("1.2.3", "1.2.4.0"),  # 4-part release
    ],
)
def test_invalid_bump(old: str, new: str) -> None:
    assert not is_valid_bump(Version(old), Version(new))

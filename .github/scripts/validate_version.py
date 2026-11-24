import sys

from packaging.version import Version


def is_valid_bump(old: Version, new: Version) -> bool:
    if (
        len(new.release) != 3
        or new.epoch
        or new.post is not None
        or new.dev is not None
        or new.local
    ):
        # not allowed: epochs, post, dev, local and non X.Y.Z releases
        # e.g. 1.2.3 -> 1!1.2.4 | 1.2.4.post1 | 1.2.4.dev0 | 1.2.4+local | 1.2.4.0
        return False
    if new == old:
        # allowed: ISO: version not moving
        # e.g. 1.2.3 -> 1.2.3, 1.2.3rc1 -> 1.2.3rc1
        return True
    major, minor, patch = old.release
    if new.release in (
        (major, minor, patch + 1),
        (major, minor + 1, 0),
        (major + 1, 0, 0),
    ):
        # allowed: NEW RELEASE: major xor minor xor patch bump, final or starting at rc0/a0/b0
        # e.g. 1.2.3 -> 1.2.4 | 1.3.0 | 2.0.0 | 1.2.4rc0 | 1.3.0a0 | 2.0.0b0
        # but not 1.2.3 -> 1.2.4rc1
        return new.pre is None or new.pre[1] == 0
    if new.release != old.release:
        # not allowed: moved M.M.P not with a valid increment
        # e.g. 1.2.3 -> 1.2.5 | 1.3.1 | 2.1.0 | 1.2.2
        return False
    if old.pre is None:
        # not allowed: same M.M.P as an already released final version
        # e.g. 1.2.3 -> 1.2.3rc0 | 1.2.3a5
        return False
    if new.pre is None:
        # allowed: FINAL RELEASE: of the current pre-release
        # e.g. 1.2.3rc8 -> 1.2.3, 1.2.3a0 -> 1.2.3
        return True
    (old_kind, old_n), (kind, n) = old.pre, new.pre
    if kind == old_kind:
        # allowed: NEXT PRE-RELEASE: same kind, number +1
        # e.g. 1.2.3rc0 -> 1.2.3rc1
        # but not 1.2.3rc1 -> 1.2.3rc3 | 1.2.3rc0
        return n == old_n + 1
    # allowed: PRE-RELEASE PROMOTION: later kind (a < b < rc), starting at 0
    # e.g. 1.2.3a1 -> 1.2.3b0, 1.2.3b2 -> 1.2.3rc0
    # but not 1.2.3b0 -> 1.2.3rc1, 1.2.3rc1 -> 1.2.3a0
    return kind > old_kind and n == 0


if __name__ == "__main__":
    old, new = map(Version, sys.argv[1:])
    bump = f"{old} -> {new}"
    if not is_valid_bump(old, new):
        raise RuntimeError(f"{bump} is not a valid version bump")
    print(bump)

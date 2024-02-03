###  setup
after having forked `streamable` and `git clone`d it locally:
```bash
cd streamable
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

### unittest
```bash
python -m unittest
```

### type checking
```bash
python -m mypy streamable tests
```

### lint
```bash
python -m autoflake --in-place --remove-all-unused-imports --remove-unused-variables --ignore-init-module -r streamable tests \
&& python -m isort streamable tests \
&& python -m black streamable tests
```

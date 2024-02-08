# Contributor's cheat sheet
## setup
```bash
git clone git@github.com:ebonnal/streamable
cd streamable
python -m venv .venv
source .venv/bin/activate
python -m pip install -r .github/workflows/requirements.txt
```

## unittest
```bash
python -m unittest
```

## check code coverage
```bash
coverage run -m unittest && coverage report
```

## check typing
```bash
python -m mypy streamable tests
```

## lint
```bash
python -m autoflake --in-place --remove-all-unused-imports --remove-unused-variables --ignore-init-module -r streamable tests \
&& python -m isort streamable tests \
&& python -m black .
```

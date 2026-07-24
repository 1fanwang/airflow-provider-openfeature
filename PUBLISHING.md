# Publishing

`airflow-provider-openfeature` is a standard PEP 517 (hatchling) package. Airflow discovers it at
runtime via the `apache_airflow_provider` / `airflow.policy` / `airflow.plugins` entry points, no
special packaging beyond a normal wheel.

## Build + verify

```bash
python -m build            # -> dist/airflow_provider_openfeature-<v>-py3-none-any.whl + .tar.gz
twine check dist/*         # metadata + README render check
```

The wheel ships only `openfeature_airflow/`, it must NOT contain `airflow/` (that would clobber
apache-airflow's namespace). Verify:

```bash
python -c "import zipfile,glob; z=zipfile.ZipFile(sorted(glob.glob('dist/*.whl'))[-1]); \
  assert not any(n.startswith('airflow/') for n in z.namelist()), 'ships airflow/ namespace!'; print('ok')"
```

## Release

Version lives in two places, bump both:
- `pyproject.toml` → `[project] version`
- `src/openfeature_airflow/__init__.py` → `__version__`

Then tag; the GitHub Action publishes to PyPI via trusted publishing (no token in the repo):

```bash
git tag v0.1.0 && git push origin v0.1.0     # triggers .github/workflows/publish.yml
```

### First-time PyPI setup (trusted publishing)

1. Reserve the name on PyPI (upload once manually, or register the project).
2. On PyPI → the project → Publishing → add a GitHub trusted publisher:
   owner `1fanwang`, repo `airflow-provider-openfeature`, workflow `publish.yml`, environment `pypi`.
3. After that, tagging `v*` publishes with no API token.

### Manual upload (TestPyPI first)

```bash
twine upload --repository testpypi dist/*
pip install -i https://test.pypi.org/simple/ airflow-provider-openfeature   # smoke test
twine upload dist/*                                                          # real PyPI
```

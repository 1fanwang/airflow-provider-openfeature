# Getting listed on the Airflow Ecosystem page

Third-party providers are listed at <https://airflow.apache.org/ecosystem/> under
**"Third-party Airflow plugins and providers"**. The page is content in the
[`apache/airflow-site`](https://github.com/apache/airflow-site) repo:
`landing-pages/site/content/en/ecosystem/_index.md`.

## Prerequisite

Publish to PyPI first (see [`../PUBLISHING.md`](../PUBLISHING.md)) so the listing points at an
installable package.

## Steps

1. Fork `apache/airflow-site`.
2. Edit `landing-pages/site/content/en/ecosystem/_index.md`, add the entry below to the third-party
   providers list (keep the list alphabetical).
3. Open a PR. Maintainers review and merge content PRs; no dev-list thread is required for an
   Ecosystem listing (that requirement is only for *in-tree* community providers).

## Draft entry

```markdown
- **[OpenFeature Provider](https://github.com/1fanwang/airflow-provider-openfeature)**:
  Evaluate feature flags and run progressive delivery (canary, blue-green, gradual pool/queue routing)
  against any OpenFeature backend (flagd, GrowthBook, Unleash, ...) from a hook, sensor, or cluster
  policy.
```

Adjust the repo/author link once the package is on PyPI and the repo is public.

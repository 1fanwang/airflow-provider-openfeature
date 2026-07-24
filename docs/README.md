# Documentation

Start with the [project README](../README.md) for the pitch and quickstart. These pages go deeper.

| Page | Read when |
|---|---|
| [architecture.md](architecture.md) | You want the mental model and the flow diagrams (placement, in-DAG eval, exposure). |
| [use-cases.md](use-cases.md) | You want a copy-pasteable recipe: migration routing, worker/executor rollout, A/B testing, kill switch. |
| [extending.md](extending.md) | You want to add a backend adapter or understand how the ecosystem plugs in. |
| [ecosystem-entry.md](ecosystem-entry.md) | You're getting the package listed on the Airflow Ecosystem page. |
| [../PUBLISHING.md](../PUBLISHING.md) | You're cutting a release to PyPI. |
| [../AGENTS.md](../AGENTS.md) | You (or a coding agent) are changing the code and need the invariants. |

## How these docs are organized

Following the [Diátaxis](https://diataxis.fr) split that Django, dbt, and most mature projects use:

- **Tutorial / quickstart**, the README's install and register-a-backend section.
- **How-to**, [use-cases.md](use-cases.md), task-oriented recipes.
- **Explanation**, [architecture.md](architecture.md), the why and the mental model.
- **Reference**, the config options and entry points in `pyproject.toml` and `provider_info.py`.

The structure mirrors how other Airflow-ecosystem and feature-flag projects present themselves:
Astronomer Cosmos and OpenLineage lead with a concept overview plus diagrams, then how-to guides and
integration pages; OpenFeature's docs separate the concepts (provider, evaluation API, hooks) from the
per-backend setup. Markdown with Mermaid renders on GitHub today, so no site build is needed at this
stage. If the project graduates to a hosted site, the natural target is a Docusaurus or Sphinx build
of this same `docs/` tree, which is what those projects run.

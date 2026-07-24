# Documentation

Start with the [project README](../README.md) for the pitch and quickstart. These pages go deeper.

| Page | Read when |
|---|---|
| [architecture.md](architecture.md) | You want the mental model and the flow diagrams (placement, in-DAG eval, exposure). |
| [use-cases.md](use-cases.md) | You want a copy-pasteable recipe: migration routing, worker/executor rollout, A/B testing, kill switch. |
| [extending.md](extending.md) | You want to add a backend adapter or understand how the ecosystem plugs in. |
| [../PUBLISHING.md](../PUBLISHING.md) | You're cutting a release to PyPI. |
| [../AGENTS.md](../AGENTS.md) | You (or a coding agent) are changing the code and need the invariants. |

## How these docs are organized

Following the [Diátaxis](https://diataxis.fr) split:

- **Tutorial / quickstart**: the README's install and register-a-backend section.
- **How-to**: [use-cases.md](use-cases.md), task-oriented recipes.
- **Explanation**: [architecture.md](architecture.md), the why and the mental model.
- **Reference**: the config options and entry points in `pyproject.toml` and `provider_info.py`.

Markdown with Mermaid renders on GitHub, so no site build is needed yet. A hosted site would be a
Docusaurus or Sphinx build of this same `docs/` tree.

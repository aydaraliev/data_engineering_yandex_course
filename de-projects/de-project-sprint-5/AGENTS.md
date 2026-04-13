# Repository Guidelines

## Project Structure & Module Organization
- `src/dags/`: place Airflow DAGs and supporting modules. Keep operator/util helpers in submodules to avoid bloating DAG files.
- `start_docker.sh`: pulls and runs the lab container with Airflow + Postgres exposed on 3000/3002/15432.
- `README.md`: base instructions and container ports; keep aligned with scripts.
- Untracked local notes belong in `agent.md` (ignored) to store credentials or scratch info.

## Build, Test, and Development Commands
- Run stack locally: `./start_docker.sh` (requires Docker; replaces existing `de-pg-cr-af` container).
- Check running containers: `docker ps --format '{{.Names}} {{.Ports}}'`.
- Connect to Postgres for quick checks: `psql postgresql://jovyan:jovyan@localhost:15432/de`.
- Add new Python deps to the container only if the base image lacks them; prefer packaging DAG-specific logic inside the DAG image rather than host venvs.

## Coding Style & Naming Conventions
- Python: 4-space indent, snake_case for variables/functions, PascalCase for classes, UPPER_SNAKE_CASE for constants.
- DAGs: keep default_args and schedule definitions at top; use descriptive DAG IDs (`de_project_<feature>`). Keep tasks small and composable.
- Avoid hard-coded secrets; use environment variables or Airflow connections where available.

## Testing Guidelines
- No formal test suite is present; verify DAG parse-ability with `airflow dags list` inside the container if available.
- For data checks, run targeted SQL against the provided Postgres instance; keep validation queries under `src/dags/sql/` if reusable.
- When adding tests in the future, mirror file paths under `tests/` with module structure and name tests `test_<module>.py`.

## Commit & Pull Request Guidelines
- Commit messages: imperative, short, and scoped (e.g., `Add script to pull and run project Docker image`).
- Group related changes per commit (DAG + SQL + doc). Avoid mixing unrelated refactors with new features.
- Pull requests: include a short summary of changes, how to run/verify (commands or queries), and note any manual steps (e.g., env vars, new connections).

## Security & Configuration Tips
- Default credentials for local Postgres: host `localhost`, port `15432`, db `de`, user/password `jovyan`; use only for local testing.
- Do not commit `.idea/`, `agent.md`, or secrets; keep ignores up to date in `.gitignore`.

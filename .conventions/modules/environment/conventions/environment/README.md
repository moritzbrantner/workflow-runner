# Environment conventions

## ENV-001 — Keep irreplaceable development state outside disposable containers

- Containers provide reproducible execution, not source, Git, credentials, worktrees, or agent-session state.

## ENV-002 — Use one canonical local service topology when services are required

- When a repository requires multiple local services or a reproducible service topology, define one canonical Docker Compose topology and reuse it across development and tests.
- Express differences with configuration, profiles, or explicit overrides rather than maintaining competing service definitions.
- Libraries, CLIs, browser-only applications, and self-contained tests do not need Compose merely for uniformity; unit tests need no external topology.

## ENV-003 — .env.example is the committed environment contract

- Keep .env local and uncommitted; commit a secret-free .env.example covering supported setup.
- Update .env.example whenever an environment variable changes.

## ENV-004 — Trust the declared CI environment

- Use the repository's ordinary setup and build or test commands. Trust the hosted runner and installed tools unless a concrete failure points to the environment.
- Do not require environment fingerprints, exact runner identity receipts, or automatic environment verification on either successful or failed CI runs.
- Investigate environment state on demand when a specific failure warrants it. Never include secrets or hashes of secrets in diagnostics.

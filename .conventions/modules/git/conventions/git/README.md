# Git conventions

## GIT-001 — Every agent run has an explicit baseline

- Define the source-of-truth starting point; do not assume a local or remote ref is current.

## GIT-002 — Separate implementation from publishing

- Implementation produces candidate changes; integration, pushing, merging, and publishing are separate steps.

## GIT-003 — Use tiered local hooks without duplicating validation logic

- Pre-commit runs only very fast deterministic checks such as format checks, linting, schema/config validation, forbidden-pattern checks, and secret scanning.
- Pre-push runs broader affected-scope checks such as typechecking and focused unit/integration tests.
- Full-repository, E2E, benchmark, compatibility, and release verification remain explicit broader tiers rather than making every commit expensive.
- Hooks invoke the same canonical repository capabilities used by humans, agents, and CI; do not implement a second copy of validation logic inside hook scripts.

## GIT-004 — Keep CI actions current

- Use maintained upstream release tags for external GitHub Actions and the current branch for shared in-house reusable workflows when their interface is compatible.
- Do not require commit-SHA pinning, recurring pin refreshes, or a separate validation job to prove the runner or action version. When an upstream change breaks a workflow, fix the affected workflow.
- Local actions such as `./.github/actions/...` are repository source and do not need an external SHA pin.

## GIT-005 — Use repository checks as reported by GitHub

- Run relevant code checks through the repository's normal local or GitHub CI paths. Do not add a second exact-head validation workflow, receipt, or runner-identity check solely to re-prove GitHub's check association.
- For a merge, use GitHub's current pull-request status and required checks. A merge precondition may guard against a concurrent head change; it does not require rerunning the validation suite.
- Release artifacts may retain source and artifact identities when those identities are part of their delivery contract.

## GIT-006 — Agents integrate their own green changes

- Executable implementation changes use a branch named `agent/<short-topic>`; open a PR and merge only after applicable repository checks pass.
- Owner-approved, **non-executable documentation-only** decisions, plans and instructions may instead be committed directly to the owning default branch when permitted, without requiring a PR or green CI. For an existing documentation-only PR, review its actual content and merge promptly even if unrelated CI is missing or failing.
- Inspect current authority and Git state before integrating; preserve document syntax, decision IDs and required human review. Do not bypass branch protection or conflate owner approval with implemented behavior.
- File extension alone is not sufficient: changes to code, configuration, executable specifications/fixtures, generated build inputs, validation contracts or security enforcement retain their applicable tests, reviews and merge gates even when expressed in Markdown.
- Agents may review and merge dependency-update, other-agent, and owner implementation PRs once applicable checks and obligations pass. Never weaken, skip, or delete a failing check to reach green.
- For an approved, non-draft PR that is ready for integration, explicitly request GitHub auto-merge or submit it to the configured merge queue (`gh pr merge` on queue-protected branches); opening the PR or enabling auto-merge at repository level does not enqueue it. If requirements are still pending, request merge-when-ready and let GitHub enforce them. Do not queue drafts, blocked/dependent work, PRs awaiting required human approval, or PRs whose integration was explicitly deferred. Verify current head, required checks, reviews, dependencies, and mergeability immediately before requesting merge; do not bypass protections.
- A repository's `AGENTS.md` may require additional human approval or narrower integration policy.

## GIT-007 — Commit in small, focused steps

- Each commit makes one coherent change and has a message that states that change.
- Separate formatting-only commits from behavior commits.

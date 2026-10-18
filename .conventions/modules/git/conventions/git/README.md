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

- Agents work on a branch named `agent/<short-topic>`, never directly on the default branch.
- Open a pull request and merge it with a merge commit, deleting the branch, once all repository checks are green.
- Agents may also review and merge dependency-update, other-agent, and owner pull requests once reviewed and green.
- Never weaken, skip, or delete a failing check to reach green.
- Exception: a repository's `AGENTS.md` may require human approval before merge.

## GIT-007 — Commit in small, focused steps

- Each commit makes one coherent change and has a message that states that change.
- Separate formatting-only commits from behavior commits.

# JOB-EXECUTOR

This application is the data import pipeline for the microdata.no platform.
Because the system is multi-tenant, it can operate on multiple datastores, and it is the only
process on the server allowed to update a datastore.

## Program flow
- The manager process starts up and verifies the state of the system
    - If any jobs are in progress but not finished on startup, we can assume that they
      got interrupted from a job-executor restart.
    - The manager resets these jobs in the appropriate manner to ensure no bad state
      before continuing
- The manager asks the datastore-api for jobs that are queued
- If a job entails a new dataset import, the manager assigns a worker
    - A worker starts in a subprocess and unpackages and validates the dataset
      using the microdata-tools package
    - The worker then transforms the metadata to the correct format for storage
    - The worker pseudonymizes columns of the data depending on metadata definitions
    - The worker optionally partitions the parquet
    - The manager can now see that the job has registered the dataset as built and
      can import it into the datastore
- If a job entails a change to the datastore, or datasets in the datastore
    - The manager processes the jobs one by one and makes changes to the datastore
      as requested

## Modules
- **job_executor/**: directory containing the source code
    - **domain/**: all core domain logic for the application
        - **manager/**: logic concerning the manager process
        - **worker/**: logic concerning the worker processes
        - **datastores.py**: logic concerning the updating of the datastores
        - **rollback.py**: logic concerning rolling back incomplete jobs
    - **adapter/**: adapters for filesystem and external services
        - **datastore_api/**: client for datastore-api that holds job information
        - **fs/**: client for the filesystem
        - **pseudonym_service.py**: client for the pseudonymization service
    - **common/**: common modules used by the whole stack
    - **config/**: configuration for application and logging
    - **app.py**: the entry point for application startup


## Development Workflow (uv)
- Use `uv` for Python package and environment management.
- Add dependencies with `uv add <package>` (and dev dependencies with `uv add --dev <package>`).
- Format code with `uv run ruff format`.
- Run autofixes with `uv run ruff check --fix`.
- Sort imports specifically with `uv run ruff check --fix --select I`.
- Run tests with `uv run pytest`

## RULES

### 1. Scope Before You Build 

**Move fast on small tasks. Plan deliberately on larger ones.**

When scoping work:
- If the user request is small, do it directly without creating a todo list.
- If the request is larger (multi-step or non-trivial), always create a todo list.
- Iterate on the todo list together with the developer until both scope and implementation are satisfactory.
- Prefer small pull requests and manageable chunks of work.
- If a request can be split into a smaller, safer increment, suggest that alternative before creating the todo list.

The test: Work should stay focused, trackable, and easy to review in small increments.

### 2. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:
- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

### 3. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

### 4. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:
- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:
- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

---

**These guidelines are working if:** fewer unnecessary changes in diffs, fewer rewrites due to overcomplication, and clarifying questions come before implementation rather than after mistakes.

### 5. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:
- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:
```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

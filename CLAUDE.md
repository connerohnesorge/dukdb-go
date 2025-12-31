# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

dukdb-go is a pure Go implementation of a DuckDB database driver. It maintains API compatibility with the original duckdb-go driver while requiring zero cgo dependencies. This enables cross-platform compilation without C toolchains and allows use in pure Go environments (TinyGo, WebAssembly, etc.).

## Development Commands

```bash
# Enter development shell (required for all commands)
nix develop

# Run tests
nix develop -c tests
# Or directly: gotestsum --format short-verbose ./...

# Run linting
nix develop -c lint
# Or directly: golangci-lint run

# Format code
nix fmt
# Includes: alejandra (Nix), gofmt, golines, goimports

# Run single test
go test -run TestName ./...

# Run tests with coverage
make check-coverage
```

## Architecture

### Pluggable Backend Architecture

The driver uses a pluggable backend design:

```
database/sql API
      │
      ▼
   Driver (driver.go) ──► registers with sql.Register("dukdb")
      │
      ▼
   Connector (connector.go) ──► manages connection lifecycle
      │
      ▼
   Conn (conn.go) ──► implements driver.Conn interfaces
      │
      ▼
   Backend interface (backend.go) ──► abstraction layer
      │
      ▼
   Engine (internal/engine/) ──► actual implementation
```

### Key Components

- **Root package (`dukdb`)**: Public API, driver registration, connection types
- **`internal/engine/`**: Core execution engine implementing Backend interface
- **`internal/catalog/`**: Schema and metadata management
- **`internal/storage/`**: Data persistence layer
- **`internal/parser/`**: SQL parsing
- **`internal/planner/`**: Query planning
- **`internal/executor/`**: Query execution
- **`internal/binder/`**: SQL binding/resolution
- **`internal/vector/`**: Columnar data representation

### Reference Implementation

The `duckdb/` and `duckdb-go/` directories contain the original DuckDB C++ source and cgo driver for reference. These are used to understand behavior and validate API compatibility.

## Key Constraints

1. **NO CGO**: Zero C dependencies or cgo usage allowed
2. **API Compatibility**: Must match duckdb-go's public API for drop-in replacement
3. **Pure Go**: Must work in TinyGo, WASM, and other pure Go environments

## Spectr Change Proposals

This project uses spectr for managing significant changes. When making proposals or architectural changes:

1. Read `spectr/AGENTS.md` for the change proposal process
2. Active proposals are in `spectr/changes/`
3. Completed proposals are archived in `spectr/changes/archive/`
4. Specifications live in `spectr/specs/`

## Linting Configuration

The project uses a minimal golangci-lint configuration (`.golangci.yml`):
- Enabled: govet, errcheck, staticcheck, unused
- Formatter: gofmt with simplify

## Testing Notes

- Uses testify for assertions (`github.com/stretchr/testify`)
- Integration tests run against the actual engine implementation
- Reference implementation in `duckdb-go/` can be used to validate expected behavior


<!-- spectr:start -->
# Spectr Instructions

These instructions are for AI assistants working in this project.

Always open `@/spectr/AGENTS.md` when the request:

- Mentions planning or proposals (words like proposal, spec, change, plan)
- Introduces new capabilities, breaking changes, architecture shifts, or big
  performance/security work
- Sounds ambiguous and you need the authoritative spec before coding

Use `@/spectr/AGENTS.md` to learn:

- How to create and apply change proposals
- Spec format and conventions
- Project structure and guidelines

When delegating tasks from a change proposal to subagents:

- Provide the proposal path: `spectr/changes/<id>/proposal.md`
- Include task context: `spectr/changes/<id>/tasks.jsonc`
- Reference delta specs: `spectr/changes/<id>/specs/<capability>/spec.md`

<!-- spectr:end -->
# YOU ARE THE ORCHESTRATOR

You are Claude Code with a 200k context window, and you ARE the orchestration
system. You manage the entire project, create todo lists, and delegate
individual tasks to specialized subagents.

The end of your context window is not a bad thing! Don't be afraid to compress your memory at the end of the context window.
Thus, you should remain diligent about delegating specific tasks, NOT multiple phases or complete proposals, to subagents.

## 🎯 Your Role: Master Orchestrator

You maintain the big picture, create comprehensive todo lists, and delegate
individual todo items to specialized subagents that work in their own context
windows.

## 🚨 YOUR MANDATORY WORKFLOW

When the user gives you a project:

### Step 1: ANALYZE & PLAN (You do this)

1. Understand the complete project scope
2. Break it down into clear, actionable todo items
3. **USE TodoWrite** to create a detailed todo list
4. Each todo should be specific enough to delegate

### Step 2: DELEGATE TO SUBAGENTS (One todo at a time)

1. Take the FIRST todo item
2. Invoke the **`coder`** subagent with that specific task
3. **Verify coder output before testing**:
   - Run `git diff --stat` and read files with significant changes (>10 lines
     modified)
   - Run `nix develop -c 'lint'` to validate code quality
   - Confirm the implementation matches the task requirements
4. The coder works in its OWN context window
5. Wait for coder to complete and report back

### Step 3: TEST THE IMPLEMENTATION

1. Take the coder's completion report
2. Invoke the **`tester`** subagent to verify
3. Tester uses Playwright MCP in its OWN context window
4. Wait for test results

### Step 4: HANDLE RESULTS

- **If tests pass**: Mark todo complete, move to next todo
- **If tests fail**: Invoke **`stuck`** agent for human input
- **If coder hits error**: They will invoke stuck agent automatically

### Step 5: ITERATE

1. Update todo list (mark completed items)
2. Move to next todo item
3. Repeat steps 2-4 until ALL todos are complete

## 🛠️ Available Subagents

### coder

**Purpose**: Implement one specific todo item

- **When to invoke**: For each coding task on your todo list
- **What to pass**: ONE specific todo item with clear requirements
- **Context**: Gets its own clean context window
- **Returns**: Implementation details and completion status
- **On error**: Will invoke stuck agent automatically

### tester

**Purpose**: Visual verification with Playwright MCP

- **When to invoke**: After EVERY coder completion
- **What to pass**: What was just implemented and what to verify
- **Context**: Gets its own clean context window
- **Returns**: Pass/fail with screenshots
- **On failure**: Will invoke stuck agent automatically

### stuck

**Purpose**: Human escalation for ANY problem

- **When to invoke**: When tests fail or you need human decision
- **What to pass**: The problem and context
- **Returns**: Human's decision on how to proceed
- **Critical**: ONLY agent that can use AskUserQuestion

## 🚨 CRITICAL RULES FOR YOU

**YOU (the orchestrator) MUST:**

1. ✅ Create detailed todo lists with TodoWrite
2. ✅ Delegate ONE todo at a time to coder
3. ✅ Test EVERY implementation with tester
4. ✅ Track progress and update todos
5. ✅ Maintain the big picture across 200k context
6. ✅ **ALWAYS create pages for EVERY link in headers/footers** - NO 404s
  allowed!

**YOU MUST NEVER:**

1. ❌ Implement code yourself (delegate to coder)
2. ❌ Skip testing (always use tester after coder)
3. ❌ Let agents use fallbacks (enforce stuck agent)
4. ❌ Lose track of progress (maintain todo list)
5. ❌ **Put links in headers/footers without creating the actual pages** - this
  causes 404s!

## 📋 Example Workflow

```text
User: "Build a React todo app"

YOU (Orchestrator):
1. Create todo list:
   [ ] Set up React project
   [ ] Create TodoList component
   [ ] Create TodoItem component
   [ ] Add state management
   [ ] Style the app
   [ ] Test all functionality

2. Invoke coder with: "Set up React project"
   → Coder works in own context, implements, reports back

3. Invoke tester with: "Verify React app runs at localhost:3000"
   → Tester uses Playwright, takes screenshots, reports success

4. Mark first todo complete

5. Invoke coder with: "Create TodoList component"
   → Coder implements in own context

6. Invoke tester with: "Verify TodoList renders correctly"
   → Tester validates with screenshots

... Continue until all todos done
```

## 🔄 The Orchestration Flow

```text
USER gives project
    ↓
YOU analyze & create todo list (TodoWrite)
    ↓
YOU invoke coder(todo #1)
    ↓
    ├─→ Error? → Coder invokes stuck → Human decides → Continue
    ↓
CODER reports completion
    ↓
YOU invoke tester(verify todo #1)
    ↓
    ├─→ Fail? → Tester invokes stuck → Human decides → Continue
    ↓
TESTER reports success
    ↓
YOU mark todo #1 complete
    ↓
YOU invoke coder(todo #2)
    ↓
... Repeat until all todos done ...
    ↓
YOU report final results to USER
```

## 🎯 Why This Works

**Your 200k context** = Big picture, project state, todos, progress
**Coder's fresh context** = Clean slate for implementing one task
**Tester's fresh context** = Clean slate for verifying one task
**Stuck's context** = Problem + human decision

Each subagent gets a focused, isolated context for their specific job!

## 💡 Key Principles

1. **You maintain state**: Todo list, project vision, overall progress
2. **Subagents are stateless**: Each gets one task, completes it, returns
3. **One task at a time**: Don't delegate multiple tasks simultaneously
4. **Always test**: Every implementation gets verified by tester
5. **Human in the loop**: Stuck agent ensures no blind fallbacks

## 🚀 Your First Action

When you receive a project:

1. **IMMEDIATELY** use TodoWrite to create comprehensive todo list
2. **IMMEDIATELY** invoke coder with first todo item
3. Wait for results, test, iterate
4. Report to user ONLY when ALL todos complete

## ⚠️ Common Mistakes to Avoid

❌ Implementing code yourself instead of delegating to coder
❌ Skipping the tester after coder completes
❌ Delegating multiple todos at once (do ONE at a time)
❌ Not maintaining/updating the todo list
❌ Reporting back before all todos are complete
❌ **Creating header/footer links without creating the actual pages** (causes
404s)
❌ **Not verifying all links work with tester** (always test navigation!)

## ✅ Success Looks Like

- Detailed todo list created immediately
- Each todo delegated to coder → tested by tester → marked complete
- Human consulted via stuck agent when problems occur
- All todos completed before final report to user
- Zero fallbacks or workarounds used
- **ALL header/footer links have actual pages created** (zero 404 errors)
- **Tester verifies ALL navigation links work** with Playwright

Note that if you are waiting for an action to complete you should not return,
you must call a Bash(sleep {best estimate of seconds to sleep until complete}).
**Verifying tester results**: Tester outputs may be incomplete or inaccurate due
to context window limitations or halucinations.

After EVERY tester success:

1. Run `nix develop -c 'lint'` and `nix develop -c 'tests'` to validate code
  quality (if fails, delegate to coder to fix)
2. Review any screenshots or visual evidence provided
3. Cross-check claims against actual code or command outputs
4. Re-run at least one test independently to validate results

Only mark a task complete after this verification passes.
When delegating tasks to coder, you should make sure to also give it the exact
task to complete, and not just a general description.
Giving the path of the specification&tasks helps subagents to refer back to the
specification.

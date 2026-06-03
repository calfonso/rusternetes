#!/usr/bin/env bash
# Prune git worktrees whose branches have already merged to main.
#
# Walks `git worktree list`, decides per worktree whether its branch
# is merged into `fork/main` (or `origin/main` as fallback), then
# removes those worktrees. Handles squash-merge (GitHub default for
# this repo) by also comparing the branch-tip tree against trees of
# commits on main.
#
# Usage:
#   bash scripts/prune-merged-worktrees.sh             # do it
#   bash scripts/prune-merged-worktrees.sh --dry-run   # just print
#   bash scripts/prune-merged-worktrees.sh --all-paths # include worktrees
#                                                       outside .claude/worktrees/
#
# By default the script ONLY touches worktrees under `.claude/worktrees/`
# (the path /batch workers use). Pass --all-paths to extend the sweep
# to every worktree git knows about — useful after a long debugging
# session in /tmp.
#
# Safety:
#   - Never touches the primary worktree (the repo root).
#   - Never touches the worktree the script is invoked from.
#   - Never touches worktrees with no branch (detached HEAD) — those
#     get reported, not removed.
#   - --dry-run prints what WOULD be removed without making changes.

set -euo pipefail

DRY_RUN=false
ALL_PATHS=false
for arg in "$@"; do
    case "$arg" in
        --dry-run|-n) DRY_RUN=true ;;
        --all-paths)  ALL_PATHS=true ;;
        --help|-h)
            sed -nE '/^# /,/^$/ s/^# ?//p' "${BASH_SOURCE[0]}" | head -40
            exit 0
            ;;
        *) echo "unknown arg: $arg" >&2; exit 2 ;;
    esac
done

# Pick upstream remote: prefer `fork` (PR target), fall back to `origin`.
remote=""
for r in fork origin; do
    if git remote get-url "$r" >/dev/null 2>&1; then
        remote="$r"
        break
    fi
done
if [ -z "$remote" ]; then
    echo "ERROR: neither 'fork' nor 'origin' remote configured" >&2
    exit 2
fi

echo "==> Using upstream '$remote/main'"
git fetch -q "$remote" main

# Trees of every commit on main. Squash-merge produces a new commit on
# main with the same tree as the branch tip — so a tree match here means
# the branch's content was merged even though the commit SHAs differ.
trees_on_main=$(git log "$remote/main" --format='%T' | sort -u)

# Context-free patch-ids for recent main commits. `git diff -U0` strips
# the @@ hunk headers' line numbers and the surrounding context lines —
# the parts that drift when nearby code changed between the rebase and
# the squash-merge. Bounded at 500 commits so the precompute stays cheap
# (~1 s on a warm cache); branches older than that fall through to the
# default-context cherry / log checks above.
echo "==> Indexing context-free patch-ids on $remote/main (last 500 commits)..."
context_free_ids_on_main=$(
    git log "$remote/main" --format='%H' -500 \
        | while read -r c; do
            git show -U0 "$c" | git patch-id --stable | awk '{print $1}'
        done \
        | sort -u
)

# Current worktree (we never remove ourselves).
self_path=$(git rev-parse --show-toplevel)

is_merged() {
    local branch=$1
    local tip
    tip=$(git rev-parse "$branch" 2>/dev/null) || return 1

    # Fast-forward / regular merge: branch tip is an ancestor of main.
    if git merge-base --is-ancestor "$tip" "$remote/main" 2>/dev/null; then
        return 0
    fi

    # Squash-merge: branch's tree is identical to some tree on main.
    local branch_tree
    branch_tree=$(git rev-parse "${branch}^{tree}" 2>/dev/null) || return 1
    if printf '%s\n' "$trees_on_main" | grep -qFx "$branch_tree"; then
        return 0
    fi

    # Every commit on the branch is already on main (cherry-picked etc).
    if [ -z "$(git log "$remote/main..$branch" --oneline 2>/dev/null)" ]; then
        return 0
    fi

    # Squash-merge after intervening main commits: the tree heuristic above
    # only fires when the branch tip's tree happens to match the squash
    # commit's tree on main, which breaks the moment another PR lands
    # between the rebase and the squash-merge (the squash commit picks up
    # the newer base tree). `git cherry` compares per-commit patch-ids
    # instead of trees, so it sees through both squash-merges and
    # identity-rewriting rebases. If no commit on the branch shows up as
    # `+` (i.e. all are accounted for on main by patch-id), the branch is
    # merged even though the tip tree differs.
    if ! git cherry "$remote/main" "$branch" 2>/dev/null | grep -q '^+'; then
        return 0
    fi

    # Squash-merge after intervening main commits that *also touched lines
    # neighboring this branch's changes*. `git cherry` (and default
    # patch-id) include @@-hunk headers and surrounding context lines in
    # the hash; those drift when another PR inserts a `pub mod ...` line
    # near the edit. `git diff -U0` strips both, and the resulting
    # patch-id matches across the rebase. Compare the branch's combined
    # context-free patch-id against the precomputed set for recent main.
    local branch_id_u0
    branch_id_u0=$(git diff -U0 "$remote/main...$branch" 2>/dev/null \
        | git patch-id --stable 2>/dev/null \
        | awk '{print $1}')
    if [ -n "$branch_id_u0" ] && \
       printf '%s\n' "$context_free_ids_on_main" | grep -qFx "$branch_id_u0"; then
        return 0
    fi

    return 1
}

# Parse `git worktree list --porcelain` into records.
mapfile -t lines < <(git worktree list --porcelain)

removed=0
kept=0
skipped=0

i=0
while [ $i -lt ${#lines[@]} ]; do
    line=${lines[$i]}
    if [[ "$line" != worktree* ]]; then
        i=$((i + 1))
        continue
    fi

    wt_path=${line#worktree }
    wt_branch=""
    wt_detached=false

    # Read record attribute lines until blank or next worktree line.
    j=$((i + 1))
    while [ $j -lt ${#lines[@]} ] && [ -n "${lines[$j]}" ]; do
        attr=${lines[$j]}
        case "$attr" in
            "branch refs/heads/"*) wt_branch=${attr#branch refs/heads/} ;;
            "detached")            wt_detached=true ;;
        esac
        j=$((j + 1))
    done
    i=$((j + 1))

    # Skip primary worktree (repo root, usually checked out on `main`).
    if [ "$wt_path" = "$self_path" ]; then
        skipped=$((skipped + 1))
        continue
    fi

    # Skip the canonical /main worktree.
    if [ "$wt_branch" = "main" ]; then
        skipped=$((skipped + 1))
        continue
    fi

    # Skip LIVE subagent worktrees (the Agent tool's `isolation: worktree`).
    # These are named `worktree-agent-<id>` on a `.../agent-<id>` path, and an
    # agent does `git reset --hard <remote>/main` at startup — so before it
    # commits, its branch tip *is* main, which `is_merged` (merge-base
    # --is-ancestor) would read as "merged" and delete out from under the
    # running agent. They are managed/cleaned by the agent harness, never by
    # this merged-PR prune sweep.
    if [[ "$wt_branch" == worktree-agent-* ]] || [[ "$wt_path" == *"/.claude/worktrees/agent-"* ]]; then
        printf 'SKIP    %s  (live subagent worktree — not a PR-prune target)\n' "$wt_path"
        skipped=$((skipped + 1))
        continue
    fi

    # Skip worktrees outside .claude/worktrees/ unless --all-paths.
    if ! $ALL_PATHS && [[ "$wt_path" != *"/.claude/worktrees/"* ]]; then
        printf 'SKIP    %s  (outside .claude/worktrees/ — pass --all-paths to include)\n' "$wt_path"
        skipped=$((skipped + 1))
        continue
    fi

    # Detached HEAD: report, don't auto-remove.
    if $wt_detached || [ -z "$wt_branch" ]; then
        printf 'SKIP    %s  (detached HEAD — manual review)\n' "$wt_path"
        skipped=$((skipped + 1))
        continue
    fi

    if is_merged "$wt_branch"; then
        printf 'REMOVE  %s  (branch %s — merged)\n' "$wt_path" "$wt_branch"
        if ! $DRY_RUN; then
            # `--force --force` (a.k.a. `-f -f`) bypasses the lock that
            # /batch workers leave on their worktrees.
            git worktree remove -f -f "$wt_path" 2>/dev/null || \
                rm -rf "$wt_path" 2>/dev/null || true
            # Drop the local branch too (its content is on main now).
            git branch -D "$wt_branch" 2>/dev/null || true
        fi
        removed=$((removed + 1))
    else
        printf 'KEEP    %s  (branch %s — not merged)\n' "$wt_path" "$wt_branch"
        kept=$((kept + 1))
    fi
done

# Tidy up any administrative metadata for worktrees we just removed.
git worktree prune

echo
echo "Summary: $removed removed, $kept kept, $skipped skipped."
$DRY_RUN && echo "(dry-run — no changes made)"

//! Branch strategies for an agent editing a repo.
//!
//! Three modes — how the agent's commits relate to the host's working tree:
//!
//! * [`HeadStrategy`] — agent works in `repo_root` directly. Refuses dirty trees.
//! * [`MergeToHeadStrategy`] — temp branch in a worktree; `--no-ff` merge back on
//!   success. Failure leaves the worktree for inspection.
//! * [`BranchStrategy`] — named branch in a worktree; the branch is the persistence
//!   point. No merge. Worktree removed only on success.
//!
//! Shells out to `git(1)`. No `libgit2`. No async. No external deps.
//!
//! Worktrees live at `<repo_root>/.moth/worktrees/<name>`.

use std::ffi::OsStr;
use std::fmt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::{SystemTime, UNIX_EPOCH};

// -- types --------------------------------------------------------------------

#[derive(Debug)]
pub enum GitError {
    Io(std::io::Error),
    Git { command: String, stderr: String, exit: i32 },
    NotARepo(PathBuf),
    DirtyTree,
    Other(String),
}

impl fmt::Display for GitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GitError::Io(e) => write!(f, "io: {e}"),
            GitError::Git { command, stderr, exit } => {
                write!(f, "git {command} failed ({exit}): {}", stderr.trim())
            }
            GitError::NotARepo(p) => write!(f, "not a git repo: {}", p.display()),
            GitError::DirtyTree => f.write_str("working tree is dirty"),
            GitError::Other(s) => f.write_str(s),
        }
    }
}

impl std::error::Error for GitError {}

impl From<std::io::Error> for GitError {
    fn from(e: std::io::Error) -> Self {
        GitError::Io(e)
    }
}

#[derive(Debug)]
pub struct Workspace {
    pub path: PathBuf,
    pub source_branch: String,
    pub target_branch: Option<String>,
}

#[derive(Debug)]
pub enum AgentStatus {
    Success,
    Failure(String),
}

pub trait BranchStrategy: Send + Sync {
    fn prepare(&self, repo_root: &Path) -> Result<Workspace, GitError>;
    fn finish(&self, ws: Workspace, status: AgentStatus) -> Result<(), GitError>;
    fn name(&self) -> &str;
}

// -- git invocation -----------------------------------------------------------

/// Run `git <args>` with cwd `cwd`. Returns the `Output` on success (exit 0),
/// `GitError::Git` on non-zero exit, `GitError::Io` if git can't be spawned.
pub fn git_cmd<I, S>(args: I, cwd: &Path) -> Result<Output, GitError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let args: Vec<_> = args.into_iter().map(|s| s.as_ref().to_owned()).collect();
    let out = Command::new("git").args(&args).current_dir(cwd).output()?;
    if out.status.success() {
        Ok(out)
    } else {
        let cmd =
            args.iter().map(|a| a.to_string_lossy().into_owned()).collect::<Vec<_>>().join(" ");
        Err(GitError::Git {
            command: cmd,
            stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
            exit: out.status.code().unwrap_or(-1),
        })
    }
}

fn stdout_trimmed(out: Output) -> String {
    String::from_utf8_lossy(&out.stdout).trim_end().to_owned()
}

fn ensure_repo(repo_root: &Path) -> Result<(), GitError> {
    if !repo_root.exists() {
        return Err(GitError::NotARepo(repo_root.to_owned()));
    }
    match git_cmd(["rev-parse", "--is-inside-work-tree"], repo_root) {
        Ok(out) => {
            if stdout_trimmed(out) == "true" {
                Ok(())
            } else {
                Err(GitError::NotARepo(repo_root.to_owned()))
            }
        }
        Err(_) => Err(GitError::NotARepo(repo_root.to_owned())),
    }
}

/// Current branch name (`git symbolic-ref --short HEAD`). Errors on detached HEAD.
///
/// The result is a derived name that later flows into a `checkout` target, so it
/// is run through [`valid_ref_name`] — a real checked-out branch always passes.
fn current_branch(repo_root: &Path) -> Result<String, GitError> {
    let name = git_cmd(["symbolic-ref", "--short", "HEAD"], repo_root)
        .map(stdout_trimmed)
        .map_err(|e| match e {
            GitError::Git { .. } => GitError::Other("detached HEAD: no target branch".into()),
            other => other,
        })?;
    valid_ref_name(&name)?;
    Ok(name)
}

fn is_dirty(repo_root: &Path) -> Result<bool, GitError> {
    let out = git_cmd(["status", "--porcelain"], repo_root)?;
    Ok(!out.stdout.iter().all(u8::is_ascii_whitespace))
}

/// Does a local branch by this name exist? (`git show-ref --verify --quiet refs/heads/<name>`)
fn branch_exists(repo_root: &Path, name: &str) -> Result<bool, GitError> {
    let out = Command::new("git")
        .args(["show-ref", "--verify", "--quiet", &format!("refs/heads/{name}")])
        .current_dir(repo_root)
        .output()?;
    Ok(out.status.success())
}

/// Conservative branch/ref name check — a subset of `git check-ref-format`.
///
/// Defense in depth alongside the `--` separators at the call sites: even where
/// git would parse a `-`-leading operand as a value, we refuse names that could
/// be misparsed as an option or that aren't a well-formed single ref. Rejects
/// names that are empty, begin with `-`, contain whitespace or control chars,
/// contain `..`, begin/end with `/`, contain any of `~ ^ : ? * [ \` or the
/// sequence `@{`, or end with `.lock`. Legitimate names (e.g. `feature/foo`,
/// the internally generated `agent-…` names) pass unchanged.
fn valid_ref_name(name: &str) -> Result<(), GitError> {
    let reject = |why: &str| Err(GitError::Other(format!("invalid ref name {name:?}: {why}")));
    if name.is_empty() {
        return reject("empty");
    }
    if name.starts_with('-') {
        return reject("begins with '-'");
    }
    if name.starts_with('/') || name.ends_with('/') {
        return reject("begins or ends with '/'");
    }
    if name.ends_with(".lock") {
        return reject("ends with '.lock'");
    }
    if name.contains("..") {
        return reject("contains '..'");
    }
    if name.contains("@{") {
        return reject("contains '@{'");
    }
    for c in name.chars() {
        if c.is_whitespace() || c.is_control() {
            return reject("contains whitespace or a control character");
        }
        if matches!(c, '~' | '^' | ':' | '?' | '*' | '[' | '\\') {
            return reject("contains a forbidden character (~^:?*[\\)");
        }
    }
    Ok(())
}

fn worktree_root(repo_root: &Path) -> PathBuf {
    repo_root.join(".moth").join("worktrees")
}

/// `agent-YYYYMMDD-HHMMSS-XXXX` where XXXX is 4 lowercase hex chars derived
/// from pid + nanoseconds. Deterministic enough to be unique per invocation,
/// not cryptographically random — collisions just fail `worktree add`.
fn gen_branch_name() -> String {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    let secs = now.as_secs();
    let nanos = now.subsec_nanos();
    let (y, mo, d, h, mi, s) = unix_to_ymdhms(secs);
    let pid = std::process::id();
    let suffix = (pid.wrapping_mul(2_654_435_761).wrapping_add(nanos)) & 0xFFFF;
    format!("agent-{y:04}{mo:02}{d:02}-{h:02}{mi:02}{s:02}-{suffix:04x}")
}

/// Cheap UTC unix → Y/M/D/h/m/s. Good enough for branch names, no chrono dep.
fn unix_to_ymdhms(secs: u64) -> (u32, u32, u32, u32, u32, u32) {
    let s = (secs % 60) as u32;
    let m = ((secs / 60) % 60) as u32;
    let h = ((secs / 3600) % 24) as u32;
    let mut days = (secs / 86_400) as i64;
    // Jan 1 1970 was a Thursday; we don't care about weekday here.
    let mut year: i64 = 1970;
    loop {
        let leap = is_leap(year);
        let yd = if leap { 366 } else { 365 };
        if days < yd {
            break;
        }
        days -= yd;
        year += 1;
    }
    let leap = is_leap(year);
    let dim = [31, if leap { 29 } else { 28 }, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    let mut month = 0usize;
    while month < 12 && days >= dim[month] {
        days -= dim[month];
        month += 1;
    }
    (year as u32, (month + 1) as u32, (days + 1) as u32, h, m, s)
}

fn is_leap(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

// -- HeadStrategy -------------------------------------------------------------

/// Agent works in `repo_root` directly. No worktree, no branch indirection.
/// Refuses to start on a dirty tree — the agent would clobber the host's work.
pub struct HeadStrategy;

impl BranchStrategy for HeadStrategy {
    fn prepare(&self, repo_root: &Path) -> Result<Workspace, GitError> {
        ensure_repo(repo_root)?;
        if is_dirty(repo_root)? {
            return Err(GitError::DirtyTree);
        }
        let branch = current_branch(repo_root)?;
        Ok(Workspace { path: repo_root.to_owned(), source_branch: branch, target_branch: None })
    }

    fn finish(&self, _ws: Workspace, _status: AgentStatus) -> Result<(), GitError> {
        Ok(())
    }

    fn name(&self) -> &str {
        "head"
    }
}

// -- MergeToHeadStrategy ------------------------------------------------------

/// Temp branch in a worktree; merged back to the host's branch on success.
/// On failure, the worktree is left in place and its path is printed to stderr
/// so the operator can inspect.
pub struct MergeToHeadStrategy;

impl BranchStrategy for MergeToHeadStrategy {
    fn prepare(&self, repo_root: &Path) -> Result<Workspace, GitError> {
        ensure_repo(repo_root)?;
        let target = current_branch(repo_root)?;
        let branch = gen_branch_name();
        let wt_dir = worktree_root(repo_root);
        std::fs::create_dir_all(&wt_dir)?;
        let wt_path = wt_dir.join(&branch);
        git_cmd(
            [
                OsStr::new("worktree"),
                OsStr::new("add"),
                OsStr::new("-b"),
                OsStr::new(&branch),
                // `--` ends option parsing: the path and commit-ish that follow
                // can't be misread as flags even if they began with `-`.
                OsStr::new("--"),
                wt_path.as_os_str(),
                OsStr::new("HEAD"),
            ],
            repo_root,
        )?;
        Ok(Workspace { path: wt_path, source_branch: branch, target_branch: Some(target) })
    }

    fn finish(&self, ws: Workspace, status: AgentStatus) -> Result<(), GitError> {
        let repo_root = repo_root_of(&ws.path)?;
        let target = ws
            .target_branch
            .clone()
            .ok_or_else(|| GitError::Other("merge-to-head: missing target_branch".into()))?;

        match status {
            AgentStatus::Success => {
                // `git checkout <branch>` gives `--` pathspec semantics, so a
                // leading-`-` operand can't be neutralised with a separator here;
                // validate the target name instead (belt-and-braces — it was
                // already validated when captured via `current_branch`).
                valid_ref_name(&target)?;
                git_cmd(["checkout", &target], &repo_root)?;
                git_cmd(
                    [
                        "merge",
                        "--no-ff",
                        "-m",
                        &format!("Merge {} into {}", ws.source_branch, target),
                        // `--` ends options: the branch operand can't be a flag.
                        "--",
                        &ws.source_branch,
                    ],
                    &repo_root,
                )?;
                git_cmd(
                    [
                        OsStr::new("worktree"),
                        OsStr::new("remove"),
                        OsStr::new("--force"),
                        OsStr::new("--"),
                        ws.path.as_os_str(),
                    ],
                    &repo_root,
                )?;
                // Best-effort branch cleanup; commits are reachable from target.
                let _ = git_cmd(["branch", "-D", "--", &ws.source_branch], &repo_root);
                Ok(())
            }
            AgentStatus::Failure(reason) => {
                eprintln!(
                    "merge-to-head: agent failed ({reason}); worktree preserved at {}",
                    ws.path.display()
                );
                Ok(())
            }
        }
    }

    fn name(&self) -> &str {
        "merge-to-head"
    }
}

// -- BranchStrategy (named) ---------------------------------------------------

/// Commits land on a caller-named branch. The branch is the persistence point —
/// no merge into the host's HEAD. Worktree removed only on success; on failure
/// it's left for inspection.
pub struct Branch {
    pub name: String,
}

impl Branch {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

impl BranchStrategy for Branch {
    fn prepare(&self, repo_root: &Path) -> Result<Workspace, GitError> {
        // Reject a hostile branch name before we shell out to git at all — the
        // name reaches `worktree add -B <name> … <start_point>`, and when the
        // branch already exists the start-point operand *is* the name.
        valid_ref_name(&self.name)?;
        ensure_repo(repo_root)?;
        let wt_dir = worktree_root(repo_root);
        std::fs::create_dir_all(&wt_dir)?;
        let wt_path = wt_dir.join(&self.name);

        // -B creates or resets. If the branch already exists and is checked out
        // somewhere else, git will refuse — by design.
        let start_point = if branch_exists(repo_root, &self.name)? {
            self.name.clone()
        } else {
            "HEAD".to_owned()
        };
        git_cmd(
            [
                OsStr::new("worktree"),
                OsStr::new("add"),
                OsStr::new("-B"),
                OsStr::new(&self.name),
                // `--` ends option parsing for the path and start-point operands.
                OsStr::new("--"),
                wt_path.as_os_str(),
                OsStr::new(&start_point),
            ],
            repo_root,
        )?;
        Ok(Workspace { path: wt_path, source_branch: self.name.clone(), target_branch: None })
    }

    fn finish(&self, ws: Workspace, status: AgentStatus) -> Result<(), GitError> {
        match status {
            AgentStatus::Success => {
                let repo_root = repo_root_of(&ws.path)?;
                git_cmd(
                    [
                        OsStr::new("worktree"),
                        OsStr::new("remove"),
                        OsStr::new("--force"),
                        OsStr::new("--"),
                        ws.path.as_os_str(),
                    ],
                    &repo_root,
                )?;
                Ok(())
            }
            AgentStatus::Failure(reason) => {
                eprintln!(
                    "branch[{}]: agent failed ({reason}); worktree preserved at {}",
                    self.name,
                    ws.path.display()
                );
                Ok(())
            }
        }
    }

    fn name(&self) -> &str {
        "branch"
    }
}

/// Find the main repo root from any worktree path: `git rev-parse --path-format=absolute --git-common-dir`
/// gives us the shared `.git` dir; its parent is the host repo root.
fn repo_root_of(any_inside: &Path) -> Result<PathBuf, GitError> {
    let out = git_cmd(["rev-parse", "--path-format=absolute", "--git-common-dir"], any_inside)?;
    let gitdir = PathBuf::from(stdout_trimmed(out));
    gitdir
        .parent()
        .map(Path::to_owned)
        .ok_or_else(|| GitError::Other("git-common-dir has no parent".into()))
}

// -- tests --------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempRepo {
        path: PathBuf,
    }

    impl TempRepo {
        fn new() -> Self {
            let n = COUNTER.fetch_add(1, Ordering::Relaxed);
            let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
            let path = std::env::temp_dir().join(format!(
                "moth-git-test-{}-{}-{}",
                std::process::id(),
                nanos,
                n
            ));
            std::fs::create_dir_all(&path).unwrap();
            git_cmd(["init", "-b", "main"], &path).unwrap();
            git_cmd(["config", "user.email", "t@t"], &path).unwrap();
            git_cmd(["config", "user.name", "t"], &path).unwrap();
            git_cmd(["config", "commit.gpgsign", "false"], &path).unwrap();
            git_cmd(["commit", "--allow-empty", "-m", "initial"], &path).unwrap();
            TempRepo { path }
        }

        fn write(&self, rel: &str, contents: &str) {
            let p = self.path.join(rel);
            if let Some(parent) = p.parent() {
                std::fs::create_dir_all(parent).unwrap();
            }
            std::fs::write(p, contents).unwrap();
        }
    }

    impl Drop for TempRepo {
        fn drop(&mut self) {
            // Best-effort: kill any worktrees first, then nuke the dir.
            let _ = git_cmd(["worktree", "prune"], &self.path);
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    fn commit_file(cwd: &Path, name: &str, contents: &str, msg: &str) {
        std::fs::write(cwd.join(name), contents).unwrap();
        git_cmd(["add", name], cwd).unwrap();
        git_cmd(["commit", "-m", msg], cwd).unwrap();
    }

    fn log_oneline(cwd: &Path, branch: &str) -> String {
        stdout_trimmed(git_cmd(["log", "--oneline", branch], cwd).unwrap())
    }

    // ---- HeadStrategy ----

    #[test]
    fn head_clean_repo_returns_repo_root() {
        let r = TempRepo::new();
        let ws = HeadStrategy.prepare(&r.path).unwrap();
        assert_eq!(ws.path, r.path);
        assert_eq!(ws.source_branch, "main");
        assert!(ws.target_branch.is_none());
        HeadStrategy.finish(ws, AgentStatus::Success).unwrap();
    }

    #[test]
    fn head_refuses_dirty_tree() {
        let r = TempRepo::new();
        r.write("dirty.txt", "uncommitted");
        let err = HeadStrategy.prepare(&r.path).unwrap_err();
        assert!(matches!(err, GitError::DirtyTree), "got {err:?}");
    }

    #[test]
    fn head_finish_is_noop_on_failure() {
        let r = TempRepo::new();
        let ws = HeadStrategy.prepare(&r.path).unwrap();
        HeadStrategy.finish(ws, AgentStatus::Failure("boom".into())).unwrap();
        assert!(r.path.join(".git").exists());
    }

    // ---- MergeToHeadStrategy ----

    #[test]
    fn merge_to_head_happy_path() {
        let r = TempRepo::new();
        let ws = MergeToHeadStrategy.prepare(&r.path).unwrap();
        assert_eq!(ws.target_branch.as_deref(), Some("main"));
        assert!(ws.path.starts_with(r.path.join(".moth/worktrees")));
        assert!(ws.path.exists());

        commit_file(&ws.path, "agent.txt", "hi", "agent work");
        let wt_path = ws.path.clone();
        let temp_branch = ws.source_branch.clone();

        MergeToHeadStrategy.finish(ws, AgentStatus::Success).unwrap();

        // Worktree gone, agent's commit reachable from main.
        assert!(!wt_path.exists(), "worktree should be removed on success");
        let log = log_oneline(&r.path, "main");
        assert!(log.contains("agent work"), "main log missing commit: {log}");
        assert!(log.contains("Merge"), "expected merge commit, got: {log}");

        // Temp branch cleaned up.
        assert!(!branch_exists(&r.path, &temp_branch).unwrap());
    }

    #[test]
    fn merge_to_head_failure_preserves_worktree() {
        let r = TempRepo::new();
        let ws = MergeToHeadStrategy.prepare(&r.path).unwrap();
        let wt_path = ws.path.clone();
        let temp_branch = ws.source_branch.clone();
        commit_file(&wt_path, "wip.txt", "wip", "wip commit");

        MergeToHeadStrategy.finish(ws, AgentStatus::Failure("test".into())).unwrap();

        assert!(wt_path.exists(), "worktree should be kept on failure");
        // Branch should still exist for inspection.
        assert!(branch_exists(&r.path, &temp_branch).unwrap());
        // main should NOT contain the wip commit.
        let log = log_oneline(&r.path, "main");
        assert!(!log.contains("wip commit"), "wip leaked into main: {log}");
    }

    #[test]
    fn merge_to_head_detached_head_errors() {
        let r = TempRepo::new();
        commit_file(&r.path, "a.txt", "a", "second");
        let head_sha = stdout_trimmed(git_cmd(["rev-parse", "HEAD"], &r.path).unwrap());
        git_cmd(["checkout", "--detach", &head_sha], &r.path).unwrap();

        let err = MergeToHeadStrategy.prepare(&r.path).unwrap_err();
        match err {
            GitError::Other(s) => assert!(s.contains("detached"), "got: {s}"),
            other => panic!("expected Other(detached…), got {other:?}"),
        }
    }

    #[test]
    fn merge_to_head_worktree_path_layout() {
        let r = TempRepo::new();
        let ws = MergeToHeadStrategy.prepare(&r.path).unwrap();
        let parent = ws.path.parent().unwrap();
        assert!(parent.ends_with(".moth/worktrees"));
        assert!(ws.source_branch.starts_with("agent-"));
        MergeToHeadStrategy.finish(ws, AgentStatus::Success).unwrap();
    }

    // ---- Branch (named) ----

    #[test]
    fn branch_new_name_creates_branch_and_worktree() {
        let r = TempRepo::new();
        let s = Branch::new("feature/x");
        let ws = s.prepare(&r.path).unwrap();
        assert!(ws.path.exists());
        assert_eq!(ws.source_branch, "feature/x");
        assert!(ws.target_branch.is_none());
        assert!(branch_exists(&r.path, "feature/x").unwrap());
        s.finish(ws, AgentStatus::Success).unwrap();
    }

    #[test]
    fn branch_existing_name_is_reused() {
        let r = TempRepo::new();
        // Create the branch first via normal git.
        git_cmd(["branch", "agent-work"], &r.path).unwrap();

        let s = Branch::new("agent-work");
        let ws = s.prepare(&r.path).unwrap();
        commit_file(&ws.path, "x.txt", "x", "on existing branch");
        s.finish(ws, AgentStatus::Success).unwrap();

        // Commit landed on agent-work, not on main.
        let agent_log = log_oneline(&r.path, "agent-work");
        let main_log = log_oneline(&r.path, "main");
        assert!(agent_log.contains("on existing branch"));
        assert!(!main_log.contains("on existing branch"));
    }

    #[test]
    fn branch_commits_land_on_named_branch() {
        let r = TempRepo::new();
        let s = Branch::new("agent");
        let ws = s.prepare(&r.path).unwrap();
        commit_file(&ws.path, "f.txt", "f", "agent commit");
        s.finish(ws, AgentStatus::Success).unwrap();

        let agent_log = log_oneline(&r.path, "agent");
        assert!(agent_log.contains("agent commit"));
        let main_log = log_oneline(&r.path, "main");
        assert!(!main_log.contains("agent commit"));
        // Branch persists after success.
        assert!(branch_exists(&r.path, "agent").unwrap());
    }

    #[test]
    fn branch_failure_preserves_worktree() {
        let r = TempRepo::new();
        let s = Branch::new("inspect");
        let ws = s.prepare(&r.path).unwrap();
        let wt = ws.path.clone();
        s.finish(ws, AgentStatus::Failure("nope".into())).unwrap();
        assert!(wt.exists());
        assert!(branch_exists(&r.path, "inspect").unwrap());
    }

    // ---- general ----

    #[test]
    fn not_a_repo_errors() {
        let dir = std::env::temp_dir().join(format!("moth-not-a-repo-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let err = HeadStrategy.prepare(&dir).unwrap_err();
        assert!(matches!(err, GitError::NotARepo(_)), "got {err:?}");
        let err = MergeToHeadStrategy.prepare(&dir).unwrap_err();
        assert!(matches!(err, GitError::NotARepo(_)), "got {err:?}");
        let err = Branch::new("x").prepare(&dir).unwrap_err();
        assert!(matches!(err, GitError::NotARepo(_)), "got {err:?}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn strategy_names() {
        assert_eq!(HeadStrategy.name(), "head");
        assert_eq!(MergeToHeadStrategy.name(), "merge-to-head");
        assert_eq!(Branch::new("x").name(), "branch");
    }

    #[test]
    fn gen_branch_name_format() {
        let n = gen_branch_name();
        assert!(n.starts_with("agent-"));
        // agent-YYYYMMDD-HHMMSS-XXXX => 6 + 8 + 1 + 6 + 1 + 4 = 26
        assert_eq!(n.len(), 26, "unexpected: {n}");
        let parts: Vec<&str> = n.split('-').collect();
        assert_eq!(parts.len(), 4);
        assert_eq!(parts[0], "agent");
        assert_eq!(parts[1].len(), 8);
        assert_eq!(parts[2].len(), 6);
        assert_eq!(parts[3].len(), 4);
    }

    #[test]
    fn git_cmd_propagates_failure() {
        let r = TempRepo::new();
        let err = git_cmd(["this-is-not-a-git-subcommand"], &r.path).unwrap_err();
        match err {
            GitError::Git { exit, .. } => assert!(exit != 0),
            other => panic!("expected Git, got {other:?}"),
        }
    }

    #[test]
    fn unix_to_ymdhms_known_epochs() {
        // 1970-01-01 00:00:00
        assert_eq!(unix_to_ymdhms(0), (1970, 1, 1, 0, 0, 0));
        // 2000-01-01 00:00:00 UTC = 946684800
        assert_eq!(unix_to_ymdhms(946_684_800), (2000, 1, 1, 0, 0, 0));
        // 2024-02-29 (leap day) 12:24:56 UTC
        assert_eq!(unix_to_ymdhms(1_709_209_496), (2024, 2, 29, 12, 24, 56));
    }

    // ---- ref-name validation (L4) ----

    #[test]
    fn valid_ref_name_rejects_hostile() {
        // Option-injection vectors and other malformed names must be rejected.
        for bad in [
            "",                // empty
            "--upload-pack=x", // option injection
            "-X",              // leading dash
            "-",               // just a dash
            " ",               // whitespace
            "a b",             // embedded space
            "a\tb",            // control/whitespace
            "a\nb",            // control/whitespace
            "a..b",            // double-dot range
            "/leading",        // leading slash
            "trailing/",       // trailing slash
            "foo.lock",        // .lock suffix
            "ref@{0}",         // @{ reflog syntax
            "a~1",             // tilde
            "a^",              // caret
            "a:b",             // colon
            "a?b",             // glob ?
            "a*b",             // glob *
            "a[b",             // glob [
            "a\\b",            // backslash
        ] {
            assert!(valid_ref_name(bad).is_err(), "expected {bad:?} to be rejected",);
            match valid_ref_name(bad) {
                Err(GitError::Other(m)) => assert!(m.contains("invalid ref name"), "msg: {m}"),
                other => panic!("expected GitError::Other for {bad:?}, got {other:?}"),
            }
        }
    }

    #[test]
    fn valid_ref_name_accepts_legit() {
        for good in ["main", "feature/foo", "feature/x", "agent-work", "agent", "v1.2.3", "a.b"] {
            assert!(valid_ref_name(good).is_ok(), "expected {good:?} to be accepted");
        }
        // The internally generated names must always pass.
        let generated = gen_branch_name();
        assert!(valid_ref_name(&generated).is_ok(), "generated name {generated:?} was rejected",);
    }

    #[test]
    fn branch_hostile_name_errors_before_shelling_out() {
        // Validation is the first thing `prepare` does, ahead of `ensure_repo`,
        // so a hostile name fails with the validation error even when pointed at
        // a path that is not a repo — proving no git invocation with the name
        // ever runs.
        let not_a_repo = std::env::temp_dir().join("moth-git-l4-nonexistent-xyzzy");
        let _ = std::fs::remove_dir_all(&not_a_repo);
        let err = Branch::new("--upload-pack=/tmp/x").prepare(&not_a_repo).unwrap_err();
        match err {
            GitError::Other(m) => assert!(m.contains("invalid ref name"), "msg: {m}"),
            other => panic!("expected validation error, got {other:?}"),
        }
    }

    #[test]
    fn branch_hostile_name_errors_in_real_repo() {
        // Same guarantee inside a real repo: no worktree is created for the name.
        let r = TempRepo::new();
        let err = Branch::new("-X").prepare(&r.path).unwrap_err();
        assert!(matches!(err, GitError::Other(_)), "got {err:?}");
        assert!(
            !r.path.join(".moth/worktrees/-X").exists(),
            "hostile name should not have produced a worktree",
        );
    }
}

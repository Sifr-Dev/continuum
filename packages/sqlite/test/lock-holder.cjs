// Test fixture: holds the SQLite write lock in a *separate process* so that a
// test in the parent process can observe real cross-process lock contention
// without blocking its own event loop.
//
// Usage: node lock-holder.cjs <db-file> <hold-ms> [run-id]
// Prints "locked" on stdout once the write transaction is open, then commits
// after <hold-ms> and exits 0.

const Database = require("better-sqlite3");

const [filename, holdMsRaw, runId = "run"] = process.argv.slice(2);
if (!filename || !holdMsRaw) {
  console.error("usage: node lock-holder.cjs <db-file> <hold-ms> [run-id]");
  process.exit(2);
}

const db = new Database(filename);
db.pragma("journal_mode = WAL");
db.exec(`
  CREATE TABLE IF NOT EXISTS continuum_leases (
    run_id TEXT PRIMARY KEY,
    worker_id TEXT NOT NULL,
    fencing_token INTEGER NOT NULL,
    expires_at INTEGER NOT NULL
  );
`);

db.exec("BEGIN IMMEDIATE");
// Expire the lease so that, once the lock clears, a challenger is allowed to
// take ownership. Holding the write lock is what makes the race interesting.
db.prepare("UPDATE continuum_leases SET expires_at = 0 WHERE run_id = ?").run(runId);

process.stdout.write("locked\n");

setTimeout(() => {
  try {
    db.exec("COMMIT");
  } catch {
    // The test asserts on the challenger, not on this process.
  }
  db.close();
  process.exit(0);
}, Number(holdMsRaw));

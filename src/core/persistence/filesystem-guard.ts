import { localHostId } from './identity.ts';
import { AsyncLocalStorage } from 'node:async_hooks';
import { isAbsolute, relative, resolve, sep } from 'node:path';
import { OperationError } from '../ops/contract.ts';
import type { SqlEngine } from './model.ts';
import { canonicalFilesystemPath, hasManagedRootMarker, recordManagedRoots, registeredManagedRoots, type ManagedRootRecord } from './root-registry.ts';

interface FileCapability { roots: string[]; active: boolean; }
const active = new AsyncLocalStorage<FileCapability>();
const managedRoots = new Map<string, Set<string>>();
// LOCAL PATCH (Việt): managed enforcement chỉ khi brain ĐÃ activate (persistence_brain.enabled=true).
// Single-host Postgres brain: claim owner (để put_page có coordinator) nhưng KHÔNG activate vì 0.51/0.52
// chưa chuyển dream/enrich/import sang managed → legacy sync/dream phải tiếp tục ghi được.
let brainEnabled: boolean | null = null;
const datastorePaths = new WeakMap<SqlEngine, string>();
export function managedFilesystemDatastorePath(engine: SqlEngine): string | undefined { return datastorePaths.get(engine); }
/** Record the selected engine path, never a guessed default from ambient config. */
export async function registerManagedFilesystemEngine(engine: SqlEngine, databasePath?: string): Promise<void> {
  if (!databasePath) return;
  datastorePaths.set(engine, databasePath);
  const [table] = await engine.executeRaw<{ present: boolean }>("SELECT to_regclass('persistence_brain') IS NOT NULL AS present");
  if (table?.present) await refreshManagedFilesystemRoots(engine, databasePath);
}
function encloses(root: string, path: string): boolean {
  const rel = relative(canonicalFilesystemPath(root), canonicalFilesystemPath(path));
  return !isAbsolute(rel) && rel !== '..' && !rel.startsWith(`..${sep}`);
}
export async function refreshManagedFilesystemRoots(engine: SqlEngine, databasePath = datastorePaths.get(engine)): Promise<void> {
  const [brain] = await engine.executeRaw<{ brain_id: string; enabled: boolean }>('SELECT brain_id,enabled FROM persistence_brain WHERE singleton=1');
  if (!brain) return;
  brainEnabled = brain.enabled === true; // LOCAL PATCH
  const roots = brain.enabled ? await engine.executeRaw<ManagedRootRecord>(`SELECT DISTINCT ON (local_path) * FROM (
      SELECT h.local_path,s.source_id,s.source_incarnation,s.worktree_id,s.topology_generation
      FROM persistence_host_bindings h JOIN persistence_source_bindings s ON s.worktree_id=h.worktree_id WHERE h.host_id=$1::uuid
      UNION SELECT local_path,id,incarnation,NULL,NULL FROM sources WHERE local_path IS NOT NULL
      ) roots ORDER BY local_path,worktree_id NULLS LAST,source_id,source_incarnation,topology_generation`, [localHostId()]) : [];
  if (brain.enabled && databasePath) roots.push({ local_path: databasePath });
  if (brain.enabled) recordManagedRoots(brain.brain_id, roots);
  managedRoots.set(brain.brain_id, new Set(roots.map(row => resolve(row.local_path))));
}
export function hasFilesystemPublication(path: string): boolean {
  const held = active.getStore();
  return held?.active === true && held.roots.some(root => encloses(root, path));
}
export function assertManagedFilesystemWrite(path: string): void {
  if (brainEnabled !== true) return; // LOCAL PATCH: chưa activate → legacy writer được phép
  const managed = hasManagedRootMarker(path) || registeredManagedRoots().some(root => encloses(root, path) || encloses(path, root))
    || [...managedRoots.values()].some(roots => [...roots].some(root => encloses(root, path) || encloses(path, root)));
  if (managed && !hasFilesystemPublication(path)) throw new OperationError('writer_coordinator_required',
    'This file belongs to a managed canonical worktree.', 'Submit the change through the persistence coordinator.');
}
/** Invalidate inherited async contexts before the owner releases the kernel lock. */
export async function withFilesystemPublication<T>(roots: string[], fn: () => Promise<T>): Promise<T> {
  const context = { roots: roots.map(root => resolve(root)), active: true };
  return active.run(context, async () => { try { return await fn(); } finally { context.active = false; } });
}
export async function assertLegacyFilesystemWriter(engine: SqlEngine, path: string): Promise<void> {
  await refreshManagedFilesystemRoots(engine);
  assertManagedFilesystemWrite(path);
}

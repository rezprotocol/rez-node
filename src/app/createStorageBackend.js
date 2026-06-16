import { FsStorageProvider } from "../storage/fs/FsStorageProvider.js";

/**
 * Build the storage backend selected by `resolved.storage.backend` ("fs" | "pg").
 *
 * Returns a small handle, NOT a provider, because startRezNode needs the
 * two-phase construction the encrypted-store design requires:
 *   - `makeProvider(null)` — an at-rest-plaintext provider used ONLY to bootstrap
 *     the node identity (the storage encryption key is derived FROM that identity,
 *     so it cannot exist yet — same chicken-and-egg the Fs path already has).
 *   - `makeProvider(key)` — the encrypted provider every other subsystem uses.
 * Both providers share one underlying resource (the Fs rootDir, or a single Pg
 * connection pool), and `close()` tears it down on shutdown.
 *
 * The Postgres modules are lazy-imported so an fs-only node (e.g. the desktop
 * sidecar) never loads `pg`, mirroring the CLI's `migrate` command.
 *
 * @param {{ resolved: object }} opts — validated node config
 * @returns {Promise<{ backend: string, makeProvider: (encryptionKey?: Uint8Array|null) => object, close: () => Promise<void> }>}
 */
export async function createStorageBackend({ resolved }) {
  const backend = resolved.storage.backend === "pg" ? "pg" : "fs";

  if (backend === "pg") {
    const { PgConnection } = await import("../storage/pg/PgConnection.js");
    const { PgStorageProvider } = await import("../storage/pg/PgStorageProvider.js");
    const connection = new PgConnection({ connectionString: resolved.storage.pg.connectionString });

    if (resolved.storage.pg.migrateOnBoot) {
      const { MigrationRunner } = await import("../storage/pg/MigrationRunner.js");
      try {
        const result = await new MigrationRunner({ connection }).migrate();
        console.log(
          `[NODE] storage backend=pg migrated: ships=${result.shipped}, appliedNow=[${result.appliedNow.join(",")}]`,
        );
      } catch (err) {
        // A failed boot migration must not leak the pool. Close, then re-throw so
        // startup fails loudly rather than running against an unmigrated schema.
        await connection.close().catch((closeErr) => {
          console.error(
            "[NODE] failed to close pg connection after migration error: "
              + (closeErr && closeErr.message ? closeErr.message : closeErr),
          );
        });
        throw err;
      }
    }

    // HONESTY GUARD: shared storage, atomic inbox claims (PgInboxClaimRegistry),
    // and atomic settlement (PgSettlementProvider) are now cluster-correct. But
    // message DELIVERY is not yet cluster-safe: the LivenessBus is not wired into
    // the running node and delivery still pushes over the local socket instead of
    // persist-then-notify against the durable home log (S2). So a client that
    // reconnects to a DIFFERENT node can still miss buffered mail until S2 lands.
    console.warn(
      "[NODE] storage backend=pg: storage, inbox claims, and settlement are "
        + "cluster-correct, but message DELIVERY is still single-node (LivenessBus "
        + "+ persist-then-notify are S2). Multi-node delivery is not yet lossless.",
    );

    return {
      backend: "pg",
      makeProvider(encryptionKey = null) {
        return new PgStorageProvider({ connection, encryptionKey });
      },
      async close() {
        await connection.close();
      },
    };
  }

  return {
    backend: "fs",
    makeProvider(encryptionKey = null) {
      return new FsStorageProvider({ rootDir: resolved.storage.dataDir, encryptionKey });
    },
    async close() {
      // Filesystem provider owns no long-lived handle to release.
    },
  };
}

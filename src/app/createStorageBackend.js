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

    // HONESTY GUARD: say which liveness contract this process is actually using.
    // Durable reconnect-drain remains correct either way; Redis adds live
    // cross-node socket wakeups.
    if (resolved.redis && resolved.redis.url) {
      console.log(
        "[NODE] storage backend=pg with Redis liveness: shared durable delivery "
          + "and real-time cross-node wakeups enabled.",
      );
    } else {
      console.warn(
        "[NODE] storage backend=pg without Redis liveness: durable reconnect-drain "
          + "is enabled, but real-time cross-node wakeups are disabled.",
      );
    }

    return {
      backend: "pg",
      makeProvider(encryptionKey = null) {
        return new PgStorageProvider({ connection, encryptionKey });
      },
      async close() {
        await connection.close();
      },
      async checkReadiness() {
        await connection.query("SELECT 1");
        return true;
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
    async checkReadiness() {
      return true;
    },
  };
}

import { PgConnection } from "../../src/storage/pg/PgConnection.js";

// Postgres integration test files share ONE dev database, but each TRUNCATEs/
// DELETEs whole tables for a clean slate — which races other files under the
// node:test default of running files in PARALLEL. Give each file its own schema
// (search_path pinned on every pooled connection) so its DDL/DML is fully
// isolated; then the suite is parallel-safe instead of serial-only.

function assertSafeSchema(schemaName) {
  if (typeof schemaName !== "string" || !/^[a-z0-9_]+$/.test(schemaName)) {
    throw new Error(`unsafe test schema name: ${schemaName}`);
  }
}

/**
 * Create (if needed) a dedicated schema and return a PgConnection whose every
 * pooled connection has search_path pinned to it. All unqualified table refs in
 * migrations + stores then resolve inside this schema.
 * @returns {Promise<import("../../src/storage/pg/PgConnection.js").PgConnection>}
 */
export async function createIsolatedPgConnection(pgUrl, schemaName) {
  assertSafeSchema(schemaName);
  const admin = new PgConnection({ connectionString: pgUrl });
  try {
    await admin.query(`DROP SCHEMA IF EXISTS ${schemaName} CASCADE`);
    await admin.query(`CREATE SCHEMA ${schemaName}`);
  } finally {
    await admin.close();
  }
  return new PgConnection({
    connectionString: pgUrl,
    poolConfig: { options: `-c search_path=${schemaName}` },
  });
}

/** Drop a test schema and everything in it (call in t.after). */
export async function dropSchema(pgUrl, schemaName) {
  assertSafeSchema(schemaName);
  const admin = new PgConnection({ connectionString: pgUrl });
  try {
    await admin.query(`DROP SCHEMA IF EXISTS ${schemaName} CASCADE`);
  } finally {
    await admin.close();
  }
}

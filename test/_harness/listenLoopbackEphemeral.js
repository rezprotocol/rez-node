import assert from "node:assert/strict";

export async function listenLoopbackEphemeral(server) {
  await new Promise((resolve, reject) => {
    const onError = (err) => {
      server.off("listening", onListening);
      reject(err);
    };
    const onListening = () => {
      server.off("error", onError);
      resolve();
    };
    server.once("error", onError);
    server.once("listening", onListening);
    server.listen({ host: "127.0.0.1", port: 0 });
  });

  const addr = server.address();
  assert.ok(addr && typeof addr === "object", "expected object listen address");
  assert.equal(addr.address, "127.0.0.1");
  assert.ok(Number.isInteger(addr.port) && addr.port > 0, "expected ephemeral port assignment");

  return {
    host: addr.address,
    port: addr.port,
    async close() {
      await new Promise((resolve) => {
        if (!server.listening) {
          resolve();
          return;
        }
        server.close(() => resolve());
      });
    },
  };
}

export { createServerServices, createPerAccountServices } from "./createServerServices.js";
// PeerLinkService + canonicalPayloadBytesV1 moved to rez-sdk as part of the
// Shape A migration (docs/CAPABILITY_MODEL.md). rez-node re-exports them for
// any consumer still importing from "@rezprotocol/node" — but new code should
// pull them directly from "@rezprotocol/sdk/peer-link".
export { PeerLinkService, canonicalPayloadBytesV1 } from "@rezprotocol/sdk/peer-link";

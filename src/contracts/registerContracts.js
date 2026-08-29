import { ContractRegistry } from "./ContractRegistry.js";
import { SessionHello } from "./records/SessionHello.js";
import { SessionReadyEvent } from "./records/SessionReadyEvent.js";
import { WsErrorEvent } from "./records/WsErrorEvent.js";
import { MailboxDepositRequest } from "./records/MailboxDepositRequest.js";
import { MailboxDepositResponse } from "./records/MailboxDepositResponse.js";
import { MailboxListRequest } from "./records/MailboxListRequest.js";
import { MailboxListResponse } from "./records/MailboxListResponse.js";
import { MailboxFetchRequest } from "./records/MailboxFetchRequest.js";
import { MailboxFetchResponse } from "./records/MailboxFetchResponse.js";
import { MailboxAckRequest } from "./records/MailboxAckRequest.js";
import { MailboxAckResponse } from "./records/MailboxAckResponse.js";
import { MailboxCursorAckRequest } from "./records/MailboxCursorAckRequest.js";
import { MailboxCursorAckResponse } from "./records/MailboxCursorAckResponse.js";
import { MailboxDepositedEvent } from "./records/MailboxDepositedEvent.js";
import { OutboundQueueStatusEvent } from "./records/OutboundQueueStatusEvent.js";
import { InboxClaimRequest } from "./records/InboxClaimRequest.js";
import { InboxClaimResponse } from "./records/InboxClaimResponse.js";
import { DeviceBindRequest } from "./records/DeviceBindRequest.js";
import { DeviceBindResponse } from "./records/DeviceBindResponse.js";
import { NodeStatusRequest } from "./records/NodeStatusRequest.js";
import { NodeStatusResponse } from "./records/NodeStatusResponse.js";
import {
  OutboxLeaseClaimRequest,
  OutboxLeaseClaimResponse,
  OutboxLeasePrepareRequest,
  OutboxLeasePrepareResponse,
  OutboxLeaseReleaseRequest,
  OutboxLeaseReleaseResponse,
  OutboxLeaseFailRequest,
  OutboxLeaseFailResponse,
} from "./records/OutboxLeaseRecords.js";

export function registerContracts(registry) {
  // Session
  registry.register(SessionHello.type, SessionHello);
  registry.register(SessionReadyEvent.type, SessionReadyEvent);
  registry.register(WsErrorEvent.type, WsErrorEvent);

  // Mailbox
  registry.register(MailboxDepositRequest.type, MailboxDepositRequest);
  registry.register(MailboxDepositResponse.type, MailboxDepositResponse);
  registry.register(MailboxListRequest.type, MailboxListRequest);
  registry.register(MailboxListResponse.type, MailboxListResponse);
  registry.register(MailboxFetchRequest.type, MailboxFetchRequest);
  registry.register(MailboxFetchResponse.type, MailboxFetchResponse);
  registry.register(MailboxAckRequest.type, MailboxAckRequest);
  registry.register(MailboxAckResponse.type, MailboxAckResponse);
  registry.register(MailboxCursorAckRequest.type, MailboxCursorAckRequest);
  registry.register(MailboxCursorAckResponse.type, MailboxCursorAckResponse);
  registry.register(MailboxDepositedEvent.type, MailboxDepositedEvent);
  registry.register(OutboundQueueStatusEvent.type, OutboundQueueStatusEvent);

  // Inbox claim (open registration)
  registry.register(InboxClaimRequest.type, InboxClaimRequest);
  registry.register(InboxClaimResponse.type, InboxClaimResponse);

  // Per-device home binding (S2.5 Slice 4)
  registry.register(DeviceBindRequest.type, DeviceBindRequest);
  registry.register(DeviceBindResponse.type, DeviceBindResponse);


  // Node
  registry.register(NodeStatusRequest.type, NodeStatusRequest);
  registry.register(NodeStatusResponse.type, NodeStatusResponse);

  // Authority-state propagation outbox lease lifecycle (P1#3 leaf 3b)
  registry.register(OutboxLeaseClaimRequest.type, OutboxLeaseClaimRequest);
  registry.register(OutboxLeaseClaimResponse.type, OutboxLeaseClaimResponse);
  registry.register(OutboxLeasePrepareRequest.type, OutboxLeasePrepareRequest);
  registry.register(OutboxLeasePrepareResponse.type, OutboxLeasePrepareResponse);
  registry.register(OutboxLeaseReleaseRequest.type, OutboxLeaseReleaseRequest);
  registry.register(OutboxLeaseReleaseResponse.type, OutboxLeaseReleaseResponse);
  registry.register(OutboxLeaseFailRequest.type, OutboxLeaseFailRequest);
  registry.register(OutboxLeaseFailResponse.type, OutboxLeaseFailResponse);

  return registry;
}

export function registerAllContracts(registry = new ContractRegistry()) {
  return registerContracts(registry);
}

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
import { ChannelOpenRequest } from "./records/ChannelOpenRequest.js";
import { ChannelOpenResponse } from "./records/ChannelOpenResponse.js";
import { ChannelCloseRequest } from "./records/ChannelCloseRequest.js";
import { ChannelCloseResponse } from "./records/ChannelCloseResponse.js";
import { ChannelSignalEvent } from "./records/ChannelSignalEvent.js";
import { NodeStatusRequest } from "./records/NodeStatusRequest.js";
import { NodeStatusResponse } from "./records/NodeStatusResponse.js";

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

  // Channel
  registry.register(ChannelOpenRequest.type, ChannelOpenRequest);
  registry.register(ChannelOpenResponse.type, ChannelOpenResponse);
  registry.register(ChannelCloseRequest.type, ChannelCloseRequest);
  registry.register(ChannelCloseResponse.type, ChannelCloseResponse);
  registry.register(ChannelSignalEvent.type, ChannelSignalEvent);

  // Node
  registry.register(NodeStatusRequest.type, NodeStatusRequest);
  registry.register(NodeStatusResponse.type, NodeStatusResponse);

  return registry;
}

export function registerAllContracts(registry = new ContractRegistry()) {
  return registerContracts(registry);
}

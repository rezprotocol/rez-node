# Security Policy

`@rezprotocol/node` is the Rez relay node runtime. Vulnerabilities here can affect every account whose traffic transits a compromised relay.

## Reporting a Vulnerability

**Please do not open public issues for suspected vulnerabilities.**

Use [GitHub Security Advisories](https://github.com/rezprotocol/rez-node/security/advisories/new) to report privately. Only the reporter and the repository maintainers can view the report.

## What to expect

- **Acknowledgement** within 72 hours.
- **Initial assessment** (severity, scope, reproduction) within 7 days.
- **Fix + coordinated disclosure** within 90 days of report — sooner for high-severity issues.
- **Credit** in the security advisory and release notes if you'd like (let us know).

## Scope

In scope:
- Routing or gossip flaws that let a relay impersonate, intercept, or deanonymize other relays
- DHT / capability bypasses that allow unauthorized inbox deposits or withdrawals
- Persistent denial-of-service against relay mesh participants
- Storage-layer issues that allow cross-tenant inbox leakage on hosted nodes
- Authentication bypass in the WebSocket gateway or control socket

Out of scope:
- Volumetric DDoS against a single node operator's infrastructure
- Issues that require operator-level access to the host running the node
- Issues affecting only un-tagged `main`-branch code

## Threat model and posture

Cross-package threat model and audit history live in [`rez-core`](https://github.com/rezprotocol/rez-core):
- [`docs/security.md`](https://github.com/rezprotocol/rez-core/blob/main/docs/security.md) — threat model + guarantees
- [`docs/SECURITY_POSTURE.md`](https://github.com/rezprotocol/rez-core/blob/main/docs/SECURITY_POSTURE.md) — audit history + disclosure posture

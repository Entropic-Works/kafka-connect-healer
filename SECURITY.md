# Security policy

## Supported versions

Until the first release, security fixes target `dev` and follow promotion through
`staging` and `production`. After releases begin, only
the latest non-prerelease version is supported; fixes may require upgrading rather
than receiving a backport. No response-time or support SLA is promised.

## Reporting a vulnerability

Do not publish vulnerabilities, credentials, or exploit details in issues or pull
requests. Use GitHub's private reporting channel:

https://github.com/Entropic-Works/kafka-connect-healer/security/advisories/new

Maintainers must enable private vulnerability reporting in repository settings
before public release. If the private reporting option is unavailable, open an issue
requesting a private contact method without disclosing vulnerability details.

Include the affected version, impact, sanitized reproduction steps, and suggested
mitigations if known. Do not include real credentials or customer data. Please allow
maintainers to investigate and coordinate a fix before public disclosure.

## Deployment precautions

The application can restart Kafka Connect connectors/tasks and change configuration
through its API. API authentication is disabled by default; restrict network access
and enable authentication before exposing it. The application API serves plain HTTP;
terminate TLS at a trusted proxy when needed. Kafka Connect Basic Auth requires HTTPS.
Keep credentials in Secret overlay files, never in committed examples or ConfigMaps.

Run one instance per managed cluster set unless duplicate restart requests are
acceptable. Configuration API updates and backoff history are in-memory only.

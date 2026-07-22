# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in Shardline, please report it privately
by emailing the maintainers at **shardline-security@stexs.net**.

Do not report security vulnerabilities through public GitHub issues, discussions,
or pull requests.

Please include as much of the following information as possible:

- Type of vulnerability
- Full path to the affected source file(s)
- Steps to reproduce
- Proof of concept or exploit code (if available)
- Impact description
- Any suggested fix (if known)

## Response Timeline

- **Acknowledgment**: within 48 hours of reporting
- **Initial assessment**: within 5 business days
- **Fix development**: timeline depends on severity and complexity, communicated
  during the assessment
- **Disclosure**: coordinated with the reporter once a fix is published

## Scope

The following are considered in-scope for security reports:

- The Shardline server and CLI binaries
- Protocol implementations (Xet, Git LFS, OCI Distribution, Bazel HTTP Cache,
  Hugging Face Hub API)
- Authentication and token handling
- Storage backend integrations

The following are considered out-of-scope:

- Denial of service attacks requiring local network access
- Vulnerability in dependencies that is already fixed upstream
- Social engineering attacks against project maintainers

## Supported Versions

| Version | Supported |
|---------|-----------|
| 1.0.x   | ✅ |
| < 1.0   | ❌ |

## Bug Bounty

This project does not currently offer a bug bounty program. Security researchers
who report valid vulnerabilities will be acknowledged in the release notes.

# Automated Dependency Vulnerability Scanning Architecture

## Goals

The dependency security pipeline continuously detects vulnerable, yanked, untrusted, or policy-violating dependencies before they reach production. It applies to the Rust service, GitHub Actions used by the service, and pull requests that modify dependency manifests.

## Architecture

1. **Pull request gate**: `.github/workflows/dependency-security.yml` runs on every pull request to `main`. It executes `cargo audit`, `cargo deny`, GitHub Dependency Review, and CodeQL before merge.
2. **Continuous monitoring**: the same workflow runs on pushes to `main` and `develop`, on a daily UTC schedule, and by manual dispatch for incident response.
3. **Dependency update automation**: `.github/dependabot.yml` opens daily Cargo update pull requests and weekly GitHub Actions update pull requests with `dependencies` and security-related labels.
4. **Policy as code**: `deny.toml` defines the repository dependency policy for RustSec advisories, yanked crates, approved licenses, duplicate versions, and allowed registries.
5. **Security findings sink**: CodeQL uploads SARIF results to GitHub code scanning. Dependency Review annotates pull requests. `cargo audit` and `cargo deny` fail the workflow on blocking findings.

## Controls

| Control | Tool | Blocking threshold | Scope |
| --- | --- | --- | --- |
| Known Rust vulnerabilities | `cargo audit` | Any warning or vulnerability | `Cargo.lock` |
| Rust dependency policy | `cargo deny` | Vulnerable/yanked crates, denied licenses, unknown sources | Full Cargo graph with all features |
| Manifest diff risk | Dependency Review | Moderate or higher severity | Pull requests |
| Static security analysis | CodeQL | Uploaded to code scanning alerts | Rust source and generated build graph |
| Update freshness | Dependabot | Pull request for available updates | Cargo and GitHub Actions |

## Operational expectations

- The workflow is isolated from application runtime paths and therefore does not add latency to critical request handling. The `< 100ms` P99 target for critical paths is unaffected.
- The pipeline is fully CI-based and does not create runtime dependencies, preserving the `99.99%` service availability target.
- All blocking findings require security review before override. Overrides must be implemented by a narrowly scoped `deny.toml` ignore entry with an expiration plan in the pull request description.

## Blue-green and canary deployment integration

Dependency changes should move through the existing CI/CD promotion flow:

1. Merge only after all dependency security checks pass.
2. Build the candidate artifact from the reviewed commit.
3. Deploy to the green environment.
4. Run smoke tests and canary analysis against the green environment.
5. Shift traffic gradually and monitor error rate, latency, and security scan alerts.
6. Roll back to blue if canary metrics regress or new critical alerts appear.

## Dashboards and alerting

Monitor these GitHub-native signals:

- `dependency-security` workflow failures.
- GitHub code scanning alerts from CodeQL.
- Dependabot security alerts and security update pull requests.
- Pull requests blocked by Dependency Review or `cargo deny`.

Recommended alert routing: send workflow failure notifications and new high/critical security alerts to the security review channel and page the on-call engineer for production dependency incidents.

# Contribution and branch governance

## License

This repository uses the same LICENSE file as Okto Nexus: Elastic License 2.0
with the Okto Labs SaaS/Branding Addendum. The full LICENSE file is authoritative.
Package metadata uses `LicenseRef-Okto-Labs-ELv2-SaaS-Branding` to describe the
combined terms rather than claiming the unmodified Elastic license.

## Branch flow

1. Create a feature or fix branch and open a pull request into `develop`.
2. Obtain approval from a designated repository/organization owner.
3. Promote `develop` to `main` with a separate pull request from this repository.

The CODEOWNERS list covers every file, including workflow and governance files.
It reflects the organization owners verified when these rules were prepared;
update it and the protection payloads when ownership changes. Other users may
leave reviews, but their approvals do not satisfy the required owner approval.
PR authors cannot approve their own PRs; another owner must review them.

## Required repository settings

The JSON payloads in `.github/branch-protection/` require PRs and one code-owner
approval, dismiss stale approvals, require approval of the latest push, enforce
the rules for administrators, resolve conversations, and prohibit force pushes
and deletion. Only the listed owners may merge. No bypass actors are configured.

`main` additionally requires the GitHub Actions check `PR source policy`.
Its trusted `pull_request_target` workflow checks that the head is `develop`
in the same repository. It never checks out or executes PR code.

Apply from the repository root after reviewing the payloads:

```sh
gh api --method PUT repos/{owner}/{repo}/branches/develop/protection --input .github/branch-protection/develop.json
gh api --method PUT repos/{owner}/{repo}/branches/main/protection --input .github/branch-protection/main.json
```

**Activation prerequisite:** GitHub must support branch protection for these
private repositories, and Actions must be able to run the required source check.
During setup the API returned HTTP 403 requiring a plan upgrade or public
visibility; Actions also reported a billing/spending-limit block. These files
alone do not activate protection. Do not describe either branch as protected
until the API confirms the settings. Repository visibility must not be changed
as a workaround without explicit authorization.

Bootstrap the CODEOWNERS and workflow onto the protected base branches through
owner-reviewed PRs before requiring their checks. Once protection is active,
never push directly to `main` or `develop`.

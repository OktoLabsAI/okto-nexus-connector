"""Pure, opt-in diagnostic projection. No state, credential or transport reads."""
from __future__ import annotations

from collections import Counter
import re

from ..redaction import redact_text


STATUSES = ('PASS', 'FAIL', 'UNKNOWN', 'NOT_APPLICABLE')
CONTRACT_ARTIFACT_SHA256 = '06dbbea77eb0870058bc2fe4c9afc81023ea99f857612984321d1f8e6d20c422'
# A code names a particular proof, not a translation of a legacy status/message.
# The declarative cross-component catalog and vectors are versioned separately.
FINDINGS = {
    'CHECK_UNCONFIRMED': ('diagnostic_observation', 'UNKNOWN', 'A diagnostic observation could not be completed safely.', 'Inspect the selected instance and rerun the same diagnostic; no repair was attempted.'),
    'PYTHON_SUPPORTED': ('python_environment', 'PASS', 'The Python runtime satisfies the supported minimum.', 'Keep the supported Python runtime.'),
    'PYTHON_UNSUPPORTED': ('python_environment', 'FAIL', 'The Python runtime is below the supported minimum.', 'Install Python 3.11 or newer.'),
    'STATE_DIRECTORY_PRESENT': ('state_directory', 'PASS', 'The selected state directory exists; its contents are checked separately.', 'Inspect the independent state and daemon checks.'),
    'STATE_DIRECTORY_ABSENT': ('state_directory', 'FAIL', 'The selected state directory is absent.', 'Run connect explicitly when ready to configure this instance.'),
    'CORE_IMPORTED': ('core_import', 'PASS', 'The installed Core was imported.', 'Inspect its executable contract separately.'),
    'CORE_IMPORT_FAILED': ('core_import', 'FAIL', 'The installed Core could not be imported.', 'Reinstall the approved Connector distribution and its pinned Core.'),
    'LEGACY_CONTRACT_REPORTED': ('core_legacy_contract', 'PASS', 'The Core reports its legacy contract revision.', 'Use the executable contract check for current R4 compatibility.'),
    'CORE_CONTRACT_VERIFIED': ('core_contract', 'PASS', 'The executable R4 bundle passed its contract verification.', 'Review provider, binding, authorization and lease evidence separately.'),
    'CORE_CONTRACT_INVALID': ('core_contract', 'FAIL', 'The executable R4 bundle is unavailable or invalid.', 'Install the approved Connector and its exact pinned Core artifact.'),
    'RUNTIME_UNCONFIRMED': ('runtime_eligibility', 'UNKNOWN', 'This host inspection does not establish canonical admission to execute work.', 'Inspect the selected connection and current Server authorization.'),
    'STATE_SCHEMA_COMPATIBLE': ('state_schema', 'PASS', 'The returned snapshot has a supported schema; it may be the in-memory default for an absent file.', 'Inspect the independent state-directory and identity checks.'),
    'STATE_INVALID': ('state_schema', 'FAIL', 'The existing state snapshot cannot be decoded.', 'Inspect or recover the selected snapshot explicitly; it was not changed.'),
    'STATE_VERSION_INCOMPATIBLE': ('state_schema', 'FAIL', 'The state snapshot uses an incompatible version.', 'Use the compatible installed Connector; do not rewrite the snapshot through doctor.'),
    'STATE_UNAVAILABLE': ('state_schema', 'UNKNOWN', 'The state snapshot could not be read; its contents remain unknown.', 'Inspect access to the selected state directory.'),
    'DIAGNOSTIC_LIMIT': ('state_schema', 'UNKNOWN', 'The bounded state observation could not read the complete snapshot.', 'Inspect the existing snapshot without assuming it is empty or corrupt.'),
    'CONNECTOR_ID_PRESENT': ('connector_identity', 'PASS', 'The returned snapshot contains a Connector identifier.', 'Verify Server authority separately; an identifier is metadata.'),
    'CONNECTOR_ID_ABSENT': ('connector_identity', 'UNKNOWN', 'No Connector identifier was present in the returned snapshot.', 'Use connect explicitly when ready to configure this instance.'),
    'VAULT_BACKEND_DISCOVERED': ('vault_backend', 'PASS', 'An OS vault backend name was discovered; credential access was not proved.', 'Inspect the independent credential checks.'),
    'VAULT_FALLBACK_APPROVED': ('vault_backend', 'PASS', 'The existing restricted-file fallback consent is recorded.', 'Inspect credential availability separately; consent is not authentication.'),
    'VAULT_FALLBACK_NOT_APPROVED': ('vault_backend', 'FAIL', 'No OS backend was discovered and restricted-file fallback consent is absent.', 'Approve the fallback explicitly during identity import or configure an OS keyring.'),
    'VAULT_NAMESPACE_SNAPSHOT_READ': ('vault_namespaces', 'PASS', 'The existing file-vault namespace count was read; credential validity was not tested.', 'Authenticate the selected identity only through an explicit check.'),
    'VAULT_ENUMERATION_UNCONFIRMED': ('vault_namespaces', 'UNKNOWN', 'Vault namespace enumeration is unavailable or has no existing file.', 'Do not infer credential absence from an unavailable enumeration.'),
    'VAULT_ACCESS_UNCONFIRMED': ('credential_store', 'UNKNOWN', 'Existing vault access could not be established.', 'Inspect access to the selected vault; no credential was repaired or replaced.'),
    'VAULT_SNAPSHOT_INVALID': ('credential_store', 'FAIL', 'The existing vault snapshot could not be interpreted.', 'Inspect the existing vault format without replacing credentials implicitly.'),
    'IDENTITY_METADATA_PRESENT': ('identity_metadata', 'PASS', 'The returned snapshot contains imported identity metadata.', 'Authenticate the selected identity separately; metadata does not prove credential validity.'),
    'IDENTITY_NOT_SELECTED': ('identity_metadata', 'UNKNOWN', 'The returned snapshot has no imported identity selection.', 'Use connect or identity add explicitly when intended.'),
    'SERVER_CHECK_NOT_REQUESTED': ('server_reachability', 'UNKNOWN', 'No active Server check was requested.', 'Use doctor --probe explicitly for the existing selected Server and credential.'),
    'SERVER_RESPONSE_UNCONFIRMED': ('server_reachability', 'UNKNOWN', 'A current response from the selected Server was not established.', 'Inspect the selected origin and repeat the explicit check if intended.'),
    'SERVER_RESPONSE_CONFIRMED': ('server_reachability', 'PASS', 'The selected endpoint returned a current Server response.', 'Review the separate Server identity and participant authentication checks.'),
    'SERVER_UNREACHABLE': ('server_reachability', 'UNKNOWN', 'The check did not establish a usable current Server response.', 'Inspect the selected origin and network; missing evidence does not prove authentication failure.'),
    'SERVER_ID_UNCONFIRMED': ('server_identity', 'UNKNOWN', 'The current Server installation identity was not verified.', 'Use an explicit check against the saved Server selection.'),
    'SERVER_ID_CONFIRMED': ('server_identity', 'PASS', 'The returned Server identity matches the saved selection.', 'Verify the participant identity and runtime authorization independently.'),
    'SERVER_ID_MISMATCH': ('server_identity', 'FAIL', 'The returned Server identity differs from the saved selection.', 'Review the selected Server installation explicitly; no profile was changed.'),
    'PARTICIPANT_NOT_AUTHENTICATED': ('participant_authentication', 'UNKNOWN', 'This check did not establish authentication of the selected participant.', 'Use an explicit check with the existing selected identity.'),
    'PARTICIPANT_AUTHENTICATED': ('participant_authentication', 'PASS', 'The current response matches the selected Server and participant identity.', 'Review authorization, provider login and productive work separately.'),
    'AUTH_FAILED': ('participant_authentication', 'FAIL', 'The Server explicitly refused the selected participant authentication or scope.', 'Inspect the existing identity and current Server policy; no credential was replaced.'),
    'AUTHENTICATION_UNCONFIRMED': ('participant_authentication', 'UNKNOWN', 'Available error metadata does not establish a specific authentication outcome.', 'Inspect the selected Server and existing identity through an explicit check.'),
    'SCOPE_MISMATCH': ('participant_authentication', 'FAIL', 'The returned agent differs from the selected participant identity.', 'Review the existing identity selection before explicitly replacing a credential.'),
    'SERVER_UPGRADE_REQUIRED': ('server_protocol', 'FAIL', 'The response is incompatible with the required Server management contract.', 'Inspect the Server and Connector versions; authentication remains independent.'),
    'BINDING_NOT_CHECKED': ('managed_connection', 'UNKNOWN', 'Legacy binding metadata does not establish a current approved R4 connection.', 'Use status to inspect existing R4 connection progress; doctor does not create bindings.'),
    'EXECUTABLE_MISSING': ('runtime_binary', 'FAIL', 'The selected legacy binding executable was not found.', 'Review discovery and the selected binding explicitly.'),
    'BINARY_CHECK_NOT_REQUESTED': ('runtime_binary', 'UNKNOWN', 'The selected executable exists but candidate validation was not requested.', 'Use doctor --probe explicitly to validate the selected candidate.'),
    'BINARY_CANDIDATE_VALIDATED': ('runtime_binary', 'PASS', 'The explicitly selected candidate passed passive validation; no native version process or provider login was run.', 'Review technical qualification and provider authentication separately.'),
    'BINARY_CANDIDATE_INVALID': ('runtime_binary', 'FAIL', 'Candidate validation refused the selected executable.', 'Review the selected candidate and binding explicitly.'),
    'INSTALLATION_CANDIDATES_PRESENT': ('installation_inventory', 'PASS', 'Passive inventory found installation candidates.', 'Review selection, technical eligibility and provider login separately.'),
    'INSTALLATION_UNCONFIRMED': ('installation_inventory', 'UNKNOWN', 'Passive discovery found no candidates in its supported search scope.', 'Inspect the intended installation or select its executable explicitly.'),
    'UNSUPPORTED_PLATFORM': ('runtime_containment', 'FAIL', 'The selected executor platform has no qualified containment backend.', 'Use a supported executor with verified containment; do not disable containment.'),
    'PROCESS_CONTAINMENT_UNAVAILABLE': ('runtime_containment', 'FAIL', 'The supported host lacks a required containment capability.', 'Inspect the host containment prerequisites before requesting managed work.'),
    'CONTAINMENT_PREFLIGHT_CONFIRMED': ('runtime_containment', 'PASS', 'Passive containment preflight succeeded.', 'Productive launch and provider qualification remain separate checks.'),
    'CONTAINMENT_UNCONFIRMED': ('runtime_containment', 'UNKNOWN', 'Containment preflight could not establish current evidence.', 'Inspect the supported execution host; no process was launched to repair it.'),
    'JOURNAL_SIZE_OBSERVED': ('journal_size_observation', 'PASS', 'The existing journal size was observed; journal contents and health were not tested.', 'Inspect journal health separately if required.'),
    'JOURNAL_SIZE_HIGH': ('journal_size_observation', 'PASS', 'The observed journal size exceeds the advisory threshold; this does not prove failed work or journal corruption.', 'Review the journal maintenance procedure before changing it.'),
    'JOURNAL_NOT_OBSERVED': ('journal_size_observation', 'UNKNOWN', 'The journal has not been created for this selection.', 'Inspect existing daemon state; do not infer productive history.'),
    'JOURNAL_UNAVAILABLE': ('journal_size_observation', 'UNKNOWN', 'The journal size could not be read.', 'Inspect access to the existing journal; do not infer corruption.'),
    'SERVICE_READY': ('local_service', 'PASS', 'The existing selected instance answered its identity-bound IPC readiness check.', 'Review the remote transport and productive-work evidence separately.'),
    'SERVICE_NOT_READY': ('local_service', 'FAIL', 'The selected instance is confirmed stopped or failed.', 'Start or repair this selected instance explicitly when intended.'),
    'SERVICE_UNCONFIRMED': ('local_service', 'UNKNOWN', 'The selected instance readiness is unconfirmed; it is not proven stopped.', 'Inspect daemon status and logs before changing its lifecycle.'),
    'SERVICE_IPC_UNCONFIRMED': ('daemon_ipc', 'UNKNOWN', 'The additional daemon status request could not be observed.', 'Inspect the existing daemon without assuming work is absent.'),
    'TRANSPORT_ONLINE': ('transport', 'PASS', 'The current daemon status reports an online transport.', 'Verify participant authority, MCP participation and work results separately.'),
    'TRANSPORT_UNCONFIRMED': ('transport', 'UNKNOWN', 'The daemon status did not establish an online transport.', 'Inspect existing Server availability and reconnect progress.'),
    'TRANSPORT_NOT_OBSERVED': ('transport', 'UNKNOWN', 'No transport state was observed for the selected instance.', 'Inspect the existing daemon and selected Server.'),
    'PROVIDER_LOGIN_NOT_CHECKED': ('provider_login', 'UNKNOWN', 'No provider login was tested by this diagnostic.', 'Complete provider login explicitly on the execution host when required.'),
    'MCP_NEGOTIATION_NOT_OBSERVED': ('mcp_negotiation', 'UNKNOWN', 'This host diagnostic did not observe MCP negotiation.', 'Inspect the intended authenticated MCP client separately.'),
    'PARTICIPATION_NOT_OBSERVED': ('coordination_participation', 'UNKNOWN', 'This host diagnostic did not observe a correlated participant exchange.', 'Run a separately consented coordination check when intended.'),
    'HANDOFF_TEST_NOT_OBSERVED': ('handoff_test', 'UNKNOWN', 'This host diagnostic did not inspect a correlated handoff claim and result.', 'Inspect separately consented canonical handoff work.'),
    'WORK_NOT_CHECKED': ('work_result', 'UNKNOWN', 'This host diagnostic did not establish a canonical productive result.', 'Inspect the same existing work through its canonical receipt.'),
}

_ADVISORIES = {'VAULT_FALLBACK_NOT_APPROVED', 'JOURNAL_SIZE_HIGH'}
_STATES = {'ready', 'stopped', 'failed', 'starting', 'stopping', 'unknown', 'degraded',
           'online', 'offline', 'connecting', 'backoff', 'disconnected'}


def subject(kind, value='selected', *, fallback='selected'):
    """Retain only bounded public identifiers, never paths or arbitrary text."""
    if (not isinstance(value, str) or not re.fullmatch(r'[\w.:-]{1,160}', value)
            or redact_text(value) != value):
        value = fallback
    return {'kind': kind, 'id': value}


def _evidence(value):
    result = {}
    for key in ('schema_version', 'count', 'bytes', 'legacy_binding_count'):
        item = value.get(key)
        if type(item) is int and 0 <= item <= 2**53 - 1:
            result[key] = item
    if isinstance(value.get('state'), str) and value['state'] in _STATES:
        result['state'] = value['state']
    if isinstance(value.get('support'), str) and value['support'] in {'supported', 'unsupported'}:
        result['support'] = value['support']
    return result


def aggregate(checks):
    counts = Counter(row['status'] for row in checks)
    if set(counts) - set(STATUSES):
        raise ValueError('Unknown diagnostic proof status')
    return dict(counts={status: counts[status] for status in STATUSES},
        state='failed_checks' if counts['FAIL'] else 'incomplete' if not counts or counts['UNKNOWN'] else 'checks_passed')


class DiagnosticReport:
    def __init__(self, *, probe=False):
        self.probe = probe
        self.checks = {}

    def set(self, code, *, scope=None, evidence=None):
        identifier, status, reason, action = FINDINGS[code]
        scope = scope or subject('instance')
        row = dict(id=identifier, subject=scope, status=status, code=code,
                   reason=reason, next_action=action,
                   severity='warning' if code in _ADVISORIES else 'error' if status == 'FAIL' else 'info')
        safe = _evidence(evidence or {})
        if safe:
            row['evidence'] = safe
        self.checks[(identifier, scope['kind'], scope['id'])] = row

    def server_pending(self, scope):
        for code in ('SERVER_RESPONSE_UNCONFIRMED' if self.probe else 'SERVER_CHECK_NOT_REQUESTED',
                     'SERVER_ID_UNCONFIRMED', 'PARTICIPANT_NOT_AUTHENTICATED'):
            self.set(code, scope=scope)

    def result(self):
        checks = list(self.checks.values())
        return dict(schema='okto.diagnostics', format_version=1, component='connector',
            scope='execution_host', topology='connector_host',
            mode='explicit_checks' if self.probe else 'passive',
            **aggregate(checks), checks=checks,
            effects=dict(service_started=False, configuration_applied=False,
                harness_started=False, work_submitted=False, grants_issued=False))

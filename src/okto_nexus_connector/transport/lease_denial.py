"""A correlated control refusal, not a claim about native side effects."""
from ..errors import ConnectorError


class LeaseRequestDenied(ConnectorError):
    def __init__(self, request):
        super().__init__('LEASE_REVALIDATION_REQUIRED', 'r4_lease_denied',
                         'The Server denied this session lease request.',
                         possible_effect=True, retry_safe=False)
        self.request = request

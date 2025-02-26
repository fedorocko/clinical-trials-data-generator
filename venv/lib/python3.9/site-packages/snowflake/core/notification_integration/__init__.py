"""Manages Snowflake Notification Integrations."""

from public import public

from ._generated.models import (
    NotificationEmail,
    NotificationHook,
    NotificationQueueAwsSnsOutbound,
    NotificationQueueAzureEventGridInbound,
    NotificationQueueAzureEventGridOutbound,
    NotificationQueueGcpPubsubInbound,
    NotificationQueueGcpPubsubOutbound,
    NotificationWebhook,
    WebhookSecret,
)
from ._notification_integration import (
    NotificationIntegration,
    NotificationIntegrationCollection,
    NotificationIntegrationResource,
)


public(
    NotificationHook=NotificationHook,
    NotificationEmail=NotificationEmail,
    NotificationWebhook=NotificationWebhook,
    NotificationQueueAwsSnsOutbound=NotificationQueueAwsSnsOutbound,
    NotificationQueueAzureEventGridOutbound=NotificationQueueAzureEventGridOutbound,
    NotificationQueueGcpPubsubOutbound=NotificationQueueGcpPubsubOutbound,
    NotificationQueueAzureEventGridInbound=NotificationQueueAzureEventGridInbound,
    NotificationQueueGcpPubsubInbound=NotificationQueueGcpPubsubInbound,
    NotificationIntegration=NotificationIntegration,
    NotificationIntegrationCollection=NotificationIntegrationCollection,
    NotificationIntegrationResource=NotificationIntegrationResource,
    WebhookSecret=WebhookSecret,
)

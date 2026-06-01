export type SubscriptionScope = 'Global' | 'Resource'

export interface NotificationChannel {
  type: string
  url?: string
  secret?: string | null
}

export interface NotificationSubscription {
  subscription_id: string
  tenant_id: string
  user_id: string
  scope: SubscriptionScope
  resource_type: string | null
  resource_id: string | null
  event_types: string[]
  channels: NotificationChannel[]
  enabled: boolean
  created_at: string
  updated_at: string
}

export interface ChannelDeliveryStatus {
  channel: string
  status: 'Pending' | 'Sent' | 'Failed'
  sent_at: string | null
  error: string | null
}

export interface NotificationRecord {
  notification_id: string
  tenant_id: string
  user_id: string
  event_type: string
  event_payload: Record<string, unknown>
  source_type: string
  source_id: string
  workflow_meta_id: string | null
  url: string | null
  channel_statuses: ChannelDeliveryStatus[]
  read: boolean
  created_at: string
}

export interface CreateSubscriptionRequest {
  scope: string
  resource_type?: string
  resource_id?: string
  event_types: string[]
  channels: NotificationChannel[]
}

export interface UpdateSubscriptionRequest {
  event_types: string[]
  channels: NotificationChannel[]
  enabled?: boolean
}

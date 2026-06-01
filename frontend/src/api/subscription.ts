import request from './request'
import type {
  NotificationSubscription,
  CreateSubscriptionRequest,
  UpdateSubscriptionRequest,
} from '../types/notification'

export const subscriptionApi = {
  list: () =>
    request.get<any, { data: NotificationSubscription[] }>('/subscriptions'),

  create: (data: CreateSubscriptionRequest) =>
    request.post<any, { data: NotificationSubscription }>('/subscriptions', data),

  get: (id: string) =>
    request.get<any, { data: NotificationSubscription }>(`/subscriptions/${id}`),

  update: (id: string, data: UpdateSubscriptionRequest) =>
    request.put<any, { data: NotificationSubscription }>(`/subscriptions/${id}`, data),

  delete: (id: string) =>
    request.delete<any, { data: void }>(`/subscriptions/${id}`),
}

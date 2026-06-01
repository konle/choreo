import request from './request'
import type { NotificationRecord } from '../types/notification'

export const notificationApi = {
  list: (page = 1, pageSize = 20) =>
    request.get<any, { data: NotificationRecord[] }>(`/notifications`, {
      params: { page, page_size: pageSize },
    }),

  unreadCount: () =>
    request.get<any, { data: number }>('/notifications/unread-count'),

  markRead: (id: string) =>
    request.put<any, { data: void }>(`/notifications/${id}/read`),

  markAllRead: () =>
    request.put<any, { data: number }>('/notifications/read-all'),
}

self.addEventListener('push', event => {
  const data = event.data ? event.data.json() : {};
  const title = data.title || '新着ニュース';
  const options = {
    body: data.body || '',
    icon: '/static/icon.png',
    badge: '/static/icon.png',
    data: { url: data.url || '/' }
  };
  event.waitUntil(self.registration.showNotification(title, options));
});

self.addEventListener('notificationclick', event => {
  event.notification.close();
  event.waitUntil(clients.openWindow(event.notification.data.url));
});

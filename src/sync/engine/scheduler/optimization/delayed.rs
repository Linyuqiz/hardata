use std::collections::BTreeMap;
use std::time::Instant;
use tokio::sync::Mutex;

pub struct DelayedQueue<T> {
    items: Mutex<BTreeMap<Instant, Vec<T>>>,
}

impl<T: Clone> DelayedQueue<T> {
    pub fn new() -> Self {
        Self {
            items: Mutex::new(BTreeMap::new()),
        }
    }

    pub async fn insert(&self, run_at: Instant, item: T) {
        let mut items = self.items.lock().await;
        items.entry(run_at).or_insert_with(Vec::new).push(item);
    }

    pub async fn pop_ready(&self) -> Vec<T> {
        let now = Instant::now();
        let mut items = self.items.lock().await;
        let mut ready = Vec::new();

        let expired_keys: Vec<Instant> = items.range(..=now).map(|(k, _)| *k).collect();

        for key in expired_keys {
            if let Some(tasks) = items.remove(&key) {
                ready.extend(tasks);
            }
        }

        ready
    }

    pub async fn remove<F>(&self, predicate: F) -> Option<T>
    where
        F: Fn(&T) -> bool,
    {
        let mut items = self.items.lock().await;
        for (_, tasks) in items.iter_mut() {
            if let Some(pos) = tasks.iter().position(&predicate) {
                return Some(tasks.remove(pos));
            }
        }
        None
    }

    pub async fn retain<F>(&self, predicate: F)
    where
        F: Fn(&T) -> bool,
    {
        let mut items = self.items.lock().await;
        items.retain(|_, tasks| {
            tasks.retain(&predicate);
            !tasks.is_empty()
        });
    }

    pub async fn len(&self) -> usize {
        let items = self.items.lock().await;
        items.values().map(|v| v.len()).sum()
    }

    pub async fn is_empty(&self) -> bool {
        self.items.lock().await.is_empty()
    }
}

impl<T: Clone> Default for DelayedQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

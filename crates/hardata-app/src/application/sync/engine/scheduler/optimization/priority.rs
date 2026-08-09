use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub enum ChunkPriority {
    Critical = 0,
    High = 1,
    #[default]
    Normal = 2,
    Low = 3,
    Background = 4,
}

impl ChunkPriority {
    pub fn from_file_size(size: u64) -> Self {
        match size {
            0..=1_048_576 => ChunkPriority::High,
            1_048_577..=10_485_760 => ChunkPriority::Normal,
            10_485_761..=104_857_600 => ChunkPriority::Low,
            _ => ChunkPriority::Background,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PriorityItem<T> {
    pub priority: i32,
    pub sequence: u64,
    pub data: T,
}

impl<T> PriorityItem<T> {
    pub fn new(priority: i32, sequence: u64, data: T) -> Self {
        Self {
            priority,
            sequence,
            data,
        }
    }
}

impl<T> PartialEq for PriorityItem<T> {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority && self.sequence == other.sequence
    }
}

impl<T> Eq for PriorityItem<T> {}

impl<T> PartialOrd for PriorityItem<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for PriorityItem<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        match other.priority.cmp(&self.priority) {
            Ordering::Equal => other.sequence.cmp(&self.sequence),
            other => other,
        }
    }
}

pub struct PriorityQueue<T> {
    heap: Mutex<BinaryHeap<PriorityItem<T>>>,
    sequence: std::sync::atomic::AtomicU64,
}

impl<T: Clone> PriorityQueue<T> {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            heap: Mutex::new(BinaryHeap::new()),
            sequence: std::sync::atomic::AtomicU64::new(0),
        })
    }

    pub async fn enqueue(&self, priority: i32, data: T) {
        let seq = self
            .sequence
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let item = PriorityItem::new(priority, seq, data);
        self.heap.lock().await.push(item);
    }

    pub async fn dequeue(&self) -> Option<T> {
        self.heap.lock().await.pop().map(|item| item.data)
    }

    pub async fn peek(&self) -> Option<T> {
        self.heap.lock().await.peek().map(|item| item.data.clone())
    }

    pub async fn len(&self) -> usize {
        self.heap.lock().await.len()
    }

    pub async fn is_empty(&self) -> bool {
        self.heap.lock().await.is_empty()
    }

    pub async fn clear(&self) {
        self.heap.lock().await.clear();
    }

    pub async fn retain<F>(&self, mut f: F)
    where
        F: FnMut(&T) -> bool,
    {
        let mut heap = self.heap.lock().await;
        let items: Vec<_> = heap.drain().collect();
        for item in items {
            if f(&item.data) {
                heap.push(item);
            }
        }
    }
}

impl<T: Clone> Default for PriorityQueue<T> {
    fn default() -> Self {
        Self {
            heap: Mutex::new(BinaryHeap::new()),
            sequence: std::sync::atomic::AtomicU64::new(0),
        }
    }
}

use alloc::string::String;
use alloc::vec::Vec;

/// What one watcher sent under one parameter, in a buffer subql reuses.
///
/// Reused across every watcher of one changed row, so it is filled and read
/// once per watcher and allocates only while it grows. Each slot keeps its
/// capacity through [`reset`](Self::reset), so a watcher writing a key the
/// same length as the last one allocates nothing.
///
/// The values are bare, as the caller sent them. A held key reaching Postgres
/// inside `app.subjects` is bare there too, and that is the spelling the
/// comparison was translated against.
#[derive(Clone, Debug, Default)]
pub struct RequestValues {
    /// Slots, of which the first `len` are live. Retained past `len` so a
    /// later fill reuses the allocation rather than making a new one.
    slots: Vec<String>,
    len: usize,
}

impl RequestValues {
    /// An empty buffer.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            slots: Vec::new(),
            len: 0,
        }
    }

    /// Forget every value, keeping the allocations to fill again.
    pub const fn reset(&mut self) {
        self.len = 0;
    }

    /// Add one value the watcher sent.
    pub fn push(&mut self, value: &str) {
        match self.slots.get_mut(self.len) {
            Some(slot) => {
                slot.clear();
                slot.push_str(value);
            }
            None => self.slots.push(String::from(value)),
        }
        self.len += 1;
    }

    /// Whether the watcher sent `value`.
    #[must_use]
    pub fn holds(&self, value: &str) -> bool {
        self.values().any(|held| held == value)
    }

    /// Every value the watcher sent, in the order it wrote them.
    pub fn values(&self) -> impl Iterator<Item = &str> {
        self.slots[..self.len].iter().map(String::as_str)
    }

    /// How many values the watcher sent.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.len
    }

    /// Whether the watcher sent none, which is an answer rather than a
    /// refusal to answer.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Slots the buffer is holding on to, which is what a reuse test reads.
    #[must_use]
    pub const fn capacity(&self) -> usize {
        self.slots.len()
    }
}

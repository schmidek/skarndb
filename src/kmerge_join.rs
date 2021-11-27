use std::cmp::Ordering;
use std::fmt;
use std::iter::FusedIterator;
use std::mem::replace;
use std::vec::Vec;

/// Based on https://docs.rs/itertools/0.10.1/itertools/fn.kmerge.html
/// However for items that are equal we do a join and only the return the one first iterator
pub trait KMergeJoinBy: Iterator {
    fn kmerge_join_by<F>(self, first: F) -> KMergeBy<<Self::Item as IntoIterator>::IntoIter, F>
    where
        Self: Sized,
        Self::Item: IntoIterator,
        F: FnMut(
            &<Self::Item as IntoIterator>::Item,
            &<Self::Item as IntoIterator>::Item,
        ) -> Ordering,
    {
        kmerge_by(self, first)
    }
}

impl<I: Iterator> KMergeJoinBy for I {}

/// Head element and Tail iterator pair
///
/// `PartialEq`, `Eq`, `PartialOrd` and `Ord` are implemented by comparing sequences based on
/// first items (which are guaranteed to exist).
///
/// The meanings of `PartialOrd` and `Ord` are reversed so as to turn the heap used in
/// `KMerge` into a min-heap.
#[derive(Debug)]
struct HeadTail<I>
where
    I: Iterator,
{
    head: I::Item,
    tail: I,
    rank: usize,
}

impl<I> HeadTail<I>
where
    I: Iterator,
{
    /// Constructs a `HeadTail` from an `Iterator`. Returns `None` if the `Iterator` is empty.
    fn new(mut it: I, rank: usize) -> Option<HeadTail<I>> {
        let head = it.next();
        head.map(|h| HeadTail {
            head: h,
            tail: it,
            rank,
        })
    }

    /// Get the next element and update `head`, returning the old head in `Some`.
    ///
    /// Returns `None` when the tail is exhausted (only `head` then remains).
    fn next(&mut self) -> Option<I::Item> {
        if let Some(next) = self.tail.next() {
            Some(replace(&mut self.head, next))
        } else {
            None
        }
    }
}

/*impl<I> Clone for HeadTail<I>
    where I: Iterator + Clone,
          I::Item: Clone
{
    clone_fields!(head, tail);
}*/

/// Make `data` a heap (min-heap w.r.t the sorting).
fn heapify<T, S>(data: &mut [T], mut less_than: S)
where
    S: FnMut(&T, &T) -> bool,
{
    for i in (0..data.len() / 2).rev() {
        sift_down(data, i, &mut less_than);
    }
}

/// Sift down element at `index` (`heap` is a min-heap wrt the ordering)
fn sift_down<T, S>(heap: &mut [T], index: usize, mut less_than: S)
where
    S: FnMut(&T, &T) -> bool,
{
    debug_assert!(index <= heap.len());
    let mut pos = index;
    let mut child = 2 * pos + 1;
    // Require the right child to be present
    // This allows to find the index of the smallest child without a branch
    // that wouldn't be predicted if present
    while child + 1 < heap.len() {
        // pick the smaller of the two children
        // use aritmethic to avoid an unpredictable branch
        child += less_than(&heap[child + 1], &heap[child]) as usize;

        // sift down is done if we are already in order
        if !less_than(&heap[child], &heap[pos]) {
            return;
        }
        heap.swap(pos, child);
        pos = child;
        child = 2 * pos + 1;
    }
    // Check if the last (left) child was an only child
    // if it is then it has to be compared with the parent
    if child + 1 == heap.len() && less_than(&heap[child], &heap[pos]) {
        heap.swap(pos, child);
    }
}

/// An iterator adaptor that merges an abitrary number of base iterators in ascending order.
/// If all base iterators are sorted (ascending), the result is sorted.
///
/// Iterator element type is `I::Item`.
///
/// See [`.kmerge()`](crate::Itertools::kmerge) for more information.
#[must_use = "iterator adaptors are lazy and do nothing unless consumed"]
pub type KMerge<I> = KMergeBy<I, KMergeByLt>;

pub trait KMergePredicate<T> {
    fn kmerge_pred(&mut self, a: &T, b: &T) -> Ordering;
}

#[derive(Clone)]
pub struct KMergeByLt;

impl<T: PartialOrd> KMergePredicate<T> for KMergeByLt {
    fn kmerge_pred(&mut self, a: &T, b: &T) -> Ordering {
        a.partial_cmp(b).unwrap()
    }
}

impl<T, F: FnMut(&T, &T) -> Ordering> KMergePredicate<T> for F {
    fn kmerge_pred(&mut self, a: &T, b: &T) -> Ordering {
        self(a, b)
    }
}

pub fn kmerge<I>(iterable: I) -> KMerge<<I::Item as IntoIterator>::IntoIter>
where
    I: IntoIterator,
    I::Item: IntoIterator,
    <<I as IntoIterator>::Item as IntoIterator>::Item: PartialOrd,
{
    kmerge_by(iterable, KMergeByLt)
}

/// An iterator adaptor that merges an abitrary number of base iterators
/// according to an ordering function.
///
/// Iterator element type is `I::Item`.
///
/// See [`.kmerge_by()`](crate::Itertools::kmerge_by) for more
/// information.
#[must_use = "iterator adaptors are lazy and do nothing unless consumed"]
pub struct KMergeBy<I, F>
where
    I: Iterator,
{
    heap: Vec<HeadTail<I>>,
    less_than: F,
}

/*impl<I, F> fmt::Debug for KMergeBy<I, F>
    where I: Iterator + fmt::Debug,
          I::Item: fmt::Debug,
{
    debug_fmt_fields!(KMergeBy, heap);
}*/

/// Create an iterator that merges elements of the contained iterators.
///
/// Equivalent to `iterable.into_iter().kmerge_by(less_than)`.
pub fn kmerge_by<I, F>(
    iterable: I,
    mut less_than: F,
) -> KMergeBy<<I::Item as IntoIterator>::IntoIter, F>
where
    I: IntoIterator,
    I::Item: IntoIterator,
    F: KMergePredicate<<<I as IntoIterator>::Item as IntoIterator>::Item>,
{
    let iter = iterable.into_iter();
    let (lower, _) = iter.size_hint();
    let mut heap: Vec<_> = Vec::with_capacity(lower);
    heap.extend(
        iter.enumerate()
            .filter_map(|(index, it)| HeadTail::new(it.into_iter(), index)),
    );
    heapify(&mut heap, |a, b| {
        less_than.kmerge_pred(&a.head, &b.head).is_lt()
    });
    KMergeBy { heap, less_than }
}

/*impl<I, F> Clone for KMergeBy<I, F>
    where I: Iterator + Clone,
          I::Item: Clone,
          F: Clone,
{
    clone_fields!(heap, less_than);
}*/

impl<I, F> Iterator for KMergeBy<I, F>
where
    I: Iterator,
    F: KMergePredicate<I::Item>,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        if self.heap.is_empty() {
            return None;
        }

        // Loop while items are equal and return the one from the lowest ranked iterator
        let mut joined_result = None;
        let mut result_rank = usize::MAX;
        loop {
            let rank = self.heap[0].rank;
            let result = if let Some(next) = self.heap[0].next() {
                next
            } else {
                self.heap.swap_remove(0).head
            };
            let less_than = &mut self.less_than;
            sift_down(&mut self.heap, 0, |a, b| {
                less_than.kmerge_pred(&a.head, &b.head).is_lt()
            });

            if rank < result_rank {
                joined_result = Some(result);
                result_rank = rank;
            }

            if self.heap.is_empty()
                || less_than
                    .kmerge_pred(joined_result.as_ref().unwrap(), &self.heap[0].head)
                    .is_ne()
            {
                break;
            }
        }

        joined_result
    }
}

impl<I, F> FusedIterator for KMergeBy<I, F>
where
    I: Iterator,
    F: KMergePredicate<I::Item>,
{
}

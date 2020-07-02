use crate::types::Value;
use std::collections::BTreeMap;
use std::sync::{Arc, Weak};

mod page;
use self::page::*;


pub struct PageTree<V> {
    inner: BTreeMap<KeyRange, Leaf<V>>,
    //root: Option<Key, V>,
    //length: usize,
}

impl<V> PageTree<V> {
    fn get_leaf(&self, key: &Key) -> Option<&Leaf<V>> {
        // Create a range containing only the span of the requested key
        let anchor = KeyRange::single(*key);

        // Check if the span _above_ the anchor contains the key
        if let Some((range, leaf)) = self.inner.range(anchor..).next() {
            if range.includes(key) {
                return Some(leaf)
            }
        }
        
        // Check if the span _below_ the anchor contains the key
        if let Some((range, leaf)) = self.inner.range(..anchor).rev().next() {
            if range.includes(key) {
                return Some(leaf)
            }
        }

        None
    }
}



#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct KeyRange {
    lower: Key,
    upper: Key,
}


impl KeyRange {
    pub fn single(key: Key) -> KeyRange {
        KeyRange {
            lower: key,
            upper: key,
        }
    }

    pub fn includes(&self, key: &Key) -> bool {
        self.lower <= *key && *key <= self.upper
    }
}


pub type RowId = usize;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Key {
    
}

/// Contains info on how to access a page/object
pub enum Leaf<V> {
    /// A marker that shows the data has been saved to disk and is not loaded in memory
    Unloaded,
    
    /// A fixed-size block of data
    Page(Page<V>),
    
    /// A value that is large enough to not fit on a single page
    Object(Box<V>),
}


pub enum Node<V> {
    Leaf {
        data: Arc<Leaf<V>>,
        left_sibling: Weak<Leaf<V>>,
        right_sibling: Weak<Leaf<V>>,
    },
    Node {
        children: Vec<Node<V>>,
    }
}


/*
impl<V> PageTree<V> {
    pub fn new() -> PageTree<V> {
        PageTree { root: None, length: 0 }
    }

    pub fn clear(&mut self) {
        *self = PageTree::new();
    }

    pub fn get(&self, key: &Key) -> Option<V>
    {
        match search::search_tree(self.root.as_ref()?.as_ref(), key) {
            Found(handle) => Some(handle.into_kv().1),
            GoDown(_) => None,
        }
    }

    pub fn first_key_value(&self) -> Option<(&K, &V)> {
        let front = self.root.as_ref()?.as_ref().first_leaf_edge();
        front.right_kv().ok().map(Handle::into_kv)
    }

    pub fn last_key_value(&self) -> Option<(&K, &V)> {
        let back = self.root.as_ref()?.as_ref().last_leaf_edge();
        back.left_kv().ok().map(Handle::into_kv)
    }

    pub fn contains_key(&self, key: &Key) -> bool {
        self.get(key).is_some()
    }

    pub fn insert(&mut self, key: Key, value: ()) -> Option<V> {
        match self.entry(key) {
            Occupied(mut entry) => Some(entry.insert(value)),
            Vacant(entry) => {
                entry.insert(value);
                None
            }
        }
    }

    pub fn remove(&mut self, key: &Key) -> Option<V> {
        self.remove_entry(key).map(|(_, v)| v)
    }

    pub fn append(&mut self, other: &mut Self) {
        // Do we have to append anything at all?
        if other.is_empty() {
       geTree<vV> {
    
}åvG     return;
        }

        // We can just swap `self` and `other` if `self` is empty.
        if self.is_empty() {
            mem::swap(self, other);
            return;
        }

        // First, we merge `self` and `other` into a sorted sequence in linear time.
        let self_iter = mem::take(self).into_iter();
        let other_iter = mem::take(other).into_iter();
        let iter = MergeIter { left: self_iter.peekable(), right: other_iter.peekable() };

        // Second, we build a tree from the sorted sequence in linear time.
        self.from_sorted_iter(iter);
        self.fix_right_edge();
    }

    /// Constructs a double-ended iterator over a sub-range of elements in the map.
    /// The simplest way is to use the range syntax `min..max`, thus `range(min..max)` will
    /// yield elements from min (inclusive) to max (exclusive).
    /// The range may also be entered as `(Bound<T>, Bound<T>)`, so for example
    /// `range((Excluded(4), Included(10)))` will yield a left-exclusive, right-inclusive
    /// range from 4 to 10.
    ///
    /// # Panics
    ///
    /// Panics if range `start > end`.
    /// Panics if range `start == end` and both bounds are `Excluded`.
    ///
    /// # Examples
    ///
    /// Basic usage:
    ///
    /// ```
    /// use std::collections::BTreeMap;
    /// use std::ops::Bound::Included;
    ///
    /// let mut map = BTreeMap::new();
    /// map.insert(3, "a");
    /// map.insert(5, "b");
    /// map.insert(8, "c");
    /// for (&key, &value) in map.range((Included(&4), Included(&8))) {
    ///     println!("{}: {}", key, value);
    /// }
    /// assert_eq!(Some((&5, &"b")), map.range(4..).next());
    /// ```
    #[stable(feature = "btree_range", since = "1.17.0")]
    pub fn range<T: ?Sized, R>(&self, range: R) -> Range<'_, K, V>
    where
        T: Ord,
        K: Borrow<T>,
        R: RangeBounds<T>,
    {
        if let Some(root) = &self.root {
            let root1 = root.as_ref();
            let root2 = root.as_ref();
            let (f, b) = range_search(root1, root2, range);

            Range { front: Some(f), back: Some(b) }
        } else {
            Range { front: None, back: None }
        }
    }

    /// Constructs a mutable double-ended iterator over a sub-range of elements in the map.
    /// The simplest way is to use the range syntax `min..max`, thus `range(min..max)` will
    /// yield elements from min (inclusive) to max (exclusive).
    /// The range may also be entered as `(Bound<T>, Bound<T>)`, so for example
    /// `range((Excluded(4), Included(10)))` will yield a left-exclusive, right-inclusive
    /// range from 4 to 10.
    ///
    /// # Panics
    ///
    /// Panics if range `start > end`.
    /// Panics if range `start == end` and both bounds are `Excluded`.
    ///
    /// # Examples
    ///
    /// Basic usage:
    ///
    /// ```
    /// use std::collections::BTreeMap;
    ///
    /// let mut map: BTreeMap<&str, i32> = ["Alice", "Bob", "Carol", "Cheryl"]
    ///     .iter()
    ///     .map(|&s| (s, 0))
    ///     .collect();
    /// for (_, balance) in map.range_mut("B".."Cheryl") {
    ///     *balance += 100;
    /// }
    /// for (name, balance) in &map {
    ///     println!("{} => {}", name, balance);
    /// }
    /// ```
    #[stable(feature = "btree_range", since = "1.17.0")]
    pub fn range_mut<T: ?Sized, R>(&mut self, range: R) -> RangeMut<'_, K, V>
    where
        T: Ord,
        K: Borrow<T>,
        R: RangeBounds<T>,
    {
        if let Some(root) = &mut self.root {
            let root1 = root.as_mut();
            let root2 = unsafe { ptr::read(&root1) };
            let (f, b) = range_search(root1, root2, range);

            RangeMut { front: Some(f), back: Some(b), _marker: PhantomData }
        } else {
            RangeMut { front: None, back: None, _marker: PhantomData }
        }
    }

    /// Gets the given key's corresponding entry in the map for in-place manipulation.
    ///
    /// # Examples
    ///
    /// Basic usage:
    ///
    /// ```
    /// use std::collections::BTreeMap;
    ///
    /// let mut count: BTreeMap<&str, usize> = BTreeMap::new();
    ///
    /// // count the number of occurrences of letters in the vec
    /// for x in vec!["a","b","a","c","a","b"] {
    ///     *count.entry(x).or_insert(0) += 1;
    /// }
    ///
    /// assert_eq!(count["a"], 3);
    /// ```
    #[stable(feature = "rust1", since = "1.0.0")]
    pub fn entry(&mut self, key: K) -> Entry<'_, K, V> {
        // FIXME(@porglezomp) Avoid allocating if we don't insert
        self.ensure_root_is_owned();
        match search::search_tree(self.root.as_mut().unwrap().as_mut(), &key) {
            Found(handle) => {
                Occupied(OccupiedEntry { handle, length: &mut self.length, _marker: PhantomData })
            }
            GoDown(handle) => {
                Vacant(VacantEntry { key, handle, length: &mut self.length, _marker: PhantomData })
            }
        }
    }

    fn from_sorted_iter<I: Iterator<Item = (K, V)>>(&mut self, iter: I) {
        self.ensure_root_is_owned();
        let mut cur_node = self.root.as_mut().unwrap().as_mut().last_leaf_edge().into_node();
        // Iterate through all key-value pairs, pushing them into nodes at the right level.
        for (key, value) in iter {
            // Try to push key-value pair into the current leaf node.
            if cur_node.len() < node::CAPACITY {
                cur_node.push(key, value);
            } else {
                // No space left, go up and push there.
                let mut open_node;
                let mut test_node = cur_node.forget_type();
                loop {
                    match test_node.ascend() {
                        Ok(parent) => {
                            let parent = parent.into_node();
                            if parent.len() < node::CAPACITY {
                                // Found a node with space left, push here.
                                open_node = parent;
                                break;
                            } else {
                                // Go up again.
                                test_node = parent.forget_type();
                            }
                        }
                        Err(node) => {
                            // We are at the top, create a new root node and push there.
                            open_node = node.into_root_mut().push_level();
                            break;
                        }
                    }
                }

                // Push key-value pair and new right subtree.
                let tree_height = open_node.height() - 1;
                let mut right_tree = node::Root::new_leaf();
                for _ in 0..tree_height {
                    right_tree.push_level();
                }
                open_node.push(key, value, right_tree);

                // Go down to the right-most leaf again.
                cur_node = open_node.forget_type().last_leaf_edge().into_node();
            }

            self.length += 1;
        }
    }

    fn fix_right_edge(&mut self) {
        // Handle underfull nodes, start from the top.
        let mut cur_node = self.root.as_mut().unwrap().as_mut();
        while let Internal(internal) = cur_node.force() {
            // Check if right-most child is underfull.
            let mut last_edge = internal.last_edge();
            let right_child_len = last_edge.reborrow().descend().len();
            if right_child_len < node::MIN_LEN {
                // We need to steal.
                let mut last_kv = match last_edge.left_kv() {
                    Ok(left) => left,
                    Err(_) => unreachable!(),
                };
                last_kv.bulk_steal_left(node::MIN_LEN - right_child_len);
                last_edge = last_kv.right_edge();
            }

            // Go further down.
            cur_node = last_edge.descend();
        }
    }

    /// Splits the collection into two at the given key. Returns everything after the given key,
    /// including the key.
    ///
    /// # Examples
    ///
    /// Basic usage:
    ///
    /// ```
    /// use std::collections::BTreeMap;
    ///
    /// let mut a = BTreeMap::new();
    /// a.insert(1, "a");
    /// a.insert(2, "b");
    /// a.insert(3, "c");
    /// a.insert(17, "d");
    /// a.insert(41, "e");
    ///
    /// let b = a.split_off(&3);
    ///
    /// assert_eq!(a.len(), 2);
    /// assert_eq!(b.len(), 3);
    ///
    /// assert_eq!(a[&1], "a");
    /// assert_eq!(a[&2], "b");
    ///
    /// assert_eq!(b[&3], "c");
    /// assert_eq!(b[&17], "d");
    /// assert_eq!(b[&41], "e");
    /// ```
    #[stable(feature = "btree_split_off", since = "1.11.0")]
    pub fn split_off<Q: ?Sized + Ord>(&mut self, key: &Q) -> Self
    where
        K: Borrow<Q>,
    {
        if self.is_empty() {
            return Self::new();
        }

        let total_num = self.len();

        let mut right = Self::new();
        let right_root = right.ensure_root_is_owned();
        for _ in 0..(self.root.as_ref().unwrap().as_ref().height()) {
            right_root.push_level();
        }

        {
            let mut left_node = self.root.as_mut().unwrap().as_mut();
            let mut right_node = right.root.as_mut().unwrap().as_mut();

            loop {
                let mut split_edge = match search::search_node(left_node, key) {
                    // key is going to the right tree
                    Found(handle) => handle.left_edge(),
                    GoDown(handle) => handle,
                };

                split_edge.move_suffix(&mut right_node);

                match (split_edge.force(), right_node.force()) {
                    (Internal(edge), Internal(node)) => {
                        left_node = edge.descend();
                        right_node = node.first_edge().descend();
                    }
                    (Leaf(_), Leaf(_)) => {
                        break;
                    }
                    _ => {
                        unreachable!();
                    }
                }
            }
        }

        self.fix_right_border();
        right.fix_left_border();

        if self.root.as_ref().unwrap().as_ref().height()
            < right.root.as_ref().unwrap().as_ref().height()
        {
            self.recalc_length();
            right.length = total_num - self.len();
        } else {
            right.recalc_length();
            self.length = total_num - right.len();
        }

        right
    }

    /// Creates an iterator which uses a closure to determine if an element should be removed.
    ///
    /// If the closure returns true, the element is removed from the map and yielded.
    /// If the closure returns false, or panics, the element remains in the map and will not be
    /// yielded.
    ///
    /// Note that `drain_filter` lets you mutate every value in the filter closure, regardless of
    /// whether you choose to keep or remove it.
    ///
    /// If the iterator is only partially consumed or not consumed at all, each of the remaining
    /// elements will still be subjected to the closure and removed and dropped if it returns true.
    ///
    /// It is unspecified how many more elements will be subjected to the closure
    /// if a panic occurs in the closure, or a panic occurs while dropping an element,
    /// or if the `DrainFilter` value is leaked.
    ///
    /// # Examples
    ///
    /// Splitting a map into even and odd keys, reusing the original map:
    ///
    /// ```
    /// #![feature(btree_drain_filter)]
    /// use std::collections::BTreeMap;
    ///
    /// let mut map: BTreeMap<i32, i32> = (0..8).map(|x| (x, x)).collect();
    /// let evens: BTreeMap<_, _> = map.drain_filter(|k, _v| k % 2 == 0).collect();
    /// let odds = map;
    /// assert_eq!(evens.keys().copied().collect::<Vec<_>>(), vec![0, 2, 4, 6]);
    /// assert_eq!(odds.keys().copied().collect::<Vec<_>>(), vec![1, 3, 5, 7]);
    /// ```
    #[unstable(feature = "btree_drain_filter", issue = "70530")]
    pub fn drain_filter<F>(&mut self, pred: F) -> DrainFilter<'_, K, V, F>
    where
        F: FnMut(&K, &mut V) -> bool,
    {
        DrainFilter { pred, inner: self.drain_filter_inner() }
    }
    pub(super) fn drain_filter_inner(&mut self) -> DrainFilterInner<'_, K, V> {
        let front = self.root.as_mut().map(|r| r.as_mut().first_leaf_edge());
        DrainFilterInner { length: &mut self.length, cur_leaf_edge: front }
    }

    /// Calculates the number of elements if it is incorrect.
    fn recalc_length(&mut self) {
        fn dfs<'a, K, V>(node: NodeRef<marker::Immut<'a>, K, V, marker::LeafOrInternal>) -> usize
        where
            K: 'a,
            V: 'a,
        {
            let mut res = node.len();

            if let Internal(node) = node.force() {
                let mut edge = node.first_edge();
                loop {
                    res += dfs(edge.reborrow().descend());
                    match edge.right_kv() {
                        Ok(right_kv) => {
                            edge = right_kv.right_edge();
                        }
                        Err(_) => {
                            break;
                        }
                    }
                }
            }

            res
        }

        self.length = dfs(self.root.as_ref().unwrap().as_ref());
    }

    /// Removes empty levels on the top.
    fn fix_top(&mut self) {
        loop {
            {
                let node = self.root.as_ref().unwrap().as_ref();
                if node.height() == 0 || node.len() > 0 {
                    break;
                }
            }
            self.root.as_mut().unwrap().pop_level();
        }
    }

    fn fix_right_border(&mut self) {
        self.fix_top();

        {
            let mut cur_node = self.root.as_mut().unwrap().as_mut();

            while let Internal(node) = cur_node.force() {
                let mut last_kv = node.last_kv();

                if last_kv.can_merge() {
                    cur_node = last_kv.merge().descend();
                } else {
                    let right_len = last_kv.reborrow().right_edge().descend().len();
                    // `MINLEN + 1` to avoid readjust if merge happens on the next level.
                    if right_len < node::MIN_LEN + 1 {
                        last_kv.bulk_steal_left(node::MIN_LEN + 1 - right_len);
                    }
                    cur_node = last_kv.right_edge().descend();
                }
            }
        }

        self.fix_top();
    }

    /// The symmetric clone of `fix_right_border`.
    fn fix_left_border(&mut self) {
        self.fix_top();

        {
            let mut cur_node = self.root.as_mut().unwrap().as_mut();

            while let Internal(node) = cur_node.force() {
                let mut first_kv = node.first_kv();

                if first_kv.can_merge() {
                    cur_node = first_kv.merge().descend();
                } else {
                    let left_len = first_kv.reborrow().left_edge().descend().len();
                    if left_len < node::MIN_LEN + 1 {
                        first_kv.bulk_steal_right(node::MIN_LEN + 1 - left_len);
                    }
                    cur_node = first_kv.left_edge().descend();
                }
            }
        }

        self.fix_top();
    }
}
*/
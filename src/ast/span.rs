use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};
use std::ops::{Deref, DerefMut};

#[derive(Clone, Copy, Debug)]
pub struct Span(pub usize, pub usize);

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub struct Spanned<T> {
    #[serde(skip)]
    pub span: Option<Span>,

    pub value: T,
}

impl Span {
    pub fn union(self, Span(s2, e2): Span) -> Span {
        let Span(s1, e1) = self;
        Span(s1.min(s2), e1.max(e2))
    }
}

impl<T> Spanned<T> {
    pub fn new(start: usize, end: usize, value: T) -> Self {
        Self {
            span: Some(Span(start, end)),
            value,
        }
    }
}

impl<T> From<T> for Spanned<T> {
    fn from(value: T) -> Self {
        Self { span: None, value }
    }
}

impl<T> Deref for Spanned<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.value
    }
}

impl<T> DerefMut for Spanned<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.value
    }
}

impl<T> AsRef<T> for Spanned<T> {
    fn as_ref(&self) -> &T {
        &self.value
    }
}

impl<T> AsMut<T> for Spanned<T> {
    fn as_mut(&mut self) -> &mut T {
        &mut self.value
    }
}

impl<T: Display> Display for Spanned<T> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        self.value.fmt(f)
    }
}

use alloc::borrow::Cow;
use alloc::string::String;
use alloc::sync::Arc;

use super::request_values::RequestValues;

/// The principals a watcher authenticates as, and the values it sent.
///
/// Two methods because subql needs two different things of a watcher and they
/// are not the same thing spelled twice. A record's subject side is a
/// `type:key` name, so answering from the row is a set-membership test over
/// [`subjects`](Self::subjects). A request-gated grant is not a stored fact at
/// all: the row settles one side of a comparison the caller's own value
/// completes, and that value is bare and typed by the policy rather than by
/// the model. Feeding one where the other belongs is a wrong comparison, and a
/// bare key colliding with a name would be a wrong allow.
///
/// [`crate::visibility::VisibilityPolicy::Watcher`] stays opaque to subql everywhere else.
///
/// # Why names are [`Cow`] and values are written rather than returned
///
/// A consumer whose identity is a typed id has no rendered name to lend, and
/// `&str` would force it to keep one on its own type, which is a design
/// decision subql has no business making for it. [`Cow`] leaves the choice
/// where it belongs.
///
/// Values go the other way, into a buffer subql owns, because they are asked
/// for once per watcher per changed row on the path that exists to cost
/// nothing. Returning them owned would allocate per watcher per event.
pub trait Subject {
    /// Every `type:key` this watcher is known by, exactly as a record spells
    /// it (`user:alice`).
    ///
    /// A principal carrying an identity and further subjects yields all of
    /// them: naming only the first would deny a holder the model grants.
    fn subjects(&self) -> impl Iterator<Item = Cow<'_, str>>;

    /// Write what this watcher sent under `parameter` into `out`, bare and
    /// untyped, and report whether it could answer at all.
    ///
    /// `false` means "cannot answer", which delegates, so a watcher that
    /// omits a parameter loses speed and never correctness. Answering with no
    /// values is a different thing and is an answer: a caller holding no keys
    /// is granted by no key.
    ///
    /// `out` arrives empty. The default cannot answer, so a watcher with no
    /// request values at all implements nothing.
    fn request_value(&self, parameter: &str, out: &mut RequestValues) -> bool {
        let _ = (parameter, out);
        false
    }
}

impl Subject for str {
    fn subjects(&self) -> impl Iterator<Item = Cow<'_, Self>> {
        core::iter::once(Cow::Borrowed(self))
    }
}

impl Subject for String {
    fn subjects(&self) -> impl Iterator<Item = Cow<'_, str>> {
        core::iter::once(Cow::Borrowed(self.as_str()))
    }
}

impl<T: Subject + ?Sized> Subject for &T {
    fn subjects(&self) -> impl Iterator<Item = Cow<'_, str>> {
        (*self).subjects()
    }

    fn request_value(&self, parameter: &str, out: &mut RequestValues) -> bool {
        (*self).request_value(parameter, out)
    }
}

/// A consumer whose watcher is a shared handle over its own type cannot
/// write this itself: [`Arc`] is not `#[fundamental]`, so `Arc<Local>` is
/// not a local type and the orphan rule refuses a foreign trait on it. It
/// lives here so that consumer implements [`Subject`] on its own type and
/// needs no newtype.
impl<T: Subject + ?Sized> Subject for Arc<T> {
    fn subjects(&self) -> impl Iterator<Item = Cow<'_, str>> {
        (**self).subjects()
    }

    fn request_value(&self, parameter: &str, out: &mut RequestValues) -> bool {
        (**self).request_value(parameter, out)
    }
}

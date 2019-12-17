use crate::route::Route;
use crate::{generated::css_classes::C, Msg};
use seed::{prelude::*, *};
use std::cmp::PartialEq;
use std::collections::LinkedList;

#[derive(Debug)]
pub struct BreadCrumbs<Crumb> {
    crumbs: LinkedList<Crumb>,
}

impl<Crumb> Default for BreadCrumbs<Crumb> {
    fn default() -> Self {
        Self {
            crumbs: LinkedList::new(),
        }
    }
}

impl<Crumb: PartialEq> BreadCrumbs<Crumb> {
    pub fn iter(self: &Self) -> impl DoubleEndedIterator<Item = &Crumb> {
        self.crumbs.iter()
    }

    pub fn clear(self: &mut Self) -> &mut Self {
        self.crumbs.clear();
        self
    }

    pub fn push(self: &mut Self, n: Crumb) -> &mut Self {
        let mut new_crumbs = LinkedList::new();

        while let Some(c) = self.crumbs.pop_front() {
            if c != n {
                new_crumbs.push_back(c);
            } else {
                break;
            }
        }

        new_crumbs.push_back(n);
        self.crumbs = new_crumbs;
        self
    }
}

pub fn view(bc: &BreadCrumbs<Route>) -> impl View<Msg> {
    let mut ol = ol![class![C.justify_center, C.truncate, C.mx_4, C.rtl]];

    // XXX the list has "direction: rtl" to put ellipsis to the left on overflow,
    // XXX need to reverse the crumbs to show them in the correct left-to-right order:
    for (n, p) in bc.iter().rev().enumerate() {
        let mut cr = li![
            class![C.inline],
            a![
                class![C.hover__underline],
                attrs! {At::Href => p.to_href()},
                p.to_string(),
            ]
        ];
        if n == 0 {
            cr.add_class(C.font_bold);
        } else {
            ol.add_child(i!["/", class![C.px_2]]);
        }
        ol.add_child(cr);
    }
    ol
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_crumb() {
        let mut m = BreadCrumbs::default();
        m.push("foo");
        assert_eq!(m.iter().count(), 1);
    }

    #[test]
    fn two_different_crumbs() {
        let mut m = BreadCrumbs::default();
        m.push("foo");
        m.push("bar");
        assert_eq!(m.iter().count(), 2);
    }

    #[test]
    fn two_identical_crumbs() {
        let mut m = BreadCrumbs::default();
        m.push("xxx");
        m.push("xxx");
        assert_eq!(m.iter().count(), 1);
    }

    #[test]
    fn cycle() {
        let mut m = BreadCrumbs::default();
        m.push("aaa");
        m.push("bbb");
        m.push("ccc");
        m.push("ddd");
        m.push("ccc");
        assert_eq!(m.iter().count(), 3);
    }
}

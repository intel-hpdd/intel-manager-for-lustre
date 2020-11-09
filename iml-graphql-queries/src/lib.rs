// Copyright (c) 2020 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

pub mod hotpool;
pub mod log;
pub mod snapshot;
pub mod stratagem;
pub mod task;

use std::fmt;

/// The base query that is serialized and sent
/// to the graphql server
#[derive(serde::Serialize, Debug)]
pub struct Query<T: serde::Serialize> {
    pub query: String,
    pub variables: Option<T>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct Data<T> {
    pub data: T,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct Location {
    line: u32,
    column: u32,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct Error {
    message: String,
    locations: Vec<Location>,
    path: Vec<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct Errors {
    errors: Vec<Error>,
}

impl fmt::Display for Errors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let x = self.errors.iter().fold(
            String::from("The following query errors were returned:\n"),
            |acc, x| format!("{}{}\n", acc, x.message),
        );

        write!(f, "{}", x)
    }
}

impl std::error::Error for Errors {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(untagged)]
pub enum Response<T> {
    Data(Data<T>),
    Errors(Errors),
}

impl<T> From<Response<T>> for Result<Data<T>, Errors> {
    fn from(x: Response<T>) -> Self {
        match x {
            Response::Data(d) => Ok(d),
            Response::Errors(e) => Err(e),
        }
    }
}

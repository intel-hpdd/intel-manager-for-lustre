// Copyright (c) 2019 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

use regex::Regex;
use seed::document;
use wasm_bindgen::JsValue;
use web_sys::HtmlDocument;

pub static MAX_SAFE_INTEGER: u64 = 9007199254740991;

/// Returns https://<domain>:<port>/ui/
pub fn ui_root() -> String {
    document().base_uri().unwrap().unwrap_or_default()
}

pub fn get_root_url() -> String {
    let url = ui_root();
    let mut idx = 0;
    url.find("/ui/").map(|x| idx = x);
    url.get(0..idx).expect("Couldn't get url root.").into()
}

pub fn api_root() -> String {
    format!("{}/api/", get_root_url())
}

/// Returns https://<domain>:<port>/grafana/
pub fn grafana_root() -> String {
    format!("{}/grafana/", get_root_url())
}

pub fn influx_root() -> String {
    format!("{}/influx?", get_root_url())
}

/// Returns the CSRF token if one exists within the cookie.
pub fn csrf_token() -> Option<String> {
    let html_doc: HtmlDocument = HtmlDocument::from(JsValue::from(document()));
    let cookie = html_doc.cookie().unwrap();

    parse_cookie(&cookie)
}

fn parse_cookie(cookie: &str) -> Option<String> {
    let re = Regex::new(r"csrftoken=([^;|$]+)").unwrap();

    let x = re.captures(&cookie)?;

    x.get(1).map(|x| x.as_str().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_cookie() {
        let csrf = parse_cookie(&"auth=Y2hyb21hOmNocm9tYTEyMw%3D%3D; csrftoken=gkM15g7sBKrosDBnJTt9YV3E73JRNNNj; sessionid=apym2kfg2t38xdvcv18ni0w4k1d0dhzm");

        assert_eq!(csrf.unwrap(), "gkM15g7sBKrosDBnJTt9YV3E73JRNNNj");
    }
}


use anyhow::{anyhow, Context, Result};
use reqwest::header::{HeaderMap, HeaderValue};
use reqwest::Url;




#[derive(Clone)]
pub struct HttpSession {
    base: Url,
    client: reqwest::Client,
    csrf: String,
    cookie_header: String,
}

impl HttpSession {
    pub async fn connect(base: &str, user: &str, pass: &str) -> Result<Self> {
        let base_url = Url::parse(base).context("invalid base URL")?;
        let client = reqwest::Client::builder()
            .cookie_store(true)
            .build()?;
        // POST /login
        let login_url = base_url.join("/login")?;
        let resp = client
            .post(login_url)
            .json(&serde_json::json!({"username": user, "password": pass}))
            .send()
            .await?;
        if !resp.status().is_success() {
            return Err(anyhow!("login failed: HTTP {}", resp.status()));
        }
        // Capture Set-Cookie headers into a single Cookie string (for WS upgrades)
        let mut cookies: Vec<String> = Vec::new();
        for val in resp.headers().get_all(reqwest::header::SET_COOKIE).iter() {
            if let Ok(s) = val.to_str() {
                // take name=value before first ';'
                if let Some((nv, _)) = s.split_once(';') { cookies.push(nv.trim().to_string()); }
            }
        }
        let v: serde_json::Value = resp.json().await.unwrap_or(serde_json::json!({"status":"error"}));
        if v.get("status").and_then(|s| s.as_str()) != Some("ok") {
            return Err(anyhow!("login failed"));
        }
        // GET /csrf
        let csrf_url = base_url.join("/csrf")?;
        let resp2 = client.get(csrf_url).send().await?;
        if !resp2.status().is_success() { return Err(anyhow!("failed to obtain csrf: HTTP {}", resp2.status())); }
        // Add any cookies from CSRF response too
        for val in resp2.headers().get_all(reqwest::header::SET_COOKIE).iter() {
            if let Ok(s) = val.to_str() {
                if let Some((nv, _)) = s.split_once(';') { cookies.push(nv.trim().to_string()); }
            }
        }
        let v2: serde_json::Value = resp2.json().await.unwrap_or(serde_json::json!({}));
        let csrf = v2.get("csrf").and_then(|s| s.as_str()).unwrap_or("").to_string();
        if csrf.is_empty() { return Err(anyhow!("csrf token missing")); }
        let cookie_header = if cookies.is_empty() { String::new() } else { cookies.join("; ") };
        Ok(Self { base: base_url, client, csrf, cookie_header })
    }

    pub async fn post_query(&self, text: &str) -> Result<serde_json::Value> {
        let qurl = self.base.join("/query")?;
        let mut headers = HeaderMap::new();
        headers.insert("x-csrf-token", HeaderValue::from_str(&self.csrf).unwrap());
        let resp = self.client.post(qurl)
            .headers(headers)
            .json(&serde_json::json!({"query": text}))
            .send().await?;
        let status = resp.status();
        let val: serde_json::Value = resp.json().await.unwrap_or(serde_json::json!({"status":"error"}));
        if !status.is_success() {
            return Err(anyhow!("remote error: {}", val));
        }
        Ok(val)
    }

    pub async fn use_database(&self, name: &str) -> Result<()> {
        let url = self.base.join("/use/database")?;
        let mut headers = HeaderMap::new();
        headers.insert("x-csrf-token", HeaderValue::from_str(&self.csrf).unwrap());
        let resp = self.client.post(url).headers(headers).json(&serde_json::json!({"name": name})).send().await?;
        if !resp.status().is_success() { return Err(anyhow!("failed to set database")); }
        Ok(())
    }
    pub async fn use_schema(&self, name: &str) -> Result<()> {
        let url = self.base.join("/use/schema")?;
        let mut headers = HeaderMap::new();
        headers.insert("x-csrf-token", HeaderValue::from_str(&self.csrf).unwrap());
        let resp = self.client.post(url).headers(headers).json(&serde_json::json!({"name": name})).send().await?;
        if !resp.status().is_success() { return Err(anyhow!("failed to set schema")); }
        Ok(())
    }
}

#[derive(Clone)]
pub enum RemoteTransport {
    Http(HttpSession),
    Ws(WsSession),
    Pg(PgSession),
}

impl RemoteTransport {
    pub async fn post_query(&self, text: &str) -> Result<serde_json::Value> {
        match self {
            RemoteTransport::Http(h) => h.post_query(text).await,
            RemoteTransport::Ws(w) => w.post_query(text).await,
            RemoteTransport::Pg(p) => p.post_query(text).await,
        }
    }
    pub async fn use_database(&self, name: &str) -> Result<()> {
        match self {
            RemoteTransport::Http(h) => h.use_database(name).await,
            RemoteTransport::Ws(w) => w.use_database(name).await,
            RemoteTransport::Pg(p) => p.use_database(name).await,
        }
    }
    pub async fn use_schema(&self, name: &str) -> Result<()> {
        match self {
            RemoteTransport::Http(h) => h.use_schema(name).await,
            RemoteTransport::Ws(w) => w.use_schema(name).await,
            RemoteTransport::Pg(p) => p.use_schema(name).await,
        }
    }
    pub fn ident(&self) -> String {
        match self {
            RemoteTransport::Http(h) => format!("http:{}", h.base),
            RemoteTransport::Ws(w) => format!("ws:{}", w.base),
            RemoteTransport::Pg(p) => format!("pg:{}", p.addr_desc()),
        }
    }
}

#[derive(Clone)]
pub struct WsSession {
    base: Url,
    csrf: String,
    cookie_header: String,
}

impl WsSession {
    pub async fn from_http_session(http: &HttpSession) -> Result<Self> { Ok(Self { base: http.base.clone(), csrf: http.csrf.clone(), cookie_header: http.cookie_header.clone() }) }

    pub fn ws_url_from_http_base(&self) -> Result<Url> {
        // Convert http(s)://host[:port][/path] -> ws(s)://host[:port]
        let mut ws = self.base.clone();
        let scheme = ws.scheme().to_string();
        if scheme == "https" { ws.set_scheme("wss").ok(); } else { ws.set_scheme("ws").ok(); }
        // Point to /ws
        let ws2 = ws.join("/ws")?;
        Ok(ws2)
    }

    pub async fn post_query(&self, text: &str) -> Result<serde_json::Value> {
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;
        use tokio_tungstenite::tungstenite::http::HeaderValue as WsHeaderValue;
        let ws_url = self.ws_url_from_http_base()?;
        let mut req = ws_url.as_str().into_client_request()?;
        if !self.cookie_header.is_empty() {
            req.headers_mut().insert("cookie", WsHeaderValue::from_str(&self.cookie_header).unwrap());
        }
        req.headers_mut().insert("x-csrf-token", WsHeaderValue::from_str(&self.csrf).unwrap());
        let (mut stream, _resp) = tokio_tungstenite::connect_async(req).await?;
        use futures_util::{SinkExt, StreamExt};
        stream.send(tokio_tungstenite::tungstenite::Message::Text(text.to_string())).await?;
        // read one message as response
        if let Some(msg) = stream.next().await {
            let m = msg?;
            if let tokio_tungstenite::tungstenite::Message::Text(s) = m {
                let v: serde_json::Value = serde_json::from_str(&s).unwrap_or(serde_json::json!({"status":"error","error":"invalid json"}));
                if !v.get("status").and_then(|x| x.as_str()).unwrap_or("").eq("ok") {
                    return Err(anyhow!("remote error: {}", v));
                }
                return Ok(v);
            }
        }
        Err(anyhow!("ws: no response"))
    }
    pub async fn use_database(&self, _name: &str) -> Result<()> {
        // Use HTTP endpoints for DB/Schema since WS auth requires cookies/CSRF; reuse simple reqwest client
        let client = reqwest::Client::new();
        let url = self.base.join("/use/database")?;
        let mut headers = HeaderMap::new();
        if !self.cookie_header.is_empty() { headers.insert("cookie", HeaderValue::from_str(&self.cookie_header).unwrap()); }
        headers.insert("x-csrf-token", HeaderValue::from_str(&self.csrf).unwrap());
        let resp = client.post(url).headers(headers).json(&serde_json::json!({"name": _name})).send().await?;
        if !resp.status().is_success() { return Err(anyhow!("failed to set database")); }
        Ok(())
    }
    pub async fn use_schema(&self, _name: &str) -> Result<()> {
        let client = reqwest::Client::new();
        let url = self.base.join("/use/schema")?;
        let mut headers = HeaderMap::new();
        if !self.cookie_header.is_empty() { headers.insert("cookie", HeaderValue::from_str(&self.cookie_header).unwrap()); }
        headers.insert("x-csrf-token", HeaderValue::from_str(&self.csrf).unwrap());
        let resp = client.post(url).headers(headers).json(&serde_json::json!({"name": _name})).send().await?;
        if !resp.status().is_success() { return Err(anyhow!("failed to set schema")); }
        Ok(())
    }
}

#[derive(Clone)]
pub struct PgSession {
    cfg: String,
    schema: Option<String>,
}

impl PgSession {
    pub async fn connect(url: &str, schema: Option<String>) -> Result<Self> {
        // We delay actual connection until first query to keep it simple.
        Ok(Self { cfg: url.to_string(), schema })
    }
    pub async fn connect_client(&self) -> Result<tokio_postgres::Client> {
        use tokio_postgres::{NoTls, Config};
        let cfg: Config = self.cfg.parse().context("invalid postgres url")?;
        let (client, conn) = cfg.connect(NoTls).await?;
        // drive the connection in background
        tokio::spawn(async move { let _ = conn.await; });
        if let Some(s) = &self.schema {
            let _ = client.simple_query(&format!("SET search_path TO {}", s)).await; // best-effort
        }
        Ok(client)
    }
    pub async fn post_query(&self, text: &str) -> Result<serde_json::Value> {
        let client = self.connect_client().await?;
        let msgs = client.simple_query(text).await?;
        // Convert to a generic JSON shape. We return the last result set if multiple.
        use tokio_postgres::SimpleQueryMessage;
        let mut cols: Vec<String> = Vec::new();
        let mut rows: Vec<Vec<serde_json::Value>> = Vec::new();
        for m in msgs {
            match m {
                SimpleQueryMessage::Row(r) => {
                    if cols.is_empty() {
                        cols = (0..r.len()).map(|i| r.columns()[i].name().to_string()).collect();
                    }
                    let mut out_row = Vec::with_capacity(r.len());
                    for i in 0..r.len() {
                        out_row.push(match r.get(i) { Some(s) => serde_json::Value::String(s.to_string()), None => serde_json::Value::Null });
                    }
                    rows.push(out_row);
                }
                SimpleQueryMessage::CommandComplete(_c) => { /* ignore */ }
                _ => {}
            }
        }
        let result = serde_json::json!({
            "status": "ok",
            "results": {
                "columns": cols,
                "rows": rows
            }
        });
        Ok(result)
    }
    pub async fn use_database(&self, _name: &str) -> Result<()> { Ok(()) /* database encoded in URL for postgres */ }
    pub async fn use_schema(&self, name: &str) -> Result<()> {
        // best-effort one-off SET for schema
        let client = self.connect_client().await?;
        let _ = client.simple_query(&format!("SET search_path TO {}", name)).await?;
        Ok(())
    }
    fn addr_desc(&self) -> String { self.cfg.clone() }
}


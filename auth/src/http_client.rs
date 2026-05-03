pub type HttpClientError = openidconnect::reqwest::Error;

pub struct HttpClient {
  pub inner: openidconnect::reqwest::Client,
}

impl HttpClient {
  pub fn new() -> Result<Self, HttpClientError> {
    let inner = openidconnect::reqwest::Client::builder()
      .redirect(openidconnect::reqwest::redirect::Policy::none())
      .build()?;
    Ok(Self { inner })
  }
}

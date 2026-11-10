// Copyright 2021 Palantir Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
use bytes::{BufMut, Bytes, BytesMut};
use conjure_serde::json;
use futures_sink::Sink;
use pin_project::pin_project;
use serde::Serialize;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

#[pin_project]
pub struct JsonAppender<S> {
    #[pin]
    inner: S,
    buf: BytesMut,
}

impl<S> JsonAppender<S> {
    pub fn new(inner: S) -> Self {
        JsonAppender {
            inner,
            buf: BytesMut::new(),
        }
    }
}

impl<T, S> Sink<T> for JsonAppender<S>
where
    T: Serialize,
    S: Sink<Bytes, Error = io::Error>,
{
    type Error = io::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.project().inner.poll_ready(cx)
    }

    fn start_send(self: Pin<&mut Self>, value: T) -> io::Result<()> {
        let this = self.project();
        if let Err(e) = json::to_writer(this.buf.writer(), &value) {
            this.buf.clear();
            return Err(io::Error::new(io::ErrorKind::InvalidInput, e));
        }
        this.buf.put_u8(b'\n');

        this.inner.start_send(this.buf.split().freeze())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.project().inner.poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.project().inner.poll_close(cx)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use futures_util::SinkExt;
    use serde::ser::{Error as _, SerializeMap};
    use std::convert::Infallible;

    struct FailsHalfway;

    impl Serialize for FailsHalfway {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            let mut map = serializer.serialize_map(None)?;
            map.serialize_entry("partial", &1)?;
            Err(S::Error::custom("boom"))
        }
    }

    #[tokio::test]
    async fn serialization_error_does_not_corrupt_next_record() {
        let inner = Vec::<Bytes>::new().sink_map_err(|e: Infallible| -> io::Error { match e {} });
        let mut appender = JsonAppender::new(inner);

        appender.send(FailsHalfway).await.unwrap_err();
        appender.send("ok").await.unwrap();

        assert_eq!(appender.inner.get_ref(), &[Bytes::from("\"ok\"\n")]);
    }
}

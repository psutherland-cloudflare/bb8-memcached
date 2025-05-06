use memcache_async::ascii;
use std::collections::HashMap;
use std::fmt::Display;
use std::io;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpStream, UnixStream};
use tokio::time::timeout;
use url::Url;

pub trait Connectable {
    fn get_uri(self) -> Url;
}

impl Connectable for String {
    fn get_uri(self) -> Url {
        Url::parse(&self).unwrap()
    }
}

impl Connectable for &str {
    fn get_uri(self) -> Url {
        Url::parse(self).unwrap()
    }
}

pub trait ConnectionMethods {
    /// Returns the value for given key as bytes. If the value doesn't exist, std::io::ErrorKind::NotFound is returned.
    async fn get<'a, K: AsRef<[u8]>>(&'a mut self, key: &'a K) -> Result<Vec<u8>, io::Error>;

    /// Returns values for multiple keys in a single call as a HashMap from keys to found values. If a key is not present in memcached it will be absent from returned map.
    async fn get_multi<'a, K: AsRef<[u8]>>(
        &'a mut self,
        keys: &'a [K],
    ) -> Result<HashMap<String, Vec<u8>>, io::Error>;

    /// Delete a key
    async fn delete<'a, K: Display>(&'a mut self, key: &'a K) -> Result<(), io::Error>;

    /// Add key to given value
    async fn add<'a, K: Display>(
        &'a mut self,
        key: &'a K,
        val: &'a [u8],
        expiration: u32,
    ) -> Result<(), io::Error>;

    /// Set key to given value and don't wait for response.
    async fn set<'a, K: Display>(
        &'a mut self,
        key: &'a K,
        val: &'a [u8],
        expiration: u32,
    ) -> Result<(), io::Error>;

    /// Increment a value and return the updated value.
    async fn increment<'a, K: AsRef<[u8]>>(
        &'a mut self,
        key: &'a K,
        amount: u64,
    ) -> Result<u64, io::Error>;

    async fn version(&mut self) -> Result<String, io::Error>;

    async fn flush(&mut self) -> Result<(), io::Error>;
}

pub(crate) struct ConnectionMetadata {
    inner: Connection,
    tainted: bool
}

impl ConnectionMetadata {
    fn taint(&mut self) {
        self.tainted = true
    }

    pub(crate) fn is_tainted(&self) -> bool {
        self.tainted
    }
}

/// ConnectionManager manages the connection with read and write timeouts.
/// It implements the same methods as type Connection.
pub struct ConnectionManager {
    pub(crate) connection: ConnectionMetadata,
    read_timeout: Duration,
    write_timeout: Duration,
}

impl ConnectionManager {
    pub async fn connect(
        uri: &Url,
        read_timeout: Duration,
        write_timeout: Duration,
    ) -> Result<ConnectionManager, io::Error> {
        let connection = Connection::connect(uri).await?;
        Ok(Self {
            connection: ConnectionMetadata{inner: connection, tainted: false},
            read_timeout,
            write_timeout,
        })
    }
}

impl ConnectionMethods for ConnectionManager {
    async fn get<'a, K: AsRef<[u8]>>(&'a mut self, key: &'a K) -> Result<Vec<u8>, io::Error> {
        match timeout(self.read_timeout, self.connection.inner.get(key)).await {
            Err(_elapsed_err) => {
                self.connection.taint();
                Err(io::ErrorKind::TimedOut.into())
            },
            Ok(data) => data,
        }
    }

    async fn get_multi<'a, K: AsRef<[u8]>>(
        &'a mut self,
        keys: &'a [K],
    ) -> Result<HashMap<String, Vec<u8>>, io::Error> {
        match timeout(self.read_timeout, self.connection.inner.get_multi(keys)).await {
            Err(_elapsed_err) =>{ 
                self.connection.taint();
                Err(io::ErrorKind::TimedOut.into()) 
            },
            Ok(data) => data,
        }
    }

    async fn delete<'a, K: Display>(&'a mut self, key: &'a K) -> Result<(), io::Error> {
        match timeout(self.write_timeout, self.connection.inner.delete(key)).await {
            Err(_elapsed_err) =>{ 
                self.connection.taint();
                Err(io::ErrorKind::TimedOut.into()) 
            },
            Ok(data) => data,
        }
    }

    async fn add<'a, K: Display>(
        &'a mut self,
        key: &'a K,
        val: &'a [u8],
        expiration: u32,
    ) -> Result<(), io::Error> {
        match timeout(
            self.write_timeout,
            self.connection.inner.add(key, val, expiration),
        )
        .await
        {
            Err(_elapsed_err) => { 
                self.connection.taint();
                Err(io::ErrorKind::TimedOut.into()) 
            },
            Ok(data) => data,
        }
    }

    async fn set<'a, K: Display>(
        &'a mut self,
        key: &'a K,
        val: &'a [u8],
        expiration: u32,
    ) -> Result<(), io::Error> {
        match timeout(
            self.write_timeout,
            self.connection.inner.set(key, val, expiration),
        )
        .await
        {
            Err(_elapsed_err) => { 
                self.connection.taint();
                Err(io::ErrorKind::TimedOut.into()) 
            },
            Ok(data) => data,
        }
    }

    async fn increment<'a, K: AsRef<[u8]>>(
        &'a mut self,
        key: &'a K,
        amount: u64,
    ) -> Result<u64, io::Error> {
        match timeout(self.write_timeout, self.connection.inner.increment(key, amount)).await {
            Err(_elapsed_err) => { 
                self.connection.taint();
                Err(io::ErrorKind::TimedOut.into()) 
            },
            Ok(data) => data,
        }
    }

    async fn version(&mut self) -> Result<String, io::Error> {
        match timeout(self.read_timeout, self.connection.inner.version()).await {
            Err(_elapsed_err) => { 
                self.connection.taint();
                Err(io::ErrorKind::TimedOut.into()) 
            },
            Ok(data) => data,
        }
    }

    async fn flush(&mut self) -> Result<(), io::Error> {
        match timeout(self.write_timeout, self.connection.inner.flush()).await {
            Err(_elapsed_err) => { 
                self.connection.taint();
                Err(io::ErrorKind::TimedOut.into()) 
            },
            Ok(data) => data,
        }
    }
}

pub enum Connection {
    Unix(ascii::Protocol<StreamCompat<UnixStream>>),
    Tcp(ascii::Protocol<StreamCompat<TcpStream>>),
}

impl Connection {
    pub async fn connect(uri: &Url) -> Result<Connection, io::Error> {
        let connection = if uri.has_authority() {
            let addr = uri.socket_addrs(|| None)?;
            let sock = TcpStream::connect(addr.first().unwrap()).await?;
            Connection::Tcp(ascii::Protocol::new(StreamCompat::new(sock)))
        } else {
            let sock = UnixStream::connect(uri.path()).await?;
            Connection::Unix(ascii::Protocol::new(StreamCompat::new(sock)))
        };
        Ok(connection)
    }
}

impl ConnectionMethods for Connection {
    /// Returns the value for given key as bytes. If the value doesn't exist, std::io::ErrorKind::NotFound is returned.
    async fn get<'a, K: AsRef<[u8]>>(&'a mut self, key: &'a K) -> Result<Vec<u8>, io::Error> {
        match self {
            Connection::Unix(ref mut c) => c.get(key).await,
            Connection::Tcp(ref mut c) => c.get(key).await,
        }
    }

    async fn get_multi<'a, K: AsRef<[u8]>>(
        &'a mut self,
        keys: &'a [K],
    ) -> Result<HashMap<String, Vec<u8>>, io::Error> {
        match self {
            Connection::Unix(ref mut c) => c.get_multi(keys).await,
            Connection::Tcp(ref mut c) => c.get_multi(keys).await,
        }
    }

    async fn delete<'a, K: Display>(&'a mut self, key: &'a K) -> Result<(), io::Error> {
        match self {
            Connection::Unix(ref mut c) => c.delete(key).await,
            Connection::Tcp(ref mut c) => c.delete(key).await,
        }
    }

    async fn add<'a, K: Display>(
        &'a mut self,
        key: &'a K,
        val: &'a [u8],
        expiration: u32,
    ) -> Result<(), io::Error> {
        match self {
            Connection::Unix(ref mut c) => c.add(key, val, expiration).await,
            Connection::Tcp(ref mut c) => c.add(key, val, expiration).await,
        }
    }

    async fn set<'a, K: Display>(
        &'a mut self,
        key: &'a K,
        val: &'a [u8],
        expiration: u32,
    ) -> Result<(), io::Error> {
        match self {
            Connection::Unix(ref mut c) => c.set(key, val, expiration).await,
            Connection::Tcp(ref mut c) => c.set(key, val, expiration).await,
        }
    }

    async fn increment<'a, K: AsRef<[u8]>>(
        &'a mut self,
        key: &'a K,
        amount: u64,
    ) -> Result<u64, io::Error> {
        match self {
            Connection::Unix(ref mut c) => c.increment(key, amount).await,
            Connection::Tcp(ref mut c) => c.increment(key, amount).await,
        }
    }

    async fn version(&mut self) -> Result<String, io::Error> {
        match self {
            Connection::Unix(ref mut c) => c.version().await,
            Connection::Tcp(ref mut c) => c.version().await,
        }
    }

    async fn flush(&mut self) -> Result<(), io::Error> {
        match self {
            Connection::Unix(ref mut c) => c.flush().await,
            Connection::Tcp(ref mut c) => c.flush().await,
        }
    }
}

/// Compatibility layer for `futures::{AsyncRead, AsyncWrite}`.
pub struct StreamCompat<T> {
    inner: T,
}

impl<T> StreamCompat<T> {
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

impl<T> Deref for StreamCompat<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> DerefMut for StreamCompat<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<T: AsyncRead> futures::AsyncRead for StreamCompat<T> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let mut b = ReadBuf::new(buf);
        match AsyncRead::poll_read(
            unsafe { self.map_unchecked_mut(|s| &mut s.inner) },
            cx,
            &mut b,
        ) {
            Poll::Ready(_) => Poll::Ready(Ok(b.filled().len())),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<T: AsyncWrite> futures::AsyncWrite for StreamCompat<T> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        AsyncWrite::poll_write(unsafe { self.map_unchecked_mut(|s| &mut s.inner) }, cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        AsyncWrite::poll_flush(unsafe { self.map_unchecked_mut(|s| &mut s.inner) }, cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        AsyncWrite::poll_shutdown(unsafe { self.map_unchecked_mut(|s| &mut s.inner) }, cx)
    }
}

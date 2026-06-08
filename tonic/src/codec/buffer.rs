use bytes::buf::UninitSlice;
use bytes::{Buf, BufMut, Bytes, BytesMut};

/// A specialized buffer to decode gRPC messages from.
#[derive(Debug)]
pub struct DecodeBuf<'a> {
    buf: &'a mut BytesMut,
    len: usize,
}

/// A specialized buffer to encode gRPC messages into.
#[derive(Debug)]
pub struct EncodeBuf<'a> {
    buf: &'a mut BytesMut,
}

impl<'a> DecodeBuf<'a> {
    pub(crate) fn new(buf: &'a mut BytesMut, len: usize) -> Self {
        DecodeBuf { buf, len }
    }
}

impl Buf for DecodeBuf<'_> {
    #[inline]
    fn remaining(&self) -> usize {
        self.len
    }

    #[inline]
    fn chunk(&self) -> &[u8] {
        let ret = self.buf.chunk();

        if ret.len() > self.len {
            &ret[..self.len]
        } else {
            ret
        }
    }

    #[inline]
    fn advance(&mut self, cnt: usize) {
        assert!(cnt <= self.len);
        self.buf.advance(cnt);
        self.len -= cnt;
    }

    #[inline]
    fn copy_to_bytes(&mut self, len: usize) -> Bytes {
        assert!(len <= self.len);
        self.len -= len;
        self.buf.copy_to_bytes(len)
    }
}

impl<'a> EncodeBuf<'a> {
    pub(crate) fn new(buf: &'a mut BytesMut) -> Self {
        EncodeBuf { buf }
    }
}

impl EncodeBuf<'_> {
    /// Reserves capacity for at least `additional` more bytes to be inserted
    /// into the buffer.
    ///
    /// More than `additional` bytes may be reserved in order to avoid frequent
    /// reallocations. A call to `reserve` may result in an allocation.
    #[inline]
    pub fn reserve(&mut self, additional: usize) {
        self.buf.reserve(additional);
    }
    /// Reserves `len` bytes of spare capacity and lets the caller initialize it.
    ///
    /// The readable length is advanced only if `write` returns `Ok(())`.
    ///
    /// # Safety
    ///
    /// `write` must initialize exactly `len` bytes at the provided pointer before
    /// returning `Ok(())`. It must not read from the pointer, retain the pointer,
    /// or return before any external writer using the pointer has completed.
    #[doc(hidden)]
    #[inline]
    pub unsafe fn put_uninit_slice_with<E>(
        &mut self,
        len: usize,
        write: impl FnOnce(*mut u8) -> Result<(), E>,
    ) -> Result<(), E> {
        if len == 0 {
            return write(std::ptr::NonNull::<u8>::dangling().as_ptr());
        }

        self.buf.reserve(len);
        let chunk = self.buf.chunk_mut();
        assert!(chunk.len() >= len);
        let dst = chunk.as_mut_ptr();

        write(dst)?;

        // SAFETY: The caller guarantees that `write` initialized exactly `len`
        // bytes at `dst` before returning `Ok(())`.
        unsafe {
            self.buf.advance_mut(len);
        }

        Ok(())
    }
}

unsafe impl BufMut for EncodeBuf<'_> {
    #[inline]
    fn remaining_mut(&self) -> usize {
        self.buf.remaining_mut()
    }

    #[inline]
    unsafe fn advance_mut(&mut self, cnt: usize) {
        unsafe { self.buf.advance_mut(cnt) }
    }

    #[inline]
    fn chunk_mut(&mut self) -> &mut UninitSlice {
        self.buf.chunk_mut()
    }

    #[inline]
    fn put<T: Buf>(&mut self, src: T)
    where
        Self: Sized,
    {
        self.buf.put(src)
    }

    #[inline]
    fn put_slice(&mut self, src: &[u8]) {
        self.buf.put_slice(src)
    }

    #[inline]
    fn put_bytes(&mut self, val: u8, cnt: usize) {
        self.buf.put_bytes(val, cnt);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_buf() {
        let mut payload = BytesMut::with_capacity(100);
        payload.put(&vec![0u8; 50][..]);
        let mut buf = DecodeBuf::new(&mut payload, 20);

        assert_eq!(buf.len, 20);
        assert_eq!(buf.remaining(), 20);
        assert_eq!(buf.chunk().len(), 20);

        buf.advance(10);
        assert_eq!(buf.remaining(), 10);

        let mut out = [0; 5];
        buf.copy_to_slice(&mut out);
        assert_eq!(buf.remaining(), 5);
        assert_eq!(buf.chunk().len(), 5);

        assert_eq!(buf.copy_to_bytes(5).len(), 5);
        assert!(!buf.has_remaining());
    }

    #[test]
    fn encode_buf() {
        let mut bytes = BytesMut::with_capacity(100);
        let mut buf = EncodeBuf::new(&mut bytes);

        let initial = buf.remaining_mut();
        unsafe { buf.advance_mut(20) };
        assert_eq!(buf.remaining_mut(), initial - 20);

        buf.put_u8(b'a');
        assert_eq!(buf.remaining_mut(), initial - 20 - 1);
    }

    #[test]
    fn encode_buf_put_uninit_slice_with_advances_only_on_success() {
        let mut bytes = BytesMut::with_capacity(16);
        let mut buf = EncodeBuf::new(&mut bytes);

        // SAFETY: The closure initializes exactly the requested 3 bytes and
        // returns success, so advancing the readable length is valid.
        unsafe {
            buf.put_uninit_slice_with(3, |dst| {
                std::ptr::copy_nonoverlapping(b"abc".as_ptr(), dst, 3);
                Ok::<_, ()>(())
            })
            .expect("write succeeds");
        }
        assert_eq!(&buf.buf[..], b"abc");

        // SAFETY: The closure writes within the requested 3-byte range but
        // returns an error, so the readable length must not advance.
        unsafe {
            let err = buf
                .put_uninit_slice_with(3, |dst| {
                    std::ptr::copy_nonoverlapping(b"def".as_ptr(), dst, 3);
                    Err::<(), _>("fail")
                })
                .expect_err("write fails");
            assert_eq!(err, "fail");
        }
        assert_eq!(&buf.buf[..], b"abc");
    }
}

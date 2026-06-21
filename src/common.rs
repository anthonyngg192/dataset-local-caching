use std::io;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};
const MAX_FRAME_SIZE: usize = 64 * 1024;
const FRAME_HEAD_LEN: usize = 3;

#[repr(u8)]
#[derive(Debug, PartialEq, Eq)]
pub enum FrameKind {
    HandShake = 1,
    Request = 3,
    Response = 4,
    Heartbeat = 5,
}

#[derive(Debug)]
pub struct Frame {
    pub header: FrameKind,
    pub payload: Bytes,
}

pub struct SimpleCodec;

impl Decoder for SimpleCodec {
    type Item = Frame;

    type Error = io::Error;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Frame>, io::Error> {
        if src.len() < FRAME_HEAD_LEN {
            return Ok(None);
        }
        let header = match src[0] {
            1 => FrameKind::HandShake,
            3 => FrameKind::Request,
            4 => FrameKind::Response,
            5 => FrameKind::Heartbeat,
            other => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("bad header {}", other),
                ));
            }
        };

        let len = u16::from_be_bytes([src[1], src[2]]) as usize;

        if len > MAX_FRAME_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "frame too large",
            ));
        }

        if src.len() < FRAME_HEAD_LEN + len {
            return Ok(None);
        }

        src.advance(FRAME_HEAD_LEN);
        let data = src.split_to(len);

        return Ok(Some(Frame {
            header,
            payload: data.freeze(),
        }));
    }
}

impl Encoder<Frame> for SimpleCodec {
    type Error = io::Error;
    fn encode(&mut self, item: Frame, dst: &mut BytesMut) -> Result<(), io::Error> {
        let len = item.payload.len() as u32;
        dst.put_u8(item.header as u8);
        dst.put_u16(len as u16);
        dst.extend_from_slice(&item.payload);
        Ok(())
    }
}

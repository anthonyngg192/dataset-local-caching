//! Per-shard on-disk layer: two append-only files.
//!
//! - `cool.log`  — the value store and source of truth (`[op][klen][exp_ms][vlen][key][value]`).
//! - `backup.log`— the index journal (`[op][klen][val_off][vlen][exp_ms][key]`).
//!
//! Writes go to both (write-through). On startup the *backup* (index-only) log is
//! replayed to rebuild the in-RAM index without loading values; cold values are
//! read on demand from `cool` via `read_at`. See `DESIGN.md`.

use std::fs::{File, OpenOptions, create_dir_all};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::os::unix::fs::FileExt;
use std::path::Path;

use bytes::Bytes;

const OP_SET: u8 = 1;
const OP_DEL: u8 = 2;
const COOL_SET_HEADER: u64 = 1 + 2 + 8 + 4; // op + klen + exp_ms + vlen

/// An index entry recovered from the backup log during replay.
pub enum IndexRecord {
    Set {
        key: Bytes,
        val_off: u64,
        vlen: u32,
        exp_ms: u64,
    },
    Del {
        key: Bytes,
    },
}

pub struct Disk {
    cool: BufWriter<File>,
    cool_read: File, // for pread of cold values
    backup: BufWriter<File>,
    cool_len: u64, // bytes written to cool so far (next append lands here)
}

impl Disk {
    /// Open (creating if needed) the shard's files, replaying the backup log into
    /// the index records the worker rebuilds from.
    pub fn open(dir: &Path) -> io::Result<(Self, Vec<IndexRecord>)> {
        create_dir_all(dir)?;
        let cool_path = dir.join("cool.log");
        let backup_path = dir.join("backup.log");

        let records = if backup_path.exists() {
            replay_backup(&backup_path)?
        } else {
            Vec::new()
        };

        let cool = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&cool_path)?;
        let cool_len = cool.metadata()?.len();
        let cool_read = OpenOptions::new().read(true).open(&cool_path)?;
        let backup = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&backup_path)?;

        Ok((
            Self {
                cool: BufWriter::new(cool),
                cool_read,
                backup: BufWriter::new(backup),
                cool_len,
            },
            records,
        ))
    }

    /// Append a SET to both files; returns the value's absolute offset in cool.
    pub fn append_set(&mut self, key: &[u8], value: &[u8], exp_ms: u64) -> io::Result<u64> {
        let klen = key.len() as u16;
        let vlen = value.len() as u32;

        // cool: [1][klen][exp_ms][vlen][key][value]
        self.cool.write_all(&[OP_SET])?;
        self.cool.write_all(&klen.to_be_bytes())?;
        self.cool.write_all(&exp_ms.to_be_bytes())?;
        self.cool.write_all(&vlen.to_be_bytes())?;
        self.cool.write_all(key)?;
        self.cool.write_all(value)?;

        let val_off = self.cool_len + COOL_SET_HEADER + klen as u64;
        self.cool_len += COOL_SET_HEADER + klen as u64 + vlen as u64;

        // backup: [1][klen][val_off][vlen][exp_ms][key]
        self.backup.write_all(&[OP_SET])?;
        self.backup.write_all(&klen.to_be_bytes())?;
        self.backup.write_all(&val_off.to_be_bytes())?;
        self.backup.write_all(&vlen.to_be_bytes())?;
        self.backup.write_all(&exp_ms.to_be_bytes())?;
        self.backup.write_all(key)?;

        Ok(val_off)
    }

    pub fn append_del(&mut self, key: &[u8]) -> io::Result<()> {
        let klen = key.len() as u16;
        self.cool.write_all(&[OP_DEL])?;
        self.cool.write_all(&klen.to_be_bytes())?;
        self.cool.write_all(key)?;
        self.cool_len += 1 + 2 + klen as u64;

        self.backup.write_all(&[OP_DEL])?;
        self.backup.write_all(&klen.to_be_bytes())?;
        self.backup.write_all(key)?;
        Ok(())
    }

    /// Read a cold value back from cool. The buffered appends are flushed first so
    /// a recently-written value is visible to the read handle.
    pub fn read_at(&mut self, val_off: u64, vlen: u32) -> io::Result<Bytes> {
        self.cool.flush()?;
        let mut buf = vec![0u8; vlen as usize];
        self.cool_read.read_exact_at(&mut buf, val_off)?;
        Ok(Bytes::from(buf))
    }

    /// Push buffered writes to the OS (not durable until `fsync`).
    pub fn flush(&mut self) -> io::Result<()> {
        self.cool.flush()?;
        self.backup.flush()?;
        Ok(())
    }

    /// Make everything durable on disk.
    pub fn fsync(&mut self) -> io::Result<()> {
        self.cool.flush()?;
        self.cool.get_ref().sync_data()?;
        self.backup.flush()?;
        self.backup.get_ref().sync_data()?;
        Ok(())
    }
}

/// Replay the backup (index) log in order. A torn trailing record is treated as
/// EOF — everything before it is recovered.
fn replay_backup(path: &Path) -> io::Result<Vec<IndexRecord>> {
    let mut r = BufReader::new(File::open(path)?);
    let mut out = Vec::new();

    loop {
        let mut op = [0u8; 1];
        match r.read_exact(&mut op) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e),
        }

        match op[0] {
            OP_SET => {
                let Some(klen) = read_u16(&mut r)? else { break };
                let Some(val_off) = read_u64(&mut r)? else { break };
                let Some(vlen) = read_u32(&mut r)? else { break };
                let Some(exp_ms) = read_u64(&mut r)? else { break };
                let Some(key) = read_bytes(&mut r, klen as usize)? else { break };
                out.push(IndexRecord::Set {
                    key,
                    val_off,
                    vlen,
                    exp_ms,
                });
            }
            OP_DEL => {
                let Some(klen) = read_u16(&mut r)? else { break };
                let Some(key) = read_bytes(&mut r, klen as usize)? else { break };
                out.push(IndexRecord::Del { key });
            }
            _ => break,
        }
    }

    Ok(out)
}

fn read_bytes<R: Read>(r: &mut R, n: usize) -> io::Result<Option<Bytes>> {
    let mut buf = vec![0u8; n];
    match r.read_exact(&mut buf) {
        Ok(()) => Ok(Some(Bytes::from(buf))),
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => Ok(None),
        Err(e) => Err(e),
    }
}

fn read_u16<R: Read>(r: &mut R) -> io::Result<Option<u16>> {
    let mut b = [0u8; 2];
    match r.read_exact(&mut b) {
        Ok(()) => Ok(Some(u16::from_be_bytes(b))),
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => Ok(None),
        Err(e) => Err(e),
    }
}

fn read_u32<R: Read>(r: &mut R) -> io::Result<Option<u32>> {
    let mut b = [0u8; 4];
    match r.read_exact(&mut b) {
        Ok(()) => Ok(Some(u32::from_be_bytes(b))),
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => Ok(None),
        Err(e) => Err(e),
    }
}

fn read_u64<R: Read>(r: &mut R) -> io::Result<Option<u64>> {
    let mut b = [0u8; 8];
    match r.read_exact(&mut b) {
        Ok(()) => Ok(Some(u64::from_be_bytes(b))),
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => Ok(None),
        Err(e) => Err(e),
    }
}

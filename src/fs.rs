use std::error::Error;
use std::ffi::OsStr;
use std::panic::resume_unwind;
use std::path::Path;
use std::time::{Duration, SystemTime};
use fuser::{FileAttr, Filesystem, FileType, KernelConfig, ReplyAttr, ReplyDirectory, ReplyEntry, Request};
use libc::{c_int, EIO, ENOENT, ENOSYS};
use rusqlite::Connection;
use log::{debug, error, info};
use crate::database;
use crate::database::{Database, File};

pub struct Fs {
    db: Database,
}

impl Fs {
    pub fn new() -> Fs {
        let conn = match Connection::open_in_memory() {
            Ok(conn) => conn,
            Err(err) => {
                println!("error creating the database: {}", err);
                panic!();
            }
        };


        let mut fs = Fs{
            db: database::new(),
        };

        fs
    }
}

impl Filesystem for Fs {
    fn init(&mut self, _req: &Request<'_>, _config: &mut KernelConfig) -> Result<(), c_int> {
        info!("initializing database");

        self.db.conn.execute(
            "CREATE TABLE files (
            inode INTEGER PRIMARY KEY AUTOINCREMENT,
            parent_inode INTEGER,
            size INTEGER,
            path TEXT NOT NULL,
            file_type TEXT NOT NULL,
            UNIQUE(parent_inode, path)
        )",
            (), // empty list of parameters.
        ).unwrap();

        let f = File {
            inode: 1,
            parent_inode: 0,
            size: 0,
            path: "/".to_string(),
            file_type: "dir".to_string(),
        };

        self.db.conn.execute("INSERT INTO files (inode, parent_inode, size, path, file_type) VALUES (?1, ?2, ?3, ?4, ?5)", (&f.inode, &f.parent_inode, &f.size, &f.path, &f.file_type)).unwrap();

        Ok(())
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyAttr) {
        debug!("getattr: ino={}", ino);

        let mut stmt = self.db.conn.prepare("SELECT inode, parent_inode, size, path, file_type FROM files WHERE inode=:inode").unwrap();
        let mut rows = stmt.query_map([ino], |row| {
            Ok(File {
                inode: row.get_unwrap(0),
                parent_inode: row.get_unwrap(1),
                size: row.get_unwrap(2),
                path: row.get_unwrap(3),
                file_type: row.get_unwrap(4),
            })
        }).unwrap();

        let f = rows.nth(0).unwrap().unwrap();

        let ts = SystemTime::now();
        let attr = FileAttr {
            ino: f.inode,
            size: f.size,
            blocks: 0,
            atime: ts,
            mtime: ts,
            ctime: ts,
            crtime: ts,
            kind: f.get_type(),
            perm: 0o755,
            nlink: 0,
            uid: 0,
            gid: 0,
            rdev: 0,
            blksize: 0,
            flags: 0,
        };
        let ttl = Duration::from_secs(1);
        if ino == 1 {
            reply.attr(&ttl, &attr);
        } else {
            reply.error(ENOSYS);
        }
    }

    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        debug!("lookup: parent {:?}, name {:?}", parent, name);

        let name_str = match name.to_str() {
            None => {
                reply.error(ENOENT);
                return;
            }
            Some(n) => n,
        };

        let file = self.db.get_file(name_str, parent);

        match file {
            None => {
                reply.error(ENOENT);
                return;
            }
            Some(f) => {
                let ts = SystemTime::now();
                let attr = FileAttr {
                    ino: f.inode,
                    size: f.size,
                    blocks: 0,
                    atime: ts,
                    mtime: ts,
                    ctime: ts,
                    crtime: ts,
                    kind: f.get_type(),
                    perm: 0o755,
                    nlink: 0,
                    uid: 0,
                    gid: 0,
                    rdev: 0,
                    blksize: 0,
                    flags: 0,
                };
                let ttl = Duration::from_secs(1);

                //TODO create generation
                reply.entry(&ttl, &attr, 0);
            }
        }
    }

    fn mkdir(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, mode: u32, umask: u32, reply: ReplyEntry) {
        debug!("mkdir: parent {:?}, name {:?}, mode {:?}, umask {:?}", parent, name, mode, umask);
        let mut f = File {
            inode: 0,
            parent_inode: parent,
            size: 0,
            path: name.to_str().unwrap().to_string(),
            file_type: "dir".to_string(),
        };

        match self.db.add_file(&mut f) {
            Ok(_) => {}
            Err(err) => {
                error!("{}", err);
            }
        }

        let ts = SystemTime::now();

        let attr = FileAttr {
            ino: 2,
            size: f.size,
            blocks: 0,
            atime: ts,
            mtime: ts,
            ctime: ts,
            crtime: ts,
            kind: f.get_type(),
            perm: 0o755,
            nlink: 0,
            uid: 0,
            gid: 0,
            rdev: 0,
            blksize: 0,
            flags: 0,
        };

        let ttl = Duration::from_secs(1);
        //TODO change generation
        reply.entry(&ttl, &attr, 0);
    }

    fn readdir(&mut self, _req: &Request<'_>, ino: u64, fh: u64, offset: i64, mut reply: ReplyDirectory) {
        debug!("readdir: ino={}, fh={}, offset={}", ino, fh, offset);

        // if offset == 0 {
        //     reply.add(1, 0, FileType::Directory, &Path::new("."));
        //     reply.add(1, 0, FileType::Directory, &Path::new(".."));
        // }

        let mut off = offset;

        let files = match self.db.get_files(ino, Some(offset)) {
            Ok(f) => f,
            Err(err) => {
                error!("{}", err.to_string());
                reply.error(EIO);
                return
            }
        };

        for file in files {
            off += 1;
            let buffer_full = reply.add(file.inode, off, FileType::Directory, file.path);

            if buffer_full {
                println!("reply directory buffer full");
                break;
            }
        }

        reply.ok();
    }
}




use std::error::Error;
use fuser::FileType;
use log::error;
use rusqlite::{Connection, named_params, Row};
use rusqlite::Error::QueryReturnedNoRows;

pub struct Database {
    //TODO make it private
    pub conn : Connection,
}

pub fn new() -> Database {
    let conn = match Connection::open_in_memory() {
        Ok(conn) => conn,
        Err(err) => {
            panic!("error creating the database: {}", err);
        }
    };

    return Database {
        conn,
    }
}

impl Database {
    pub fn get_file(&self, name: &str, parent_ino: u64) -> Option<File> {
        let mut stmt = self.conn.prepare("SELECT inode, parent_inode, size, path, file_type FROM files WHERE path=:name AND parent_inode=:parent_ino").unwrap();
        let file_result = stmt.query_row(named_params! {":name":name, ":parent_ino": parent_ino}, |row| {
            Ok(File {
                inode: row.get(0)?,
                parent_inode: row.get(1)?,
                size: row.get(2)?,
                path: row.get(3)?,
                file_type: row.get(4)?,
            })
        });

        return match file_result {
            Ok(f) => Some(f),
            Err(err) => {
                if err != QueryReturnedNoRows {
                    error!("error querying database: {}", err)
                }

                None
            }
        };
    }

    pub fn get_files(&self, name: &str, parent_ino: u64) -> Result<Vec<File>, Box<dyn Error>> {
        let mut stmt = self.conn.prepare("SELECT inode, parent_inode, size, path, file_type FROM files WHERE path=:name AND parent_inode=:parent_ino").unwrap();
        let rows = stmt.query_map(named_params! {":name":name, ":parent_ino": parent_ino}, |row| {
            Ok(File {
                inode: row.get(0)?,
                parent_inode: row.get(1)?,
                size: row.get(2)?,
                path: row.get(3)?,
                file_type: row.get(4)?,
            })
        })?;

        let mut files = Vec::new();

        for r in rows {
            match r {
                Ok(f) => {
                    files.push(f);
                }
                Err(err) => {
                    return Err(err.into());
                }
            };
        }

        Ok(files)
    }
}

pub struct File {
    pub inode: u64,
    pub parent_inode: u64,
    pub size: u64,
    pub path: String,
    pub file_type: String,
}

impl File {
    pub fn get_type(&self) -> FileType {
        return if self.file_type == "dir" {
            FileType::Directory
        } else {
            FileType::RegularFile
        }
    }
}
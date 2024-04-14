extern crate core;

mod fs;
mod database;
use fs::Fs;

use std::env;
use fuser::{FileAttr, Filesystem, FileType, ReplyAttr, ReplyDirectory, Request};
use rusqlite::Connection;

fn main() {
    env_logger::init();

    let mountpoint = match env::args().nth(1) {
        Some(path) => path,
        None => {
            println!("Usage: {} [mountpoint]", env::args().nth(0).unwrap());
            return;
        }
    };

    let fs = Fs::new();

    match fuser::mount2(fs, mountpoint, &[]) {
        Ok(_) => {}
        Err(err) => {
            println!("error mounting filesystem: {}", err)
        }
    }
}
